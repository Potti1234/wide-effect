import {
  SamplingConfig,
  SegmentStagingConfig,
  normalizeSamplingConfig,
  type FlowSpan,
  type SegmentStagingConfig as SegmentStagingConfigType,
} from "@wide-effect/core";
import {
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Ref,
  Schema,
} from "effect";
import type { SpanBatch, SpanExporter } from "./exporter.js";
import {
  makeDeliveryHealthTracker,
  type DeliveryHealthSnapshot,
  type DeliveryHealthTracker,
} from "./delivery-health.js";
import { makeSegmentStager, type SegmentStager } from "./segment-staging.js";
import { CompletedSpanSink } from "./sink.js";

export type OverflowPolicy = "drop-newest" | "drop-oldest";

export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly delay: Duration.Input;
  readonly jitter: number;
  readonly jitterValue?: (attempt: number, exporter: string) => number;
}

export interface ExporterCircuitOptions {
  readonly failureThreshold: number;
  readonly openDuration: Duration.Input;
  readonly probeTimeout: Duration.Input;
}

export interface DeliveryOptions {
  readonly exporters: ReadonlyArray<SpanExporter>;
  readonly capacity: number;
  readonly overflow: OverflowPolicy;
  readonly maxBatchSize: number;
  readonly maxBatchAge: Duration.Input;
  readonly attemptTimeout: Duration.Input;
  readonly shutdownTimeout: Duration.Input;
  readonly retry: RetryPolicy;
  readonly circuit?: ExporterCircuitOptions;
  readonly sampling: SamplingConfig;
  readonly deterministicValue?: (span: FlowSpan) => number;
  readonly staging?: SegmentStagingConfigType | undefined;
}

export type {
  DeliveryHealthSnapshot,
  ExporterHealthSnapshot,
  PipelineHealthSnapshot,
} from "./delivery-health.js";

export interface DeliveryService {
  readonly health: Effect.Effect<DeliveryHealthSnapshot>;
  readonly flush: Effect.Effect<void>;
  readonly shutdown: Effect.Effect<void>;
}

export class Delivery extends Context.Service<Delivery, DeliveryService>()(
  "@wide-effect/effect/Delivery",
) {}

type Control =
  | { readonly _tag: "Flush"; readonly done: Deferred.Deferred<void> }
  | {
      readonly _tag: "Shutdown";
    };
type Next =
  | { readonly _tag: "Span"; readonly span: FlowSpan }
  | {
      readonly _tag: "Control";
      readonly control: Control;
    };

export const deliveryLayer = (input: DeliveryOptions) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const options = validateOptions(input);
      const spans = yield* options.overflow === "drop-newest"
        ? Queue.dropping<FlowSpan>(options.capacity)
        : Queue.sliding<FlowSpan>(options.capacity);
      const controls = yield* Queue.bounded<Control>(1);
      const shutdownDone = yield* Deferred.make<void>();
      const exporterNames = validateExporterNames(options.exporters);
      const health = yield* makeDeliveryHealthTracker(exporterNames);
      const circuitEntries = yield* Effect.forEach(options.exporters, (exporter) =>
        Ref.make<CircuitState>({ _tag: "Closed", failures: 0 }).pipe(
          Effect.map((state) => [exporter, state] as const),
        ),
      );
      const circuits = new Map(circuitEntries);
      let stopping = false;

      const exportBatch = (batch: SpanBatch) =>
        options.exporters.length === 0
          ? Effect.void
          : Effect.forEach(
              options.exporters,
              (exporter) => exportWithRetry(exporter, batch, options, health, circuits),
              { concurrency: "unbounded", discard: true },
            ).pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => health.incrementPipeline("shutdownDropped", batch.length)),
              ),
            );
      const stager = makeSegmentStager({
        config: options.staging,
        sampling: options.sampling,
        deterministicValue: options.deterministicValue,
        emit: exportBatch,
        health: { record: health.incrementPipeline },
      });

      const flushExporters = Effect.forEach(
        options.exporters,
        (exporter) =>
          runExporterOperation(exporter, () => exporter.flush, options, health, circuits),
        { concurrency: "unbounded", discard: true },
      );

      const drain = Effect.gen(function* () {
        const queued = yield* Queue.clear(spans);
        for (const span of queued) yield* stager.accept(span, yield* Clock.currentTimeMillis);
      });

      const consumer = consumerLoop(
        spans,
        controls,
        stager,
        drain,
        flushExporters,
        shutdownDone,
      ).pipe(
        Effect.catchCause(() => Deferred.succeed(shutdownDone, undefined)),
        Effect.asVoid,
      );

      const fiber = yield* Effect.forkScoped(consumer);

      const shutdown = Effect.suspend(() => {
        if (!stopping) {
          stopping = true;
          health.setAccepting(false);
          return Queue.offer(controls, { _tag: "Shutdown" }).pipe(
            Effect.andThen(Deferred.await(shutdownDone)),
            Effect.andThen(shutdownExporters(options, health, circuits)),
            Effect.ensuring(Queue.shutdown(spans)),
            Effect.ensuring(Queue.shutdown(controls)),
            Effect.ensuring(Fiber.interrupt(fiber)),
            Effect.ensuring(Deferred.succeed(shutdownDone, undefined)),
            Effect.timeoutOption(options.shutdownTimeout),
            Effect.asVoid,
          );
        }
        return Deferred.await(shutdownDone).pipe(
          Effect.timeoutOption(options.shutdownTimeout),
          Effect.asVoid,
        );
      });

      const flush = Effect.suspend(() => {
        if (stopping) return Deferred.await(shutdownDone);
        return Effect.gen(function* () {
          const done = yield* Deferred.make<void>();
          yield* Queue.offer(controls, { _tag: "Flush", done });
          yield* Deferred.await(done);
        });
      });

      yield* Effect.addFinalizer(() => shutdown);

      const sink = CompletedSpanSink.of({
        unsafeOffer: (span) => {
          health.incrementPipeline("completed");
          if (stopping) {
            health.incrementPipeline("shutdownDropped");
            return;
          }
          const full = Queue.sizeUnsafe(spans) >= options.capacity;
          const offered = Queue.offerUnsafe(spans, span);
          if (full) {
            health.incrementPipeline("overflowDropped");
          }
          if (!offered) {
            if (!full) health.incrementPipeline("overflowDropped");
            return;
          }
          health.incrementPipeline("accepted");
        },
      });
      const service = Delivery.of({
        health: Effect.suspend(() =>
          health.snapshot({
            queueDepth: Queue.sizeUnsafe(spans),
            openSegments: stager.openCount(),
            stagedBytes: stager.stagedBytes(),
          }),
        ),
        flush,
        shutdown,
      });
      return Context.make(CompletedSpanSink, sink).pipe(Context.add(Delivery, service));
    }),
  );

function consumerLoop(
  spans: Queue.Queue<FlowSpan>,
  controls: Queue.Queue<Control>,
  stager: SegmentStager,
  drain: Effect.Effect<void>,
  flushExporters: Effect.Effect<void>,
  shutdownDone: Deferred.Deferred<void>,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    while (true) {
      const now = yield* Clock.currentTimeMillis;
      const next = Effect.raceFirst(
        Queue.take(spans).pipe(Effect.map((span): Next => ({ _tag: "Span", span }))),
        Queue.take(controls).pipe(Effect.map((control): Next => ({ _tag: "Control", control }))),
      );
      const nextDeadline = stager.nextDeadline(now);
      const maybeItem =
        nextDeadline === undefined
          ? Option.some(yield* next)
          : yield* next.pipe(
              Effect.timeoutOption(Duration.millis(Math.max(0, nextDeadline - now))),
            );
      if (Option.isNone(maybeItem)) {
        yield* stager.finalizeDue(yield* Clock.currentTimeMillis);
        continue;
      }
      const item = maybeItem.value;
      if (item._tag === "Control") {
        yield* drain;
        yield* stager.finalizeAll(yield* Clock.currentTimeMillis);
        yield* flushExporters;
        if (item.control._tag === "Shutdown") {
          yield* Deferred.succeed(shutdownDone, undefined);
          return;
        }
        yield* Deferred.succeed(item.control.done, undefined);
        continue;
      }
      yield* stager.accept(item.span, yield* Clock.currentTimeMillis);
    }
  });
}

type CircuitState =
  | { readonly _tag: "Closed"; readonly failures: number }
  | { readonly _tag: "Retrying"; readonly failures: number }
  | {
      readonly _tag: "Open";
      readonly reopenAt: number;
      readonly stuckFiber?: Fiber.Fiber<void, unknown>;
    }
  | { readonly _tag: "HalfOpen" };

type ExporterCircuitState = Ref.Ref<CircuitState>;

function enterCircuit(
  circuit: ExporterCircuitState,
  options: ResolvedDeliveryOptions,
  health: DeliveryHealthTracker,
  exporterName: string,
): Effect.Effect<"normal" | "probe" | "blocked"> {
  return Effect.gen(function* () {
    const state = yield* Ref.get(circuit);
    if (state._tag !== "Open") return state._tag === "HalfOpen" ? "blocked" : "normal";
    if (state.stuckFiber !== undefined && state.stuckFiber.pollUnsafe() === undefined)
      return "blocked";
    const now = yield* Clock.currentTimeMillis;
    if (now < state.reopenAt) return "blocked";
    yield* Ref.set(circuit, { _tag: "HalfOpen" });
    health.incrementExporter(exporterName, "circuitTransitions");
    return "probe";
  });
}

function markRetrying(circuit: ExporterCircuitState): Effect.Effect<void> {
  return Ref.update(circuit, (state) =>
    state._tag === "Closed" ? { _tag: "Retrying" as const, failures: state.failures } : state,
  );
}

function closeCircuit(
  circuit: ExporterCircuitState,
  health: DeliveryHealthTracker,
  exporterName: string,
): Effect.Effect<void> {
  return Effect.flatMap(Ref.get(circuit), (state) => {
    if (state._tag === "Closed" && state.failures === 0) return Effect.void;
    return Ref.set(circuit, { _tag: "Closed", failures: 0 }).pipe(
      Effect.tap(() =>
        Effect.sync(() => health.incrementExporter(exporterName, "circuitTransitions")),
      ),
    );
  });
}

function failCircuit(
  circuit: ExporterCircuitState,
  options: ResolvedDeliveryOptions,
  health: DeliveryHealthTracker,
  exporterName: string,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const state = yield* Ref.get(circuit);
    if (state._tag === "HalfOpen") {
      yield* openCircuit(circuit, options, health, exporterName);
      return;
    }
    const failures =
      (state._tag === "Closed" || state._tag === "Retrying" ? state.failures : 0) + 1;
    if (failures >= options.circuit.failureThreshold) {
      yield* openCircuit(circuit, options, health, exporterName);
    } else {
      yield* Ref.set(circuit, { _tag: "Closed", failures });
    }
  });
}

function openCircuit(
  circuit: ExporterCircuitState,
  options: ResolvedDeliveryOptions,
  health: DeliveryHealthTracker,
  exporterName: string,
  stuckFiber?: Fiber.Fiber<void, unknown>,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    yield* Ref.set(circuit, {
      _tag: "Open",
      reopenAt: now + options.circuit.openDurationMs,
      ...(stuckFiber === undefined ? {} : { stuckFiber }),
    });
    health.incrementExporter(exporterName, "circuitTransitions");
  });
}

function recordTerminalFailure(
  health: DeliveryHealthTracker,
  exporterName: string,
  spanCount: number,
): void {
  health.incrementExporter(exporterName, "terminalFailedBatches");
  health.incrementExporter(exporterName, "terminalFailedSpans", spanCount);
}

function exportWithRetry(
  exporter: SpanExporter,
  batch: SpanBatch,
  options: ResolvedDeliveryOptions,
  health: DeliveryHealthTracker,
  circuits: ReadonlyMap<SpanExporter, ExporterCircuitState>,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const exporterName = safeExporterName(exporter);
    const circuit = circuits.get(exporter);
    if (circuit === undefined) return;
    const gate = yield* enterCircuit(circuit, options, health, exporterName);
    if (gate === "blocked") {
      recordTerminalFailure(health, exporterName, batch.length);
      return;
    }
    const maxAttempts = gate === "probe" ? 1 : options.retry.maxAttempts;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      health.incrementExporter(exporterName, "batchesAttempted");
      health.incrementExporter(exporterName, "spansAttempted", batch.length);
      const result = yield* attemptExporter(
        () => exporter.export(batch),
        gate === "probe" ? options.circuit.probeTimeoutMs : options.attemptTimeout,
      );
      if (result === "success") {
        yield* closeCircuit(circuit, health, exporterName);
        health.incrementExporter(exporterName, "batchesDelivered");
        health.incrementExporter(exporterName, "spansDelivered", batch.length);
        return;
      }
      if (typeof result !== "string") {
        health.incrementExporter(exporterName, "timeouts");
        yield* openCircuit(circuit, options, health, exporterName, result.fiber);
        recordTerminalFailure(health, exporterName, batch.length);
        return;
      }
      if (attempt === maxAttempts) {
        yield* failCircuit(circuit, options, health, exporterName);
        recordTerminalFailure(health, exporterName, batch.length);
        return;
      }
      yield* markRetrying(circuit);
      health.incrementExporter(exporterName, "retryAttempts");
      yield* Effect.sleep(retryDelay(options.retry, attempt, exporterName));
    }
  });
}

function runExporterOperation(
  exporter: SpanExporter,
  operation: () => Effect.Effect<void, unknown>,
  options: ResolvedDeliveryOptions,
  health: DeliveryHealthTracker,
  circuits: ReadonlyMap<SpanExporter, ExporterCircuitState>,
  operationName: "flush" | "shutdown" = "flush",
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const exporterName = safeExporterName(exporter);
    const circuit = circuits.get(exporter);
    if (circuit === undefined) return;
    const circuitState = yield* Ref.get(circuit);
    if (circuitState._tag === "Open" || circuitState._tag === "HalfOpen") return;
    const result = yield* attemptExporter(operation, options.attemptTimeout);
    if (result === "failure")
      health.incrementExporter(
        exporterName,
        operationName === "flush" ? "flushFailures" : "shutdownFailures",
      );
    if (typeof result !== "string") {
      health.incrementExporter(exporterName, "timeouts");
      health.incrementExporter(
        exporterName,
        operationName === "flush" ? "flushTimeouts" : "shutdownTimeouts",
      );
      yield* openCircuit(circuit, options, health, exporterName, result.fiber);
    }
  });
}

function attemptExporter(
  operation: () => Effect.Effect<void, unknown>,
  timeout: Duration.Input,
): Effect.Effect<
  "success" | "failure" | { readonly _tag: "Timeout"; readonly fiber: Fiber.Fiber<void, unknown> }
> {
  return Effect.gen(function* () {
    const fiber = yield* Effect.suspend(operation).pipe(Effect.forkDetach);
    const result = yield* Fiber.await(fiber).pipe(Effect.timeoutOption(timeout));
    if (Option.isNone(result)) {
      fiber.interruptUnsafe();
      return { _tag: "Timeout" as const, fiber };
    }
    return Exit.isSuccess(result.value) ? ("success" as const) : ("failure" as const);
  });
}

function shutdownExporters(
  options: ResolvedDeliveryOptions,
  health: DeliveryHealthTracker,
  circuits: ReadonlyMap<SpanExporter, ExporterCircuitState>,
) {
  return Effect.forEach(
    options.exporters,
    (exporter) =>
      runExporterOperation(
        exporter,
        () => exporter.shutdown,
        options,
        health,
        circuits,
        "shutdown",
      ),
    { concurrency: "unbounded", discard: true },
  );
}

function retryDelay(policy: RetryPolicy, attempt: number, exporter: string): Duration.Duration {
  const base = Duration.toMillis(policy.delay);
  const value = normalizeRatio(policy.jitterValue?.(attempt, exporter) ?? 0.5);
  const factor = 1 - policy.jitter + value * policy.jitter * 2;
  return Duration.millis(Math.max(0, base * factor));
}

function normalizeRatio(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(1, Math.max(0, value));
}

function safeExporterName(exporter: SpanExporter): string {
  try {
    const name = exporter.name;
    return typeof name === "string" && name.length > 0 ? name.slice(0, 128) : "exporter";
  } catch {
    return "exporter";
  }
}

type ResolvedDeliveryOptions = Omit<DeliveryOptions, "staging" | "circuit"> & {
  readonly staging: SegmentStagingConfigType;
  readonly circuit: ResolvedExporterCircuitOptions;
};

interface ResolvedExporterCircuitOptions {
  readonly failureThreshold: number;
  readonly openDurationMs: number;
  readonly probeTimeoutMs: number;
}

function validateOptions(options: DeliveryOptions): ResolvedDeliveryOptions {
  if (!Number.isInteger(options.capacity) || options.capacity < 1)
    throw new RangeError("capacity must be positive");
  if (!Number.isInteger(options.maxBatchSize) || options.maxBatchSize < 1)
    throw new RangeError("maxBatchSize must be positive");
  if (!Number.isInteger(options.retry.maxAttempts) || options.retry.maxAttempts < 1)
    throw new RangeError("maxAttempts must be positive");
  if (
    !Number.isFinite(options.retry.jitter) ||
    options.retry.jitter < 0 ||
    options.retry.jitter > 1
  )
    throw new RangeError("jitter must be between zero and one");
  const circuit = options.circuit ?? defaultExporterCircuit;
  const deliveryLimits = Schema.decodeUnknownSync(
    Schema.Struct({
      failureThreshold: Schema.Number,
      openDurationMs: Schema.Number,
      probeTimeoutMs: Schema.Number,
      maxAttempts: Schema.Number,
    }),
    { onExcessProperty: "error" },
  )({
    failureThreshold: circuit.failureThreshold,
    openDurationMs: Duration.toMillis(circuit.openDuration),
    probeTimeoutMs: Duration.toMillis(circuit.probeTimeout),
    maxAttempts: options.retry.maxAttempts,
  });
  if (!Number.isInteger(deliveryLimits.failureThreshold) || deliveryLimits.failureThreshold < 1)
    throw new RangeError("circuit failureThreshold must be positive");
  if (!Number.isFinite(deliveryLimits.openDurationMs) || deliveryLimits.openDurationMs < 0)
    throw new RangeError("circuit openDuration must be non-negative");
  if (!Number.isFinite(deliveryLimits.probeTimeoutMs) || deliveryLimits.probeTimeoutMs < 0)
    throw new RangeError("circuit probeTimeout must be non-negative");
  if (!Number.isInteger(deliveryLimits.maxAttempts) || deliveryLimits.maxAttempts < 1)
    throw new RangeError("maxAttempts must be positive");
  return {
    ...options,
    sampling: normalizeSamplingConfig(
      Schema.decodeUnknownSync(SamplingConfig)(options.sampling, { onExcessProperty: "error" }),
    ),
    staging: Schema.decodeUnknownSync(SegmentStagingConfig)(
      options.staging ?? defaultSegmentStaging,
      { onExcessProperty: "error" },
    ),
    circuit: {
      failureThreshold: deliveryLimits.failureThreshold,
      openDurationMs: deliveryLimits.openDurationMs,
      probeTimeoutMs: deliveryLimits.probeTimeoutMs,
    },
  };
}

const defaultExporterCircuit: ExporterCircuitOptions = {
  failureThreshold: 1,
  openDuration: "30 seconds",
  probeTimeout: "5 seconds",
};

const defaultSegmentStaging: SegmentStagingConfigType = {
  maxOpenSegments: 1_024,
  maxSpansPerSegment: 256,
  maxBytesPerSegment: 4_194_304,
  maxTotalBytes: 67_108_864,
  rootGraceMs: 100,
  maxSegmentAgeMs: 60_000,
  overflow: "drop-oldest",
  decisionCacheCapacity: 2_048,
  decisionCacheMaxAgeMs: 60_000,
};

function validateExporterNames(exporters: ReadonlyArray<SpanExporter>): Array<string> {
  const names = exporters.map(safeExporterName);
  const seen = new Set<string>();
  for (const name of names) {
    if (!/^[a-z][a-z0-9._-]{0,63}$/.test(name))
      throw new RangeError("exporter names must be stable lowercase machine names");
    if (seen.has(name)) throw new RangeError(`duplicate exporter name: ${name}`);
    seen.add(name);
  }
  return names;
}
