import { decideSampling, type FlowSpan, type SamplingConfig } from "@wide-effect/core";
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
} from "effect";
import type { SpanBatch, SpanExporter } from "./exporter.js";
import { CompletedSpanSink } from "./sink.js";

export type OverflowPolicy = "drop-newest" | "drop-oldest";

export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly delay: Duration.Input;
  readonly jitter: number;
  readonly jitterValue?: (attempt: number, exporter: string) => number;
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
  readonly sampling: SamplingConfig;
  readonly deterministicValue?: (span: FlowSpan) => number;
}

export interface DeliveryHealthSnapshot {
  readonly accepting: boolean;
  readonly queueDepth: number;
  readonly accepted: number;
  readonly sampled: number;
  readonly exported: number;
  readonly retried: number;
  readonly dropped: number;
  readonly overflowed: number;
  readonly exporterFailures: number;
  readonly attemptTimeouts: number;
  readonly disabledExporters: number;
  readonly flushTimeouts: number;
}

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
      const health = makeHealth();
      const disabledExporters = new WeakSet<SpanExporter>();
      let stopping = false;

      const processBatch = (incoming: ReadonlyArray<FlowSpan>) =>
        Effect.suspend(() => {
          const retained: Array<FlowSpan> = [];
          for (const span of incoming) {
            const decision = decideSampling(options.sampling, {
              operation: span.operation,
              outcome: span.outcome,
              durationMs: span.durationMs,
              deterministicValue: normalizeRatio(
                options.deterministicValue?.(span) ?? deterministicRatio(span.traceId),
              ),
            });
            if (decision.keep) retained.push(span);
            else health.dropped++;
          }
          health.sampled += retained.length;
          if (retained.length === 0 || options.exporters.length === 0) return Effect.void;
          const batch: SpanBatch = [retained[0]!, ...retained.slice(1)];
          return Effect.forEach(
            options.exporters,
            (exporter) => exportWithRetry(exporter, batch, options, health, disabledExporters),
            { concurrency: "unbounded", discard: true },
          );
        });

      const flushExporters = Effect.forEach(
        options.exporters,
        (exporter) =>
          runExporterOperation(exporter, () => exporter.flush, options, health, disabledExporters),
        { concurrency: "unbounded", discard: true },
      );

      const drain = Effect.gen(function* () {
        const queued = yield* Queue.clear(spans);
        for (let offset = 0; offset < queued.length; offset += options.maxBatchSize) {
          yield* processBatch(queued.slice(offset, offset + options.maxBatchSize));
        }
      });

      const consumer = consumerLoop(
        spans,
        controls,
        options,
        processBatch,
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
          health.accepting = false;
          return Queue.offer(controls, { _tag: "Shutdown" }).pipe(
            Effect.andThen(Deferred.await(shutdownDone)),
            Effect.andThen(shutdownExporters(options, health, disabledExporters)),
            Effect.ensuring(Queue.shutdown(spans)),
            Effect.ensuring(Queue.shutdown(controls)),
            Effect.ensuring(Fiber.interrupt(fiber)),
            Effect.ensuring(Deferred.succeed(shutdownDone, undefined)),
            Effect.timeoutOption(options.shutdownTimeout),
            Effect.tap((result) =>
              Effect.sync(() => {
                if (Option.isNone(result)) health.flushTimeouts++;
              }),
            ),
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
          if (!health.accepting) {
            health.dropped++;
            return;
          }
          const full = Queue.sizeUnsafe(spans) >= options.capacity;
          const offered = Queue.offerUnsafe(spans, span);
          if (full) {
            health.overflowed++;
            health.dropped++;
          }
          if (!offered) {
            if (!full) health.dropped++;
            return;
          }
          health.accepted++;
        },
      });
      const service = Delivery.of({
        health: Effect.sync(() => snapshotHealth(health, Queue.sizeUnsafe(spans))),
        flush,
        shutdown,
      });
      return Context.make(CompletedSpanSink, sink).pipe(Context.add(Delivery, service));
    }),
  );

function consumerLoop(
  spans: Queue.Queue<FlowSpan>,
  controls: Queue.Queue<Control>,
  options: DeliveryOptions,
  processBatch: (batch: ReadonlyArray<FlowSpan>) => Effect.Effect<void>,
  drain: Effect.Effect<void>,
  flushExporters: Effect.Effect<void>,
  shutdownDone: Deferred.Deferred<void>,
): Effect.Effect<void> {
  const next = Effect.raceFirst(
    Queue.take(spans).pipe(Effect.map((span): Next => ({ _tag: "Span", span }))),
    Queue.take(controls).pipe(Effect.map((control): Next => ({ _tag: "Control", control }))),
  );
  return Effect.gen(function* () {
    while (true) {
      const item = yield* next;
      if (item._tag === "Control") {
        yield* drain;
        yield* flushExporters;
        if (item.control._tag === "Shutdown") {
          yield* Deferred.succeed(shutdownDone, undefined);
          return;
        }
        yield* Deferred.succeed(item.control.done, undefined);
        continue;
      }
      const batch: Array<FlowSpan> = [item.span];
      const started = yield* Clock.currentTimeNanos;
      const maxAgeNanos = Duration.toNanosUnsafe(options.maxBatchAge);
      while (batch.length < options.maxBatchSize) {
        const elapsed = (yield* Clock.currentTimeNanos) - started;
        const remaining = maxAgeNanos - elapsed;
        if (remaining <= 0n) break;
        const following = yield* next.pipe(Effect.timeoutOption(Duration.nanos(remaining)));
        if (Option.isNone(following)) break;
        if (following.value._tag === "Span") {
          batch.push(following.value.span);
          continue;
        }
        yield* processBatch(batch);
        yield* drain;
        yield* flushExporters;
        if (following.value.control._tag === "Shutdown") {
          yield* Deferred.succeed(shutdownDone, undefined);
          return;
        }
        yield* Deferred.succeed(following.value.control.done, undefined);
        batch.length = 0;
        break;
      }
      if (batch.length > 0) yield* processBatch(batch);
    }
  });
}

function exportWithRetry(
  exporter: SpanExporter,
  batch: SpanBatch,
  options: DeliveryOptions,
  health: MutableHealth,
  disabledExporters: WeakSet<SpanExporter>,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    if (disabledExporters.has(exporter)) return;
    for (let attempt = 1; attempt <= options.retry.maxAttempts; attempt++) {
      const result = yield* attemptExporter(() => exporter.export(batch), options, health);
      if (result === "success") {
        health.exported += batch.length;
        return;
      }
      health.exporterFailures++;
      if (result === "timeout") {
        disabledExporters.add(exporter);
        health.disabledExporters++;
        return;
      }
      if (attempt === options.retry.maxAttempts) return;
      health.retried++;
      yield* Effect.sleep(retryDelay(options.retry, attempt, safeExporterName(exporter)));
    }
  });
}

function runExporterOperation(
  exporter: SpanExporter,
  operation: () => Effect.Effect<void, unknown>,
  options: DeliveryOptions,
  health: MutableHealth,
  disabledExporters: WeakSet<SpanExporter>,
): Effect.Effect<void> {
  if (disabledExporters.has(exporter)) return Effect.void;
  return Effect.flatMap(attemptExporter(operation, options, health), (success) =>
    Effect.sync(() => {
      if (success !== "success") health.exporterFailures++;
      if (success === "timeout") {
        disabledExporters.add(exporter);
        health.disabledExporters++;
      }
    }),
  );
}

function attemptExporter(
  operation: () => Effect.Effect<void, unknown>,
  options: DeliveryOptions,
  health: MutableHealth,
): Effect.Effect<"success" | "failure" | "timeout"> {
  return Effect.gen(function* () {
    const fiber = yield* Effect.suspend(operation).pipe(Effect.forkDetach);
    const result = yield* Fiber.await(fiber).pipe(Effect.timeoutOption(options.attemptTimeout));
    if (Option.isNone(result)) {
      health.attemptTimeouts++;
      fiber.interruptUnsafe();
      return "timeout" as const;
    }
    return Exit.isSuccess(result.value) ? ("success" as const) : ("failure" as const);
  });
}

function shutdownExporters(
  options: DeliveryOptions,
  health: MutableHealth,
  disabledExporters: WeakSet<SpanExporter>,
) {
  return Effect.forEach(
    options.exporters,
    (exporter) =>
      runExporterOperation(exporter, () => exporter.shutdown, options, health, disabledExporters),
    { concurrency: "unbounded", discard: true },
  );
}

function retryDelay(policy: RetryPolicy, attempt: number, exporter: string): Duration.Duration {
  const base = Duration.toMillis(policy.delay);
  const value = normalizeRatio(policy.jitterValue?.(attempt, exporter) ?? 0.5);
  const factor = 1 - policy.jitter + value * policy.jitter * 2;
  return Duration.millis(Math.max(0, base * factor));
}

function deterministicRatio(traceId: string): number {
  return Number.parseInt(traceId.slice(0, 13), 16) / 0xfffffffffffff;
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

function validateOptions(options: DeliveryOptions): DeliveryOptions {
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
  return options;
}

interface MutableHealth {
  accepting: boolean;
  accepted: number;
  sampled: number;
  exported: number;
  retried: number;
  dropped: number;
  overflowed: number;
  exporterFailures: number;
  attemptTimeouts: number;
  disabledExporters: number;
  flushTimeouts: number;
}

function makeHealth(): MutableHealth {
  return {
    accepting: true,
    accepted: 0,
    sampled: 0,
    exported: 0,
    retried: 0,
    dropped: 0,
    overflowed: 0,
    exporterFailures: 0,
    attemptTimeouts: 0,
    disabledExporters: 0,
    flushTimeouts: 0,
  };
}

function snapshotHealth(health: MutableHealth, queueDepth: number): DeliveryHealthSnapshot {
  return { ...health, queueDepth };
}
