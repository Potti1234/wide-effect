import { decodeFlowSpan, encodeFlowSpan, type FlowSpan } from "@wide-effect/core";
import { Effect, Option, Ref, Schema, Semaphore } from "effect";

export type SpanBatch = readonly [FlowSpan, ...ReadonlyArray<FlowSpan>];
export type ExporterOperation = "export" | "flush" | "shutdown";

const ExporterFailure = Schema.Struct({
  exporter: Schema.String,
  operation: Schema.Literals(["export", "flush", "shutdown"]),
  message: Schema.String,
});
export type ExporterFailure = typeof ExporterFailure.Type;

export class SpanExporterError extends Schema.TaggedError<SpanExporterError>()(
  "SpanExporterError",
  {
    exporter: Schema.String,
    operation: Schema.Literals(["export", "flush", "shutdown"]),
    message: Schema.String,
    failures: Schema.Array(ExporterFailure),
  },
) {}

export interface SpanExporter {
  readonly name: string;
  readonly export: (batch: SpanBatch) => Effect.Effect<void, SpanExporterError>;
  readonly flush: Effect.Effect<void, SpanExporterError>;
  readonly shutdown: Effect.Effect<void, SpanExporterError>;
}

export interface ExporterLifecycle {
  readonly ensureOpen: (operation: ExporterOperation) => Effect.Effect<void, SpanExporterError>;
  readonly run: <A>(
    operation: Exclude<ExporterOperation, "shutdown">,
    effect: Effect.Effect<A, SpanExporterError>,
  ) => Effect.Effect<A, SpanExporterError>;
  readonly shutdown: Effect.Effect<void, SpanExporterError>;
}

export const makeExporterLifecycle = (
  name: string,
  shutdownTimeoutMs = 30_000,
): Effect.Effect<ExporterLifecycle> =>
  Effect.gen(function* () {
    const closed = yield* Ref.make(false);
    const semaphore = yield* Semaphore.make(1);
    const ensureOpen = (operation: ExporterOperation) =>
      Ref.get(closed).pipe(
        Effect.flatMap((isClosed) =>
          isClosed
            ? Effect.fail(exporterError(name, operation, "exporter is shut down"))
            : Effect.void,
        ),
      );
    return {
      ensureOpen,
      run: (operation, effect) =>
        semaphore.withPermit(ensureOpen(operation).pipe(Effect.andThen(effect))),
      shutdown: semaphore.withPermit(Ref.set(closed, true)).pipe(
        Effect.timeoutOption(shutdownTimeoutMs),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(exporterError(name, "shutdown", "exporter shutdown timed out")),
            onSome: () => Effect.void,
          }),
        ),
      ),
    };
  });

export function exporterError(
  exporter: string,
  operation: ExporterOperation,
  message: string,
  failures: ReadonlyArray<ExporterFailure> = [],
): SpanExporterError {
  return new SpanExporterError({
    exporter: safeText(exporter, "exporter"),
    operation,
    message: safeText(message, "exporter operation failed"),
    failures: failures.slice(0, 64).map((failure) => ({
      exporter: safeText(failure.exporter, "exporter"),
      operation: failure.operation,
      message: safeText(failure.message, "exporter operation failed"),
    })),
  });
}

export function serializeSpan(span: FlowSpan): Effect.Effect<string, SpanExporterError> {
  return Effect.try({
    try: () => JSON.stringify(encodeFlowSpan(decodeFlowSpan(span))),
    catch: (cause) =>
      exporterError("serialization", "export", safeErrorMessage(cause, "invalid FlowSpan")),
  });
}

export interface FanOutExporterOptions {
  readonly attemptTimeoutMs?: number;
}

export const fanOutExporter = (
  name: string,
  exporters: SpanBatchOfExporters,
  options: FanOutExporterOptions = {},
): SpanExporter => ({
  name,
  export: (batch) =>
    isolate(name, "export", exporters, (exporter) => exporter.export(batch), options),
  flush: isolate(name, "flush", exporters, (exporter) => exporter.flush, options),
  shutdown: isolate(name, "shutdown", exporters, (exporter) => exporter.shutdown, options),
});

export type SpanBatchOfExporters = readonly [SpanExporter, ...ReadonlyArray<SpanExporter>];

function isolate(
  name: string,
  operation: ExporterOperation,
  exporters: SpanBatchOfExporters,
  run: (exporter: SpanExporter) => Effect.Effect<void, SpanExporterError>,
  options: FanOutExporterOptions,
): Effect.Effect<void, SpanExporterError> {
  return Effect.forEach(
    exporters,
    (exporter) => isolateOne(exporter, operation, run, options.attemptTimeoutMs ?? 30_000),
    { concurrency: "unbounded" },
  ).pipe(
    Effect.flatMap((results) => {
      const failures = results.filter(
        (failure): failure is ExporterFailure => failure !== undefined,
      );
      return failures.length === 0
        ? Effect.void
        : Effect.fail(
            exporterError(
              name,
              operation,
              `${failures.length} exporter operation(s) failed`,
              failures,
            ),
          );
    }),
  );
}

function isolateOne(
  exporter: SpanExporter,
  operation: ExporterOperation,
  run: (exporter: SpanExporter) => Effect.Effect<void, SpanExporterError>,
  timeoutMs: number,
): Effect.Effect<ExporterFailure | undefined> {
  return Effect.suspend(() => run(exporter)).pipe(
    Effect.exit,
    Effect.timeoutOption(Math.max(0, timeoutMs)),
    Effect.map(
      Option.match({
        onNone: () => ({
          exporter: safeExporterName(exporter),
          operation,
          message: "exporter operation timed out",
        }),
        onSome: (exit) =>
          exit._tag === "Success"
            ? undefined
            : {
                exporter: safeExporterName(exporter),
                operation,
                message: "exporter operation failed",
              },
      }),
    ),
  );
}

export function safeErrorMessage(_cause: unknown, fallback: string): string {
  // Boundary failures can echo paths, payloads, or rejected values. Exporter
  // errors deliberately expose stable context rather than an untrusted cause.
  return fallback;
}

function safeText(value: string, fallback: string): string {
  const safe = value
    .replace(/[^\x20-\x7e]/g, "?")
    .trim()
    .slice(0, 256);
  return safe || fallback;
}

function safeExporterName(exporter: SpanExporter): string {
  try {
    return typeof exporter.name === "string" ? safeText(exporter.name, "exporter") : "exporter";
  } catch {
    return "exporter";
  }
}
