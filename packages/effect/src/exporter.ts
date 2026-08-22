import { decodeFlowSpan, encodeFlowSpan, type FlowSpan } from "@wide-effect/core";
import { Effect, Ref, Schema } from "effect";

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
  readonly shutdown: Effect.Effect<void>;
}

export const makeExporterLifecycle = (name: string): Effect.Effect<ExporterLifecycle> =>
  Effect.gen(function* () {
    const closed = yield* Ref.make(false);
    return {
      ensureOpen: (operation) =>
        Ref.get(closed).pipe(
          Effect.flatMap((isClosed) =>
            isClosed
              ? Effect.fail(exporterError(name, operation, "exporter is shut down"))
              : Effect.void,
          ),
        ),
      shutdown: Ref.set(closed, true),
    };
  });

export function exporterError(
  exporter: string,
  operation: ExporterOperation,
  message: string,
  failures: ReadonlyArray<ExporterFailure> = [],
): SpanExporterError {
  return new SpanExporterError({ exporter, operation, message, failures });
}

export function serializeSpan(span: FlowSpan): Effect.Effect<string, SpanExporterError> {
  return Effect.try({
    try: () => JSON.stringify(encodeFlowSpan(decodeFlowSpan(span))),
    catch: (cause) =>
      exporterError("serialization", "export", safeErrorMessage(cause, "invalid FlowSpan")),
  });
}

export const fanOutExporter = (name: string, exporters: SpanBatchOfExporters): SpanExporter => ({
  name,
  export: (batch) => isolate(name, "export", exporters, (exporter) => exporter.export(batch)),
  flush: isolate(name, "flush", exporters, (exporter) => exporter.flush),
  shutdown: isolate(name, "shutdown", exporters, (exporter) => exporter.shutdown),
});

export type SpanBatchOfExporters = readonly [SpanExporter, ...ReadonlyArray<SpanExporter>];

function isolate(
  name: string,
  operation: ExporterOperation,
  exporters: SpanBatchOfExporters,
  run: (exporter: SpanExporter) => Effect.Effect<void, SpanExporterError>,
): Effect.Effect<void, SpanExporterError> {
  return Effect.forEach(
    exporters,
    (exporter) =>
      Effect.matchEffect(run(exporter), {
        onFailure: (error) => Effect.succeed<ExporterFailure>(toFailure(error, exporter.name)),
        onSuccess: () => Effect.succeed<ExporterFailure | undefined>(undefined),
      }),
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

function toFailure(error: SpanExporterError, fallbackName: string): ExporterFailure {
  return {
    exporter: error.exporter || fallbackName,
    operation: error.operation,
    message: error.message,
  };
}

export function safeErrorMessage(_cause: unknown, fallback: string): string {
  // Boundary failures can echo paths, payloads, or rejected values. Exporter
  // errors deliberately expose stable context rather than an untrusted cause.
  return fallback;
}
