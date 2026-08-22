import { decodeFlowSpan, type FlowSpan } from "@wide-effect/core";
import { Effect, Ref } from "effect";
import {
  exporterError,
  makeExporterLifecycle,
  safeErrorMessage,
  type SpanBatch,
  type SpanExporter,
} from "./exporter.js";

export interface InMemorySpanExporter {
  readonly exporter: SpanExporter;
  readonly spans: Effect.Effect<ReadonlyArray<FlowSpan>>;
  readonly dropped: Effect.Effect<number>;
  readonly clear: Effect.Effect<void>;
}

export interface InMemorySpanExporterOptions {
  readonly name?: string;
  readonly capacity?: number;
  readonly overflow?: "drop-oldest" | "drop-newest";
}

/** An explicit exporter useful outside tracer unit tests; unlike CompletedSpanSink it is Effectful. */
export const makeInMemorySpanExporter = (
  options: string | InMemorySpanExporterOptions = {},
): Effect.Effect<InMemorySpanExporter> =>
  Effect.gen(function* () {
    const resolved = typeof options === "string" ? { name: options } : options;
    const name = resolved.name ?? "memory";
    const capacity = Math.max(1, Math.floor(resolved.capacity ?? 1_024));
    const overflow = resolved.overflow ?? "drop-oldest";
    const state = yield* Ref.make<{ spans: Array<FlowSpan>; dropped: number }>({
      spans: [],
      dropped: 0,
    });
    const lifecycle = yield* makeExporterLifecycle(name);
    return {
      exporter: {
        name,
        export: (batch) =>
          lifecycle.run(
            "export",
            validateBatch(name, batch).pipe(
              Effect.flatMap((validated) =>
                Ref.update(state, (current) => {
                  const spans = current.spans.slice();
                  let dropped = current.dropped;
                  for (const span of validated) {
                    if (spans.length < capacity) spans.push(span);
                    else {
                      dropped++;
                      if (overflow === "drop-oldest") {
                        spans.shift();
                        spans.push(span);
                      }
                    }
                  }
                  return { spans, dropped };
                }),
              ),
            ),
          ),
        flush: lifecycle.run("flush", Effect.void),
        shutdown: lifecycle.shutdown,
      },
      spans: Ref.get(state).pipe(Effect.map((current) => [...current.spans])),
      dropped: Ref.get(state).pipe(Effect.map((current) => current.dropped)),
      clear: Ref.set(state, { spans: [], dropped: 0 }),
    };
  });

function validateBatch(name: string, batch: SpanBatch) {
  return Effect.try({
    try: () => batch.map((span) => decodeFlowSpan(span)),
    catch: (cause) =>
      exporterError(name, "export", safeErrorMessage(cause, "invalid FlowSpan batch")),
  });
}
