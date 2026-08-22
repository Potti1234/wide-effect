import type { FlowSpan } from "@wide-effect/core";
import { Effect, Ref } from "effect";
import { makeExporterLifecycle, type SpanExporter } from "./exporter.js";

export interface InMemorySpanExporter {
  readonly exporter: SpanExporter;
  readonly spans: Effect.Effect<ReadonlyArray<FlowSpan>>;
  readonly clear: Effect.Effect<void>;
}

/** An explicit exporter useful outside tracer unit tests; unlike CompletedSpanSink it is Effectful. */
export const makeInMemorySpanExporter = (name = "memory"): Effect.Effect<InMemorySpanExporter> =>
  Effect.gen(function* () {
    const state = yield* Ref.make<ReadonlyArray<FlowSpan>>([]);
    const lifecycle = yield* makeExporterLifecycle(name);
    return {
      exporter: {
        name,
        export: (batch) =>
          lifecycle
            .ensureOpen("export")
            .pipe(Effect.andThen(Ref.update(state, (spans) => [...spans, ...batch]))),
        flush: lifecycle.ensureOpen("flush"),
        shutdown: lifecycle.shutdown,
      },
      spans: Ref.get(state),
      clear: Ref.set(state, []),
    };
  });
