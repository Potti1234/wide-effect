import type { FlowSpan } from "@wide-effect/core";
import { Context, Effect, Layer } from "effect";

export interface CompletedSpanSinkService {
  /** Non-throwing admission boundary invoked synchronously by `Tracer.Span.end`. */
  readonly unsafeOffer: (span: FlowSpan) => void;
}

export class CompletedSpanSink extends Context.Service<
  CompletedSpanSink,
  CompletedSpanSinkService
>()("@wide-effect/effect/CompletedSpanSink") {}

export const completedSpanSinkLayer = (sink: CompletedSpanSinkService) =>
  Layer.succeed(CompletedSpanSink, sink);

export const noopCompletedSpanSinkLayer = completedSpanSinkLayer({ unsafeOffer: () => undefined });

export interface InMemoryCompletedSpansService {
  readonly spans: Effect.Effect<ReadonlyArray<FlowSpan>>;
  readonly dropped: Effect.Effect<number>;
  readonly clear: Effect.Effect<void>;
}

export class InMemoryCompletedSpans extends Context.Service<
  InMemoryCompletedSpans,
  InMemoryCompletedSpansService
>()("@wide-effect/effect/InMemoryCompletedSpans") {}

export interface InMemoryCompletedSpanSinkOptions {
  readonly capacity?: number;
  readonly overflow?: "drop-oldest" | "drop-newest";
}

export const inMemoryCompletedSpanSinkLayer = (options: InMemoryCompletedSpanSinkOptions = {}) => {
  const capacity = Math.max(1, Math.floor(options.capacity ?? 1_024));
  const overflow = options.overflow ?? "drop-oldest";
  return Layer.effectContext(
    Effect.acquireRelease(
      Effect.sync(() => makeInMemoryContext(capacity, overflow)),
      (context) => Context.get(context, InMemoryCompletedSpans).clear,
    ),
  );
};

function makeInMemoryContext(capacity: number, overflow: "drop-oldest" | "drop-newest") {
  const state: Array<FlowSpan> = [];
  let dropped = 0;
  const store = InMemoryCompletedSpans.of({
    spans: Effect.sync(() => [...state]),
    dropped: Effect.sync(() => dropped),
    clear: Effect.sync(() => {
      state.length = 0;
      dropped = 0;
    }),
  });
  const sink = CompletedSpanSink.of({
    unsafeOffer: (span) => {
      if (state.length < capacity) state.push(span);
      else {
        dropped++;
        if (overflow === "drop-oldest") {
          state.shift();
          state.push(span);
        }
      }
    },
  });
  return Context.make(InMemoryCompletedSpans, store).pipe(Context.add(CompletedSpanSink, sink));
}
