import type { FlowSpan } from "@wide-effect/core";
import { Context, Effect, Layer } from "effect";

export interface CompletedSpanSinkService {
  /** Non-throwing admission boundary invoked synchronously by `Tracer.Span.end`. */
  readonly unsafeOffer: (span: FlowSpan) => void;
}

export class CompletedSpanSink extends Context.Reference<CompletedSpanSinkService>(
  "@wide-effect/effect/CompletedSpanSink",
  { defaultValue: () => ({ unsafeOffer: () => undefined }) },
) {}

export const completedSpanSinkLayer = (sink: CompletedSpanSinkService) =>
  Layer.succeed(CompletedSpanSink, sink);

export const noopCompletedSpanSinkLayer = completedSpanSinkLayer({ unsafeOffer: () => undefined });

export interface InMemoryCompletedSpansService {
  readonly spans: Effect.Effect<ReadonlyArray<FlowSpan>>;
  readonly clear: Effect.Effect<void>;
}

export class InMemoryCompletedSpans extends Context.Service<
  InMemoryCompletedSpans,
  InMemoryCompletedSpansService
>()("@wide-effect/effect/InMemoryCompletedSpans") {}

export const inMemoryCompletedSpanSinkLayer = Layer.effectContext(
  Effect.sync(() => {
    let state: ReadonlyArray<FlowSpan> = [];
    const store = InMemoryCompletedSpans.of({
      spans: Effect.sync(() => state),
      clear: Effect.sync(() => {
        state = [];
      }),
    });
    const sink = CompletedSpanSink.of({
      unsafeOffer: (span) => {
        state = [...state, span];
      },
    });
    return Context.make(InMemoryCompletedSpans, store).pipe(Context.add(CompletedSpanSink, sink));
  }),
);
