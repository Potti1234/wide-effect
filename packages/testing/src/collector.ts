import type { FlowSpan, SpanId, TraceId } from "@wide-effect/core";
import { Context, Effect, Layer, Ref } from "effect";
import { deterministicIdsLayer, type DeterministicIdsOptions } from "./deterministic-ids.js";

export interface CompletedSpanCollectorService {
  readonly collect: (span: FlowSpan) => Effect.Effect<void>;
  readonly collectAll: (spans: ReadonlyArray<FlowSpan>) => Effect.Effect<void>;
  readonly spans: Effect.Effect<ReadonlyArray<FlowSpan>>;
  readonly spansForTrace: (traceId: TraceId) => Effect.Effect<ReadonlyArray<FlowSpan>>;
  readonly find: (spanId: SpanId) => Effect.Effect<FlowSpan | undefined>;
  readonly clear: Effect.Effect<void>;
}

export class CompletedSpanCollector extends Context.Service<
  CompletedSpanCollector,
  CompletedSpanCollectorService
>()("@wide-effect/testing/CompletedSpanCollector") {}

export const completedSpanCollectorLayer = Layer.effect(
  CompletedSpanCollector,
  Effect.gen(function* () {
    const state = yield* Ref.make<ReadonlyArray<FlowSpan>>([]);
    return CompletedSpanCollector.of({
      collect: (span) => Ref.update(state, (spans) => [...spans, span]),
      collectAll: (incoming) =>
        incoming.length === 0 ? Effect.void : Ref.update(state, (spans) => [...spans, ...incoming]),
      spans: Ref.get(state),
      spansForTrace: (traceId) =>
        Effect.map(Ref.get(state), (spans) => spans.filter((span) => span.traceId === traceId)),
      find: (spanId) =>
        Effect.map(Ref.get(state), (spans) => spans.find((span) => span.spanId === spanId)),
      clear: Ref.set(state, []),
    });
  }),
);

export const testingLayer = (options: DeterministicIdsOptions = {}) =>
  Layer.mergeAll(completedSpanCollectorLayer, deterministicIdsLayer(options));
