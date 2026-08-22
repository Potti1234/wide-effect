import {
  InteractionId,
  RequestId,
  SpanId,
  TraceId,
  type InteractionId as InteractionIdType,
  type RequestId as RequestIdType,
  type SpanId as SpanIdType,
  type TraceId as TraceIdType,
} from "@wide-effect/core";
import { Context, Effect, Layer, Ref } from "effect";
import { Schema } from "effect";

export interface DeterministicIdsService {
  readonly nextTraceId: Effect.Effect<TraceIdType>;
  readonly nextSpanId: Effect.Effect<SpanIdType>;
  readonly nextInteractionId: Effect.Effect<InteractionIdType>;
  readonly nextRequestId: Effect.Effect<RequestIdType>;
}

export class DeterministicIds extends Context.Service<DeterministicIds, DeterministicIdsService>()(
  "@wide-effect/testing/DeterministicIds",
) {}

export interface DeterministicIdsOptions {
  readonly seed?: number;
  readonly prefix?: string;
}

export const deterministicIdsLayer = (options: DeterministicIdsOptions = {}) =>
  Layer.effect(
    DeterministicIds,
    Effect.gen(function* () {
      const counter = yield* Ref.make(normalizeSeed(options.seed));
      const next = Ref.getAndUpdate(counter, (value) => value + 1);
      const nextHex = (length: number) =>
        Effect.map(next, (value) => value.toString(16).padStart(length, "0").slice(-length));
      const prefix = normalizePrefix(options.prefix);
      return DeterministicIds.of({
        nextTraceId: Effect.map(nextHex(32), Schema.decodeUnknownSync(TraceId)),
        nextSpanId: Effect.map(nextHex(16), Schema.decodeUnknownSync(SpanId)),
        nextInteractionId: Effect.map(next, (value) =>
          Schema.decodeUnknownSync(InteractionId)(`${prefix}interaction-${value}`),
        ),
        nextRequestId: Effect.map(next, (value) =>
          Schema.decodeUnknownSync(RequestId)(`${prefix}request-${value}`),
        ),
      });
    }),
  );

function normalizeSeed(seed: number | undefined): number {
  if (seed === undefined) return 1;
  if (!Number.isSafeInteger(seed) || seed < 1) {
    throw new RangeError("Deterministic ID seed must be a positive safe integer");
  }
  return seed;
}

function normalizePrefix(prefix: string | undefined): string {
  if (prefix === undefined || prefix.length === 0) return "test-";
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(prefix)) {
    throw new RangeError("Deterministic ID prefix contains unsupported characters");
  }
  return prefix.endsWith("-") ? prefix : `${prefix}-`;
}
