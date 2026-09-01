import {
  InteractionId,
  RequestId,
  type InteractionId as InteractionIdType,
  type RequestId as RequestIdType,
} from "@wide-effect/core";
import { Effect, Option, Schema } from "effect";

export const correlationAttributeKeys = {
  interactionId: "wide.interaction.id",
  requestId: "wide.request.id",
} as const;

/** Enriches the caller's current span; invalid runtime input is ignored. */
export const annotateInteractionId = Effect.fnUntraced(function* (
  interactionId: InteractionIdType,
) {
  const decoded = Schema.decodeUnknownOption(InteractionId)(interactionId);
  if (Option.isSome(decoded))
    yield* Effect.annotateCurrentSpan(correlationAttributeKeys.interactionId, decoded.value);
});

/** Request IDs are local-hop metadata and are never propagated implicitly. */
export const annotateRequestId = Effect.fnUntraced(function* (requestId: RequestIdType) {
  const decoded = Schema.decodeUnknownOption(RequestId)(requestId);
  if (Option.isSome(decoded))
    yield* Effect.annotateCurrentSpan(correlationAttributeKeys.requestId, decoded.value);
});

export const setInteractionId = annotateInteractionId;
export const setRequestId = annotateRequestId;

/** Applies a validated interaction ID to every span created by the supplied local workflow. */
export function withInteractionId<A, E, R>(
  effect: Effect.Effect<A, E, R>,
  interactionId: InteractionIdType,
): Effect.Effect<A, E, R> {
  const decoded = Schema.decodeUnknownOption(InteractionId)(interactionId);
  return Option.isNone(decoded)
    ? effect
    : effect.pipe(Effect.annotateSpans(correlationAttributeKeys.interactionId, decoded.value));
}

export function promoteCorrelation(attributes: Record<string, unknown>): {
  readonly interactionId?: InteractionIdType;
  readonly requestId?: RequestIdType;
} {
  const interactionId = Schema.decodeUnknownOption(InteractionId)(
    attributes[correlationAttributeKeys.interactionId],
  );
  const requestId = Schema.decodeUnknownOption(RequestId)(
    attributes[correlationAttributeKeys.requestId],
  );
  delete attributes[correlationAttributeKeys.interactionId];
  delete attributes[correlationAttributeKeys.requestId];
  return {
    ...(Option.isNone(interactionId) ? {} : { interactionId: interactionId.value }),
    ...(Option.isNone(requestId) ? {} : { requestId: requestId.value }),
  };
}
