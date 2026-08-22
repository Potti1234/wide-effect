import { normalizeAttributes, type LogLevel } from "@wide-effect/core";
import { Clock, Effect, Option } from "effect";
import { ObservabilityConfig } from "./config.js";

export const annotate = Effect.fnUntraced(function* (
  attributes: Readonly<Record<string, unknown>>,
) {
  const config = yield* ObservabilityConfig;
  yield* Effect.annotateCurrentSpan(normalizeAttributes(attributes, config.attributePolicy));
});

export const event = Effect.fnUntraced(function* (
  name: string,
  attributes: Readonly<Record<string, unknown>> = {},
  level?: LogLevel,
) {
  const current = yield* Effect.option(Effect.currentSpan);
  if (Option.isNone(current)) return;
  const config = yield* ObservabilityConfig;
  const time = yield* Clock.currentTimeNanos;
  current.value.event(name, time, {
    ...normalizeAttributes(attributes, config.attributePolicy),
    ...(level === undefined ? {} : { "effect.logLevel": level.toUpperCase() }),
  });
});
