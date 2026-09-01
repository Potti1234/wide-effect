import {
  normalizeAttributes,
  type AttributePolicy,
  type NormalizationLimits,
  type SafeAttributes,
} from "@wide-effect/core";

const semanticLogName = /^[a-z0-9](?:[a-z0-9_-]*)(?:\.[a-z0-9](?:[a-z0-9_-]*))*$/;
const maxSemanticLogNameLength = 128;

export interface SanitizedSpanEvent {
  readonly name: string;
  readonly attributes: SafeAttributes;
}

/**
 * Converts an Effect tracer-logger event at the first trusted boundary. Effect rc.108 renders
 * arbitrary log arguments into the event name and pretty-prints causes into `effect.cause`; neither
 * representation is safe canonical telemetry.
 */
export function sanitizeSpanEvent(
  name: string,
  attributes: Record<string, unknown>,
  policy: AttributePolicy,
  limits?: NormalizationLimits,
): SanitizedSpanEvent {
  const logEvent = isEffectLogEvent(attributes);
  const filtered = logEvent ? withoutCause(attributes) : attributes;
  return {
    name: logEvent ? safeLogName(name) : name,
    attributes: normalizeAttributes(filtered, policy, limits),
  };
}

function isEffectLogEvent(attributes: Record<string, unknown>): boolean {
  const level = attributes["effect.logLevel"];
  return (
    typeof level === "string" &&
    (level === "TRACE" ||
      level === "DEBUG" ||
      level === "INFO" ||
      level === "WARN" ||
      level === "ERROR" ||
      level === "FATAL")
  );
}

function withoutCause(attributes: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const key of Object.keys(attributes)) {
    if (key === "effect.cause") continue;
    const descriptor = Object.getOwnPropertyDescriptor(attributes, key);
    if (descriptor !== undefined && "value" in descriptor) output[key] = descriptor.value;
  }
  return output;
}

function safeLogName(name: string): string {
  return name.length <= maxSemanticLogNameLength && semanticLogName.test(name)
    ? name
    : "effect.log";
}
