import { Schema } from "effect";
import { SpanOutcome } from "./telemetry.js";

export const SamplingConfig = Schema.Struct({
  successRatio: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  slowThresholdMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  retainOutcomes: Schema.Array(SpanOutcome),
  importantOperations: Schema.Array(Schema.String.check(Schema.isMaxLength(256))),
});
export type SamplingConfig = typeof SamplingConfig.Type;
export const SamplingDecision = Schema.Struct({
  keep: Schema.Boolean,
  reason: Schema.Literals(["outcome", "slow", "important", "sampled", "dropped"]),
});
export type SamplingDecision = typeof SamplingDecision.Type;
export const SamplingInput = Schema.Struct({
  operation: Schema.String,
  outcome: SpanOutcome,
  durationMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  deterministicValue: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
});
export type SamplingInput = typeof SamplingInput.Type;
export function decideSampling(config: SamplingConfig, input: SamplingInput): SamplingDecision {
  if (config.retainOutcomes.includes(input.outcome)) return { keep: true, reason: "outcome" };
  if (input.durationMs >= config.slowThresholdMs) return { keep: true, reason: "slow" };
  if (config.importantOperations.includes(input.operation))
    return { keep: true, reason: "important" };
  return input.deterministicValue < config.successRatio
    ? { keep: true, reason: "sampled" }
    : { keep: false, reason: "dropped" };
}
