import { Schema } from "effect";
import { SpanOutcome } from "./telemetry.js";

export const HttpStatus = Schema.Finite.check(
  Schema.makeFilter((value) => Number.isInteger(value) || "HTTP status must be an integer"),
  Schema.isBetween({ minimum: 100, maximum: 599 }),
);
export type HttpStatus = typeof HttpStatus.Type;

export const HttpStatusRange = Schema.Struct({
  minimum: HttpStatus,
  maximum: HttpStatus,
}).check(
  Schema.makeFilter((range) => range.minimum <= range.maximum || "invalid HTTP status range"),
);
export type HttpStatusRange = typeof HttpStatusRange.Type;

const HttpStatusRanges = Schema.Array(HttpStatusRange).check(Schema.isMaxLength(16));
export const HttpOutcomePolicy = Schema.Struct({
  serverErrorRanges: HttpStatusRanges,
  clientErrorRanges: HttpStatusRanges,
});
export type HttpOutcomePolicy = typeof HttpOutcomePolicy.Type;

const PositiveInteger = Schema.Finite.check(
  Schema.makeFilter((value) => Number.isInteger(value) || "must be an integer"),
  Schema.isGreaterThan(0),
);
export const SegmentStagingConfig = Schema.Struct({
  maxOpenSegments: PositiveInteger,
  maxSpansPerSegment: PositiveInteger,
  maxBytesPerSegment: PositiveInteger,
  maxTotalBytes: PositiveInteger,
  rootGraceMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  maxSegmentAgeMs: PositiveInteger,
  overflow: Schema.Literals(["drop-oldest", "drop-newest"]),
  decisionCacheCapacity: PositiveInteger,
  decisionCacheMaxAgeMs: PositiveInteger,
});
export type SegmentStagingConfig = typeof SegmentStagingConfig.Type;

export const SamplingConfig = Schema.Struct({
  successRatio: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  slowThresholdMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  retainOutcomes: Schema.Array(SpanOutcome),
  importantOperations: Schema.Array(Schema.String.check(Schema.isMaxLength(256))),
  retainHttpStatuses: Schema.optionalKey(Schema.Array(HttpStatus).check(Schema.isMaxLength(32))),
  retainHttpStatusRanges: Schema.optionalKey(HttpStatusRanges),
});
export type SamplingConfig = typeof SamplingConfig.Type;
export const SamplingDecision = Schema.Struct({
  keep: Schema.Boolean,
  reason: Schema.Literals(["outcome", "http-status", "slow", "important", "sampled", "dropped"]),
});
export type SamplingDecision = typeof SamplingDecision.Type;
export const SamplingInput = Schema.Struct({
  operation: Schema.String,
  outcome: SpanOutcome,
  durationMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  deterministicValue: Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  httpStatus: Schema.optionalKey(HttpStatus),
});
export type SamplingInput = typeof SamplingInput.Type;

export function normalizeSamplingConfig(config: SamplingConfig): SamplingConfig {
  const statuses = config.retainHttpStatuses;
  const ranges = config.retainHttpStatusRanges;
  return {
    ...config,
    ...(statuses === undefined ? {} : { retainHttpStatuses: [...new Set(statuses)].toSorted() }),
    ...(ranges === undefined ? {} : { retainHttpStatusRanges: mergeStatusRanges(ranges) }),
  };
}

export function decideSampling(config: SamplingConfig, input: SamplingInput): SamplingDecision {
  const httpStatus = input.httpStatus;
  if (config.retainOutcomes.includes(input.outcome)) return { keep: true, reason: "outcome" };
  if (
    httpStatus !== undefined &&
    (config.retainHttpStatuses?.includes(httpStatus) === true ||
      config.retainHttpStatusRanges?.some(
        (range) => httpStatus >= range.minimum && httpStatus <= range.maximum,
      ) === true)
  )
    return { keep: true, reason: "http-status" };
  if (input.durationMs >= config.slowThresholdMs) return { keep: true, reason: "slow" };
  if (config.importantOperations.includes(input.operation))
    return { keep: true, reason: "important" };
  return config.successRatio === 1 || input.deterministicValue < config.successRatio
    ? { keep: true, reason: "sampled" }
    : { keep: false, reason: "dropped" };
}

function mergeStatusRanges(ranges: ReadonlyArray<HttpStatusRange>): Array<HttpStatusRange> {
  const sorted = ranges.toSorted((left, right) => left.minimum - right.minimum);
  const merged: Array<HttpStatusRange> = [];
  for (const range of sorted) {
    const previous = merged.at(-1);
    if (previous === undefined || range.minimum > previous.maximum + 1) {
      merged.push(range);
    } else {
      merged[merged.length - 1] = {
        minimum: previous.minimum,
        maximum: Math.max(previous.maximum, range.maximum),
      };
    }
  }
  return merged;
}
