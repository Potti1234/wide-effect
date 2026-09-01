import { Schema } from "effect";
import { InteractionId, RequestId, SegmentId, SpanId, TraceId } from "./identifiers.js";
import { SafeAttributes } from "./safe-value.js";

const Text = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
export const CanonicalUtcTimestamp = Schema.String.check(
  Schema.makeFilter(
    (value) =>
      (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) &&
        new Date(value).toISOString() === value) ||
      "invalid canonical UTC timestamp",
  ),
);
export const MAX_FLOW_SPAN_BYTES = 1_048_576;
/** Allows sub-millisecond tracer durations beside millisecond UTC timestamps. */
export const FLOW_SPAN_DURATION_TOLERANCE_MS = 1;
export const ServiceMetadata = Schema.Struct({
  name: Text,
  version: Text,
  environment: Text,
  deploymentId: Schema.optionalKey(Text),
  commitHash: Schema.optionalKey(Text),
  region: Schema.optionalKey(Text),
  instanceId: Schema.optionalKey(Text),
});
export type ServiceMetadata = typeof ServiceMetadata.Type;
export const SpanKind = Schema.Literals(["internal", "server", "client", "producer", "consumer"]);
export type SpanKind = typeof SpanKind.Type;
export const SpanOutcome = Schema.Literals(["success", "failure", "defect", "interrupted"]);
export type SpanOutcome = typeof SpanOutcome.Type;
const FailureMachineName = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(64),
  Schema.isPattern(/^[A-Za-z][A-Za-z0-9_.:-]*$/),
);
const FailureText = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(256),
  Schema.isPattern(/^[^\r\n\u2028\u2029]*$/),
);
export const FailureSummary = Schema.Struct({
  type: Schema.Literals(["typed", "defect", "interruption", "http"]),
  category: Schema.optionalKey(FailureMachineName),
  code: Schema.optionalKey(FailureMachineName),
  retryable: Schema.optionalKey(Schema.Boolean),
  summary: Schema.optionalKey(FailureText),
});
export type FailureSummary = typeof FailureSummary.Type;
export const LogLevel = Schema.Literals(["trace", "debug", "info", "warn", "error", "fatal"]);
export type LogLevel = typeof LogLevel.Type;
export const SpanEvent = Schema.Struct({
  name: Text,
  timestamp: CanonicalUtcTimestamp,
  level: Schema.optionalKey(LogLevel),
  attributes: SafeAttributes,
});
export type SpanEvent = typeof SpanEvent.Type;
const FlowSpanFields = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  traceId: TraceId,
  spanId: SpanId,
  segmentId: Schema.optionalKey(SegmentId),
  parentSpanId: Schema.optionalKey(SpanId),
  interactionId: Schema.optionalKey(InteractionId),
  requestId: Schema.optionalKey(RequestId),
  service: ServiceMetadata,
  operation: Text,
  kind: SpanKind,
  outcome: SpanOutcome,
  failure: Schema.optionalKey(FailureSummary),
  startTime: CanonicalUtcTimestamp,
  endTime: CanonicalUtcTimestamp,
  durationMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  attributes: SafeAttributes,
  events: Schema.Array(SpanEvent).check(Schema.isMaxLength(256)),
});
export const FlowSpan = FlowSpanFields.check(
  Schema.makeFilter((span) => {
    const elapsedMs = Date.parse(span.endTime) - Date.parse(span.startTime);
    return elapsedMs >= 0 || "span endTime precedes startTime";
  }),
  Schema.makeFilter((span) => {
    const elapsedMs = Date.parse(span.endTime) - Date.parse(span.startTime);
    return (
      Math.abs(elapsedMs - span.durationMs) <= FLOW_SPAN_DURATION_TOLERANCE_MS ||
      `span duration differs from timestamps by more than ${FLOW_SPAN_DURATION_TOLERANCE_MS}ms`
    );
  }),
  Schema.makeFilter(
    (span) =>
      new TextEncoder().encode(JSON.stringify(span)).byteLength <= MAX_FLOW_SPAN_BYTES ||
      "span exceeds encoded byte limit",
  ),
);
export type FlowSpan = typeof FlowSpan.Type;
export const decodeFlowSpan = Schema.decodeUnknownSync(FlowSpan, {
  onExcessProperty: "error",
});
export const encodeFlowSpan = Schema.encodeSync(FlowSpan);
