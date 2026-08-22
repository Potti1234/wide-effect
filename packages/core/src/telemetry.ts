import { Schema } from "effect";
import { InteractionId, RequestId, SpanId, TraceId } from "./identifiers.js";
import { SafeAttributes } from "./safe-value.js";

const Text = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const Timestamp = Schema.String.check(
  Schema.makeFilter(
    (value) => (!Number.isNaN(Date.parse(value)) && value.endsWith("Z")) || "invalid UTC timestamp",
  ),
);
export const ServiceMetadata = Schema.Struct({
  name: Text,
  version: Text,
  environment: Text,
  deploymentId: Schema.optionalKey(Text),
});
export type ServiceMetadata = typeof ServiceMetadata.Type;
export const SpanKind = Schema.Literals(["internal", "server", "client", "producer", "consumer"]);
export type SpanKind = typeof SpanKind.Type;
export const SpanOutcome = Schema.Literals(["success", "failure", "defect", "interrupted"]);
export type SpanOutcome = typeof SpanOutcome.Type;
export const LogLevel = Schema.Literals(["trace", "debug", "info", "warn", "error", "fatal"]);
export type LogLevel = typeof LogLevel.Type;
export const SpanEvent = Schema.Struct({
  name: Text,
  timestamp: Timestamp,
  level: Schema.optionalKey(LogLevel),
  attributes: SafeAttributes,
});
export type SpanEvent = typeof SpanEvent.Type;
export const FlowSpan = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  traceId: TraceId,
  spanId: SpanId,
  parentSpanId: Schema.optionalKey(SpanId),
  interactionId: Schema.optionalKey(InteractionId),
  requestId: Schema.optionalKey(RequestId),
  service: ServiceMetadata,
  operation: Text,
  kind: SpanKind,
  outcome: SpanOutcome,
  startTime: Timestamp,
  endTime: Timestamp,
  durationMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
  attributes: SafeAttributes,
  events: Schema.Array(SpanEvent).check(Schema.isMaxLength(256)),
});
export type FlowSpan = typeof FlowSpan.Type;
export const decodeFlowSpan = Schema.decodeUnknownSync(FlowSpan, { onExcessProperty: "error" });
export const encodeFlowSpan = Schema.encodeSync(FlowSpan);
