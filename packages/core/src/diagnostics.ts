import { Schema } from "effect";
import { InteractionId, RequestId, TraceId } from "./identifiers.js";
import { SafeAttributes } from "./safe-value.js";

const Text = Schema.String.check(Schema.isMaxLength(2_048));
const Entry = Schema.Struct({
  timestamp: Schema.String.check(Schema.isMaxLength(64)),
  operation: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
  summary: Text,
  attributes: SafeAttributes,
});
export const DiagnosticBundle = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  traceId: TraceId,
  interactionId: Schema.optionalKey(InteractionId),
  requestId: Schema.optionalKey(RequestId),
  summary: Schema.Struct({ title: Text, outcome: Text }),
  timeline: Schema.Array(Entry).check(Schema.isMaxLength(512)),
  context: SafeAttributes,
  evidence: Schema.Array(Entry).check(Schema.isMaxLength(256)),
});
export type DiagnosticBundle = typeof DiagnosticBundle.Type;
export const decodeDiagnosticBundle = Schema.decodeUnknownSync(DiagnosticBundle, {
  onExcessProperty: "error",
});
