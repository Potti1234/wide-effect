import { Schema } from "effect";
import { InteractionId, RequestId, TraceId } from "./identifiers.js";
import { SafeAttributes } from "./safe-value.js";
import { CanonicalUtcTimestamp } from "./telemetry.js";

const Text = Schema.String.check(Schema.isMaxLength(2_048));
export const MAX_DIAGNOSTIC_BUNDLE_BYTES = 1_048_576;
const Entry = Schema.Struct({
  timestamp: CanonicalUtcTimestamp,
  operation: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
  summary: Text,
  attributes: SafeAttributes,
});
const DiagnosticBundleFields = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  traceId: TraceId,
  interactionId: Schema.optionalKey(InteractionId),
  requestId: Schema.optionalKey(RequestId),
  summary: Schema.Struct({ title: Text, outcome: Text }),
  timeline: Schema.Array(Entry).check(Schema.isMaxLength(512)),
  context: SafeAttributes,
  evidence: Schema.Array(Entry).check(Schema.isMaxLength(256)),
});
export const DiagnosticBundle = DiagnosticBundleFields.check(
  Schema.makeFilter(
    (bundle) =>
      new TextEncoder().encode(JSON.stringify(bundle)).byteLength <= MAX_DIAGNOSTIC_BUNDLE_BYTES ||
      "diagnostic bundle exceeds encoded byte limit",
  ),
);
export type DiagnosticBundle = typeof DiagnosticBundle.Type;
export const decodeDiagnosticBundle = Schema.decodeUnknownSync(DiagnosticBundle, {
  onExcessProperty: "error",
});
