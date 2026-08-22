import { Schema } from "effect";

function hexId<const Name extends string>(length: number, name: Name) {
  const pattern = new RegExp(`^[0-9a-f]{${length}}$`);
  return Schema.String.check(
    Schema.isPattern(pattern),
    Schema.makeFilter((value) => value !== "0".repeat(length) || `${name} cannot be all zero`),
  ).pipe(Schema.brand(name));
}

export const TraceId = hexId(32, "TraceId");
export type TraceId = typeof TraceId.Type;
export const parseTraceId = Schema.decodeUnknownSync(TraceId);
export const SpanId = hexId(16, "SpanId");
export type SpanId = typeof SpanId.Type;
export const parseSpanId = Schema.decodeUnknownSync(SpanId);

const boundedId = <const Name extends string>(name: Name) =>
  Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(128),
    Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:-]*$/),
  ).pipe(Schema.brand(name));

export const InteractionId = boundedId("InteractionId");
export type InteractionId = typeof InteractionId.Type;
export const RequestId = boundedId("RequestId");
export type RequestId = typeof RequestId.Type;

export const TraceParent = Schema.Struct({
  version: Schema.Literal("00"),
  traceId: TraceId,
  parentId: SpanId,
  traceFlags: Schema.Literals(["00", "01"]),
});
export type TraceParent = typeof TraceParent.Type;

export const TraceState = Schema.String.check(
  Schema.isMaxLength(512),
  Schema.makeFilter(
    (value) =>
      (value.length > 0 &&
        value.split(",").length <= 32 &&
        new Set(value.split(",").map((member) => member.split("=", 1)[0])).size ===
          value.split(",").length &&
        value
          .split(",")
          .every((member) =>
            /^[a-z0-9][_0-9a-z*/-]{0,255}=[\x21-\x2B\x2D-\x3C\x3E-\x7E]{1,256}$/.test(member),
          )) ||
      "invalid tracestate",
  ),
).pipe(Schema.brand("TraceState"));
export type TraceState = typeof TraceState.Type;

export const TraceCarrier = Schema.Struct({
  traceparent: TraceParent,
  tracestate: Schema.optionalKey(TraceState),
});
export type TraceCarrier = typeof TraceCarrier.Type;
