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
export const parseInteractionId = Schema.decodeUnknownSync(InteractionId);
export const RequestId = boundedId("RequestId");
export type RequestId = typeof RequestId.Type;
export const parseRequestId = Schema.decodeUnknownSync(RequestId);

export const TraceParent = Schema.Struct({
  version: Schema.Literal("00"),
  traceId: TraceId,
  parentId: SpanId,
  traceFlags: Schema.Literals(["00", "01"]),
});
export type TraceParent = typeof TraceParent.Type;

export const TraceState = Schema.String.check(
  Schema.isMaxLength(512),
  Schema.makeFilter((value) => isTraceState(value) || "invalid tracestate"),
).pipe(Schema.brand("TraceState"));
export type TraceState = typeof TraceState.Type;
export const parseTraceState = Schema.decodeUnknownSync(TraceState);

function isTraceState(value: string): boolean {
  if (value.length === 0) return false;
  const members = value.split(",");
  if (members.length > 32) return false;
  const keys = new Set<string>();
  for (const member of members) {
    if (member.trim() !== member) return false;
    const equals = member.indexOf("=");
    if (equals <= 0 || equals !== member.lastIndexOf("=")) return false;
    const key = member.slice(0, equals);
    const memberValue = member.slice(equals + 1);
    const validKey =
      /^[a-z][a-z0-9_*/-]{0,255}$/.test(key) ||
      /^[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13}$/.test(key);
    if (
      !validKey ||
      !/^[\x20-\x2b\x2d-\x3c\x3e-\x7e]{1,256}$/.test(memberValue) ||
      memberValue[0] === " " ||
      memberValue.at(-1) === " " ||
      keys.has(key)
    )
      return false;
    keys.add(key);
  }
  return true;
}

export const TraceCarrier = Schema.Struct({
  traceparent: TraceParent,
  tracestate: Schema.optionalKey(TraceState),
});
export type TraceCarrier = typeof TraceCarrier.Type;
