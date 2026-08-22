import type { TraceCarrier, TraceParent, TraceState } from "./identifiers.js";
import { parseSpanId, parseTraceId, parseTraceState } from "./identifiers.js";

export type CarrierErrorReason =
  | "missing"
  | "multiple"
  | "length"
  | "version"
  | "format"
  | "trace-id"
  | "parent-id"
  | "flags"
  | "members"
  | "duplicate-key";

export type CarrierResult<A> =
  | { readonly ok: true; readonly value: A }
  | { readonly ok: false; readonly reason: CarrierErrorReason };

const traceParentPattern = /^00-([0-9a-f]{32})-([0-9a-f]{16})-(00|01)$/;
const simpleKey = /^[a-z][a-z0-9_*/-]{0,255}$/;
const tenantKey = /^[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13}$/;
const valuePattern = /^[\x20-\x2b\x2d-\x3c\x3e-\x7e]{1,256}$/;

export function parseTraceParentHeader(value: string | undefined): CarrierResult<TraceParent> {
  if (value === undefined || value.length === 0) return failure("missing");
  if (value.includes(",")) return failure("multiple");
  if (value.length !== 55) return failure("length");
  if (!value.startsWith("00-")) return failure("version");
  const match = traceParentPattern.exec(value);
  if (match === null) {
    const fields = value.split("-");
    if (fields.length !== 4) return failure("format");
    if (!/^[0-9a-f]{32}$/.test(fields[1] ?? "")) return failure("trace-id");
    if (!/^[0-9a-f]{16}$/.test(fields[2] ?? "")) return failure("parent-id");
    return failure("flags");
  }
  if (match[1] === "0".repeat(32)) return failure("trace-id");
  if (match[2] === "0".repeat(16)) return failure("parent-id");
  return {
    ok: true,
    value: {
      version: "00",
      traceId: parseTraceId(match[1]!),
      parentId: parseSpanId(match[2]!),
      traceFlags: match[3] === "01" ? "01" : "00",
    },
  };
}

export function formatTraceParent(parent: TraceParent): string {
  return `${parent.version}-${parent.traceId}-${parent.parentId}-${parent.traceFlags}`;
}

export function parseTraceStateHeader(value: string | undefined): CarrierResult<TraceState> {
  if (value === undefined || value.length === 0) return failure("missing");
  if (value.length > 512) return failure("length");
  const members = value.split(",");
  if (members.length > 32) return failure("members");
  const keys = new Set<string>();
  for (const member of members) {
    if (member.trim() !== member) return failure("format");
    const equals = member.indexOf("=");
    if (equals <= 0 || equals !== member.lastIndexOf("=")) return failure("format");
    const key = member.slice(0, equals);
    const stateValue = member.slice(equals + 1);
    if ((!simpleKey.test(key) && !tenantKey.test(key)) || !valuePattern.test(stateValue)) {
      return failure("format");
    }
    if (stateValue[0] === " " || stateValue.at(-1) === " ") return failure("format");
    if (keys.has(key)) return failure("duplicate-key");
    keys.add(key);
  }
  return { ok: true, value: parseTraceState(value) };
}

export function extractTraceCarrier(headers: Headers): CarrierResult<TraceCarrier> {
  const parent = parseTraceParentHeader(headers.get("traceparent") ?? undefined);
  if (!parent.ok) return parent;
  const stateValue = headers.get("tracestate");
  if (stateValue === null) return { ok: true, value: { traceparent: parent.value } };
  const state = parseTraceStateHeader(stateValue);
  return state.ok
    ? { ok: true, value: { traceparent: parent.value, tracestate: state.value } }
    : state;
}

export function injectTraceCarrier(headers: Headers, carrier: TraceCarrier): Headers {
  const output = new Headers(headers);
  output.set("traceparent", formatTraceParent(carrier.traceparent));
  if (carrier.tracestate === undefined) output.delete("tracestate");
  else output.set("tracestate", carrier.tracestate);
  return output;
}

function failure(reason: CarrierErrorReason): CarrierResult<never> {
  return { ok: false, reason };
}
