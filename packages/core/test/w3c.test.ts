import { describe, expect, it } from "vitest";
import {
  extractTraceCarrier,
  formatTraceParent,
  parseTraceParentHeader,
  parseTraceStateHeader,
} from "../src/index.js";

const valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

describe("W3C trace context", () => {
  it("strictly parses and formats traceparent", () => {
    const parsed = parseTraceParentHeader(valid);
    expect(parsed.ok).toBe(true);
    if (parsed.ok) expect(formatTraceParent(parsed.value)).toBe(valid);
  });

  it.each([
    "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
    "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
    "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-02",
    `${valid}-extra`,
    ` ${valid}`,
    `${valid},${valid}`,
  ])("rejects invalid traceparent %s", (value) => {
    expect(parseTraceParentHeader(value).ok).toBe(false);
  });

  it("parses valid simple and tenant tracestate members", () => {
    expect(parseTraceStateHeader("vendor=value,tenant@system=opaque value").ok).toBe(true);
  });

  it.each([
    "vendor=a,vendor=b",
    "Vendor=value",
    " vendor=value",
    "vendor=value ",
    "vendor=a=b",
    "vendor=value, other=value",
    `${Array.from({ length: 33 }, (_, index) => `v${index}=x`).join(",")}`,
    `vendor=${"x".repeat(257)}`,
  ])("rejects invalid tracestate %s", (value) => {
    expect(parseTraceStateHeader(value).ok).toBe(false);
  });

  it("rejects an invalid tracestate for an otherwise valid carrier", () => {
    const headers = new Headers({ traceparent: valid, tracestate: "vendor=a,vendor=b" });
    expect(extractTraceCarrier(headers).ok).toBe(false);
  });
});
