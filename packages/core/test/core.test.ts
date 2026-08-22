import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
  FlowSpan,
  SafeValue,
  TraceCarrier,
  decideSampling,
  normalizeAttributes,
  normalizeValue,
  parseSpanId,
  parseTraceId,
} from "../src/index.js";

const traceId = "0123456789abcdef0123456789abcdef";
const spanId = "0123456789abcdef";

describe("boundary contracts", () => {
  it("validates identifiers and strict carriers", () => {
    expect(parseTraceId(traceId)).toBe(traceId);
    expect(parseSpanId(spanId)).toBe(spanId);
    for (const invalid of ["0".repeat(32), traceId.toUpperCase(), "short"]) {
      expect(() => parseTraceId(invalid)).toThrow();
    }
    const decode = Schema.decodeUnknownSync(TraceCarrier, { onExcessProperty: "error" });
    expect(
      decode({ traceparent: { version: "00", traceId, parentId: spanId, traceFlags: "01" } })
        .traceparent.traceId,
    ).toBe(traceId);
    expect(() =>
      decode({
        traceparent: { version: "00", traceId, parentId: spanId, traceFlags: "01" },
        baggage: "secret=value",
      }),
    ).toThrow();
  });

  it("round trips the golden FlowSpan and rejects schema drift", () => {
    const fixture = {
      schemaVersion: 1,
      traceId,
      spanId,
      service: { name: "example-api", version: "1.0.0", environment: "test" },
      operation: "Example.read",
      kind: "server",
      outcome: "success",
      startTime: "2026-08-22T20:00:00.000Z",
      endTime: "2026-08-22T20:00:00.010Z",
      durationMs: 10,
      attributes: { "http.method": "GET" },
      events: [],
    };
    const decoded = Schema.decodeUnknownSync(FlowSpan, { onExcessProperty: "error" })(fixture);
    expect(Schema.encodeSync(FlowSpan)(decoded)).toEqual(fixture);
    expect(() =>
      Schema.decodeUnknownSync(FlowSpan, { onExcessProperty: "error" })({
        ...fixture,
        schemaVersion: 2,
      }),
    ).toThrow();
  });
});

describe("safe normalization", () => {
  it("redacts sensitive accessors without evaluating them", () => {
    let read = false;
    const value = Object.defineProperty({ visible: "yes" }, "password", {
      enumerable: true,
      get() {
        read = true;
        return "never expose";
      },
    });
    expect(normalizeValue(value)).toEqual({ password: "[REDACTED]", visible: "yes" });
    expect(read).toBe(false);
    expect(
      normalizeValue({ accessToken: "secret", apiKey: "secret", encrypted_value: "secret" }),
    ).toEqual({
      accessToken: "[REDACTED]",
      apiKey: "[REDACTED]",
      encrypted_value: "[REDACTED]",
    });
  });

  it("makes cycles, unsupported values, accessors, and hostile proxies opaque", () => {
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    expect(normalizeValue(cycle)).toEqual({ self: "[opaque:cycle]" });
    expect(normalizeValue(new Date())).toBe("[opaque:object]");
    expect(normalizeValue(undefined)).toBe("[opaque:undefined]");
    expect(normalizeValue(1n)).toBe("[opaque:bigint]");
    expect(normalizeValue(Number.NaN)).toBe("[opaque:number]");
    const hostile = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error("hidden-secret");
        },
      },
    );
    expect(normalizeValue(hostile)).toBe("[opaque:uninspectable]");
  });

  it("bounds values and requires registered application fields", () => {
    expect(
      normalizeAttributes(
        { "fractal.run.id": "run_1", "http.method": "GET", unknown: "drop" },
        { allowedKeys: new Set(["http.method"]), allowedPrefixes: ["fractal."] },
      ),
    ).toEqual({ "fractal.run.id": "run_1", "http.method": "GET" });
    expect(
      normalizeValue("abcdefgh", {
        maxBytes: 4,
        maxDepth: 2,
        maxEntries: 2,
        maxStringLength: 4,
      }),
    ).toBe("abcd");
    expect(
      new TextEncoder().encode(
        JSON.stringify(
          normalizeValue(
            { ["k".repeat(100)]: "value" },
            { maxBytes: 16, maxDepth: 2, maxEntries: 2, maxStringLength: 16 },
          ),
        ),
      ).byteLength,
    ).toBeLessThanOrEqual(16);
    expect(() => Schema.decodeUnknownSync(SafeValue)("x".repeat(2_049))).toThrow();
    expect(() => Schema.decodeUnknownSync(SafeValue)(Number.POSITIVE_INFINITY)).toThrow();
  });
});

describe("sampling", () => {
  it("is deterministic and prioritizes retention rules", () => {
    const config = {
      successRatio: 0.1,
      slowThresholdMs: 1_000,
      retainOutcomes: ["failure" as const],
      importantOperations: ["Billing.charge"],
    };
    expect(
      decideSampling(config, {
        operation: "Read",
        outcome: "failure",
        durationMs: 1,
        deterministicValue: 1,
      }).reason,
    ).toBe("outcome");
    expect(
      decideSampling(config, {
        operation: "Read",
        outcome: "success",
        durationMs: 1,
        deterministicValue: 0.2,
      }),
    ).toEqual({ keep: false, reason: "dropped" });
  });
});
