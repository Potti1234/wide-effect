import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
  FlowSpan,
  DiagnosticBundle,
  MAX_DIAGNOSTIC_BUNDLE_BYTES,
  MAX_FLOW_SPAN_BYTES,
  SafeAttributes,
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
    const decode = Schema.decodeUnknownSync(TraceCarrier, {
      onExcessProperty: "error",
    });
    expect(
      decode({
        traceparent: {
          version: "00",
          traceId,
          parentId: spanId,
          traceFlags: "01",
        },
      }).traceparent.traceId,
    ).toBe(traceId);
    expect(() =>
      decode({
        traceparent: {
          version: "00",
          traceId,
          parentId: spanId,
          traceFlags: "01",
        },
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
    const decoded = Schema.decodeUnknownSync(FlowSpan, {
      onExcessProperty: "error",
    })(fixture);
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
    expect(normalizeValue(value)).toEqual({ visible: "yes" });
    expect(read).toBe(false);
    expect(
      normalizeValue({
        accessToken: "secret",
        apiKey: "secret",
        encrypted_value: "secret",
      }),
    ).toEqual({});
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
        {
          allowedKeys: new Set(["http.method"]),
          allowedPrefixes: ["fractal."],
        },
      ),
    ).toEqual({ "fractal.run.id": "run_1", "http.method": "GET" });
    expect(
      normalizeAttributes(
        { "url.full": "https://user:secret@example.test/?token=secret", "url.scheme": "https" },
        { allowedPrefixes: ["url."], deniedKeys: new Set(["url.full"]) },
      ),
    ).toEqual({ "url.scheme": "https" });
    expect(
      normalizeValue("abcdefgh", {
        maxBytes: 4,
        maxDepth: 2,
        maxEntries: 2,
        maxStringLength: 4,
      }),
    ).toBe("ab");
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

  it("enforces UTF-8 byte limits without emitting lone surrogates", () => {
    const limits = {
      maxBytes: 6,
      maxDepth: 2,
      maxEntries: 2,
      maxStringLength: 10,
    };
    const normalized = normalizeValue("😀😀", limits);
    expect(normalized).toBe("😀");
    expect(new TextEncoder().encode(JSON.stringify(normalized)).byteLength).toBeLessThanOrEqual(6);
    expect(normalizeValue("\ud800", limits)).toBe("");
    expect(() => normalizeValue("x", { ...limits, maxBytes: 1 })).toThrow(RangeError);
  });

  it("drops unsafe keys and rejects them at the schema boundary", () => {
    const secret = "never-expose-this";
    const normalized = normalizeAttributes(
      {
        [`fractal.password.${secret}`]: "value",
        "fractal.safe": "yes",
        "fractal.💣": "no",
        ["fractal." + "x".repeat(300)]: "no",
      },
      { allowedPrefixes: ["fractal."] },
    );
    expect(normalized).toEqual({ "fractal.safe": "yes" });
    expect(JSON.stringify(normalized)).not.toContain(secret);
    for (const key of ["authorization", `safe.token.${secret}`, "bad key", "x".repeat(257)]) {
      expect(() => Schema.decodeUnknownSync(SafeAttributes)({ [key]: "value" })).toThrow();
    }
    expect(() =>
      Schema.decodeUnknownSync(SafeAttributes)({
        safe: { nestedPassword: secret },
      }),
    ).toThrow();
  });

  it("handles hostile reflection, accessors, symbols, depth, and width", () => {
    let accessorRead = false;
    const accessor = Object.defineProperty({}, "visible", {
      enumerable: true,
      get() {
        accessorRead = true;
        return "secret";
      },
    });
    expect(normalizeValue(accessor)).toEqual({ visible: "[opaque:accessor]" });
    expect(accessorRead).toBe(false);
    const prototypeTrap = new Proxy(
      {},
      {
        getPrototypeOf: () => {
          throw new Error("secret");
        },
      },
    );
    const descriptorTrap = new Proxy(
      {},
      {
        ownKeys: () => ["visible"],
        getOwnPropertyDescriptor: () => {
          throw new Error("secret");
        },
      },
    );
    expect(normalizeValue(prototypeTrap)).toBe("[opaque:uninspectable]");
    expect(normalizeValue(descriptorTrap)).toBe("[opaque:uninspectable]");
    expect(normalizeValue({ visible: "yes", [Symbol("secret")]: "hidden" })).toEqual({
      visible: "yes",
    });
    expect(
      normalizeValue([1, 2, 3, 4], {
        maxBytes: 64,
        maxDepth: 2,
        maxEntries: 2,
        maxStringLength: 10,
      }),
    ).toEqual([1, 2]);
    expect(
      normalizeValue(
        { a: { b: { c: "hidden" } } },
        {
          maxBytes: 64,
          maxDepth: 2,
          maxEntries: 10,
          maxStringLength: 20,
        },
      ),
    ).toEqual({ a: { b: "[opaque:max-depth]" } });
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
    expect(
      decideSampling(
        { ...config, successRatio: 1 },
        {
          operation: "Read",
          outcome: "success",
          durationMs: 1,
          deterministicValue: 1,
        },
      ),
    ).toEqual({ keep: true, reason: "sampled" });
  });
});

describe("encoded envelope limits and temporal consistency", () => {
  const fixture = {
    schemaVersion: 1 as const,
    traceId,
    spanId,
    service: { name: "api", version: "1", environment: "test" },
    operation: "Example.read",
    kind: "server" as const,
    outcome: "success" as const,
    startTime: "2026-08-22T20:00:00.000Z",
    endTime: "2026-08-22T20:00:00.010Z",
    durationMs: 10,
    attributes: {},
    events: [],
  };

  it("requires canonical timestamps and consistent duration", () => {
    const decode = Schema.decodeUnknownSync(FlowSpan);
    expect(decode({ ...fixture, durationMs: 10.5 }).durationMs).toBe(10.5);
    for (const invalid of [
      { ...fixture, startTime: "2026-08-22T20:00:00Z" },
      { ...fixture, startTime: "2026-02-30T20:00:00.000Z" },
      { ...fixture, endTime: "2026-08-22T19:59:59.999Z", durationMs: 0 },
      { ...fixture, durationMs: 12 },
    ]) {
      expect(() => decode(invalid)).toThrow();
    }
  });

  it("rejects envelopes that exceed their total encoded limits", () => {
    const largeAttributes = Object.fromEntries(
      Array.from({ length: 40 }, (_, index) => [`safe.field${index}`, "x".repeat(32_000)]),
    );
    expect(
      new TextEncoder().encode(JSON.stringify({ ...fixture, attributes: largeAttributes }))
        .byteLength,
    ).toBeGreaterThan(MAX_FLOW_SPAN_BYTES);
    expect(() =>
      Schema.decodeUnknownSync(FlowSpan)({
        ...fixture,
        attributes: largeAttributes,
      }),
    ).toThrow();

    const entry = {
      timestamp: fixture.startTime,
      operation: "Example.read",
      summary: "x".repeat(2_048),
      attributes: Object.fromEntries(
        Array.from({ length: 32 }, (_, index) => [`safe.field${index}`, "x".repeat(1_000)]),
      ),
    };
    const bundle = {
      schemaVersion: 1,
      traceId,
      summary: { title: "failure", outcome: "failure" },
      timeline: Array.from({ length: 40 }, () => entry),
      context: {},
      evidence: [],
    };
    expect(new TextEncoder().encode(JSON.stringify(bundle)).byteLength).toBeGreaterThan(
      MAX_DIAGNOSTIC_BUNDLE_BYTES,
    );
    expect(() => Schema.decodeUnknownSync(DiagnosticBundle)(bundle)).toThrow();
  });
});
