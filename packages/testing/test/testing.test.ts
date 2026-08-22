import { type FlowSpan, parseSpanId, parseTraceId } from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { describe, it } from "@effect/vitest";
import {
  CompletedSpanCollector,
  DeterministicIds,
  TraceAssertionError,
  assertRedacted,
  assertTraceTree,
  buildTraceForest,
  completedSpanCollectorLayer,
  deterministicIdsLayer,
  makeTestRequest,
  makeTestResponse,
} from "../src/index.js";

const traceId = parseTraceId("0123456789abcdef0123456789abcdef");
const parentId = parseSpanId("0123456789abcdef");
const childId = parseSpanId("1123456789abcdef");

function span(
  operation: string,
  spanId: typeof parentId,
  parentSpanId?: typeof parentId,
  attributes: FlowSpan["attributes"] = {},
): FlowSpan {
  return {
    schemaVersion: 1,
    traceId,
    spanId,
    ...(parentSpanId === undefined ? {} : { parentSpanId }),
    service: { name: "test", version: "1", environment: "test" },
    operation,
    kind: "internal",
    outcome: "success",
    startTime: "2026-08-22T00:00:00.000Z",
    endTime: "2026-08-22T00:00:00.001Z",
    durationMs: 1,
    attributes,
    events: [],
  };
}

describe("deterministic services", () => {
  const layer = Layer.mergeAll(deterministicIdsLayer({ seed: 10 }), completedSpanCollectorLayer);

  it.effect("generates stable IDs without shared module state", () =>
    Effect.gen(function* () {
      const ids = yield* DeterministicIds;
      expect(yield* ids.nextTraceId).toBe("0000000000000000000000000000000a");
      expect(yield* ids.nextSpanId).toBe("000000000000000b");
      expect(yield* ids.nextRequestId).toBe("test-request-12");
    }).pipe(Effect.provide(layer)),
  );

  it.effect("collects immutable snapshots and filters traces", () =>
    Effect.gen(function* () {
      const collector = yield* CompletedSpanCollector;
      yield* collector.collect(span("root", parentId));
      const snapshot = yield* collector.spans;
      yield* collector.collect(span("child", childId, parentId));
      expect(snapshot).toHaveLength(1);
      expect(yield* collector.spansForTrace(traceId)).toHaveLength(2);
      expect((yield* collector.find(childId))?.operation).toBe("child");
      yield* collector.clear;
      expect(yield* collector.spans).toEqual([]);
    }).pipe(Effect.provide(layer)),
  );
});

describe("trace assertions", () => {
  it("builds and checks ancestry", () => {
    const spans = [
      span("root", parentId, undefined, { route: "/test" }),
      span("child", childId, parentId),
    ];
    expect(buildTraceForest(spans)[0]?.children[0]?.span.operation).toBe("child");
    expect(() =>
      assertTraceTree(spans, {
        operation: "root",
        attributes: { route: "/test" },
        children: [{ operation: "child", outcome: "success" }],
      }),
    ).not.toThrow();
  });

  it("rejects duplicates, missing parents, and readable mismatches", () => {
    expect(() => buildTraceForest([span("one", parentId), span("two", parentId)])).toThrow(
      /duplicate span id/,
    );
    expect(() => buildTraceForest([span("orphan", childId, parentId)])).toThrow(/missing parent/);
    expect(() => assertTraceTree([span("actual", parentId)], { operation: "expected" })).toThrow(
      new TraceAssertionError("trace[0]: expected operation expected, received actual"),
    );
  });

  it("detects leaked values and permits explicit redaction markers", () => {
    expect(() =>
      assertRedacted([span("safe", parentId, undefined, { password: "[REDACTED]" })], {
        forbiddenValues: ["hunter2"],
      }),
    ).not.toThrow();
    expect(() =>
      assertRedacted([span("unsafe", parentId, undefined, { note: "contains hunter2" })], {
        forbiddenValues: ["hunter2"],
      }),
    ).toThrow(/forbidden value/);
  });
});

describe("HTTP fixtures", () => {
  it("uses platform Request and Response objects", () => {
    const request = makeTestRequest("https://api.example.test/runs", {
      method: "POST",
      headers: { traceparent: "test" },
      body: "{}",
    });
    const response = makeTestResponse(202, { headers: { "x-request-id": "req-1" } });
    expect(request).toBeInstanceOf(Request);
    expect(request.headers.get("traceparent")).toBe("test");
    expect(response).toBeInstanceOf(Response);
    expect(response.status).toBe(202);
  });
});
