import {
  decodeFlowSpan,
  parseInteractionId,
  parseRequestId,
  parseSegmentId,
  parseSpanId,
  parseTraceId,
  type FlowSpan,
} from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Effect } from "effect";
import { describe, it } from "@effect/vitest";
import { mapClickHouseRow } from "./mapping.js";

const span: FlowSpan = {
  schemaVersion: 1,
  traceId: parseTraceId("1".repeat(32)),
  spanId: parseSpanId("2".repeat(16)),
  segmentId: parseSegmentId("3".repeat(16)),
  parentSpanId: parseSpanId("4".repeat(16)),
  interactionId: parseInteractionId("interaction-1"),
  requestId: parseRequestId("request-1"),
  service: {
    name: "api",
    version: "1.2.3",
    environment: "test",
    deploymentId: "deploy-1",
    commitHash: "abc123",
    region: "eu-central-1",
    instanceId: "api-1",
  },
  operation: "http.server POST /jobs",
  kind: "server",
  outcome: "success",
  failure: { type: "http", category: "client", code: "job.invalid", retryable: false },
  startTime: "2026-09-01T00:00:00.000Z",
  endTime: "2026-09-01T00:00:00.010Z",
  durationMs: 10,
  attributes: {
    "http.request.method": "POST",
    "http.route": "/jobs",
    "http.response.status_code": 400,
  },
  events: [],
};

describe("ClickHouse span mapping", () => {
  it.effect("maps typed status, failure, deployment, and correlation evidence", () =>
    Effect.gen(function* () {
      const row = yield* mapClickHouseRow(span, "2026-09-01T00:00:01.000Z");
      expect(row).toMatchObject({
        trace_id: span.traceId,
        segment_id: span.segmentId,
        deployment_id: "deploy-1",
        commit_hash: "abc123",
        http_status: 400,
        failure_code: "job.invalid",
        interaction_id: "interaction-1",
        request_id: "request-1",
      });
      expect(decodeFlowSpan(JSON.parse(row.canonical_envelope))).toEqual(span);
    }),
  );

  it.effect("rejects forged input and never creates an unsanitized raw payload", () =>
    Effect.gen(function* () {
      const forged = { ...span, attributes: { authorization: "Bearer never-store" } } as FlowSpan;
      yield* Effect.flip(mapClickHouseRow(forged, "2026-09-01T00:00:01.000Z"));
      expect(JSON.stringify(span)).not.toContain("never-store");
    }),
  );
});
