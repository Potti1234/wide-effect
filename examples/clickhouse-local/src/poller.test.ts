import { expect } from "@effect/vitest";
import { Effect } from "effect";
import { TestClock } from "effect/testing";
import { describe, it } from "@effect/vitest";
import type { ClickHouseSpanRow } from "./mapping.js";
import { makeAlertPoller } from "./poller.js";

const rule = {
  id: "returned.400",
  service: "api",
  statusCodes: [400],
  minimumOccurrences: 1,
  windowSeconds: 60,
  cooldownSeconds: 300,
};

describe("cursor alert poller", () => {
  it.effect("claims duplicate 400 rows once across repeated and concurrent polls", () => {
    const row = fixtureRow();
    let tickets = 0;
    return Effect.gen(function* () {
      const poller = yield* makeAlertPoller(
        rule,
        { read: () => Effect.succeed([row, row]) },
        () => Effect.sync(() => tickets++),
        pollerOptions(),
      );
      yield* Effect.all([poller.pollOnce, poller.pollOnce], { concurrency: "unbounded" });
      yield* poller.pollOnce;
      expect(tickets).toBe(1);
      expect(yield* poller.incidents).toEqual([
        expect.objectContaining({ state: "ticketed", attempts: 1 }),
      ]);
      expect(yield* poller.cursor).toEqual({ ingestedAt: row.ingested_at, spanId: row.span_id });
    });
  });

  it.effect("discovers a late lookback row and retries with TestClock", () => {
    const row = fixtureRow();
    let reads = 0;
    let attempts = 0;
    return Effect.gen(function* () {
      const poller = yield* makeAlertPoller(
        rule,
        {
          read: (_cursor, lookbackMs) => {
            reads++;
            expect(lookbackMs).toBe(5_000);
            return Effect.succeed(reads === 1 ? [] : [row]);
          },
        },
        () => {
          attempts++;
          return attempts === 1 ? Effect.fail("temporary") : Effect.void;
        },
        pollerOptions(),
      );
      yield* poller.pollOnce;
      yield* poller.pollOnce;
      expect((yield* poller.incidents)[0]?.state).toBe("retry_wait");
      yield* TestClock.adjust("10 millis");
      yield* poller.pollOnce;
      expect(attempts).toBe(2);
      expect((yield* poller.incidents)[0]?.state).toBe("ticketed");
    });
  });

  it.effect("enforces occurrence windows and cooldown for new incidents", () => {
    const first = fixtureRow();
    const second = {
      ...fixtureRow(),
      span_id: "3".repeat(16),
      ingested_at: "2026-09-01T00:00:02.000Z",
    };
    let rows: ReadonlyArray<ClickHouseSpanRow> = [first];
    let tickets = 0;
    return Effect.gen(function* () {
      const poller = yield* makeAlertPoller(
        { ...rule, minimumOccurrences: 2, cooldownSeconds: 1 },
        { read: () => Effect.succeed(rows) },
        () => Effect.sync(() => tickets++),
        pollerOptions(),
      );
      yield* poller.pollOnce;
      expect(tickets).toBe(0);
      rows = [first, second];
      yield* poller.pollOnce;
      expect(tickets).toBe(1);
      const third = {
        ...second,
        span_id: "4".repeat(16),
        ingested_at: "2026-09-01T00:00:03.000Z",
      };
      rows = [second, third];
      yield* poller.pollOnce;
      expect(tickets).toBe(1);
      yield* TestClock.adjust("1 second");
      yield* poller.pollOnce;
      expect(tickets).toBe(2);
    });
  });
});

function pollerOptions() {
  return {
    lookbackMs: 5_000,
    retryDelayMs: 10,
    maxAttempts: 3,
    now: Effect.clockWith((clock) => clock.currentTimeMillis),
  };
}

function fixtureRow(): ClickHouseSpanRow {
  return {
    schema_version: 1,
    trace_id: "1".repeat(32),
    span_id: "2".repeat(16),
    parent_span_id: null,
    segment_id: "2".repeat(16),
    service_name: "api",
    service_version: "1",
    environment: "test",
    deployment_id: null,
    commit_hash: null,
    region: null,
    instance_id: null,
    operation: "http.server POST /jobs",
    kind: "server",
    outcome: "success",
    start_time: "2026-09-01T00:00:00.000Z",
    end_time: "2026-09-01T00:00:00.010Z",
    duration_ms: 10,
    http_method: "POST",
    http_route: "/jobs",
    http_status: 400,
    failure_type: null,
    failure_category: null,
    failure_code: null,
    failure_retryable: null,
    interaction_id: null,
    request_id: null,
    ingested_at: "2026-09-01T00:00:01.000Z",
    canonical_envelope: "{}",
  };
}
