import { parseSegmentId, parseSpanId, parseTraceId, type FlowSpan } from "@wide-effect/core";
import { Clock, Effect } from "effect";
import {
  CompletedSpanSink,
  Delivery,
  deliveryLayer,
  exporterError,
  type DeliveryHealthSnapshot,
  type SpanExporter,
} from "@wide-effect/effect";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { mapClickHouseRow, type ClickHouseSpanRow } from "../src/mapping.js";
import { makeAlertPoller } from "../src/poller.js";
import { buildTriageEvidence } from "../src/triage.js";

const project = "wide-effect-clickhouse-proof";
const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
const compose = resolve(root, "docker-compose.yml");
const endpoint = "http://127.0.0.1:18123/";
const headers = {
  "x-clickhouse-user": "wide_effect",
  "x-clickhouse-key": "local-only-wide-effect",
};

try {
  await docker("up", "-d", "--wait");
  await query(await Bun.file(resolve(root, "schema.sql")).text());
  const businessResult = "business-ok";
  await docker("stop", "clickhouse");
  let outageObserved = false;
  try {
    await query("SELECT 1");
  } catch {
    outageObserved = true;
  }
  if (!outageObserved || businessResult !== "business-ok")
    throw new Error("outage isolation failed");
  await docker("start", "clickhouse");
  await waitForClickHouse();

  const rows = await Effect.runPromise(makeRows());
  const trigger = rows[0];
  if (trigger === undefined) throw new Error("missing trigger row");
  await insert([...rows, trigger]);
  const physicalRows = Number(await query("SELECT count() FROM wide_effect_spans"));
  const distinctSpanIdentities = Number(
    await query("SELECT uniqExact(tuple(trace_id, span_id)) FROM wide_effect_spans"),
  );

  let tickets = 0;
  const poller = await Effect.runPromise(
    makeAlertPoller(
      {
        id: "returned.400",
        service: "api",
        operations: ["http.server POST /jobs"],
        statusCodes: [400],
        minimumOccurrences: 1,
        windowSeconds: 60,
        cooldownSeconds: 300,
      },
      { read: () => Effect.succeed([...rows, trigger]) },
      () => Effect.sync(() => tickets++),
      { lookbackMs: 5_000, retryDelayMs: 10, maxAttempts: 3, now: Clock.currentTimeMillis },
    ),
  );
  await Effect.runPromise(poller.pollOnce);
  await Effect.runPromise(poller.pollOnce);
  if (tickets !== 1) throw new Error(`expected one ticket, received ${tickets}`);

  const triage = await Effect.runPromise(
    buildTriageEvidence(
      trigger,
      rows,
      { repository: "local/wide-effect", commit: "abc123", symbols: ["submitJob"] },
      { fetch: () => Effect.succeed([{ filename: "src/jobs.ts", symbol: "submitJob" }]) },
      { maxSpans: 16, maxBytes: 200_000, windowMs: 60_000 },
    ),
  );
  const deliveryHealth = await deliveryRecoveryEvidence();
  const recentStatusPlan = await query(
    "EXPLAIN indexes = 1 SELECT count() FROM wide_effect_spans WHERE service_name = 'api' AND start_time >= toDateTime64('2026-09-01 00:00:00', 3, 'UTC') AND http_status = 400",
  );
  const traceLookupPlan = await query(
    `EXPLAIN indexes = 1 SELECT * FROM wide_effect_spans WHERE trace_id = '${trigger.trace_id}'`,
  );
  console.log(
    JSON.stringify(
      {
        physicalRows,
        distinctSpanIdentities,
        tickets,
        outageObserved,
        sampleTicket: triage.ticket,
        deliveryOutageHealth: deliveryHealth.outage,
        deliveryRecoveryHealth: deliveryHealth.recovery,
        recentStatusPlan: recentStatusPlan.split("\n").filter(Boolean),
        traceLookupPlan: traceLookupPlan.split("\n").filter(Boolean),
      },
      null,
      2,
    ),
  );
} finally {
  await docker("down", "--volumes", "--remove-orphans");
}

async function deliveryRecoveryEvidence(): Promise<{
  readonly outage: DeliveryHealthSnapshot;
  readonly recovery: DeliveryHealthSnapshot;
}> {
  let available = false;
  const exporter: SpanExporter = {
    name: "clickhouse-proof",
    export: () =>
      available
        ? Effect.void
        : Effect.fail(exporterError("clickhouse-proof", "export", "fixture unavailable")),
    flush: Effect.void,
    shutdown: Effect.void,
  };
  return Effect.runPromise(
    Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer(flowSpan(6, "proof.outage", 400));
      yield* delivery.flush;
      const outage = yield* delivery.health;
      available = true;
      sink.unsafeOffer(flowSpan(7, "proof.recovery", 400));
      yield* delivery.flush;
      const recovery = yield* delivery.health;
      yield* delivery.shutdown;
      return { outage, recovery };
    }).pipe(
      Effect.provide(
        deliveryLayer({
          exporters: [exporter],
          capacity: 16,
          overflow: "drop-newest",
          maxBatchSize: 16,
          maxBatchAge: "10 millis",
          attemptTimeout: "1 second",
          shutdownTimeout: "1 second",
          retry: { maxAttempts: 1, delay: 0, jitter: 0 },
          circuit: { failureThreshold: 1, openDuration: 0, probeTimeout: "1 second" },
          sampling: {
            successRatio: 1,
            slowThresholdMs: 1_000,
            retainOutcomes: ["failure", "defect", "interrupted"],
            importantOperations: [],
          },
        }),
      ),
    ),
  );
}

async function docker(...args: ReadonlyArray<string>): Promise<void> {
  const child = Bun.spawn(["docker", "compose", "-p", project, "-f", compose, ...args], {
    stdout: "inherit",
    stderr: "inherit",
  });
  if ((await child.exited) !== 0) throw new Error(`docker compose ${args[0] ?? "command"} failed`);
}

async function query(sql: string): Promise<string> {
  const url = new URL(endpoint);
  const response = await fetch(url, { method: "POST", headers, body: sql });
  if (!response.ok) throw new Error(`ClickHouse query failed with status ${response.status}`);
  return (await response.text()).trim();
}

async function insert(rows: ReadonlyArray<ClickHouseSpanRow>): Promise<void> {
  const url = new URL(endpoint);
  url.searchParams.set("query", "INSERT INTO wide_effect_spans FORMAT JSONEachRow");
  url.searchParams.set("date_time_input_format", "best_effort");
  const response = await fetch(url, {
    method: "POST",
    headers,
    body: `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`,
  });
  if (!response.ok) throw new Error(`ClickHouse insert failed with status ${response.status}`);
}

async function waitForClickHouse(): Promise<void> {
  for (let attempt = 0; attempt < 60; attempt++) {
    try {
      if ((await query("SELECT 1")) === "1") return;
    } catch {
      await Bun.sleep(500);
    }
  }
  throw new Error("ClickHouse did not recover within 30 seconds");
}

function makeRows() {
  return Effect.forEach(
    [
      flowSpan(1, "http.server POST /jobs", 400),
      flowSpan(2, "job.validate", undefined, { failure: true }),
      flowSpan(3, "job.slow", 200, { durationMs: 2_000 }),
      flowSpan(4, "job.routine", 200),
      flowSpan(5, "job.late", 400, { late: true }),
    ],
    (span, index) => mapClickHouseRow(span, `2026-09-01T00:00:01.00${index}Z`),
  );
}

function flowSpan(
  value: number,
  operation: string,
  status?: number,
  options: {
    readonly failure?: boolean;
    readonly durationMs?: number;
    readonly late?: boolean;
  } = {},
): FlowSpan {
  const durationMs = options.durationMs ?? 10;
  const start = Date.parse("2026-09-01T00:00:00.000Z") + value;
  return {
    schemaVersion: 1,
    traceId: parseTraceId("1".repeat(32)),
    spanId: parseSpanId(value.toString().repeat(16)),
    segmentId: parseSegmentId("a".repeat(16)),
    service: {
      name: "api",
      version: "1.0.0",
      environment: "local",
      deploymentId: "proof-1",
      commitHash: "abc123",
      region: "local",
      instanceId: "api-1",
    },
    operation,
    kind: "server",
    outcome: options.failure ? "failure" : "success",
    ...(options.failure
      ? { failure: { type: "typed" as const, category: "validation", code: "job.invalid" } }
      : {}),
    startTime: new Date(start).toISOString(),
    endTime: new Date(start + durationMs).toISOString(),
    durationMs,
    attributes: {
      ...(status === undefined ? {} : { "http.response.status_code": status }),
      ...(options.late ? { "wide.segment.late": true } : {}),
    },
    events: [],
  };
}
