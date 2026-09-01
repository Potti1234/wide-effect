import {
  parseSegmentId,
  parseSpanId,
  parseTraceId,
  type FlowSpan,
  type SamplingConfig,
  type SegmentStagingConfig,
} from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { describe, it } from "@effect/vitest";
import {
  CompletedSpanSink,
  Delivery,
  deliveryLayer,
  exporterError,
  type DeliveryOptions,
  type SpanBatch,
  type SpanExporter,
} from "../src/index.js";

const keepAll: SamplingConfig = {
  successRatio: 1,
  slowThresholdMs: 1_000,
  retainOutcomes: ["failure", "defect", "interrupted"],
  importantOperations: [],
};

function options(
  exporters: ReadonlyArray<SpanExporter>,
  overrides: Partial<DeliveryOptions> = {},
): DeliveryOptions {
  return {
    exporters,
    capacity: 16,
    overflow: "drop-newest",
    maxBatchSize: 10,
    maxBatchAge: "100 millis",
    attemptTimeout: "1 second",
    shutdownTimeout: "1 second",
    retry: { maxAttempts: 1, delay: 0, jitter: 0 },
    sampling: keepAll,
    ...overrides,
  };
}

function memoryExporter(name = "memory") {
  const batches: Array<SpanBatch> = [];
  const exporter: SpanExporter = {
    name,
    export: (batch) =>
      Effect.sync(() => {
        batches.push(batch);
      }),
    flush: Effect.void,
    shutdown: Effect.void,
  };
  return { batches, exporter };
}

describe("bounded delivery", () => {
  it.effect("flushes a partial batch at its configured age", () => {
    const output = memoryExporter();
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      sink.unsafeOffer(span(1, "aged"));
      yield* Effect.yieldNow;
      yield* TestClock.adjust("100 millis");
      yield* Effect.yieldNow;
      expect(output.batches.flat()).toHaveLength(1);
    }).pipe(Effect.provide(deliveryLayer(options([output.exporter]))));
  });

  it.effect("never blocks admission and records drop-newest overflow", () => {
    const output = memoryExporter();
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      for (let index = 1; index <= 5; index++) sink.unsafeOffer(span(index, `span-${index}`));
      const before = yield* delivery.health;
      expect(before.accepted).toBe(2);
      expect(before.overflowed).toBe(3);
      expect(before.dropped).toBe(3);
      yield* delivery.flush;
      expect(output.batches.flat().map((item) => item.operation)).toEqual(["span-1", "span-2"]);
    }).pipe(
      Effect.provide(deliveryLayer(options([output.exporter], { capacity: 2, maxBatchSize: 2 }))),
    );
  });

  it.effect("tail-samples deterministically after completion", () => {
    const output = memoryExporter();
    const sampling: SamplingConfig = {
      successRatio: 0,
      slowThresholdMs: 50,
      retainOutcomes: ["failure"],
      importantOperations: ["important"],
      retainHttpStatuses: [400],
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer(span(1, "failure", { outcome: "failure" }));
      sink.unsafeOffer(span(2, "slow", { durationMs: 50 }));
      sink.unsafeOffer(span(3, "important"));
      sink.unsafeOffer(span(4, "routine"));
      sink.unsafeOffer({
        ...span(5, "returned-400"),
        attributes: { "http.response.status_code": 400 },
      });
      yield* delivery.flush;
      expect(output.batches.flat().map((item) => item.operation)).toEqual([
        "failure",
        "slow",
        "important",
        "returned-400",
      ]);
      const health = yield* delivery.health;
      expect(health.sampled).toBe(4);
      expect(health.dropped).toBe(1);
    }).pipe(Effect.provide(deliveryLayer(options([output.exporter], { sampling }))));
  });

  it.effect("keeps or drops each completed local segment coherently", () => {
    const output = memoryExporter();
    const sampling: SamplingConfig = {
      successRatio: 0,
      slowThresholdMs: 1_000,
      retainOutcomes: ["failure"],
      importantOperations: [],
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      const kept = localSegment(10, [
        { operation: "failed-child", outcome: "failure" },
        { operation: "sibling", outcome: "success" },
        { operation: "root", outcome: "success", root: true },
      ]);
      const dropped = localSegment(20, [
        { operation: "routine-child", outcome: "success" },
        { operation: "routine-root", outcome: "success", root: true },
      ]);
      for (const item of [...kept, ...dropped]) sink.unsafeOffer(item);
      yield* delivery.flush;
      expect(output.batches).toHaveLength(1);
      expect(output.batches[0]?.map((item) => item.operation)).toEqual([
        "failed-child",
        "sibling",
        "root",
      ]);
      expect((yield* delivery.health).dropped).toBe(2);
    }).pipe(Effect.provide(deliveryLayer(options([output.exporter], { sampling }))));
  });

  it.effect("retains a complete returned-400 segment identically across exporter fan-out", () => {
    const left = memoryExporter("left");
    const right = memoryExporter("right");
    const sampling: SamplingConfig = {
      successRatio: 0,
      slowThresholdMs: 1_000,
      retainOutcomes: [],
      importantOperations: [],
      retainHttpStatuses: [400],
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const segment = localSegment(25, [
        { operation: "status-child", outcome: "success" },
        { operation: "status-sibling", outcome: "success" },
        { operation: "status-root", outcome: "success", root: true },
      ]);
      const statusChild = segment[0];
      if (statusChild === undefined) return;
      segment[0] = {
        ...statusChild,
        attributes: { "http.response.status_code": 400 },
      };
      for (const item of segment) sink.unsafeOffer(item);
      const delivery = yield* Delivery;
      yield* delivery.flush;
      expect(left.batches).toEqual(right.batches);
      expect(left.batches[0]?.map((item) => item.operation)).toEqual([
        "status-child",
        "status-sibling",
        "status-root",
      ]);
    }).pipe(Effect.provide(deliveryLayer(options([left.exporter, right.exporter], { sampling }))));
  });

  it.effect("caches segment decisions for detached late children", () => {
    const output = memoryExporter();
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      const [root] = localSegment(30, [{ operation: "kept-root", outcome: "success", root: true }]);
      if (root === undefined) return;
      sink.unsafeOffer(root);
      yield* delivery.flush;
      sink.unsafeOffer({
        ...span(31, "late-child"),
        traceId: root.traceId,
        segmentId: root.segmentId,
        parentSpanId: root.spanId,
      });
      yield* delivery.flush;
      expect(output.batches).toHaveLength(2);
      expect(output.batches[1]?.[0]?.attributes["wide.segment.late"]).toBe(true);
      expect((yield* delivery.health).lateSpans).toBe(1);
    }).pipe(Effect.provide(deliveryLayer(options([output.exporter]))));
  });

  it.effect("keeps same-trace local segments separate and bounds open segments", () => {
    const output = memoryExporter();
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const first = localSegment(32, [
        { operation: "first-root", outcome: "success", root: true },
      ])[0];
      const second = localSegment(33, [
        { operation: "second-root", outcome: "success", root: true },
      ])[0];
      if (first === undefined || second === undefined) return;
      sink.unsafeOffer(first);
      sink.unsafeOffer({ ...second, traceId: first.traceId });
      const delivery = yield* Delivery;
      yield* delivery.flush;
      expect(output.batches.flat().map((item) => item.operation)).toEqual(["first-root"]);
      const health = yield* delivery.health;
      expect(health.segmentDrops).toBe(1);
      expect(health.dropped).toBe(1);
    }).pipe(
      Effect.provide(
        deliveryLayer(
          options([output.exporter], {
            staging: staging({ maxOpenSegments: 1, overflow: "drop-newest" }),
          }),
        ),
      ),
    );
  });

  it.effect("retains a bounded critical subset when a segment exceeds its span limit", () => {
    const output = memoryExporter();
    const sampling: SamplingConfig = {
      successRatio: 0,
      slowThresholdMs: 1_000,
      retainOutcomes: ["failure"],
      importantOperations: [],
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      const segment = localSegment(40, [
        { operation: "failed", outcome: "failure" },
        { operation: "routine", outcome: "success" },
        { operation: "bounded-root", outcome: "success", root: true },
      ]);
      for (const item of segment) sink.unsafeOffer(item);
      yield* delivery.flush;
      expect(output.batches[0]?.map((item) => item.operation)).toEqual(["failed", "bounded-root"]);
      const root = output.batches[0]?.find((item) => item.operation === "bounded-root");
      expect(root?.attributes["wide.segment.dropped_spans"]).toBe(1);
      expect((yield* delivery.health).dropped).toBe(1);
    }).pipe(
      Effect.provide(
        deliveryLayer(
          options([output.exporter], {
            sampling,
            staging: staging({ maxSpansPerSegment: 2 }),
          }),
        ),
      ),
    );
  });

  it.effect("finalizes a missing-root segment at its hard maximum age", () => {
    const output = memoryExporter();
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      sink.unsafeOffer({
        ...span(50, "missing-root-child", { outcome: "failure" }),
        segmentId: parseSegmentId("f".repeat(16)),
        parentSpanId: parseSpanId("e".repeat(16)),
      });
      yield* Effect.yieldNow;
      yield* TestClock.adjust("10 millis");
      yield* Effect.yieldNow;
      expect(output.batches.flat().map((item) => item.operation)).toEqual(["missing-root-child"]);
    }).pipe(
      Effect.provide(
        deliveryLayer(
          options([output.exporter], {
            staging: staging({ maxSegmentAgeMs: 10 }),
          }),
        ),
      ),
    );
  });

  it.effect("maps trace identifiers across the full deterministic sampling range", () => {
    const output = memoryExporter();
    const sampling: SamplingConfig = {
      successRatio: 0.75,
      slowThresholdMs: 1_000,
      retainOutcomes: [],
      importantOperations: [],
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer({
        ...span(1, "high-ratio"),
        traceId: parseTraceId("ffffffffffffffffffffffffffffffff"),
      });
      yield* delivery.flush;
      expect(output.batches).toHaveLength(0);
      expect((yield* delivery.health).dropped).toBe(1);
    }).pipe(
      Effect.provide(
        deliveryLayer(options([output.exporter], { sampling, deterministicValue: () => 1 })),
      ),
    );
  });

  it.effect("retries on a deterministic TestClock schedule", () => {
    let attempts = 0;
    const exporter: SpanExporter = {
      name: "retry",
      export: () =>
        Effect.suspend(() => {
          attempts++;
          return attempts < 3
            ? Effect.fail(exporterError("retry", "export", "retryable"))
            : Effect.void;
        }),
      flush: Effect.void,
      shutdown: Effect.void,
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer(span(1, "retry"));
      const flushing = yield* delivery.flush.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("10 millis");
      yield* Effect.yieldNow;
      yield* TestClock.adjust("10 millis");
      yield* Fiber.join(flushing);
      expect(attempts).toBe(3);
      expect((yield* delivery.health).retried).toBe(2);
    }).pipe(
      Effect.provide(
        deliveryLayer(
          options([exporter], {
            retry: { maxAttempts: 3, delay: "10 millis", jitter: 0 },
          }),
        ),
      ),
    );
  });

  it.effect("isolates a malicious exporter from healthy fan-out", () => {
    const output = memoryExporter("healthy");
    const malicious: SpanExporter = {
      name: "malicious",
      export: () => {
        throw new Error("secret payload");
      },
      flush: Effect.die("flush defect"),
      shutdown: Effect.interrupt,
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer(span(1, "fanout"));
      yield* delivery.flush;
      expect(output.batches.flat()).toHaveLength(1);
      expect((yield* delivery.health).exporterFailures).toBeGreaterThanOrEqual(1);
    }).pipe(Effect.provide(deliveryLayer(options([malicious, output.exporter]))));
  });

  it.effect("opens a lifetime circuit after one hung exporter attempt", () => {
    let attempts = 0;
    const hung: SpanExporter = {
      name: "hung",
      export: () => {
        attempts++;
        return Effect.uninterruptible(Effect.never);
      },
      flush: Effect.void,
      shutdown: Effect.void,
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer(span(1, "first"));
      const first = yield* delivery.flush.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("10 millis");
      yield* Fiber.join(first);
      sink.unsafeOffer(span(2, "second"));
      yield* delivery.flush;
      const health = yield* delivery.health;
      expect(attempts).toBe(1);
      expect(health.attemptTimeouts).toBe(1);
      expect(health.disabledExporters).toBe(1);
    }).pipe(
      Effect.provide(
        deliveryLayer(options([hung], { attemptTimeout: "10 millis", maxBatchAge: "1 hour" })),
      ),
    );
  });

  it.effect("bounds shutdown while an exporter attempt is still uninterruptible", () => {
    const hung: SpanExporter = {
      name: "shutdown-hung",
      export: () => Effect.uninterruptible(Effect.never),
      flush: Effect.void,
      shutdown: Effect.void,
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer(span(1, "shutdown"));
      const shuttingDown = yield* delivery.shutdown.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* TestClock.adjust("10 millis");
      yield* Fiber.join(shuttingDown);
      const health = yield* delivery.health;
      expect(health.accepting).toBe(false);
      expect(health.flushTimeouts).toBe(1);
    }).pipe(
      Effect.provide(
        deliveryLayer(
          options([hung], {
            attemptTimeout: "1 hour",
            shutdownTimeout: "10 millis",
            maxBatchAge: "1 hour",
          }),
        ),
      ),
    );
  });
});

function span(
  value: number,
  operation: string,
  overrides: Partial<Pick<FlowSpan, "durationMs" | "outcome">> = {},
): FlowSpan {
  return {
    schemaVersion: 1,
    traceId: parseTraceId(value.toString(16).padStart(32, "0")),
    spanId: parseSpanId(value.toString(16).padStart(16, "0")),
    service: { name: "delivery-test", version: "1", environment: "test" },
    operation,
    kind: "internal",
    outcome: overrides.outcome ?? "success",
    startTime: "2026-08-23T00:00:00.000Z",
    endTime: "2026-08-23T00:00:00.000Z",
    durationMs: overrides.durationMs ?? 0,
    attributes: {},
    events: [],
  };
}

function staging(overrides: Partial<SegmentStagingConfig> = {}): SegmentStagingConfig {
  return {
    maxOpenSegments: 16,
    maxSpansPerSegment: 16,
    maxBytesPerSegment: 1_000_000,
    maxTotalBytes: 4_000_000,
    rootGraceMs: 10,
    maxSegmentAgeMs: 1_000,
    overflow: "drop-oldest",
    decisionCacheCapacity: 16,
    decisionCacheMaxAgeMs: 1_000,
    ...overrides,
  };
}

function localSegment(
  seed: number,
  definitions: ReadonlyArray<{
    readonly operation: string;
    readonly outcome: FlowSpan["outcome"];
    readonly root?: boolean;
  }>,
): Array<FlowSpan> {
  const rootValue = seed + definitions.findIndex((definition) => definition.root);
  const rootSpanId = parseSpanId(rootValue.toString(16).padStart(16, "0"));
  const segmentId = parseSegmentId(rootSpanId);
  const traceId = parseTraceId(seed.toString(16).padStart(32, "0"));
  return definitions.map((definition, index) => {
    const item = span(seed + index, definition.operation, { outcome: definition.outcome });
    return {
      ...item,
      traceId,
      segmentId,
      ...(definition.root ? {} : { parentSpanId: rootSpanId }),
    };
  });
}
