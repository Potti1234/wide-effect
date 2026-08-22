import { parseSpanId, parseTraceId, type FlowSpan, type SamplingConfig } from "@wide-effect/core";
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
    };
    return Effect.gen(function* () {
      const sink = yield* CompletedSpanSink;
      const delivery = yield* Delivery;
      sink.unsafeOffer(span(1, "failure", { outcome: "failure" }));
      sink.unsafeOffer(span(2, "slow", { durationMs: 50 }));
      sink.unsafeOffer(span(3, "important"));
      sink.unsafeOffer(span(4, "routine"));
      yield* delivery.flush;
      expect(output.batches.flat().map((item) => item.operation)).toEqual([
        "failure",
        "slow",
        "important",
      ]);
      const health = yield* delivery.health;
      expect(health.sampled).toBe(3);
      expect(health.dropped).toBe(1);
    }).pipe(Effect.provide(deliveryLayer(options([output.exporter], { sampling }))));
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
    }).pipe(Effect.provide(deliveryLayer(options([output.exporter], { sampling }))));
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
