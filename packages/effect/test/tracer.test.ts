import { parseSpanId, parseTraceId, type FlowSpan } from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Effect, Fiber, Layer, Logger, Tracer } from "effect";
import { TestClock } from "effect/testing";
import { describe, it } from "@effect/vitest";
import {
  InMemoryCompletedSpans,
  annotate,
  configLayer,
  event,
  idGeneratorLayer,
  inMemoryCompletedSpanSinkLayer,
  loggerLayer,
  tracerLayer,
} from "../src/index.js";

const service = { name: "test-service", version: "1", environment: "test" } as const;

const makeTestLayer = (humanLogger?: Logger.Logger<unknown, void>) => {
  let next = 1;
  const dependencies = Layer.mergeAll(
    configLayer({ service, attributePolicy: { allowedPrefixes: ["test.", "effect."] } }),
    idGeneratorLayer({
      nextTraceId: () => parseTraceId((next++).toString(16).padStart(32, "0")),
      nextSpanId: () => parseSpanId((next++).toString(16).padStart(16, "0")),
    }),
    inMemoryCompletedSpanSinkLayer,
  );
  return Layer.merge(tracerLayer, loggerLayer({ humanLogger })).pipe(
    Layer.provideMerge(dependencies),
  );
};

describe("wide Effect tracer", () => {
  it.effect("captures nested and concurrent fibers with deterministic ancestry", () =>
    Effect.gen(function* () {
      yield* Effect.all(
        [
          Effect.void.pipe(Effect.withSpan("child.a")),
          Effect.void.pipe(Effect.withSpan("child.b")),
        ],
        { concurrency: "unbounded" },
      ).pipe(Effect.withSpan("root"));
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(spans).toHaveLength(3);
      const root = operation(spans, "root");
      expect(operation(spans, "child.a").parentSpanId).toBe(root.spanId);
      expect(operation(spans, "child.b").parentSpanId).toBe(root.spanId);
      expect(new Set(spans.map((span) => span.traceId)).size).toBe(1);
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("classifies typed failures and defects", () =>
    Effect.gen(function* () {
      yield* Effect.exit(Effect.fail("typed").pipe(Effect.withSpan("failure")));
      yield* Effect.exit(Effect.die("defect").pipe(Effect.withSpan("defect")));
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(operation(spans, "failure").outcome).toBe("failure");
      expect(operation(spans, "defect").outcome).toBe("defect");
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("classifies interruption", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.never.pipe(Effect.withSpan("interrupted"), Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(operation(spans, "interrupted").outcome).toBe("interrupted");
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("retains sampled=false parent ancestry and still completes the span", () =>
    Effect.gen(function* () {
      const parent = Tracer.externalSpan({
        traceId: parseTraceId("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        spanId: parseSpanId("bbbbbbbbbbbbbbbb"),
        sampled: false,
      });
      const linked = Tracer.externalSpan({
        traceId: parseTraceId("cccccccccccccccccccccccccccccccc"),
        spanId: parseSpanId("dddddddddddddddd"),
      });
      yield* Effect.void.pipe(
        Effect.withSpan("unsampled-child", {
          parent,
          links: [{ span: linked, attributes: { relationship: "test" } }],
        }),
      );
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      const child = operation(spans, "unsampled-child");
      expect(child.traceId).toBe(parent.traceId);
      expect(child.parentSpanId).toBe(parent.spanId);
      expect(child.attributes["effect.sampled"]).toBe(false);
      expect(child.attributes["effect.links"]).toEqual([
        {
          traceId: linked.traceId,
          spanId: linked.spanId,
          attributes: { relationship: "test" },
        },
      ]);
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("uses bigint clock timing and preserves same-time event order", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.gen(function* () {
        yield* event("first", { "test.value": 1 });
        yield* event("second", { "test.value": 2 });
        yield* Effect.sleep("10 millis");
      }).pipe(Effect.withSpan("timed"), Effect.forkChild);
      yield* TestClock.adjust("10 millis");
      yield* Fiber.join(fiber);
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      const timed = operation(spans, "timed");
      expect(timed.durationMs).toBe(10);
      expect(timed.events.map((item) => item.name)).toEqual(["first", "second"]);
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("normalizes annotations and makes helpers safe outside spans", () =>
    Effect.gen(function* () {
      yield* annotate({ "test.visible": "yes", password: "secret", ignored: "drop" });
      yield* event("outside", { "test.value": 1 });
      yield* Effect.gen(function* () {
        yield* annotate({ "test.visible": "yes", password: "secret", ignored: "drop" });
      }).pipe(Effect.withSpan("annotated"));
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(operation(spans, "annotated").attributes).toMatchObject({ "test.visible": "yes" });
      expect(JSON.stringify(spans)).not.toContain("secret");
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("records each Effect log as exactly one span event with custom human logging", () =>
    Effect.suspend(() => {
      const messages: Array<unknown> = [];
      const human = Logger.make(({ message }) => {
        messages.push(message);
      });
      return Effect.gen(function* () {
        yield* Effect.logInfo("hello").pipe(Effect.withSpan("logged"));
        const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
        expect(
          operation(spans, "logged").events.filter((item) => item.name === "hello"),
        ).toHaveLength(1);
        expect(messages).toHaveLength(1);
      }).pipe(Effect.provide(makeTestLayer(human)));
    }),
  );

  it.effect("records one log event with the default logger composition", () =>
    Effect.gen(function* () {
      yield* Effect.logInfo("default-log").pipe(Effect.withSpan("default-logged"));
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(
        operation(spans, "default-logged").events.filter((item) => item.name === "default-log"),
      ).toHaveLength(1);
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("uses a safe no-op sink when no sink is provided", () => {
    let next = 100;
    const dependencies = Layer.mergeAll(
      configLayer({ service }),
      idGeneratorLayer({
        nextTraceId: () => parseTraceId((next++).toString(16).padStart(32, "0")),
        nextSpanId: () => parseSpanId((next++).toString(16).padStart(16, "0")),
      }),
    );
    return Effect.void.pipe(
      Effect.withSpan("discarded"),
      Effect.provide(Layer.merge(tracerLayer, loggerLayer()).pipe(Layer.provide(dependencies))),
    );
  });
});

function operation(spans: ReadonlyArray<FlowSpan>, name: string): FlowSpan {
  const found = spans.find((span) => span.operation === name);
  if (!found) throw new Error(`Missing span ${name}`);
  return found;
}
