import {
  FlowSpan,
  parseInteractionId,
  parseRequestId,
  parseSpanId,
  parseTraceId,
  type FlowSpan as FlowSpanType,
} from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Context, Effect, Exit, Fiber, Layer, Logger, Option, Schema, Tracer } from "effect";
import { TestClock } from "effect/testing";
import { describe, it } from "@effect/vitest";
import {
  CompletedSpanSink,
  InMemoryCompletedSpans,
  annotateFailure,
  annotate,
  annotateInteractionId,
  annotateRequestId,
  configLayer,
  event,
  idGeneratorLayer,
  inMemoryCompletedSpanSinkLayer,
  loggerLayer,
  noopCompletedSpanSinkLayer,
  tracerLayer,
  withInteractionId,
} from "../src/index.js";

const service = { name: "test-service", version: "1", environment: "test" } as const;

const makeTestLayer = (
  humanLogger?: Logger.Logger<unknown, void>,
  allowedPrefixes: ReadonlyArray<string> = ["test.", "effect."],
) => {
  let next = 1;
  const dependencies = Layer.mergeAll(
    configLayer({ service, attributePolicy: { allowedPrefixes } }),
    idGeneratorLayer({
      nextTraceId: () => parseTraceId((next++).toString(16).padStart(32, "0")),
      nextSpanId: () => parseSpanId((next++).toString(16).padStart(16, "0")),
    }),
    inMemoryCompletedSpanSinkLayer(),
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
      expect(root.segmentId).toBe(root.spanId);
      expect(operation(spans, "child.a").segmentId).toBe(root.segmentId);
      expect(operation(spans, "child.b").segmentId).toBe(root.segmentId);
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

  it.effect("captures only safe structured failure evidence", () =>
    Effect.gen(function* () {
      const sentinel = "secret-never-export-failure";
      let messageReads = 0;
      const typed = Object.defineProperty({ _tag: "Database.Unavailable" }, "message", {
        enumerable: true,
        get: () => {
          messageReads++;
          return sentinel;
        },
      });
      yield* Effect.gen(function* () {
        yield* annotateFailure({
          category: "dependency",
          code: "database.timeout",
          retryable: true,
          summary: "database did not respond in time",
        });
        return yield* Effect.fail(typed);
      }).pipe(Effect.withSpan("enriched-failure"), Effect.exit);
      yield* Effect.die(
        new Proxy(
          {},
          {
            ownKeys: () => {
              throw new Error(sentinel);
            },
          },
        ),
      ).pipe(Effect.withSpan("hostile-defect"), Effect.exit);
      yield* Effect.gen(function* () {
        yield* annotateFailure({ summary: `invalid\n${sentinel}` });
        return yield* Effect.fail("plain typed failure");
      }).pipe(Effect.withSpan("invalid-summary"), Effect.exit);
      yield* annotateFailure({ summary: "safe outside a span" });

      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(operation(spans, "enriched-failure").failure).toEqual({
        type: "typed",
        category: "dependency",
        code: "database.timeout",
        retryable: true,
        summary: "database did not respond in time",
      });
      expect(operation(spans, "hostile-defect").failure).toEqual({ type: "defect" });
      expect(operation(spans, "invalid-summary").failure).toEqual({ type: "typed" });
      expect(messageReads).toBe(0);
      expect(JSON.stringify(spans)).not.toContain(sentinel);
      expect(JSON.stringify(spans)).not.toContain("wide.failure");
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("classifies interruption", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.never.pipe(Effect.withSpan("interrupted"), Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(operation(spans, "interrupted").outcome).toBe("interrupted");
      expect(operation(spans, "interrupted").failure).toEqual({ type: "interruption" });
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
      expect(child.segmentId).toBe(child.spanId);
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

  it.effect("promotes validated correlation identifiers and strips reserved attributes", () =>
    Effect.gen(function* () {
      const interactionId = parseInteractionId("interaction-42");
      const requestId = parseRequestId("request-api-42");
      yield* Effect.gen(function* () {
        yield* annotateInteractionId(interactionId);
        yield* annotateRequestId(requestId);
        yield* Effect.annotateCurrentSpan("test.requestId", "application-value");
        yield* Effect.void.pipe(Effect.withSpan("correlated-child"));
      }).pipe(
        (effect) => withInteractionId(effect, interactionId),
        Effect.withSpan("correlated-root"),
      );
      yield* Effect.gen(function* () {
        yield* Effect.annotateCurrentSpan({
          "wide.interaction.id": "invalid value with spaces",
          "wide.request.id": "also invalid",
        });
      }).pipe(Effect.withSpan("invalid-correlation"));
      yield* annotateInteractionId(interactionId);
      yield* annotateRequestId(requestId);

      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      const root = operation(spans, "correlated-root");
      const child = operation(spans, "correlated-child");
      expect(root.interactionId).toBe(interactionId);
      expect(root.requestId).toBe(requestId);
      expect(root.attributes["test.requestId"]).toBe("application-value");
      expect(child.interactionId).toBe(interactionId);
      expect(child.requestId).toBeUndefined();
      expect(operation(spans, "invalid-correlation").interactionId).toBeUndefined();
      expect(JSON.stringify(spans)).not.toContain("wide.interaction.id");
      expect(JSON.stringify(spans)).not.toContain("wide.request.id");
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect(
    "drops sensitive server attributes without dropping the span or changing its exit",
    () =>
      Effect.gen(function* () {
        const secret = "Bearer production-secret-never-store";
        let secretReads = 0;
        const headers = { "x-request-id": "request-123" };
        Object.defineProperty(headers, "authorization", {
          enumerable: true,
          get: () => {
            secretReads++;
            return secret;
          },
        });
        const exit = yield* Effect.exit(
          Effect.gen(function* () {
            yield* Effect.annotateCurrentSpan("http.request.method", "GET");
            yield* Effect.annotateCurrentSpan("http.request.header.authorization", secret);
            yield* Effect.annotateCurrentSpan("http.request.header.cookie", `session=${secret}`);
            yield* Effect.annotateCurrentSpan("http.request.headers", headers);
            return 42;
          }).pipe(Effect.withSpan("http.server.request", { kind: "server" })),
        );
        const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));

        expect(Exit.isSuccess(exit)).toBe(true);
        if (Exit.isSuccess(exit)) expect(exit.value).toBe(42);
        expect(spans).toHaveLength(1);
        const span = operation(spans, "http.server.request");
        expect(span.attributes["http.request.method"]).toBe("GET");
        expect(span.attributes["http.request.header.authorization"]).toBeUndefined();
        expect(span.attributes["http.request.header.cookie"]).toBeUndefined();
        expect(span.attributes["http.request.headers"]).toEqual({ "x-request-id": "request-123" });
        expect(secretReads).toBe(0);
        expect(JSON.stringify(span)).not.toContain(secret);
      }).pipe(Effect.provide(makeTestLayer(undefined, ["http.", "effect."]))),
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

  it.effect("keeps semantic log names and safe structured annotations", () =>
    Effect.gen(function* () {
      yield* Effect.logInfo("worker.lease.claimed").pipe(
        Effect.annotateLogs({ "test.run.id": "run-1", authorization: "never-export" }),
        Effect.withSpan("logged"),
      );
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      const logged = operation(spans, "logged");
      expect(logged.events).toHaveLength(1);
      expect(logged.events[0]).toMatchObject({
        name: "worker.lease.claimed",
        level: "info",
        attributes: { "test.run.id": "run-1", "effect.logLevel": "INFO" },
      });
      expect(JSON.stringify(logged)).not.toContain("never-export");
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("replaces unsafe log messages and removes formatted causes", () =>
    Effect.gen(function* () {
      const sentinel = "secret-never-export-log-ingress";
      yield* Effect.logInfo("request.failed", {
        authorization: sentinel,
        cookie: sentinel,
        token: sentinel,
        password: sentinel,
        prompt: sentinel,
        toolOutput: sentinel,
        terminalOutput: sentinel,
      }).pipe(Effect.withSpan("multi-argument"));
      yield* Effect.logInfo(`https://user:${sentinel}@example.test/private?token=${sentinel}`).pipe(
        Effect.withSpan("url-message"),
      );
      yield* Effect.logInfo(`prose with whitespace ${sentinel}`).pipe(Effect.withSpan("prose"));
      yield* Effect.logInfo("x".repeat(129)).pipe(Effect.withSpan("overlong"));
      yield* Effect.fail(sentinel).pipe(
        Effect.ignore({ log: "Error", message: "request.failed" }),
        Effect.withSpan("failure-log"),
      );
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      for (const name of ["multi-argument", "url-message", "prose", "overlong"]) {
        expect(operation(spans, name).events).toHaveLength(1);
        expect(operation(spans, name).events[0]?.name).toBe("effect.log");
      }
      const failure = operation(spans, "failure-log");
      expect(failure.events).toHaveLength(1);
      expect(failure.events[0]?.name).toBe("request.failed");
      expect(failure.events[0]?.attributes["effect.cause"]).toBeUndefined();
      expect(JSON.stringify(spans)).not.toContain(sentinel);
      for (const span of spans)
        expect(() => Schema.decodeUnknownSync(FlowSpan)(span)).not.toThrow();
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
      Effect.provide(
        Layer.merge(tracerLayer, loggerLayer()).pipe(
          Layer.provide(Layer.merge(dependencies, noopCompletedSpanSinkLayer)),
        ),
      ),
    );
  });

  it.effect("sanitizes names and emits schema-valid spans", () =>
    Effect.gen(function* () {
      yield* Effect.void.pipe(Effect.withSpan(`  ${"x".repeat(300)}\n`));
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(spans).toHaveLength(1);
      expect(spans[0]?.operation).toHaveLength(256);
      expect(() => Schema.decodeUnknownSync(FlowSpan)(spans[0])).not.toThrow();
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("cannot throw from completion with a throwing sink or extreme clock values", () => {
    let next = 900;
    const dependencies = Layer.mergeAll(
      configLayer({ service }),
      idGeneratorLayer({
        nextTraceId: () => parseTraceId((next++).toString(16).padStart(32, "0")),
        nextSpanId: () => parseSpanId((next++).toString(16).padStart(16, "0")),
      }),
      Layer.succeed(CompletedSpanSink, {
        unsafeOffer: () => {
          throw new Error("sink unavailable");
        },
      }),
    );
    const live = Layer.merge(tracerLayer.pipe(Layer.provide(dependencies)), dependencies);
    return Effect.gen(function* () {
      const tracer = yield* Effect.tracer;
      const span = tracer.span({
        name: "extreme",
        parent: Option.none(),
        annotations: Context.empty(),
        links: [],
        startTime: 10n ** 40n,
        kind: "internal",
        root: true,
        sampled: true,
      });
      expect(() => span.end(-(10n ** 40n), Exit.succeed(undefined))).not.toThrow();
    }).pipe(Effect.provide(live));
  });

  it.effect("discards hostile parents without evaluating them twice", () =>
    Effect.gen(function* () {
      const parent = Tracer.externalSpan({
        traceId: parseTraceId("a".repeat(32)),
        spanId: parseSpanId("b".repeat(16)),
      });
      let reads = 0;
      Object.defineProperty(parent, "traceId", {
        get: () => {
          reads++;
          throw new Error("hostile");
        },
      });
      yield* Effect.void.pipe(Effect.withSpan("safe-root", { parent }));
      const spans = yield* InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));
      expect(operation(spans, "safe-root").parentSpanId).toBeUndefined();
      expect(reads).toBe(1);
    }).pipe(Effect.provide(makeTestLayer())),
  );

  it.effect("bounds the in-memory sink with explicit drop-oldest behavior", () => {
    let next = 1_000;
    const dependencies = Layer.mergeAll(
      configLayer({ service }),
      idGeneratorLayer({
        nextTraceId: () => parseTraceId((next++).toString(16).padStart(32, "0")),
        nextSpanId: () => parseSpanId((next++).toString(16).padStart(16, "0")),
      }),
      inMemoryCompletedSpanSinkLayer({ capacity: 2, overflow: "drop-oldest" }),
    );
    const live = Layer.merge(tracerLayer.pipe(Layer.provide(dependencies)), dependencies);
    return Effect.gen(function* () {
      yield* Effect.void.pipe(Effect.withSpan("one"));
      yield* Effect.void.pipe(Effect.withSpan("two"));
      yield* Effect.void.pipe(Effect.withSpan("three"));
      const store = yield* InMemoryCompletedSpans;
      expect((yield* store.spans).map((span) => span.operation)).toEqual(["two", "three"]);
      expect(yield* store.dropped).toBe(1);
    }).pipe(Effect.provide(live));
  });
});

function operation(spans: ReadonlyArray<FlowSpanType>, name: string): FlowSpanType {
  for (const span of spans) Schema.decodeUnknownSync(FlowSpan)(span);
  const found = spans.find((span) => span.operation === name);
  if (!found) throw new Error(`Missing span ${name}`);
  return found;
}
