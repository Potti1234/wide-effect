import { parseSpanId, parseTraceId, parseTraceState, type TraceCarrier } from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Effect, Fiber, Layer, Option } from "effect";
import {
  HttpClient,
  HttpClientResponse,
  HttpRouter,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";
import { describe, it } from "@effect/vitest";
import {
  CurrentTraceState,
  InMemoryCompletedSpans,
  configLayer,
  idGeneratorLayer,
  inMemoryCompletedSpanSinkLayer,
  injectBrowserCarrier,
  tracerLayer,
  w3cServerTracer,
  withTraceStatePropagation,
} from "../src/index.js";

const upstream = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";

describe("Effect HTTP propagation", () => {
  it("preserves web-handler adapter metadata while normalizing W3C headers", async () => {
    const Routes = HttpRouter.add(
      "GET",
      "/items",
      Effect.gen(function* () {
        const span = yield* Effect.currentSpan;
        const state = yield* CurrentTraceState;
        const request = yield* HttpServerRequest.HttpServerRequest;
        return HttpServerResponse.jsonUnsafe({
          headers: request.headers,
          parentSpanId: Option.map(span.parent, (parent) => parent.spanId).pipe(
            Option.getOrUndefined,
          ),
          traceId: span.traceId,
          tracestate: Option.getOrUndefined(state),
        });
      }),
    ).pipe(Layer.provide(HttpServer.layerServices));
    const { dispose, handler } = HttpRouter.toWebHandler(Routes, {
      disableLogger: true,
      middleware: w3cServerTracer,
    });

    try {
      const continued = await handler(
        new Request("https://api.example.test/items", {
          headers: { traceparent: upstream, tracestate: "vendor=state" },
        }),
      );
      expect(continued.status).toBe(200);
      await expect(continued.json()).resolves.toEqual({
        headers: { traceparent: upstream, tracestate: "vendor=state" },
        parentSpanId: "bbbbbbbbbbbbbbbb",
        traceId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        tracestate: "vendor=state",
      });

      const fresh = await handler(
        new Request("https://api.example.test/items", {
          headers: {
            traceparent: `${upstream},${upstream}`,
            tracestate: "vendor=a,vendor=b",
            b3: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-1",
          },
        }),
      );
      expect(fresh.status).toBe(200);
      const body: unknown = await fresh.json();
      expect(body).toMatchObject({ headers: {} });
      expect(body).not.toMatchObject({ parentSpanId: "bbbbbbbbbbbbbbbb" });
    } finally {
      await dispose();
    }
  });

  it.effect("delegates one continued server span to HttpMiddleware.tracer", () =>
    Effect.suspend(() => {
      let observedState = "missing";
      return Effect.gen(function* () {
        const request = HttpServerRequest.fromWeb(
          new Request("https://api.example.test/items", {
            headers: { traceparent: upstream, tracestate: "vendor=state" },
          }),
        );
        yield* w3cServerTracer(
          Effect.gen(function* () {
            const state = yield* CurrentTraceState;
            observedState = Option.getOrElse(state, () => "missing");
            return HttpServerResponse.empty();
          }),
        ).pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, request));
        expect(observedState).toBe("vendor=state");
        yield* Effect.yieldNow;
        const spans = yield* completedSpans;
        expect(spans).toHaveLength(1);
        expect(spans[0]).toMatchObject({
          operation: "http.server GET",
          kind: "server",
          traceId: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          parentSpanId: "bbbbbbbbbbbbbbbb",
        });
      }).pipe(Effect.provide(testTracingLayer()));
    }),
  );

  it.effect("strips malformed W3C and B3 input before starting a fresh server root", () =>
    Effect.gen(function* () {
      const request = HttpServerRequest.fromWeb(
        new Request("https://api.example.test/items", {
          headers: {
            traceparent: `${upstream},${upstream}`,
            tracestate: "vendor=a,vendor=b",
            b3: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-1",
          },
        }),
      );
      yield* w3cServerTracer(Effect.succeed(HttpServerResponse.empty())).pipe(
        Effect.provideService(HttpServerRequest.HttpServerRequest, request),
      );
      yield* Effect.yieldNow;
      const spans = yield* completedSpans;
      expect(spans).toHaveLength(1);
      expect(spans[0]?.traceId).not.toBe("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
      expect(spans[0]?.parentSpanId).toBeUndefined();
    }).pipe(Effect.provide(testTracingLayer())),
  );

  it.effect("closes a failed server request span exactly once", () =>
    Effect.gen(function* () {
      const request = HttpServerRequest.fromWeb(new Request("https://api.example.test/failure"));
      yield* Effect.exit(
        w3cServerTracer(Effect.fail("expected failure")).pipe(
          Effect.provideService(HttpServerRequest.HttpServerRequest, request),
        ),
      );
      yield* Effect.yieldNow;
      const spans = (yield* completedSpans).filter((span) => span.kind === "server");
      expect(spans).toHaveLength(1);
      expect(spans[0]?.outcome).toBe("failure");
    }).pipe(Effect.provide(testTracingLayer())),
  );

  it.effect("uses the built-in single client span and forwards scoped tracestate", () => {
    const requests: Array<HttpClientRequestSnapshot> = [];
    const base = HttpClient.make((request) =>
      Effect.sync(() => {
        requests.push({ headers: request.headers });
        return HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }));
      }),
    );
    const client = withTraceStatePropagation(base);
    return Effect.gen(function* () {
      yield* client
        .get("https://worker.example.test/jobs", {
          headers: { traceparent: upstream, tracestate: "caller=replace" },
        })
        .pipe(
          Effect.provideService(CurrentTraceState, Option.some(parseTraceState("vendor=state"))),
          Effect.withSpan("caller"),
        );
      const spans = yield* completedSpans;
      expect(spans.filter((span) => span.kind === "client")).toHaveLength(1);
      const clientSpan = spans.find((span) => span.kind === "client");
      expect(requests).toHaveLength(1);
      expect(requests[0]?.headers.traceparent).toContain(`-${clientSpan?.spanId}-`);
      expect(requests[0]?.headers.traceparent).not.toBe(upstream);
      expect(requests[0]?.headers.tracestate).toBe("vendor=state");
      expect(requests[0]?.headers.b3).toBeDefined();
    }).pipe(Effect.provide(testTracingLayer()));
  });

  it.effect("closes an interrupted client span exactly once", () => {
    const client = withTraceStatePropagation(HttpClient.make(() => Effect.never));
    return Effect.gen(function* () {
      const fiber = yield* client.get("https://worker.example.test/hung").pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);
      const spans = yield* completedSpans;
      const clients = spans.filter((span) => span.kind === "client");
      expect(clients).toHaveLength(1);
      expect(clients[0]?.outcome).toBe("interrupted");
    }).pipe(Effect.provide(testTracingLayer()));
  });
});

describe("browser carrier injection", () => {
  it("defaults to same-origin and replaces caller correlation", () => {
    const injected = injectBrowserCarrier(
      new Request("https://app.example.test/api", { headers: { traceparent: "attacker" } }),
      carrier,
      { currentOrigin: "https://app.example.test" },
    );
    expect(injected.headers.get("traceparent")).toBe(upstream);
    expect(injected.headers.get("tracestate")).toBe("vendor=state");
  });

  it("strips correlation outside the exact origin allowlist", () => {
    const blocked = injectBrowserCarrier(
      new Request("https://evil.example.test/api", { headers: { traceparent: "attacker" } }),
      carrier,
      { currentOrigin: "https://app.example.test", allowedOrigins: ["https://api.example.test"] },
    );
    expect(blocked.headers.get("traceparent")).toBeNull();
    const allowed = injectBrowserCarrier(new Request("https://api.example.test/api"), carrier, {
      currentOrigin: "https://app.example.test",
      allowedOrigins: ["https://api.example.test"],
    });
    expect(allowed.headers.get("traceparent")).toBe(upstream);
  });
});

interface HttpClientRequestSnapshot {
  readonly headers: Readonly<Record<string, string | undefined>>;
}

const carrier: TraceCarrier = {
  traceparent: {
    version: "00",
    traceId: parseTraceId("a".repeat(32)),
    parentId: parseSpanId("b".repeat(16)),
    traceFlags: "01",
  },
  tracestate: parseTraceState("vendor=state"),
};

const completedSpans = InMemoryCompletedSpans.pipe(Effect.flatMap((store) => store.spans));

function testTracingLayer() {
  let next = 1;
  const dependencies = Layer.mergeAll(
    configLayer({ service: { name: "http-test", version: "1", environment: "test" } }),
    idGeneratorLayer({
      nextTraceId: () => parseTraceId((next++).toString(16).padStart(32, "0")),
      nextSpanId: () => parseSpanId((next++).toString(16).padStart(16, "0")),
    }),
    inMemoryCompletedSpanSinkLayer(),
  );
  return Layer.merge(tracerLayer.pipe(Layer.provide(dependencies)), dependencies);
}
