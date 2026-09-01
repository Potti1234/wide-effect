import {
  parseSpanId,
  parseTraceId,
  parseTraceState,
  type HttpOutcomePolicy,
  type TraceCarrier,
} from "@wide-effect/core";
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
      const sentinel = "secret-never-export-http-server";
      return Effect.gen(function* () {
        const request = HttpServerRequest.fromWeb(
          new Request(`https://api.example.test/items/${sentinel}?token=${sentinel}`, {
            headers: {
              traceparent: upstream,
              tracestate: "vendor=state",
              authorization: `Bearer ${sentinel}`,
              cookie: `session=${sentinel}`,
            },
          }),
        );
        yield* w3cServerTracer(
          Effect.gen(function* () {
            const state = yield* CurrentTraceState;
            observedState = Option.getOrElse(state, () => "missing");
            yield* Effect.annotateCurrentSpan("http.route", "/items/:id");
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
        expect(spans[0]?.attributes).toMatchObject({
          "http.request.method": "GET",
          "http.response.status_code": 204,
          "http.route": "/items/:id",
          "url.scheme": "http",
        });
        expect(spans[0]?.attributes["url.full"]).toBeUndefined();
        expect(spans[0]?.attributes["url.path"]).toBeUndefined();
        expect(spans[0]?.attributes["url.query"]).toBeUndefined();
        expect(JSON.stringify(spans)).not.toContain(sentinel);
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

  it.effect("classifies real server response statuses independently from Effect success", () =>
    Effect.gen(function* () {
      for (const status of [200, 400, 404, 500]) {
        const request = HttpServerRequest.fromWeb(
          new Request(`https://api.example.test/status/${status}`),
        );
        yield* w3cServerTracer(Effect.succeed(HttpServerResponse.empty({ status }))).pipe(
          Effect.provideService(HttpServerRequest.HttpServerRequest, request),
        );
      }
      yield* Effect.yieldNow;
      const spans = (yield* completedSpans).filter((span) => span.kind === "server");
      expect(
        spans.map((span) => [span.attributes["http.response.status_code"], span.outcome]),
      ).toEqual([
        [200, "success"],
        [400, "success"],
        [404, "success"],
        [500, "failure"],
      ]);
      expect(spans.at(-1)?.failure).toEqual({ type: "http", code: "http.500" });
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

  it.effect("excludes unsafe client URL fields under the default policy", () => {
    const sentinel = "secret-never-export-http-client";
    const base = HttpClient.make((request) =>
      Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 204 }))),
    );
    return Effect.gen(function* () {
      yield* base.get(
        `https://user:${sentinel}@worker.example.test/private/${sentinel}?token=${sentinel}#${sentinel}`,
      );
      const spans = yield* completedSpans;
      const client = spans.find((span) => span.kind === "client");
      expect(client?.attributes).toMatchObject({
        "http.request.method": "GET",
        "http.response.status_code": 204,
        "server.address": "https://worker.example.test",
        "url.scheme": "https",
      });
      expect(client?.attributes["url.full"]).toBeUndefined();
      expect(client?.attributes["url.path"]).toBeUndefined();
      expect(client?.attributes["url.query"]).toBeUndefined();
      expect(JSON.stringify(spans)).not.toContain(sentinel);
    }).pipe(Effect.provide(testTracingLayer()));
  });

  it.effect("records only an explicitly sanitized client path", () => {
    const sentinel = "secret-never-export-http-path";
    let sanitizedPaths = 0;
    const base = HttpClient.make((request) =>
      Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 200 }))),
    );
    return Effect.gen(function* () {
      yield* base.get(`https://worker.example.test/users/${sentinel}`);
      const spans = yield* completedSpans;
      const client = spans.find((span) => span.kind === "client");
      expect(sanitizedPaths).toBe(1);
      expect(client?.attributes["url.path"]).toBe("/users/:id");
      expect(JSON.stringify(spans)).not.toContain(sentinel);
    }).pipe(
      Effect.provide(
        testTracingLayer({
          sanitizeHttpPath: () => {
            sanitizedPaths++;
            return "/users/:id";
          },
        }),
      ),
    );
  });

  it.effect("classifies real client statuses with the default policy", () => {
    const makeClient = (status: number) =>
      HttpClient.make((request) =>
        Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status }))),
      );
    return Effect.gen(function* () {
      yield* makeClient(400).get("https://worker.example.test/default-400");
      yield* makeClient(500).get("https://worker.example.test/default-500");
      const spans = (yield* completedSpans).filter((span) => span.kind === "client");
      expect(spans.map((span) => span.outcome)).toEqual(["success", "failure"]);
    }).pipe(Effect.provide(testTracingLayer()));
  });

  it.effect("allows client 4xx classification as an explicit policy", () => {
    const client = HttpClient.make((request) =>
      Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 400 }))),
    );
    return Effect.gen(function* () {
      yield* client.get("https://worker.example.test/custom-400");
      const spans = (yield* completedSpans).filter((span) => span.kind === "client");
      expect(spans[0]?.outcome).toBe("failure");
      expect(spans[0]?.failure).toEqual({ type: "http", code: "http.400" });
    }).pipe(
      Effect.provide(
        testTracingLayer({
          httpOutcomePolicy: {
            serverErrorRanges: [{ minimum: 500, maximum: 599 }],
            clientErrorRanges: [
              { minimum: 400, maximum: 499 },
              { minimum: 500, maximum: 599 },
            ],
          },
        }),
      ),
    );
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

function testTracingLayer(
  options: {
    readonly sanitizeHttpPath?: ((path: string) => string) | undefined;
    readonly httpOutcomePolicy?: HttpOutcomePolicy | undefined;
  } = {},
) {
  let next = 1;
  const dependencies = Layer.mergeAll(
    configLayer({
      service: { name: "http-test", version: "1", environment: "test" },
      sanitizeHttpPath: options.sanitizeHttpPath,
      httpOutcomePolicy: options.httpOutcomePolicy,
    }),
    idGeneratorLayer({
      nextTraceId: () => parseTraceId((next++).toString(16).padStart(32, "0")),
      nextSpanId: () => parseSpanId((next++).toString(16).padStart(16, "0")),
    }),
    inMemoryCompletedSpanSinkLayer(),
  );
  return Layer.merge(tracerLayer.pipe(Layer.provide(dependencies)), dependencies);
}
