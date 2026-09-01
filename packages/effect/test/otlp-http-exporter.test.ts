import {
  InteractionId,
  RequestId,
  parseSpanId,
  parseTraceId,
  type FlowSpan,
} from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Effect, Fiber, Schema } from "effect";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { describe, it } from "@effect/vitest";
import { SpanExporterError } from "../src/index.js";
import * as BrowserEntry from "../src/index.js";
import { makeOtlpHttpJsonExporter } from "../src/server.js";

const span: FlowSpan = {
  schemaVersion: 1,
  traceId: parseTraceId("0123456789abcdef0123456789abcdef"),
  spanId: parseSpanId("0123456789abcdef"),
  parentSpanId: parseSpanId("1111111111111111"),
  interactionId: Schema.decodeUnknownSync(InteractionId)("interaction-1"),
  requestId: Schema.decodeUnknownSync(RequestId)("request-1"),
  service: {
    name: "wide-effect-test",
    version: "1.2.3",
    environment: "test",
    deploymentId: "deploy-7",
    commitHash: "0123456789abcdef",
    region: "eu-central-1",
    instanceId: "api-7",
  },
  operation: "Checkout.submit",
  kind: "server",
  outcome: "failure",
  failure: {
    type: "typed",
    category: "payment",
    code: "payment.declined",
    retryable: false,
    summary: "payment provider declined the request",
  },
  startTime: "2026-08-22T20:00:00.000Z",
  endTime: "2026-08-22T20:00:00.125Z",
  durationMs: 125,
  attributes: {
    "checkout.cart.size": 2,
    "checkout.flags": [true, "priority"],
    "checkout.summary": { currency: "EUR" },
  },
  events: [
    {
      name: "payment.declined",
      timestamp: "2026-08-22T20:00:00.100Z",
      level: "error",
      attributes: { "payment.reason": "insufficient_funds" },
    },
  ],
};

describe("OTLP/HTTP JSON exporter", () => {
  it("is absent from the browser-safe package graph", () => {
    expect("makeOtlpHttpJsonExporter" in BrowserEntry).toBe(false);
  });

  it.effect("maps canonical resources, spans, events, and status exactly", () => {
    const requests: Array<{
      headers: Readonly<Record<string, string | undefined>>;
      body: unknown;
    }> = [];
    const client = HttpClient.make((request) =>
      Effect.sync(() => {
        requests.push({ headers: request.headers, body: request.body });
        return HttpClientResponse.fromWeb(request, new Response(null, { status: 200 }));
      }),
    );
    return Effect.gen(function* () {
      const exporter = yield* makeOtlpHttpJsonExporter({
        endpoint: "https://collector.example.test/v1/traces",
        headers: { "x-collector-key": "configured-value" },
        scopeName: "test.instrumentation",
        scopeVersion: "4.5.6",
      });
      yield* exporter.export([span]);
      expect(requests).toHaveLength(1);
      expect(requests[0]?.headers.accept).toBe("application/json");
      expect(requests[0]?.headers["content-type"]).toBe("application/json");
      expect(requests[0]?.headers["x-collector-key"]).toBe("configured-value");
      expect(decodeJsonBody(requests[0]?.body)).toEqual({
        resourceSpans: [
          {
            resource: {
              attributes: [
                keyValue("service.name", { stringValue: "wide-effect-test" }),
                keyValue("service.version", { stringValue: "1.2.3" }),
                keyValue("deployment.environment.name", { stringValue: "test" }),
                keyValue("wide_effect.deployment.id", { stringValue: "deploy-7" }),
                keyValue("vcs.ref.head.revision", { stringValue: "0123456789abcdef" }),
                keyValue("cloud.region", { stringValue: "eu-central-1" }),
                keyValue("service.instance.id", { stringValue: "api-7" }),
              ],
            },
            scopeSpans: [
              {
                scope: { name: "test.instrumentation", version: "4.5.6" },
                spans: [
                  {
                    traceId: "0123456789abcdef0123456789abcdef",
                    spanId: "0123456789abcdef",
                    parentSpanId: "1111111111111111",
                    name: "Checkout.submit",
                    kind: 2,
                    startTimeUnixNano: "1787428800000000000",
                    endTimeUnixNano: "1787428800125000000",
                    attributes: [
                      keyValue("checkout.cart.size", { doubleValue: 2 }),
                      keyValue("checkout.flags", {
                        arrayValue: {
                          values: [{ boolValue: true }, { stringValue: "priority" }],
                        },
                      }),
                      keyValue("checkout.summary", {
                        kvlistValue: {
                          values: [keyValue("currency", { stringValue: "EUR" })],
                        },
                      }),
                      keyValue("wide_effect.schema.version", { doubleValue: 1 }),
                      keyValue("wide_effect.outcome", { stringValue: "failure" }),
                      keyValue("wide_effect.duration_ms", { doubleValue: 125 }),
                      keyValue("wide_effect.interaction.id", { stringValue: "interaction-1" }),
                      keyValue("wide_effect.request.id", { stringValue: "request-1" }),
                      keyValue("wide_effect.failure.type", { stringValue: "typed" }),
                      keyValue("wide_effect.failure.category", { stringValue: "payment" }),
                      keyValue("wide_effect.failure.code", { stringValue: "payment.declined" }),
                      keyValue("wide_effect.failure.retryable", { boolValue: false }),
                      keyValue("wide_effect.failure.summary", {
                        stringValue: "payment provider declined the request",
                      }),
                    ],
                    events: [
                      {
                        timeUnixNano: "1787428800100000000",
                        name: "payment.declined",
                        attributes: [
                          keyValue("payment.reason", { stringValue: "insufficient_funds" }),
                          keyValue("log.severity.text", { stringValue: "ERROR" }),
                        ],
                      },
                    ],
                    status: { code: 2 },
                  },
                ],
              },
            ],
          },
        ],
      });
    }).pipe(Effect.provideService(HttpClient.HttpClient, client));
  });

  it.effect("groups spans by resource and maps all OTLP span kinds and statuses", () => {
    let payload: unknown;
    const client = HttpClient.make((request) =>
      Effect.sync(() => {
        payload = decodeJsonBody(request.body);
        return HttpClientResponse.fromWeb(request, new Response(null, { status: 202 }));
      }),
    );
    const kinds = ["internal", "server", "client", "producer", "consumer"] as const;
    return Effect.gen(function* () {
      const exporter = yield* makeOtlpHttpJsonExporter({
        endpoint: "http://localhost:4318/v1/traces",
      });
      const {
        parentSpanId: _parent,
        interactionId: _interaction,
        requestId: _request,
        ...root
      } = span;
      const spans = kinds.map((kind, index) => ({
        ...root,
        spanId: parseSpanId((index + 1).toString(16).padStart(16, "0")),
        kind,
        outcome: index === 0 ? ("success" as const) : ("interrupted" as const),
        service: index === 4 ? { ...span.service, name: "worker" } : span.service,
      }));
      yield* exporter.export(spans as [FlowSpan, ...Array<FlowSpan>]);
      const resources = (payload as OtlpPayload).resourceSpans;
      expect(resources).toHaveLength(2);
      expect(resources[0]?.scopeSpans[0]?.spans.map((item) => item.kind)).toEqual([1, 2, 3, 4]);
      expect(resources[0]?.scopeSpans[0]?.spans.map((item) => item.status.code)).toEqual([
        1, 2, 2, 2,
      ]);
      expect(resources[1]?.scopeSpans[0]?.spans[0]?.kind).toBe(5);
    }).pipe(Effect.provideService(HttpClient.HttpClient, client));
  });

  it.effect("returns typed sanitized failures for responses, defects, and invalid input", () => {
    const secret = "Bearer must-never-leak";
    const cases = [
      HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(secret, { status: 503, statusText: secret }),
          ),
        ),
      ),
      HttpClient.make(() => Effect.die(new Error(secret))),
    ];
    return Effect.gen(function* () {
      for (const client of cases) {
        const exporter = yield* makeOtlpHttpJsonExporter({
          endpoint: "https://collector.example.test/v1/traces",
          headers: { authorization: secret },
        }).pipe(Effect.provideService(HttpClient.HttpClient, client));
        const error = yield* Effect.flip(exporter.export([span]));
        expect(error).toBeInstanceOf(SpanExporterError);
        expect(JSON.stringify(error)).not.toContain(secret);
      }

      const configError = yield* Effect.flip(
        makeOtlpHttpJsonExporter({
          endpoint: `https://user:${secret}@collector.example.test/v1/traces`,
          headers: { "bad\nheader": secret },
        }).pipe(Effect.provideService(HttpClient.HttpClient, cases[0]!)),
      );
      expect(configError).toBeInstanceOf(SpanExporterError);
      expect(JSON.stringify(configError)).not.toContain(secret);

      const exporter = yield* makeOtlpHttpJsonExporter({
        endpoint: "https://collector.example.test/v1/traces",
      }).pipe(Effect.provideService(HttpClient.HttpClient, cases[0]!));
      const forged = { ...span, attributes: { authorization: secret } } as FlowSpan;
      const spanError = yield* Effect.flip(exporter.export([forged]));
      expect(spanError).toBeInstanceOf(SpanExporterError);
      expect(JSON.stringify(spanError)).not.toContain(secret);
    });
  });

  it.effect("applies the configured timeout and preserves interruption", () => {
    const client = HttpClient.make(() => Effect.never);
    return Effect.gen(function* () {
      const exporter = yield* makeOtlpHttpJsonExporter({
        endpoint: "https://collector.example.test/v1/traces",
        timeoutMs: 50,
      });
      const fiber = yield* exporter.export([span]).pipe(Effect.forkChild);
      yield* TestClock.adjust("50 millis");
      const error = yield* Effect.flip(Fiber.join(fiber));
      expect(error).toBeInstanceOf(SpanExporterError);
      expect(error.message).toBe("OTLP request failed");

      const interrupted = yield* exporter.export([span]).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(interrupted);
      const exit = yield* Fiber.await(interrupted);
      expect(exit._tag).toBe("Failure");
      expect(JSON.stringify(exit)).toContain("Interrupt");
    }).pipe(Effect.provideService(HttpClient.HttpClient, client));
  });
});

function decodeJsonBody(body: unknown): unknown {
  if (
    typeof body !== "object" ||
    body === null ||
    !("_tag" in body) ||
    body._tag !== "Uint8Array" ||
    !("body" in body) ||
    !(body.body instanceof Uint8Array)
  ) {
    throw new Error("expected Uint8Array request body");
  }
  return JSON.parse(new TextDecoder().decode(body.body));
}

function keyValue(key: string, value: unknown) {
  return { key, value };
}

interface OtlpPayload {
  readonly resourceSpans: ReadonlyArray<{
    readonly scopeSpans: readonly [
      {
        readonly spans: ReadonlyArray<{
          readonly kind: number;
          readonly status: { readonly code: number };
        }>;
      },
    ];
  }>;
}
