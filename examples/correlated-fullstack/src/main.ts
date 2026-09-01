import {
  parseInteractionId,
  parseRequestId,
  parseSpanId,
  parseTraceId,
  type FlowSpan,
  type TraceCarrier,
} from "@wide-effect/core";
import {
  CompletedSpanSink,
  CurrentTraceState,
  annotateRequestId,
  completedSpanSinkLayer,
  configLayer,
  idGeneratorLayer,
  tracerLayer,
  withInteractionId,
  withInteractionPropagation,
  w3cServerTracer,
  withTraceStatePropagation,
} from "@wide-effect/effect";
import { injectCommandTraceHeaders } from "@wide-effect/foldkit";
import { assertTraceTree, formatTraceForest } from "@wide-effect/testing";
import { Effect, Layer, Option } from "effect";
import {
  HttpClient,
  HttpClientResponse,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";

const collected: Array<FlowSpan> = [];
const interactionId = parseInteractionId("interaction-42");
const apiRequestId = parseRequestId("request-api-42");
const workerRequestId = parseRequestId("request-worker-42");
const sink = CompletedSpanSink.of({ unsafeOffer: (span) => collected.push(span) });

let nextId = 1;
const sharedIds = idGeneratorLayer({
  nextTraceId: () => parseTraceId(nextHex(32)),
  nextSpanId: () => parseSpanId(nextHex(16)),
});

const runtimeLayer = (service: string) => {
  const dependencies = Layer.mergeAll(
    configLayer({
      service: { name: service, version: "0.1.0-example", environment: "local" },
      attributePolicy: {
        allowedPrefixes: ["effect.", "foldkit.", "http.", "example."],
      },
    }),
    sharedIds,
    completedSpanSinkLayer(sink),
  );
  return Layer.merge(tracerLayer.pipe(Layer.provide(dependencies)), dependencies);
};

const browserLayer = runtimeLayer("example-web");
const apiLayer = runtimeLayer("example-api");
const workerLayer = runtimeLayer("example-worker");

const workerApp = Effect.gen(function* () {
  yield* annotateRequestId(workerRequestId);
  yield* Effect.annotateCurrentSpan("example.job.id", "job-42");
  yield* Effect.logInfo("worker.job.accepted").pipe(
    Effect.annotateLogs({ "example.queue": "default" }),
  );
  yield* Effect.withSpan(Effect.void, "Worker.persistResult", {
    attributes: { "example.result": "stored" },
  });
  return HttpServerResponse.empty();
});

const workerTransport = HttpClient.make((request) =>
  w3cServerTracer(workerApp).pipe(
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      HttpServerRequest.fromWeb(
        new Request(request.url, { method: request.method, headers: request.headers }),
      ),
    ),
    Effect.provide(workerLayer),
    Effect.map((response) =>
      HttpClientResponse.fromWeb(request, new Response(null, { status: response.status })),
    ),
    Effect.orDie,
  ),
);

const apiApp = Effect.gen(function* () {
  yield* annotateRequestId(apiRequestId);
  yield* Effect.annotateCurrentSpan({
    "example.request.id": "request-42",
    // The key is rejected before it can enter the completed span.
    "example.api.token": "secret-that-must-not-survive",
  });
  yield* Effect.logInfo("api.request.validated").pipe(
    Effect.annotateLogs({ "example.route": "/jobs" }),
  );
  const workerClient = withInteractionPropagation(
    withTraceStatePropagation(workerTransport),
    interactionId,
  );
  yield* workerClient.post("https://worker.example.test/jobs");
  return HttpServerResponse.empty();
});

const callApi = Effect.gen(function* () {
  const current = yield* Effect.currentSpan;
  const carrier: TraceCarrier = {
    traceparent: {
      version: "00",
      traceId: parseTraceId(current.traceId),
      parentId: parseSpanId(current.spanId),
      traceFlags: current.sampled ? "01" : "00",
    },
  };
  const request = injectCommandTraceHeaders(
    new Request("https://api.example.test/jobs", { method: "POST" }),
    carrier,
    {
      currentOrigin: "https://app.example.test",
      allowedOrigins: ["https://api.example.test"],
    },
    interactionId,
  );
  return yield* w3cServerTracer(apiApp).pipe(
    Effect.provideService(HttpServerRequest.HttpServerRequest, HttpServerRequest.fromWeb(request)),
    Effect.provideService(CurrentTraceState, Option.none()),
    Effect.provide(apiLayer),
  );
});

const program = callApi.pipe(
  Effect.withSpan("foldkit.command.SubmitJob", {
    attributes: {
      "foldkit.message.tag": "SubmitJob",
    },
  }),
  (effect) => withInteractionId(effect, interactionId),
  Effect.provide(browserLayer),
);

await Effect.runPromise(program);
await Effect.runPromise(Effect.yieldNow);

assertTraceTree(collected, {
  operation: "foldkit.command.SubmitJob",
  children: [
    {
      operation: "http.server POST",
      children: [
        {
          operation: "http.client POST",
          children: [
            {
              operation: "http.server POST",
              children: [{ operation: "Worker.persistResult" }],
            },
          ],
        },
      ],
    },
  ],
});

if (collected.some((span) => span.interactionId !== interactionId)) {
  throw new Error("interaction correlation did not continue across every hop");
}
const requestIds = collected.flatMap((span) =>
  span.requestId === undefined ? [] : [span.requestId],
);
if (
  requestIds.length !== 2 ||
  !requestIds.includes(apiRequestId) ||
  !requestIds.includes(workerRequestId)
) {
  throw new Error("request IDs were not distinct per server hop");
}

const encoded = JSON.stringify(collected);
if (encoded.includes("secret-that-must-not-survive")) {
  throw new Error("redaction assertion failed: secret fixture survived");
}

console.log("Correlated browser -> API -> worker trace:\n");
console.log(formatTraceForest(collected));
console.log(`\n${collected.length} completed spans; secret fixture absent.`);

function nextHex(length: number) {
  return (nextId++).toString(16).padStart(length, "0");
}
