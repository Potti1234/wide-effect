import {
  decodeFlowSpan,
  type FlowSpan,
  type SafeAttributes,
  type SafeValue,
  type ServiceMetadata,
  type SpanEvent,
} from "@wide-effect/core";
import { Cause, Effect, Option, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import {
  exporterError,
  makeExporterLifecycle,
  type SpanBatch,
  type SpanExporter,
} from "./exporter.js";

const OtlpHttpExporterConfig = Schema.Struct({
  endpoint: Schema.String.check(
    Schema.makeFilter((value) => isHttpEndpoint(value) || "endpoint must be an HTTP(S) URL"),
  ),
  headers: Schema.optionalKey(
    Schema.Record(
      Schema.String.check(Schema.isPattern(/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/)),
      Schema.String.check(Schema.isMaxLength(8_192), Schema.isPattern(/^[^\r\n]*$/)),
    ),
  ),
  timeoutMs: Schema.optionalKey(
    Schema.Finite.check(
      Schema.makeFilter((value) => Number.isInteger(value) || "timeoutMs must be an integer"),
      Schema.isGreaterThan(0),
      Schema.isLessThanOrEqualTo(300_000),
    ),
  ),
  scopeName: Schema.optionalKey(
    Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
  ),
  scopeVersion: Schema.optionalKey(
    Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
  ),
});

export interface OtlpHttpExporterOptions {
  readonly endpoint: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly timeoutMs?: number;
  readonly scopeName?: string;
  readonly scopeVersion?: string;
}

/** Creates an OTLP/HTTP JSON adapter. Batching, retry, and shutdown draining belong to Delivery. */
export const makeOtlpHttpJsonExporter = (
  options: OtlpHttpExporterOptions,
): Effect.Effect<SpanExporter, ReturnType<typeof exporterError>, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const config = yield* Schema.decodeUnknownEffect(OtlpHttpExporterConfig)(options, {
      onExcessProperty: "error",
    }).pipe(
      Effect.mapError(() =>
        exporterError("otlp-http-json", "export", "invalid OTLP exporter configuration"),
      ),
    );
    const client = yield* HttpClient.HttpClient;
    const lifecycle = yield* makeExporterLifecycle("otlp-http-json");
    const timeoutMs = config.timeoutMs ?? 10_000;

    return {
      name: "otlp-http-json",
      export: (batch) =>
        lifecycle.run(
          "export",
          Effect.gen(function* () {
            const payload = yield* makePayload(batch, config.scopeName, config.scopeVersion);
            let request = HttpClientRequest.post(config.endpoint).pipe(
              HttpClientRequest.setHeaders(config.headers ?? {}),
              HttpClientRequest.acceptJson,
            );
            request = yield* HttpClientRequest.bodyJson(request, payload);
            const response = yield* client
              .execute(request)
              .pipe(
                Effect.withTracerEnabled(false),
                Effect.flatMap(HttpClientResponse.filterStatusOk),
                Effect.timeoutOption(timeoutMs),
              );
            if (Option.isNone(response)) {
              return yield* Effect.fail(
                exporterError("otlp-http-json", "export", "OTLP request timed out"),
              );
            }
          }).pipe(sanitizeFailure("export")),
        ),
      flush: lifecycle.run("flush", Effect.void),
      shutdown: lifecycle.shutdown,
    };
  });

export interface OtlpExportTraceServiceRequest {
  readonly resourceSpans: ReadonlyArray<OtlpResourceSpans>;
}

interface OtlpResourceSpans {
  readonly resource: { readonly attributes: ReadonlyArray<OtlpKeyValue> };
  readonly scopeSpans: readonly [
    {
      readonly scope: { readonly name: string; readonly version?: string };
      readonly spans: ReadonlyArray<OtlpSpan>;
    },
  ];
}

interface OtlpSpan {
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId?: string;
  readonly name: string;
  readonly kind: number;
  readonly startTimeUnixNano: string;
  readonly endTimeUnixNano: string;
  readonly attributes: ReadonlyArray<OtlpKeyValue>;
  readonly events: ReadonlyArray<OtlpEvent>;
  readonly status: { readonly code: number };
}

interface OtlpEvent {
  readonly timeUnixNano: string;
  readonly name: string;
  readonly attributes: ReadonlyArray<OtlpKeyValue>;
}

interface OtlpKeyValue {
  readonly key: string;
  readonly value: OtlpAnyValue;
}

type OtlpAnyValue =
  | { readonly stringValue: string }
  | { readonly doubleValue: number }
  | { readonly boolValue: boolean }
  | { readonly arrayValue: { readonly values: ReadonlyArray<OtlpAnyValue> } }
  | { readonly kvlistValue: { readonly values: ReadonlyArray<OtlpKeyValue> } };

function makePayload(
  batch: SpanBatch,
  scopeName = "@wide-effect/effect",
  scopeVersion?: string,
): Effect.Effect<OtlpExportTraceServiceRequest, ReturnType<typeof exporterError>> {
  return Effect.try({
    try: () => {
      const groups = new Map<string, { service: ServiceMetadata; spans: Array<FlowSpan> }>();
      for (const input of batch) {
        const span = decodeFlowSpan(input);
        const key = JSON.stringify(span.service);
        const group = groups.get(key);
        if (group === undefined) groups.set(key, { service: span.service, spans: [span] });
        else group.spans.push(span);
      }
      return {
        resourceSpans: [...groups.values()].map(({ service, spans }) => ({
          resource: { attributes: resourceAttributes(service) },
          scopeSpans: [
            {
              scope: {
                name: scopeName,
                ...(scopeVersion === undefined ? {} : { version: scopeVersion }),
              },
              spans: spans.map(mapSpan),
            },
          ],
        })),
      };
    },
    catch: () => exporterError("otlp-http-json", "export", "invalid canonical span batch"),
  });
}

function mapSpan(span: FlowSpan): OtlpSpan {
  const canonicalAttributes: SafeAttributes = {
    ...span.attributes,
    "wide_effect.schema.version": span.schemaVersion,
    "wide_effect.outcome": span.outcome,
    "wide_effect.duration_ms": span.durationMs,
    ...(span.interactionId === undefined
      ? {}
      : { "wide_effect.interaction.id": span.interactionId }),
    ...(span.requestId === undefined ? {} : { "wide_effect.request.id": span.requestId }),
    ...(span.failure === undefined
      ? {}
      : {
          "wide_effect.failure.type": span.failure.type,
          ...(span.failure.category === undefined
            ? {}
            : { "wide_effect.failure.category": span.failure.category }),
          ...(span.failure.code === undefined
            ? {}
            : { "wide_effect.failure.code": span.failure.code }),
          ...(span.failure.retryable === undefined
            ? {}
            : { "wide_effect.failure.retryable": span.failure.retryable }),
          ...(span.failure.summary === undefined
            ? {}
            : { "wide_effect.failure.summary": span.failure.summary }),
        }),
  };
  return {
    traceId: span.traceId,
    spanId: span.spanId,
    ...(span.parentSpanId === undefined ? {} : { parentSpanId: span.parentSpanId }),
    name: span.operation,
    kind: spanKind(span.kind),
    startTimeUnixNano: unixNanos(span.startTime),
    endTimeUnixNano: unixNanos(span.endTime),
    attributes: attributes(canonicalAttributes),
    events: span.events.map(mapEvent),
    status: { code: span.outcome === "success" ? 1 : 2 },
  };
}

function mapEvent(event: SpanEvent): OtlpEvent {
  const eventAttributes: SafeAttributes =
    event.level === undefined
      ? event.attributes
      : { ...event.attributes, "log.severity.text": event.level.toUpperCase() };
  return {
    timeUnixNano: unixNanos(event.timestamp),
    name: event.name,
    attributes: attributes(eventAttributes),
  };
}

function resourceAttributes(service: ServiceMetadata): ReadonlyArray<OtlpKeyValue> {
  return attributes({
    "service.name": service.name,
    "service.version": service.version,
    "deployment.environment.name": service.environment,
    ...(service.deploymentId === undefined
      ? {}
      : { "wide_effect.deployment.id": service.deploymentId }),
    ...(service.commitHash === undefined ? {} : { "vcs.ref.head.revision": service.commitHash }),
    ...(service.region === undefined ? {} : { "cloud.region": service.region }),
    ...(service.instanceId === undefined ? {} : { "service.instance.id": service.instanceId }),
  });
}

function attributes(values: SafeAttributes): ReadonlyArray<OtlpKeyValue> {
  return Object.entries(values).map(([key, value]) => ({ key, value: anyValue(value) }));
}

function anyValue(value: SafeValue): OtlpAnyValue {
  if (value === null) return { stringValue: "null" };
  if (typeof value === "string") return { stringValue: value };
  if (typeof value === "number") return { doubleValue: value };
  if (typeof value === "boolean") return { boolValue: value };
  if (Array.isArray(value)) return { arrayValue: { values: value.map(anyValue) } };
  return {
    kvlistValue: {
      values: Object.entries(value).map(([key, nested]) => ({ key, value: anyValue(nested) })),
    },
  };
}

function spanKind(kind: FlowSpan["kind"]): number {
  switch (kind) {
    case "internal":
      return 1;
    case "server":
      return 2;
    case "client":
      return 3;
    case "producer":
      return 4;
    case "consumer":
      return 5;
  }
}

function unixNanos(timestamp: string): string {
  return (BigInt(Date.parse(timestamp)) * 1_000_000n).toString();
}

function sanitizeFailure(operation: "export") {
  return <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, ReturnType<typeof exporterError>, R> =>
    effect.pipe(
      Effect.mapError(() => exporterError("otlp-http-json", operation, "OTLP request failed")),
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause)
          ? Effect.failCause(cause)
          : Effect.fail(exporterError("otlp-http-json", operation, "OTLP request failed")),
      ),
    );
}

function isHttpEndpoint(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      (url.protocol === "http:" || url.protocol === "https:") &&
      url.username === "" &&
      url.password === ""
    );
  } catch {
    return false;
  }
}
