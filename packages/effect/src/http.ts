import {
  extractTraceCarrier,
  formatTraceParent,
  InteractionId,
  RequestId,
  parseTraceStateHeader,
  type TraceCarrier,
  type InteractionId as InteractionIdType,
  type RequestId as RequestIdType,
  type TraceState,
} from "@wide-effect/core";
import { Context, Effect, Option, Schema } from "effect";
import { annotateInteractionId, annotateRequestId } from "./correlation.js";
import {
  Headers as EffectHeaders,
  HttpClient,
  HttpClientRequest,
  HttpMiddleware,
  HttpServerRequest,
} from "effect/unstable/http";

const correlationHeaders = [
  "traceparent",
  "tracestate",
  "b3",
  "x-b3-traceid",
  "x-b3-spanid",
  "x-b3-sampled",
  "x-b3-flags",
  "x-wide-interaction-id",
  "x-wide-request-id",
] as const;

export const CurrentTraceState = Context.Reference<Option.Option<TraceState>>(
  "@wide-effect/effect/CurrentTraceState",
  { defaultValue: Option.none },
);

/** Adds a validated, server-generated request ID; inbound request IDs are always discarded. */
export const w3cServerTracerWithRequestId = (
  nextRequestId: () => RequestIdType,
): typeof HttpMiddleware.tracer =>
  HttpMiddleware.make((app) =>
    w3cServerTracer(
      Effect.suspend(() => {
        let requestId: Option.Option<RequestIdType>;
        try {
          requestId = Schema.decodeUnknownOption(RequestId)(nextRequestId());
        } catch {
          requestId = Option.none();
        }
        return Option.isNone(requestId)
          ? app
          : annotateRequestId(requestId.value).pipe(Effect.andThen(app));
      }),
    ),
  );

/**
 * Validates W3C headers before delegating span ownership to Effect's built-in server tracer.
 * Invalid or B3-only input is stripped and therefore starts a fresh root. Baggage is ignored.
 */
export const w3cServerTracer: typeof HttpMiddleware.tracer = HttpMiddleware.make((app) =>
  Effect.flatMap(HttpServerRequest.HttpServerRequest, (request) => {
    const carrier = extractTraceCarrier(new Headers(request.headers));
    let headers = EffectHeaders.removeMany(request.headers, correlationHeaders);
    let traceState = Option.none<TraceState>();
    if (carrier.ok) {
      headers = EffectHeaders.set(
        headers,
        "traceparent",
        formatTraceParent(carrier.value.traceparent),
      );
      if (carrier.value.tracestate !== undefined) {
        headers = EffectHeaders.set(headers, "tracestate", carrier.value.tracestate);
        traceState = Option.some(carrier.value.tracestate);
      }
    }
    const normalizedRequest = preserveAdapterMetadata(request, request.modify({ headers }));
    const interactionId = Schema.decodeUnknownOption(InteractionId)(
      request.headers["x-wide-interaction-id"],
    );
    const correlatedApp = Option.isNone(interactionId)
      ? app
      : annotateInteractionId(interactionId.value).pipe(Effect.andThen(app));
    return HttpMiddleware.tracer(correlatedApp).pipe(
      Effect.provideService(HttpServerRequest.HttpServerRequest, normalizedRequest),
      Effect.provideService(CurrentTraceState, traceState),
    );
  }),
);

/**
 * Request adapters may attach transport state as own properties that are not part of the public
 * HttpServerRequest contract. `modify` intentionally returns a request view, so forward only
 * properties absent from that public view to keep the adapter responsible for its own metadata.
 */
function preserveAdapterMetadata(
  original: HttpServerRequest.HttpServerRequest,
  modified: HttpServerRequest.HttpServerRequest,
): HttpServerRequest.HttpServerRequest {
  return new Proxy(modified, {
    get(target, property) {
      return Reflect.has(target, property)
        ? Reflect.get(target, property, target)
        : Reflect.get(original, property, original);
    },
  });
}

/** Adds validated tracestate to Effect's existing client propagation without creating another span. */
export const withTraceStatePropagation = <E, R>(
  client: HttpClient.HttpClient.With<E, R>,
): HttpClient.HttpClient.With<E, R> =>
  HttpClient.mapRequestEffect(client, (request) =>
    Effect.map(CurrentTraceState, (state) =>
      Option.match(state, {
        onNone: () => HttpClientRequest.removeHeader(request, "tracestate"),
        onSome: (value) => HttpClientRequest.setHeader(request, "tracestate", value),
      }),
    ),
  );

/** Explicitly propagates untrusted interaction metadata; request IDs remain local to each hop. */
export const withInteractionPropagation = <E, R>(
  client: HttpClient.HttpClient.With<E, R>,
  interactionId: InteractionIdType,
): HttpClient.HttpClient.With<E, R> => {
  const decoded = Schema.decodeUnknownOption(InteractionId)(interactionId);
  return Option.isNone(decoded)
    ? client
    : HttpClient.mapRequest(client, (request) =>
        HttpClientRequest.setHeader(request, "x-wide-interaction-id", decoded.value),
      );
};

export interface BrowserOriginPolicy {
  readonly currentOrigin: string;
  readonly allowedOrigins?: ReadonlyArray<string>;
}

/**
 * Returns a new Request and never patches global fetch. Existing correlation is replaced.
 * Same-origin is allowed by default; additional destinations require an exact origin allowlist.
 */
export function injectBrowserCarrier(
  request: Request,
  carrier: TraceCarrier,
  policy: BrowserOriginPolicy,
  interactionId?: InteractionIdType,
): Request {
  const currentOrigin = parseOrigin(policy.currentOrigin);
  const destination = new URL(request.url).origin;
  const allowed =
    destination === currentOrigin ||
    (policy.allowedOrigins ?? []).some((origin) => parseOrigin(origin) === destination);
  const headers = new Headers(request.headers);
  for (const name of correlationHeaders) headers.delete(name);
  if (allowed) {
    headers.set("traceparent", formatTraceParent(carrier.traceparent));
    if (carrier.tracestate !== undefined) headers.set("tracestate", carrier.tracestate);
    const decodedInteractionId = Schema.decodeUnknownOption(InteractionId)(interactionId);
    if (Option.isSome(decodedInteractionId))
      headers.set("x-wide-interaction-id", decodedInteractionId.value);
  }
  return new Request(request, { headers });
}

export function readForwardedTraceState(headers: Headers): Option.Option<TraceState> {
  const parsed = parseTraceStateHeader(headers.get("tracestate") ?? undefined);
  return parsed.ok ? Option.some(parsed.value) : Option.none();
}

function parseOrigin(value: string): string {
  const url = new URL(value);
  if (url.origin === "null" || url.username !== "" || url.password !== "") {
    throw new TypeError("origin must be an absolute HTTP(S) origin");
  }
  if ((url.protocol !== "http:" && url.protocol !== "https:") || url.href !== `${url.origin}/`) {
    throw new TypeError("origin must not include a path, query, or fragment");
  }
  return url.origin;
}
