import {
  extractTraceCarrier,
  formatTraceParent,
  parseTraceStateHeader,
  type TraceCarrier,
  type TraceState,
} from "@wide-effect/core";
import { Context, Effect, Option } from "effect";
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
] as const;

export const CurrentTraceState = Context.Reference<Option.Option<TraceState>>(
  "@wide-effect/effect/CurrentTraceState",
  { defaultValue: Option.none },
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
    return HttpMiddleware.tracer(app).pipe(
      Effect.provideService(HttpServerRequest.HttpServerRequest, request.modify({ headers })),
      Effect.provideService(CurrentTraceState, traceState),
    );
  }),
);

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
