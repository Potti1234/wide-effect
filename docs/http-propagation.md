# HTTP propagation

`w3cServerTracer` validates W3C `traceparent` and `tracestate`, removes every inbound B3 header, and
then delegates the single server span to Effect's `HttpMiddleware.tracer`. Malformed or B3-only input
starts a fresh root. `withTraceStatePropagation` only injects the separately scoped tracestate value;
Effect's existing client creates the single client span and replaces `traceparent`. Effect rc.108 also
emits compact `b3` on outbound requests. Baggage is intentionally unsupported.

Browser propagation is explicit through `injectBrowserCarrier`; it does not patch `fetch`. Same-origin
requests are allowed by default. Cross-origin requests require an exact HTTP(S) origin in
`allowedOrigins`.

For cross-origin browser calls, configure the server to:

- allow request headers `traceparent` and `tracestate`;
- expose any response support identifier headers the application adds;
- include the exact calling origin in `Access-Control-Allow-Origin` (credentials cannot use `*`).

Do not use trace fields as authentication, authorization, tenancy, or actor identity.
