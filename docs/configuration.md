# API and configuration

Install only the packages needed by a runtime, together with the exact supported Effect/Foldkit peer
versions. Compose observability once at an application runtime root.

`@wide-effect/core` owns decoded wire and persistence contracts. Use `normalizeAttributes` with exact
allowed keys or application-owned prefixes before adding domain fields. Use `decodeFlowSpan` and
`decodeDiagnosticBundle` at external and persistence boundaries.

`@wide-effect/effect` provides `configLayer`, `idGeneratorLayer`, `tracerLayer`, `loggerLayer`, and a
`CompletedSpanSink`. A production composition normally routes the sink into `deliveryLayer`, which
owns bounded batching, sampling, retries, exporter isolation, flush, and shutdown. `annotate` and
`event` enrich the current Effect span; operation lifecycles should remain named Effect workflows and
spans rather than an ambient request object.

`configLayer` uses a deny-first HTTP attribute policy. Raw full URLs, queries, paths, and header
containers are excluded even if a broad custom prefix admits `url.*` or `http.*`. To record a client
path, pass a programmatic `sanitizeHttpPath` callback that maps it to a bounded route-like value; do
not source callbacks from external configuration.

The main Effect entry point is browser-safe. Import NDJSON and OTLP/HTTP exporters only from
`@wide-effect/effect/server`. Foldkit applications use `instrumentApplication`,
`registerDevToolsObserver`, and `makeTimelineBuffer`; see the package README for registration order.

Application configuration must supply service identity, environment, deployment/version, sampling,
delivery bounds, exporter destinations, and an attribute allowlist. Secrets and credentials belong in
consumer-owned configuration services and must never be attached as telemetry attributes.
