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

`httpOutcomePolicy` contains bounded server and client error ranges. Both default to 500–599; adding
400–499 for clients or servers is an explicit semantic choice. Delivery sampling accepts exact
statuses and ranges, validates them through Effect Schema, removes duplicate exact statuses, and
merges overlapping or adjacent ranges.

Use `withInteractionId` for a local correlated workflow and pass a branded interaction ID explicitly
to browser or downstream HTTP propagation. Use `w3cServerTracerWithRequestId` with a server-owned
branded ID generator for per-hop request correlation. Malformed values are omitted rather than
making span completion fail.

The main Effect entry point is browser-safe. Import NDJSON and OTLP/HTTP exporters only from
`@wide-effect/effect/server`. Foldkit applications use `instrumentApplication`,
`registerDevToolsObserver`, and `makeTimelineBuffer`; see the package README for registration order.

Application configuration must supply service identity, environment, deployment/version, sampling,
delivery bounds, exporter destinations, and an attribute allowlist. Secrets and credentials belong in
consumer-owned configuration services and must never be attached as telemetry attributes.

Service metadata may additionally include bounded `commitHash`, `region`, and `instanceId` values.
Supply them through decoded runtime configuration, not per-request annotations. The OTLP adapter maps
these to resource attributes; instance identity remains span/resource evidence and must not become a
metric label.

`captureLimits` bounds each live span before completion: attribute count/bytes, event count/bytes,
per-event attributes/bytes, and link count/bytes. The decoded combined byte budget cannot exceed
900,000 bytes, leaving envelope and drop-counter headroom below the 1 MiB `FlowSpan` ceiling.
