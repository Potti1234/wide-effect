# Security model

Telemetry is untrusted data. Decode external carriers, stored spans, uploaded diagnostics, and
exporter configuration before use. Trace and interaction identifiers are support correlation only;
they never establish a user, tenant, or authorization decision.

Redaction occurs before enqueueing. Attribute normalization rejects standard sensitive keys, avoids
invoking accessors, bounds recursion and encoded size, and replaces cycles or unsupported instances
with opaque summaries. Consumers still must allowlist application fields: do not collect cookies,
authorization headers, credentials, tokens, prompts, tool or terminal output, source content, or
arbitrary state.

Persisted Effect log events use a static semantic name such as `worker.lease.claimed` and structured
values supplied with `Effect.annotateLogs`. Names must be lower-case dot-separated segments and are
bounded to 128 characters; unsafe, prose, URL-shaped, multi-argument, or dynamic messages become
`effect.log`. Pretty-printed causes are discarded at the tracer boundary. An optional human logger
may still render the original application arguments to its own output, which is outside the
canonical telemetry contract and must be secured by the consuming application.

Foldkit capture never reads or exports the Model. It records approved Message/Command metadata in a
count-, byte-, and age-bounded timeline. Diagnostic upload endpoints remain consumer-owned and must
authenticate the caller, authorize trace access, enforce retention, decode the bundle, and apply a
response size limit.

Browser propagation defaults to same-origin. Additional HTTP(S) origins must be exact allowlist
entries; never patch global `fetch`. W3C baggage is unsupported. For collector credentials, prefer a
server-side OTLP adapter and ensure exporter errors cannot echo header values, URLs containing
credentials, response bodies, or transport causes.
