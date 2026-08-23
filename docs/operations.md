# Operations

Create one observability composition at each API or worker runtime root and close its Scope during
graceful shutdown. Use stable, domain-qualified operation and event names. Put request, trace, actor,
run, task, URL, and path values on spans, not metric labels.

Expose low-cardinality delivery health to the application's existing health system. Alert on sustained
overflow or export failure, not isolated retry noise. Treat an exporter outage as degraded
observability: requests continue, memory remains bounded, and shutdown terminates at its configured
limit.

For incident lookup, store or return trace/support IDs separately from authentication. Persist the
versioned `FlowSpan` envelope only after decoding it, index consumer-owned lookup fields, and define
retention locally. The NDJSON exporter is append-only but does not promise `fsync`; the OTLP adapter is
server-only and sends canonical spans mapped to OTLP JSON.

Before release, run `bun run check`. This builds, tests, packs every package, installs the tarballs in
an isolated consumer, imports all public entry points, checks the browser graph, and verifies a single
Effect installation. `bun run release:publish` performs that gate before Changesets publishing, but
must only be invoked from an authenticated release environment after the example and consumer gates
pass.
