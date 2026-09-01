# Operations

Create one observability composition at each API or worker runtime root and close its Scope during
graceful shutdown. Use stable, domain-qualified operation and event names. Put request, trace, actor,
run, task, URL, and path values on spans, not metric labels.

Expose low-cardinality delivery health to the application's existing health system. Alert on sustained
overflow or export failure, not isolated retry noise. Treat an exporter outage as degraded
observability: requests continue, memory remains bounded, and shutdown terminates at its configured
limit.

Size segment staging from the documented limits: the hard ceiling is `maxTotalBytes` plus queue
capacity times the maximum encoded `FlowSpan`, decision-cache entries, and small map overhead. Monitor
`openSegments`, `segmentDrops`, `lateSpans`, overflow, and dropped-span totals without attaching trace
or segment IDs as metric labels.

The default active-span capture budget is 128 attributes/65,536 bytes, 256 events/262,144 cumulative
bytes with at most 32 attributes and 8,192 bytes per event, and 16 links/131,072 bytes. This is at
most 458,752 accepted telemetry bytes plus bounded object/array overhead per active span. Size
application concurrency so that this worst case and segment staging fit the process memory budget.
Rejected live entries increment `wide.dropped.attributes`, `wide.dropped.events`, or
`wide.dropped.links` on completion; these counters are reserved low-cardinality fields.

For incident lookup, store or return trace/support IDs separately from authentication. Persist the
versioned `FlowSpan` envelope only after decoding it, index consumer-owned lookup fields, and define
retention locally. The NDJSON exporter is append-only but does not promise `fsync`; the OTLP adapter is
server-only and sends canonical spans mapped to OTLP JSON.

Record deployment, commit, region, and instance metadata once in the runtime service configuration.
Use those resource fields for rollout correlation, never as unbounded metric labels.

Before release, run `bun run check`. This builds, tests, packs every package, installs the tarballs in
an isolated consumer, imports all public entry points, checks the browser graph, and verifies a single
Effect installation. `bun run release:publish` performs that gate before Changesets publishing, but
must only be invoked from an authenticated release environment after the example and consumer gates
pass.
