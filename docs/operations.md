# Operations

Create one observability composition at each API or worker runtime root and close its Scope during
graceful shutdown. Use stable, domain-qualified operation and event names. Put request, trace, actor,
run, task, URL, and path values on spans, not metric labels.

Expose low-cardinality delivery health to the application's existing health system. Alert on sustained
overflow or export failure, not isolated retry noise. Treat an exporter outage as degraded
observability: requests continue, memory remains bounded, and shutdown terminates at its configured
limit.

`Delivery.health` is organized as one pipeline snapshot plus a bounded record keyed by validated,
configuration-time exporter names. Pipeline counters distinguish completed, accepted, retained,
sampled-out, overflow-dropped, and shutdown-dropped spans. Exporter counters distinguish attempts,
successful deliveries, retries, terminal failures, timeouts, circuit transitions, and flush/shutdown
failures. `retained` counts each unique span once; exporter span counts intentionally count fan-out.
Admission reconciles as `completed = accepted + admission overflow + shutdown rejection`; accepted
spans later become retained, sampled out, or staging-overflow dropped. During forced shutdown, retained
work that cannot finish additionally increments `shutdownDropped`.

The same deltas update Effect metrics named `wide_effect_delivery_pipeline_spans_total` and
`wide_effect_delivery_exporter_total`; queue depth, open segments, and staged bytes use gauges with
the corresponding `wide_effect_delivery_*` names. Labels are limited to fixed state values and the
validated exporter name. The synchronous tracer completion hook uses a small atomic `Ref` adapter;
all asynchronous staging, retry, and exporter transitions remain owned by the scoped consumer fiber.
Circuit transition counts include opening, entering half-open, reopening, and returning to closed.
An open circuit rejects a retained batch as terminally failed for that exporter; it does not affect
delivery to other exporters or the business operation.

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
retention locally. The server-only NDJSON exporter serializes ownership, rotates before a configured
byte or time limit, and deletes oldest archives to satisfy both count and byte retention. Every
complete line is independently schema-decodable. On restart, a non-empty active file is archived, so
a crash-damaged final line cannot contaminate new records; readers should ignore only an incomplete
final archive line. Rotation or retention failure fails that exporter operation without changing a
business Effect. `fsync` is opt-in and can materially reduce throughput. The OTLP adapter sends
canonical spans mapped to OTLP JSON.

Record deployment, commit, region, and instance metadata once in the runtime service configuration.
Use those resource fields for rollout correlation, never as unbounded metric labels.

Before release, run `bun run check`. This builds, tests, packs every package, installs the tarballs in
an isolated consumer, imports all public entry points, checks the browser graph, and verifies a single
Effect installation. `bun run release:publish` performs that gate before Changesets publishing, but
must only be invoked from an authenticated release environment after the example and consumer gates
pass.
