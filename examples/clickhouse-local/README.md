# ClickHouse local triage proof

This consumer-owned example maps schema-decoded canonical spans to typed ClickHouse query columns and
keeps the canonical envelope as `canonical_envelope`. It never stores a second unsanitized payload.
The exporter uses a static `INSERT ... FORMAT JSONEachRow` query and sends values in the request body;
credentials are headers and sanitized exporter errors never echo them.

The table uses `ReplacingMergeTree(ingested_at)` with stable span identity in the ordering key. This
is an at-least-once strategy, not an exactly-once claim: queries that require one physical version use
`FINAL` or `argMax`, while the alert workflow deduplicates by rule, trace, and span identity.
ClickHouse database policy, seven-day fixture TTL, credentials, indexes, polling, and ticket workflow
remain in this example rather than the reusable packages.

The alert proof decodes bounded rules containing values, never SQL fragments. It reads with a stable
`(ingested_at, span_id)` cursor and an explicit overlap window, then atomically claims the deterministic
`(rule_id, trace_id, span_id)` incident key. Duplicate rows, repeated polls, and concurrent pollers
therefore dispatch once. Its in-memory state fixture models `claimed`, `triaging`, `retry_wait`,
`ticketed`, and `needs_human`; a real consumer must put those claims and leases in its durable workflow
database. ClickHouse is evidence storage, not a mutable job queue.

The triage stage fetches same-trace envelopes within explicit time, count, and byte limits and decodes
each envelope again before building the shared `DiagnosticBundle`. Timeline ordering is deterministic.
Repository access is a separate authorized source request keyed by allowlisted repository and commit;
bounded filenames/symbols/patches are workflow inputs only and are never copied into `FlowSpan`,
ClickHouse, the diagnostic bundle, or the simulated ticket. Ticket analysis is marked as untrusted
automation and contains a trace query command instead of embedded raw telemetry.

Run deterministic checks without Docker:

```sh
bun run typecheck
bun run test
```

The Docker-backed chaos proof and query-plan commands are documented in the runbook added alongside
that opt-in script. ClickHouse binds to loopback and uses local-only credentials.
