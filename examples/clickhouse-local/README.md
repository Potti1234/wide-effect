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

Run deterministic checks without Docker:

```sh
bun run typecheck
bun run test
```

The Docker-backed chaos proof and query-plan commands are documented in the runbook added alongside
that opt-in script. ClickHouse binds to loopback and uses local-only credentials.
