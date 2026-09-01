# Docker proof runbook

Prerequisites: Bun 1.3.14, Docker Engine 28+ with Compose v2, and free loopback port `18123`.
The Compose file binds ClickHouse only to `127.0.0.1`, uses fixture-only credentials, and creates the
isolated project `wide-effect-clickhouse-proof`.

Run `bun run example:clickhouse:docker`. The script starts the verified Compose project, applies only
`wide_effect_spans`, simulates an outage and recovery, inserts status/failure/slow/routine/late
fixtures plus a duplicate, runs the idempotent poller and bounded triage builder, prints row
counts/query plans/a redacted ticket, and always runs the exact project cleanup with volumes.

| Scenario               | Evidence                                            |
| ---------------------- | --------------------------------------------------- |
| Returned 400           | Typed status row and one simulated ticket           |
| Typed Effect failure   | Bounded failure columns                             |
| Slow success           | Duration evidence in the canonical row              |
| Routine success        | Deterministic sampling in delivery tests            |
| ClickHouse unavailable | Telemetry fails while `business-ok` is unchanged    |
| ClickHouse recovers    | Later insert/query succeeds                         |
| Duplicate retry        | ReplacingMergeTree identity plus one incident claim |
| Late child             | `wide.segment.late` in the canonical envelope       |
| Queue/staging overflow | Reconciled bounded delivery tests                   |
| Shutdown backlog       | Bounded shutdown/terminal-drop tests                |
| Secret input           | Adversarial mapping/export/triage tests             |

Do not reuse the fixture credentials or seven-day TTL in production.

## Observed local evidence

On 2026-09-01 with Docker Engine 28.3.2, Compose 2.38.2, Bun 1.3.14, and
`clickhouse/clickhouse-server:24.8-alpine`, the proof completed and cleaned up successfully:

- 5 physical rows and 5 distinct span identities after a duplicate returned-400 insert;
- exactly 1 simulated ticket and `outageObserved: true`;
- outage health: 1 retained and 1 terminally failed span, with 1 circuit transition;
- recovery health: 2 retained, 1 delivered, 1 terminally failed span, with 3 transitions;
- recent-status planning used time, partition, primary-key, and `status_minmax` indexes;
- trace lookup used the trace primary-key condition and `trace_bloom` skip index.

The redacted ticket contains service, operation, status, deployment, commit, a local trace query,
an untrusted-automation notice, and owner recommendation—no raw telemetry, source, prompt, tool output,
terminal output, or credentials.

Deliberately consumer-owned: table/TTL/index policy, credentials, insert idempotence queries, alert
rules, cursor/lookback, durable leases and workflow state, repository authorization, source selection,
prompt/model provider, ticket destination, and human approval.
