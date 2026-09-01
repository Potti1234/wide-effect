# Secure wide-event workflows

Create one span for each meaningful service hop. In Effect rc.108, a named `Effect.fn` is already a
traced boundary, so enrich it directly:

```ts
const claimRun = Effect.fn("worker.run.claim")(function* (runId: string) {
  yield* annotate({ "app.run.id": runId });
  yield* Effect.logInfo("worker.run.claimed").pipe(Effect.annotateLogs({ "app.run.id": runId }));
  return yield* claim(runId);
});
```

For a plain or untraced function, use `withWideSpan` instead. Never wrap the same named `Effect.fn`
boundary with it, because that creates two spans.

```ts
const loadAccount = (accountId: string) =>
  load(accountId).pipe(
    withWideSpan("account.load", {
      kind: "client",
      attributes: { "app.account.id": accountId },
      failure: () => ({
        category: "dependency",
        code: "account.unavailable",
        retryable: true,
      }),
    }),
  );
```

Business IDs are useful high-cardinality span attributes after validation and allowlisting. They must
not be metric labels. Use stable domain-qualified operation/event names; attach values with
`annotate`, `event`, or `Effect.annotateLogs`. `annotateFailure` records only a bounded category,
code, retryable flag, and optional single-line summary. The Effect exit remains the authority for
whether the span failed.

HTTP status and Effect outcome are independent. A handler may successfully return 400 while the span
outcome remains `success`. Configure `retainHttpStatuses: [400]` or bounded ranges when that response
is incident evidence. Configure `httpOutcomePolicy` separately only when a status should also classify
the span outcome as failed.

Use branded `InteractionId` for an explicitly propagated multi-hop support correlation and branded
`RequestId` for one server hop. `setInteractionId` and `setRequestId` enrich the current span;
`withInteractionId` scopes descendants. Neither value is identity or authorization evidence.

Supply environment context once through decoded service configuration:

```ts
configLayer({
  service: {
    name: "worker",
    version: "1.4.0",
    environment: "production",
    deploymentId: "worker-2026-09-01",
    commitHash: "a1b2c3d",
    region: "eu-central-1",
    instanceId: "worker-17",
  },
  attributePolicy: { allowedPrefixes: ["app."] },
});
```

Call `markImportant(reason)` for operational evidence or `markAudit(reason)` for an audit-retained
span. Both retain the coherent local segment through normal redaction, live bounds, buffering,
delivery, and exporter failure handling. A late child follows the cached segment decision and is
marked `wide.segment.late`; neither marker creates a second telemetry path.

Operators should inspect `Delivery.health.pipeline` for unique-span accounting and
`Delivery.health.exporters[name]` for per-exporter attempts, delivery, retries, terminal failures,
timeouts, circuit transitions, and lifecycle failures. The equivalent Effect metrics use bounded
state labels plus validated exporter names. A circuit permits one half-open probe only after cooldown
and after any timed-out fiber has actually completed.

Import the rotating NDJSON and OTLP adapters from `@wide-effect/effect/server`. Bound NDJSON active
bytes and retained count/bytes; choose fsync deliberately. ClickHouse table shape, credentials, TTL,
indexes, idempotence, alert cursors, and AI workflow state belong to the consumer. Persist only the
schema-decoded canonical `FlowSpan`, never a second unsanitized raw payload.

wide-effect never captures these by default:

- cookies, authorization headers, credentials, passwords, or tokens;
- prompts, model/tool output, terminal output, or source content;
- raw full URLs, queries, paths, or header containers;
- Effect causes, error messages, stacks, defects, or exporter response bodies;
- a Foldkit Model or arbitrary application state.

An authorized triage workflow may fetch bounded repository context later using an allowlisted
repository and commit. Source remains outside telemetry and diagnostic persistence.
