# @wide-effect/effect

Effect-native tracing and export primitives for wide-effect.

Use stable semantic log names and annotations for queryable span events:

```ts
Effect.logInfo("worker.run.claimed").pipe(Effect.annotateLogs({ "app.run.id": runId }));
```

The tracer replaces unsafe or dynamic log messages with `effect.log` and never persists Effect's
pretty-printed cause. A configured human logger still receives the original application message and
therefore remains an application-owned output boundary.

Use `annotateFailure` before a typed failure when automated triage needs an explicitly sanitized
category, code, retryability flag, or short single-line summary. The completed span derives the
failure type from the Effect `Exit`; raw failure messages, causes, defects, and stacks are not read.

Use `withWideSpan` when a workflow needs a new span plus initial safe attributes, a span kind, typed
failure summarization, or important/audit retention:

```ts
const claimRun = (runId: string) =>
  claim(runId).pipe(
    withWideSpan("worker.run.claim", {
      kind: "consumer",
      attributes: { "app.run.id": runId },
      failure: summarizeClaimFailure,
    }),
  );
```

In Effect rc.108, named `Effect.fn("operation")` already creates a traced function boundary. Do not
wrap that same boundary in `withWideSpan`, which would create two spans. Use one named `Effect.fn`
with `annotate`, `event`, `annotateFailure`, `markImportant`, or `markAudit` inside it, or use an
untraced/plain function with `withWideSpan`. All helpers are composable Effects and are safe when no
current span exists. `setInteractionId` and `setRequestId` accept branded correlation values; request
IDs remain local-hop evidence.

The main entry point is browser-safe and exports the exporter contract, fan-out composition,
in-memory exporter, and direct JSON console exporter. Import the append-only NDJSON and OTLP/HTTP JSON
adapters from `@wide-effect/effect/server`. NDJSON requires Effect's `FileSystem` service; OTLP
requires an Effect `HttpClient` and maps canonical completed spans without installing another tracer.

Exporter calls accept non-empty completed-span batches. Direct exporter calls and fan-out attempts are
bounded, `export` and `flush` fail after shutdown, and shutdown waits for admitted work up to its
configured bound. The in-memory exporter has a finite capacity and explicit overflow policy.

NDJSON writes are serialized and bounded by active-file and archive retention limits. New directories
request mode `0700` and files request `0600` on platforms that honor POSIX modes; Windows access is
governed by the file's ACL. `fsync` is configurable as `never`, `on-flush`, or `each-batch` and has a
corresponding latency cost. Buffering, retries, and tail sampling belong to the delivery layer.

The OTLP adapter accepts an HTTP(S) `/v1/traces` endpoint, optional static headers, request timeout,
and instrumentation scope metadata. Its errors are typed and sanitized: collector response bodies,
header values, endpoint credentials, and transport causes are never copied into exporter failures.
