# @wide-effect/effect

Effect-native tracing and export primitives for wide-effect.

Use stable semantic log names and annotations for queryable span events:

```ts
Effect.logInfo("worker.run.claimed").pipe(Effect.annotateLogs({ "app.run.id": runId }));
```

The tracer replaces unsafe or dynamic log messages with `effect.log` and never persists Effect's
pretty-printed cause. A configured human logger still receives the original application message and
therefore remains an application-owned output boundary.

The main entry point is browser-safe and exports the exporter contract, fan-out composition,
in-memory exporter, and direct JSON console exporter. Import the append-only NDJSON and OTLP/HTTP JSON
adapters from `@wide-effect/effect/server`. NDJSON requires Effect's `FileSystem` service; OTLP
requires an Effect `HttpClient` and maps canonical completed spans without installing another tracer.

Exporter calls accept non-empty completed-span batches. Direct exporter calls and fan-out attempts are
bounded, `export` and `flush` fail after shutdown, and shutdown waits for admitted work up to its
configured bound. The in-memory exporter has a finite capacity and explicit overflow policy.

NDJSON appends are serialized and new files request owner-only mode (`0600`) on platforms that honor
POSIX modes. Windows access is governed by the file's ACL. Completion means the Effect `FileSystem`
append returned successfully; it is not an `fsync` durability guarantee. Buffering, retries, and tail
sampling belong to the delivery layer.

The OTLP adapter accepts an HTTP(S) `/v1/traces` endpoint, optional static headers, request timeout,
and instrumentation scope metadata. Its errors are typed and sanitized: collector response bodies,
header values, endpoint credentials, and transport causes are never copied into exporter failures.
