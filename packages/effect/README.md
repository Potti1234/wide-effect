# @wide-effect/effect

Effect-native tracing and export primitives for wide-effect.

The main entry point is browser-safe and exports the exporter contract, fan-out composition,
in-memory exporter, and direct JSON console exporter. Import the append-only NDJSON adapter from
`@wide-effect/effect/server`; it requires Effect's `FileSystem` service.

Exporter calls accept non-empty completed-span batches. Direct exporter calls and fan-out attempts are
bounded, `export` and `flush` fail after shutdown, and shutdown waits for admitted work up to its
configured bound. The in-memory exporter has a finite capacity and explicit overflow policy.

NDJSON appends are serialized and new files request owner-only mode (`0600`) on platforms that honor
POSIX modes. Windows access is governed by the file's ACL. Completion means the Effect `FileSystem`
append returned successfully; it is not an `fsync` durability guarantee. Buffering, retries, and tail
sampling belong to the delivery layer.
