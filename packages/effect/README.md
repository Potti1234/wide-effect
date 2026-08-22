# @wide-effect/effect

Effect-native tracing and export primitives for wide-effect.

The main entry point is browser-safe and exports the exporter contract, fan-out composition,
in-memory exporter, and direct JSON console exporter. Import the append-only NDJSON adapter from
`@wide-effect/effect/server`; it requires Effect's `FileSystem` service.

Exporter calls accept non-empty completed-span batches. `export` and `flush` fail after shutdown,
while `shutdown` is idempotent. NDJSON writes complete at the `FileSystem.writeFileString` boundary,
so its `flush` operation has no additional buffered data to drain. Buffering, retries, and sampling
belong to the later delivery layer.
