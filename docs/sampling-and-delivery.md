# Sampling and delivery

Instrumentation creates every meaningful span. Tail sampling decides only after completion, when
outcome and duration are known. Configured failures, slow operations, and important operations or
actors are retained; routine successful traces use a deterministic decision so tests and incident
comparisons are repeatable.

`deliveryLayer` uses a bounded queue and non-blocking offers. Choose `drop-newest` when preserving the
oldest accepted evidence matters, or `drop-oldest` when recent evidence is more useful. Configure a
finite batch size/age, retry count and backoff, attempt timeout, concurrency, and final flush timeout.
Retry state is bounded, and one failed or hung exporter is isolated from the others.

Capacity is an operational budget, not a reliability guarantee. Monitor accepted, sampled,
exported, retried, dropped, overflowed, and flush-timeout counts. Exporter failure never changes a
business Effect's result. Shutdown must close the scoped runtime and inspect its explicit flush
result; do not assume process exit drains telemetry.
