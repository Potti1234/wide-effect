# Sampling and delivery

Instrumentation creates every meaningful span. Tail sampling decides only after completion, when
outcome and duration are known. Configured failures, slow operations, and important operations or
actors are retained; routine successful traces use a deterministic decision so tests and incident
comparisons are repeatable. `markImportant` and `markAudit` retain the entire local segment through
the same redaction, bounds, queue, and exporter path; their optional reason remains span evidence and
never becomes a metric label.

Each tracer-created span carries a local `segmentId`. A local root—or a span continued from an
external parent—starts a segment using its own span identity; descendants created by the same tracer
inherit it. Links never affect ownership. The field is additive in schema version 1 so older stored
envelopes without it continue to decode.

HTTP status is independent evidence. Successful server and client Effects are classified as failed
for 5xx responses by default; 4xx responses remain successful unless `httpOutcomePolicy` explicitly
includes them. Sampling can retain bounded exact `retainHttpStatuses` or merged
`retainHttpStatusRanges` independently, so a returned 400 can be kept with reason `http-status` even
when `successRatio` is zero. Only normalized integer statuses from 100 through 599 participate.

`deliveryLayer` uses a bounded queue and non-blocking offers. Choose `drop-newest` when preserving the
oldest accepted evidence matters, or `drop-oldest` when recent evidence is more useful. Configure a
finite batch size/age, retry count and backoff, attempt timeout, concurrency, and final flush timeout.
Retry state is bounded, and one failed or hung exporter is isolated from the others. Configure each
exporter's circuit with a positive failure threshold, finite open duration, and finite half-open probe
timeout. The circuit moves from closed through retrying to open, permits one probe after the cooldown,
and closes only after that probe succeeds. A failed probe reopens it.

A timed-out exporter fiber is interrupted and observed before any probe is allowed. If third-party
exporter code remains in an uninterruptible region, the circuit admits no further operation for that
exporter, preventing repeated fiber leaks while healthy exporters continue. In-process Effect code
cannot forcibly terminate such a fiber; use process or worker isolation when stronger containment is
required.

Capacity is an operational budget, not a reliability guarantee. Monitor the exact nested
`Delivery.health` pipeline and per-exporter counters rather than deriving unique-span totals from
fan-out attempts. Pipeline retention is counted once; exporter attempts, deliveries, retries, and
terminal failures are counted independently for every configured exporter. Exporter failure never
changes a business Effect's result. Shutdown must close the scoped runtime and inspect its explicit
flush result; do not assume process exit drains telemetry.

Delivery stages normalized spans by local segment and makes one aggregate decision. A retained
outcome, configured HTTP status, slow span, or important operation retains the ordered segment;
otherwise one deterministic trace-plus-segment decision applies to every span. Ordering is start
time, end time, then span ID. Root completion starts a bounded grace period, while missing roots close
at `maxSegmentAgeMs`.

The decoded `staging` budget includes maximum open segments, spans and bytes per segment, total staged
bytes, grace/age bounds, overflow policy, and a bounded expiring late-decision cache. Routine overflow
drops a whole segment. If critical evidence exceeds a per-segment limit, delivery retains the local
root, configured retained spans, and available ancestors where they fit, and adds
`wide.segment.dropped_spans` to the root. A late span follows the cached decision: kept segments export
it once with `wide.segment.late = true`; dropped segments discard it. Shutdown stops admission,
finalizes every open segment, then flushes exporters within the existing global timeout.
