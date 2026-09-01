import { Effect, Metric, MutableRef, Ref } from "effect";

export interface PipelineHealthSnapshot {
  readonly completed: number;
  readonly accepted: number;
  readonly retained: number;
  readonly sampledOut: number;
  readonly overflowDropped: number;
  readonly shutdownDropped: number;
  readonly queueDepth: number;
  readonly openSegments: number;
  readonly stagedBytes: number;
  readonly segmentDrops: number;
  readonly lateSpans: number;
}

export interface ExporterHealthSnapshot {
  readonly batchesAttempted: number;
  readonly spansAttempted: number;
  readonly batchesDelivered: number;
  readonly spansDelivered: number;
  readonly retryAttempts: number;
  readonly terminalFailedBatches: number;
  readonly terminalFailedSpans: number;
  readonly timeouts: number;
  readonly circuitTransitions: number;
  readonly flushFailures: number;
  readonly flushTimeouts: number;
  readonly shutdownFailures: number;
  readonly shutdownTimeouts: number;
}

export interface DeliveryHealthSnapshot {
  readonly accepting: boolean;
  readonly pipeline: PipelineHealthSnapshot;
  readonly exporters: Readonly<Record<string, ExporterHealthSnapshot>>;
}

export type PipelineCounter = Exclude<
  keyof PipelineHealthSnapshot,
  "queueDepth" | "openSegments" | "stagedBytes"
>;
export type ExporterCounter = keyof ExporterHealthSnapshot;

interface HealthState {
  readonly accepting: boolean;
  readonly pipeline: Omit<PipelineHealthSnapshot, "queueDepth" | "openSegments" | "stagedBytes">;
  readonly exporters: Readonly<Record<string, ExporterHealthSnapshot>>;
}

export interface DeliveryHealthTracker {
  readonly incrementPipeline: (counter: PipelineCounter, amount?: number) => void;
  readonly incrementExporter: (name: string, counter: ExporterCounter, amount?: number) => void;
  readonly setAccepting: (accepting: boolean) => void;
  readonly snapshot: (gauges: {
    readonly queueDepth: number;
    readonly openSegments: number;
    readonly stagedBytes: number;
  }) => Effect.Effect<DeliveryHealthSnapshot>;
}

const pipelineCounter = Metric.counter("wide_effect_delivery_pipeline_spans_total", {
  incremental: true,
});
const exporterCounter = Metric.counter("wide_effect_delivery_exporter_total", {
  incremental: true,
});
const queueDepthGauge = Metric.gauge("wide_effect_delivery_queue_depth");
const openSegmentsGauge = Metric.gauge("wide_effect_delivery_open_segments");
const stagedBytesGauge = Metric.gauge("wide_effect_delivery_staged_bytes");

export const makeDeliveryHealthTracker = (
  exporterNames: ReadonlyArray<string>,
): Effect.Effect<DeliveryHealthTracker> =>
  Effect.gen(function* () {
    const state = yield* Ref.make<HealthState>({
      accepting: true,
      pipeline: emptyPipeline(),
      exporters: Object.fromEntries(exporterNames.map((name) => [name, emptyExporter()])),
    });
    const metricContext = yield* Effect.context<never>();

    // Effect span completion calls CompletedSpanSink synchronously. This is the
    // deliberately small atomic bridge into Ref; the consumer fiber owns all
    // asynchronous staging, sampling, retry, and exporter transitions.
    const incrementPipeline = (counter: PipelineCounter, amount = 1) => {
      if (amount <= 0) return;
      MutableRef.update(state.ref, (current) => ({
        ...current,
        pipeline: { ...current.pipeline, [counter]: current.pipeline[counter] + amount },
      }));
      pipelineCounter
        .pipe(Metric.withAttributes({ state: pipelineMetricState(counter) }))
        .updateUnsafe(amount, metricContext);
    };

    const incrementExporter = (name: string, counter: ExporterCounter, amount = 1) => {
      if (amount <= 0) return;
      MutableRef.update(state.ref, (current) => {
        const exporter = current.exporters[name];
        if (exporter === undefined) return current;
        return {
          ...current,
          exporters: {
            ...current.exporters,
            [name]: { ...exporter, [counter]: exporter[counter] + amount },
          },
        };
      });
      exporterCounter
        .pipe(Metric.withAttributes({ exporter: name, state: counter }))
        .updateUnsafe(amount, metricContext);
    };

    return {
      incrementPipeline,
      incrementExporter,
      setAccepting: (accepting) =>
        MutableRef.update(state.ref, (current) => ({ ...current, accepting })),
      snapshot: (gauges) =>
        Effect.gen(function* () {
          yield* Metric.update(queueDepthGauge, gauges.queueDepth);
          yield* Metric.update(openSegmentsGauge, gauges.openSegments);
          yield* Metric.update(stagedBytesGauge, gauges.stagedBytes);
          const current = yield* Ref.get(state);
          return {
            accepting: current.accepting,
            pipeline: { ...current.pipeline, ...gauges },
            exporters: current.exporters,
          };
        }),
    };
  });

function emptyPipeline(): HealthState["pipeline"] {
  return {
    completed: 0,
    accepted: 0,
    retained: 0,
    sampledOut: 0,
    overflowDropped: 0,
    shutdownDropped: 0,
    segmentDrops: 0,
    lateSpans: 0,
  };
}

function emptyExporter(): ExporterHealthSnapshot {
  return {
    batchesAttempted: 0,
    spansAttempted: 0,
    batchesDelivered: 0,
    spansDelivered: 0,
    retryAttempts: 0,
    terminalFailedBatches: 0,
    terminalFailedSpans: 0,
    timeouts: 0,
    circuitTransitions: 0,
    flushFailures: 0,
    flushTimeouts: 0,
    shutdownFailures: 0,
    shutdownTimeouts: 0,
  };
}

function pipelineMetricState(counter: PipelineCounter): string {
  switch (counter) {
    case "completed":
      return "completed";
    case "accepted":
      return "accepted";
    case "retained":
      return "retained";
    case "sampledOut":
      return "sampled_out";
    case "overflowDropped":
      return "overflow_dropped";
    case "shutdownDropped":
      return "shutdown_dropped";
    case "segmentDrops":
      return "segment_drops";
    case "lateSpans":
      return "late_spans";
  }
}
