import {
  decideSampling,
  type FlowSpan,
  type SamplingConfig,
  type SamplingDecision,
  type SegmentStagingConfig,
} from "@wide-effect/core";
import { Effect } from "effect";
import type { SpanBatch } from "./exporter.js";
import { retentionAttributeKeys } from "./helpers.js";

interface OpenSegment {
  readonly id: string;
  readonly traceId: string;
  readonly createdAt: number;
  rootCompletedAt?: number;
  spans: Array<FlowSpan>;
  bytes: number;
  droppedSpans: number;
}

interface CachedDecision {
  readonly keep: boolean;
  readonly dropKind?: "sampled" | "overflow";
  readonly expiresAt: number;
}

export interface SegmentStagingHealth {
  readonly record: (
    counter: "retained" | "sampledOut" | "overflowDropped" | "segmentDrops" | "lateSpans",
    amount?: number,
  ) => void;
}

export interface SegmentStager {
  readonly accept: (span: FlowSpan, now: number) => Effect.Effect<void>;
  readonly finalizeDue: (now: number) => Effect.Effect<void>;
  readonly finalizeAll: (now: number) => Effect.Effect<void>;
  readonly nextDeadline: (now: number) => number | undefined;
  readonly openCount: () => number;
  readonly stagedBytes: () => number;
}

export function makeSegmentStager(options: {
  readonly config: SegmentStagingConfig;
  readonly sampling: SamplingConfig;
  readonly deterministicValue?: ((span: FlowSpan) => number) | undefined;
  readonly emit: (batch: SpanBatch) => Effect.Effect<void>;
  readonly health: SegmentStagingHealth;
}): SegmentStager {
  const open = new Map<string, OpenSegment>();
  const decisions = new Map<string, CachedDecision>();
  let totalBytes = 0;

  const cacheDecision = (id: string, decision: CachedDecision) => {
    decisions.delete(id);
    decisions.set(id, decision);
    while (decisions.size > options.config.decisionCacheCapacity) {
      const oldest = decisions.keys().next().value;
      if (typeof oldest !== "string") break;
      decisions.delete(oldest);
    }
  };

  const purgeDecisions = (now: number) => {
    for (const [id, decision] of decisions) {
      if (decision.expiresAt <= now) decisions.delete(id);
    }
  };

  const finalize = (segment: OpenSegment, now: number): Effect.Effect<void> => {
    open.delete(segment.id);
    totalBytes -= segment.bytes;
    const ordered = orderSpans(segment.spans);
    const decision = decideSegment(options.sampling, ordered, options.deterministicValue);
    cacheDecision(segment.id, {
      keep: decision.keep,
      ...(decision.keep ? {} : { dropKind: "sampled" }),
      expiresAt: now + options.config.decisionCacheMaxAgeMs,
    });
    if (!decision.keep) {
      options.health.record("sampledOut", ordered.length);
      options.health.record("overflowDropped", segment.droppedSpans);
      return Effect.void;
    }
    const completed = annotateIncompleteRoot(ordered, segment.droppedSpans);
    options.health.record("retained", completed.length);
    options.health.record("overflowDropped", segment.droppedSpans);
    const [first, ...rest] = completed;
    return first === undefined ? Effect.void : options.emit([first, ...rest]);
  };

  const evictSegment = (segment: OpenSegment, now: number) => {
    open.delete(segment.id);
    totalBytes -= segment.bytes;
    options.health.record("overflowDropped", segment.spans.length + segment.droppedSpans);
    options.health.record("segmentDrops");
    cacheDecision(segment.id, {
      keep: false,
      dropKind: "overflow",
      expiresAt: now + options.config.decisionCacheMaxAgeMs,
    });
  };

  const makeRoomForSegment = (incomingCritical: boolean, now: number): boolean => {
    if (open.size < options.config.maxOpenSegments) return true;
    if (options.config.overflow === "drop-newest" && !incomingCritical) return false;
    const candidates = [...open.values()];
    const routine = candidates.find(
      (segment) => !segment.spans.some((span) => isRetainedEvidence(span, options.sampling)),
    );
    const victim =
      routine ?? (options.config.overflow === "drop-oldest" ? candidates[0] : undefined);
    if (victim === undefined) return false;
    evictSegment(victim, now);
    return true;
  };

  const accept = (span: FlowSpan, now: number): Effect.Effect<void> => {
    purgeDecisions(now);
    const id = span.segmentId ?? span.spanId;
    const cached = decisions.get(id);
    if (cached !== undefined) {
      options.health.record("lateSpans");
      if (!cached.keep) {
        options.health.record(cached.dropKind === "overflow" ? "overflowDropped" : "sampledOut");
        return Effect.void;
      }
      const late = markLate(span);
      options.health.record("retained");
      return options.emit([late]);
    }

    let segment = open.get(id);
    const bytes = encodedBytes(span);
    if (segment === undefined) {
      if (!makeRoomForSegment(isRetainedEvidence(span, options.sampling), now)) {
        options.health.record("overflowDropped");
        options.health.record("segmentDrops");
        cacheDecision(id, {
          keep: false,
          dropKind: "overflow",
          expiresAt: now + options.config.decisionCacheMaxAgeMs,
        });
        return Effect.void;
      }
      segment = {
        id,
        traceId: span.traceId,
        createdAt: now,
        spans: [],
        bytes: 0,
        droppedSpans: 0,
      };
      open.set(id, segment);
    }

    const root = span.spanId === id;
    const critical =
      root ||
      isRetainedEvidence(span, options.sampling) ||
      isAncestorOfRetained(span, segment.spans, options.sampling);
    const exceedsSegment =
      segment.spans.length >= options.config.maxSpansPerSegment ||
      segment.bytes + bytes > options.config.maxBytesPerSegment;
    const exceedsTotal = totalBytes + bytes > options.config.maxTotalBytes;
    if (exceedsSegment || exceedsTotal) {
      const replaceIndex = critical
        ? segment.spans.findIndex(
            (candidate) =>
              candidate.spanId !== id &&
              !isRetainedEvidence(candidate, options.sampling) &&
              !isAncestorOfRetained(candidate, segment.spans, options.sampling),
          )
        : -1;
      if (replaceIndex < 0) {
        segment.droppedSpans++;
        return Effect.void;
      }
      const removed = segment.spans[replaceIndex];
      if (removed === undefined) {
        segment.droppedSpans++;
        return Effect.void;
      }
      const removedBytes = encodedBytes(removed);
      segment.spans.splice(replaceIndex, 1);
      segment.bytes -= removedBytes;
      totalBytes -= removedBytes;
      segment.droppedSpans++;
    }
    if (
      segment.spans.length >= options.config.maxSpansPerSegment ||
      segment.bytes + bytes > options.config.maxBytesPerSegment ||
      totalBytes + bytes > options.config.maxTotalBytes
    ) {
      segment.droppedSpans++;
      return Effect.void;
    }
    segment.spans.push(span);
    segment.bytes += bytes;
    totalBytes += bytes;
    if (root) segment.rootCompletedAt = now;
    return Effect.void;
  };

  const finalizeDue = (now: number): Effect.Effect<void> =>
    Effect.forEach(
      [...open.values()].filter((segment) => deadline(segment, options.config) <= now),
      (segment) => finalize(segment, now),
      { discard: true },
    );

  const finalizeAll = (now: number): Effect.Effect<void> =>
    Effect.forEach([...open.values()], (segment) => finalize(segment, now), { discard: true });

  return {
    accept,
    finalizeDue,
    finalizeAll,
    nextDeadline: () => {
      let earliest: number | undefined;
      for (const segment of open.values()) {
        const value = deadline(segment, options.config);
        if (earliest === undefined || value < earliest) earliest = value;
      }
      return earliest;
    },
    openCount: () => open.size,
    stagedBytes: () => totalBytes,
  };
}

function deadline(segment: OpenSegment, config: SegmentStagingConfig): number {
  return Math.min(
    segment.createdAt + config.maxSegmentAgeMs,
    segment.rootCompletedAt === undefined
      ? Number.POSITIVE_INFINITY
      : segment.rootCompletedAt + config.rootGraceMs,
  );
}

function decideSegment(
  config: SamplingConfig,
  spans: ReadonlyArray<FlowSpan>,
  customDeterministicValue?: ((span: FlowSpan) => number) | undefined,
): SamplingDecision {
  for (const span of spans) {
    if (span.attributes[retentionAttributeKeys.important] === true)
      return { keep: true, reason: "important" };
    const status = httpStatus(span);
    const decision = decideSampling(
      { ...config, successRatio: 0 },
      {
        operation: span.operation,
        outcome: span.outcome,
        durationMs: span.durationMs,
        deterministicValue: 1,
        ...(status === undefined ? {} : { httpStatus: status }),
      },
    );
    if (decision.keep && decision.reason !== "sampled") return decision;
  }
  const representative =
    spans.find((span) => span.spanId === (span.segmentId ?? span.spanId)) ?? spans[0];
  if (representative === undefined) return { keep: false, reason: "dropped" };
  const value = normalizeRatio(
    customDeterministicValue?.(representative) ??
      deterministicRatio(
        `${representative.traceId}${representative.segmentId ?? representative.spanId}`,
      ),
  );
  return value < config.successRatio
    ? { keep: true, reason: "sampled" }
    : { keep: false, reason: "dropped" };
}

function isRetainedEvidence(span: FlowSpan, config: SamplingConfig): boolean {
  if (span.attributes[retentionAttributeKeys.important] === true) return true;
  const status = httpStatus(span);
  return decideSampling(
    { ...config, successRatio: 0 },
    {
      operation: span.operation,
      outcome: span.outcome,
      durationMs: span.durationMs,
      deterministicValue: 1,
      ...(status === undefined ? {} : { httpStatus: status }),
    },
  ).keep;
}

function isAncestorOfRetained(
  span: FlowSpan,
  spans: ReadonlyArray<FlowSpan>,
  config: SamplingConfig,
): boolean {
  const parents = new Set(
    spans
      .filter((candidate) => isRetainedEvidence(candidate, config))
      .flatMap((candidate) =>
        candidate.parentSpanId === undefined ? [] : [candidate.parentSpanId],
      ),
  );
  return parents.has(span.spanId);
}

function httpStatus(span: FlowSpan): number | undefined {
  const value = span.attributes["http.response.status_code"];
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599
    ? value
    : undefined;
}

function orderSpans(spans: ReadonlyArray<FlowSpan>): Array<FlowSpan> {
  return spans.toSorted((left, right) =>
    left.startTime === right.startTime
      ? left.endTime === right.endTime
        ? left.spanId.localeCompare(right.spanId)
        : left.endTime.localeCompare(right.endTime)
      : left.startTime.localeCompare(right.startTime),
  );
}

function annotateIncompleteRoot(spans: Array<FlowSpan>, dropped: number): Array<FlowSpan> {
  if (dropped === 0) return spans;
  const rootIndex = spans.findIndex((span) => span.spanId === (span.segmentId ?? span.spanId));
  if (rootIndex < 0) return spans;
  const root = spans[rootIndex];
  if (root === undefined) return spans;
  const updated = {
    ...root,
    attributes: { ...root.attributes, "wide.segment.dropped_spans": dropped },
  };
  const output = spans.slice();
  output[rootIndex] = updated;
  return output;
}

function markLate(span: FlowSpan): FlowSpan {
  return { ...span, attributes: { ...span.attributes, "wide.segment.late": true } };
}

function encodedBytes(span: FlowSpan): number {
  return new TextEncoder().encode(JSON.stringify(span)).byteLength;
}

function deterministicRatio(value: string): number {
  let hash = 2_166_136_261;
  for (const character of value) {
    hash ^= character.charCodeAt(0);
    hash = Math.imul(hash, 16_777_619);
  }
  return (hash >>> 0) / 0x1_0000_0000;
}

function normalizeRatio(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(1, Math.max(0, value));
}
