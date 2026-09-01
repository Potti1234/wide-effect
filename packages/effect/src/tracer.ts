import {
  normalizeAttributes,
  normalizeValue,
  defaultNormalizationLimits,
  SegmentId,
  SpanId,
  TraceId,
  type FlowSpan,
  type FailureSummary,
  type LogLevel as CanonicalLogLevel,
  type SafeAttributes,
  type SafeValue,
  type SegmentId as SegmentIdType,
  type SpanId as SpanIdType,
  type SpanEvent,
  type SpanOutcome,
  type TraceId as TraceIdType,
} from "@wide-effect/core";
import { Cause, Context, Effect, Exit, Layer, Option, Schema, Tracer } from "effect";
import { ObservabilityConfig } from "./config.js";
import { promoteCorrelation } from "./correlation.js";
import { readFailureAnnotation } from "./failure.js";
import { SpanIdGenerator } from "./ids.js";
import { sanitizeSpanEvent } from "./log-event.js";
import { CompletedSpanSink, type CompletedSpanSinkService } from "./sink.js";

export const tracerLayer = Layer.effect(
  Tracer.Tracer,
  Effect.gen(function* () {
    const config = yield* ObservabilityConfig;
    const ids = yield* SpanIdGenerator;
    const sink = yield* CompletedSpanSink;
    return Tracer.make({
      span: (options) => new WideSpan(options, config, ids, sink),
    });
  }),
);

class WideSpan implements Tracer.Span {
  readonly _tag = "Span";
  readonly name: string;
  readonly spanId: SpanIdType;
  readonly traceId: TraceIdType;
  readonly segmentId: SegmentIdType;
  readonly parent: Option.Option<Tracer.AnySpan>;
  readonly annotations: Context.Context<never>;
  readonly sampled: boolean;
  readonly kind: Tracer.SpanKind;
  readonly attributes = new Map<string, SafeValue>();
  readonly links: Array<Tracer.SpanLink> = [];
  readonly events: Array<RawEvent> = [];
  status: Tracer.SpanStatus;
  private ended = false;
  private attributeBytes = 0;
  private eventBytes = 0;
  private linkBytes = 0;
  private droppedAttributes = 0;
  private droppedEvents = 0;
  private droppedLinks = 0;

  constructor(
    options: Parameters<Tracer.Tracer["span"]>[0],
    private readonly config: ObservabilityConfig["Service"],
    ids: SpanIdGenerator["Service"],
    private readonly sink: CompletedSpanSinkService,
  ) {
    this.name = options.name;
    this.parent = options.root ? Option.none() : snapshotParent(options.parent);
    this.annotations = options.annotations;
    this.sampled = options.sampled;
    this.kind = options.kind;
    this.spanId = ids.nextSpanId();
    this.traceId = Option.match(this.parent, {
      onNone: ids.nextTraceId,
      onSome: (parent) => Schema.decodeUnknownSync(TraceId)(parent.traceId),
    });
    this.segmentId =
      Option.isSome(this.parent) && this.parent.value instanceof WideSpan
        ? this.parent.value.segmentId
        : Schema.decodeUnknownSync(SegmentId)(this.spanId);
    this.status = { _tag: "Started", startTime: options.startTime };
    this.addLinks(options.links);
  }

  attribute(key: string, value: unknown): void {
    if (this.ended) return;
    const candidate = this.sanitizeAttributeValue(key, value);
    const sanitized = normalizeAttributes({ [key]: candidate }, this.config.attributePolicy, {
      ...defaultNormalizationLimits,
      maxEntries: defaultNormalizationLimits.maxEntries,
      maxBytes: Math.max(2, this.config.captureLimits.maxAttributeBytes),
    });
    if (!Object.hasOwn(sanitized, key)) return;
    const normalized = sanitized[key];
    if (normalized === undefined) return;
    const previous = this.attributes.get(key);
    const previousBytes = previous === undefined ? 0 : attributeEntryBytes(key, previous);
    const nextBytes = attributeEntryBytes(key, normalized);
    if (
      (previous === undefined &&
        this.attributes.size >= this.config.captureLimits.maxAttributeCount) ||
      this.attributeBytes - previousBytes + nextBytes > this.config.captureLimits.maxAttributeBytes
    ) {
      this.droppedAttributes++;
      return;
    }
    this.attributes.set(key, normalized);
    this.attributeBytes = this.attributeBytes - previousBytes + nextBytes;
  }

  private sanitizeAttributeValue(key: string, value: unknown): unknown {
    if (
      key !== "url.path" ||
      this.config.sanitizeHttpPath === undefined ||
      typeof value !== "string"
    )
      return value;
    try {
      return this.config.sanitizeHttpPath(value);
    } catch {
      return undefined;
    }
  }

  event(name: string, startTime: bigint, attributes: Record<string, unknown> = {}): void {
    if (!this.ended) {
      const sanitized = sanitizeSpanEvent(name, attributes, this.config.attributePolicy, {
        ...defaultNormalizationLimits,
        maxEntries: Math.max(
          this.config.captureLimits.maxEventAttributeCount,
          this.config.captureLimits.maxEventAttributeCount * 4,
        ),
        maxBytes: Math.max(2, this.config.captureLimits.maxEventAttributeBytes),
      });
      const boundedAttributes = Object.fromEntries(
        Object.entries(sanitized.attributes).slice(
          0,
          this.config.captureLimits.maxEventAttributeCount,
        ),
      );
      const safeEventName = safeName(sanitized.name, "event");
      const bytes = encodedBytes({ name: safeEventName, attributes: boundedAttributes });
      if (
        this.events.length >= this.config.captureLimits.maxEventCount ||
        this.eventBytes + bytes > this.config.captureLimits.maxEventBytes
      ) {
        this.droppedEvents++;
        return;
      }
      this.events.push({
        name: safeEventName,
        startTime,
        attributes: boundedAttributes,
        sequence: this.events.length,
      });
      this.eventBytes += bytes;
    }
  }

  addLinks(links: ReadonlyArray<Tracer.SpanLink>): void {
    if (this.ended) return;
    for (let index = 0; index < links.length; index++) {
      if (this.links.length >= this.config.captureLimits.maxLinkCount) {
        this.droppedLinks += links.length - index;
        return;
      }
      const normalized = safeLink(links[index]);
      if (normalized === undefined) {
        this.droppedLinks++;
        continue;
      }
      const bytes = encodedBytes({
        traceId: normalized.span.traceId,
        spanId: normalized.span.spanId,
        attributes: normalized.attributes,
      });
      if (this.linkBytes + bytes > this.config.captureLimits.maxLinkBytes) {
        this.droppedLinks += links.length - index;
        return;
      }
      this.links.push(normalized);
      this.linkBytes += bytes;
    }
  }

  end(endTime: bigint, exit: Exit.Exit<unknown, unknown>): void {
    if (this.ended) return;
    this.ended = true;
    const startTime = this.status.startTime;
    this.status = { _tag: "Ended", startTime, endTime, exit };
    try {
      const completed = this.complete(startTime, endTime, exit);
      this.sink.unsafeOffer(completed);
    } catch {
      // A telemetry sink is never allowed to alter the business Exit.
    }
  }

  private complete(
    startTimeNanos: bigint,
    endTimeNanos: bigint,
    exit: Exit.Exit<unknown, unknown>,
  ): FlowSpan {
    const rawAttributes = Object.fromEntries(this.attributes);
    const correlation = promoteCorrelation(rawAttributes);
    const explicitFailure = readFailureAnnotation(rawAttributes);
    const httpStatus = readHttpStatus(rawAttributes);
    const outcome = classifyOutcome(exit, this.kind, httpStatus, this.config.httpOutcomePolicy);
    const failure = failureSummary(exit, outcome, httpStatus, explicitFailure);
    if (this.droppedAttributes > 0)
      rawAttributes["wide.dropped.attributes"] = this.droppedAttributes;
    if (this.droppedEvents > 0) rawAttributes["wide.dropped.events"] = this.droppedEvents;
    if (this.droppedLinks > 0) rawAttributes["wide.dropped.links"] = this.droppedLinks;
    rawAttributes["effect.sampled"] = this.sampled;
    if (this.links.length > 0) {
      rawAttributes["effect.links"] = normalizeValue(
        this.links.map(({ attributes, span }) => ({
          traceId: span.traceId,
          spanId: span.spanId,
          attributes,
        })),
        {
          ...defaultNormalizationLimits,
          maxEntries: this.config.captureLimits.maxLinkCount * 8,
          maxBytes: Math.max(2, this.config.captureLimits.maxLinkBytes),
        },
      );
    }
    const parent = Option.getOrUndefined(this.parent);
    return {
      schemaVersion: 1,
      traceId: this.traceId,
      spanId: this.spanId,
      segmentId: this.segmentId,
      ...(parent === undefined
        ? {}
        : { parentSpanId: Schema.decodeUnknownSync(SpanId)(parent.spanId) }),
      ...correlation,
      service: this.config.service,
      operation: safeName(this.name, "unnamed-span"),
      kind: this.kind,
      outcome,
      ...(failure === undefined ? {} : { failure }),
      startTime: timestamp(startTimeNanos),
      endTime: timestamp(endTimeNanos),
      durationMs: durationMillis(startTimeNanos, endTimeNanos),
      attributes: normalizeAttributes(rawAttributes, this.config.attributePolicy, {
        ...defaultNormalizationLimits,
        maxEntries: 10_000,
        maxBytes: Math.max(
          2,
          this.config.captureLimits.maxAttributeBytes +
            this.config.captureLimits.maxLinkBytes +
            4_096,
        ),
      }),
      events: this.events
        .toSorted((left, right) =>
          left.startTime === right.startTime
            ? left.sequence - right.sequence
            : left.startTime < right.startTime
              ? -1
              : 1,
        )
        .map(normalizeEvent),
    };
  }
}

function failureSummary(
  exit: Exit.Exit<unknown, unknown>,
  outcome: SpanOutcome,
  httpStatus: number | undefined,
  explicit: Omit<FailureSummary, "type">,
): FailureSummary | undefined {
  if (Exit.isSuccess(exit))
    return outcome === "failure" && httpStatus !== undefined
      ? { type: "http", code: `http.${httpStatus}`, ...explicit }
      : undefined;
  const type = exit.cause.reasons.some(Cause.isDieReason)
    ? "defect"
    : exit.cause.reasons.some(Cause.isInterruptReason)
      ? "interruption"
      : "typed";
  const automaticCode = type === "typed" ? taggedFailureCode(exit.cause) : undefined;
  return {
    type,
    ...(automaticCode === undefined ? {} : { code: automaticCode }),
    ...explicit,
  };
}

function taggedFailureCode(cause: Cause.Cause<unknown>): string | undefined {
  const failure = cause.reasons.find(Cause.isFailReason);
  if (failure === undefined || failure.error === null || typeof failure.error !== "object")
    return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(failure.error, "_tag");
    if (
      descriptor === undefined ||
      !("value" in descriptor) ||
      typeof descriptor.value !== "string"
    )
      return undefined;
    return /^[A-Za-z][A-Za-z0-9_.:-]{0,63}$/.test(descriptor.value) ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

const decodeTraceId = Schema.decodeUnknownOption(TraceId);
const decodeSpanId = Schema.decodeUnknownOption(SpanId);

function snapshotParent(parent: Option.Option<Tracer.AnySpan>): Option.Option<Tracer.AnySpan> {
  if (Option.isNone(parent)) return Option.none();
  if (parent.value instanceof WideSpan) return parent;
  try {
    const traceId = decodeTraceId(parent.value.traceId);
    const spanId = decodeSpanId(parent.value.spanId);
    if (Option.isNone(traceId) || Option.isNone(spanId)) return Option.none();
    return Option.some(
      Tracer.externalSpan({
        traceId: traceId.value,
        spanId: spanId.value,
        sampled: parent.value.sampled,
      }),
    );
  } catch {
    return Option.none();
  }
}

function safeLink(link: Tracer.SpanLink | undefined): Tracer.SpanLink | undefined {
  if (link === undefined) return undefined;
  try {
    const parent = snapshotParent(Option.some(link.span));
    if (Option.isNone(parent)) return undefined;
    return {
      span: parent.value,
      // Link attributes are nested under the policy-approved `effect.links`
      // container, so retain every safe key while still dropping secrets.
      attributes: normalizeAttributes(link.attributes, { allowedPrefixes: [""] }),
    };
  } catch {
    return undefined;
  }
}

interface RawEvent {
  readonly name: string;
  readonly startTime: bigint;
  readonly attributes: SafeAttributes;
  readonly sequence: number;
}

function normalizeEvent(event: RawEvent): SpanEvent {
  const level = readLogLevel(event.attributes["effect.logLevel"]);
  return {
    name: event.name,
    timestamp: timestamp(event.startTime),
    ...(level === undefined ? {} : { level }),
    attributes: event.attributes,
  };
}

function readLogLevel(value: unknown): CanonicalLogLevel | undefined {
  if (typeof value !== "string") return undefined;
  const level = value.toLowerCase();
  return level === "trace" ||
    level === "debug" ||
    level === "info" ||
    level === "warn" ||
    level === "error" ||
    level === "fatal"
    ? level
    : undefined;
}

function classifyOutcome(
  exit: Exit.Exit<unknown, unknown>,
  kind: Tracer.SpanKind,
  httpStatus: number | undefined,
  policy: ObservabilityConfig["Service"]["httpOutcomePolicy"],
): SpanOutcome {
  if (Exit.isSuccess(exit)) {
    if (httpStatus === undefined) return "success";
    const ranges =
      kind === "server"
        ? policy.serverErrorRanges
        : kind === "client"
          ? policy.clientErrorRanges
          : [];
    return ranges.some((range) => httpStatus >= range.minimum && httpStatus <= range.maximum)
      ? "failure"
      : "success";
  }
  if (exit.cause.reasons.some(Cause.isDieReason)) return "defect";
  if (exit.cause.reasons.some(Cause.isInterruptReason)) return "interrupted";
  return "failure";
}

function readHttpStatus(attributes: Record<string, unknown>): number | undefined {
  const value = attributes["http.response.status_code"];
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599
    ? value
    : undefined;
}

function timestamp(nanos: bigint): string {
  const milliseconds = nanos / 1_000_000n;
  const minimum = -8_640_000_000_000_000n;
  const maximum = 8_640_000_000_000_000n;
  const bounded =
    milliseconds < minimum ? minimum : milliseconds > maximum ? maximum : milliseconds;
  return new Date(Number(bounded)).toISOString();
}

function durationMillis(start: bigint, end: bigint): number {
  const nanos = end > start ? end - start : 0n;
  const whole = nanos / 1_000_000n;
  const fraction = nanos % 1_000_000n;
  const value = Number(whole) + Number(fraction) / 1_000_000;
  return Number.isFinite(value) ? value : Number.MAX_VALUE;
}

function safeName(value: string, fallback: string): string {
  const normalized = String(normalizeValue(value))
    .replace(/[^\x20-\x7e]/g, "?")
    .trim()
    .slice(0, 256);
  return normalized || fallback;
}

function attributeEntryBytes(key: string, value: SafeValue): number {
  return encodedBytes({ [key]: value });
}

function encodedBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

export type { SafeAttributes };
