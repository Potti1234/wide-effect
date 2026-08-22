import {
  normalizeAttributes,
  normalizeValue,
  SpanId,
  TraceId,
  type FlowSpan,
  type LogLevel as CanonicalLogLevel,
  type SafeAttributes,
  type SpanId as SpanIdType,
  type SpanEvent,
  type SpanOutcome,
  type TraceId as TraceIdType,
} from "@wide-effect/core";
import { Cause, Context, Effect, Exit, Layer, Option, Schema, Tracer } from "effect";
import { ObservabilityConfig } from "./config.js";
import { SpanIdGenerator } from "./ids.js";
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
  readonly parent: Option.Option<Tracer.AnySpan>;
  readonly annotations: Context.Context<never>;
  readonly sampled: boolean;
  readonly kind: Tracer.SpanKind;
  readonly attributes = new Map<string, unknown>();
  readonly links: Array<Tracer.SpanLink>;
  readonly events: Array<RawEvent> = [];
  status: Tracer.SpanStatus;
  private ended = false;

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
    this.links = safeLinks(options.links);
    this.spanId = ids.nextSpanId();
    this.traceId = Option.match(this.parent, {
      onNone: ids.nextTraceId,
      onSome: (parent) => Schema.decodeUnknownSync(TraceId)(parent.traceId),
    });
    this.status = { _tag: "Started", startTime: options.startTime };
  }

  attribute(key: string, value: unknown): void {
    if (!this.ended) this.attributes.set(key, value);
  }

  event(name: string, startTime: bigint, attributes: Record<string, unknown> = {}): void {
    if (!this.ended)
      this.events.push({ name, startTime, attributes, sequence: this.events.length });
  }

  addLinks(links: ReadonlyArray<Tracer.SpanLink>): void {
    if (!this.ended) this.links.push(...links);
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
    rawAttributes["effect.sampled"] = this.sampled;
    if (this.links.length > 0) {
      rawAttributes["effect.links"] = this.links.map(({ attributes, span }) => ({
        traceId: span.traceId,
        spanId: span.spanId,
        attributes,
      }));
    }
    const parent = Option.getOrUndefined(this.parent);
    return {
      schemaVersion: 1,
      traceId: this.traceId,
      spanId: this.spanId,
      ...(parent === undefined
        ? {}
        : { parentSpanId: Schema.decodeUnknownSync(SpanId)(parent.spanId) }),
      service: this.config.service,
      operation: safeName(this.name, "unnamed-span"),
      kind: this.kind,
      outcome: classifyOutcome(exit),
      startTime: timestamp(startTimeNanos),
      endTime: timestamp(endTimeNanos),
      durationMs: durationMillis(startTimeNanos, endTimeNanos),
      attributes: normalizeAttributes(rawAttributes, this.config.attributePolicy),
      events: this.events
        .toSorted((left, right) =>
          left.startTime === right.startTime
            ? left.sequence - right.sequence
            : left.startTime < right.startTime
              ? -1
              : 1,
        )
        .slice(0, 256)
        .map((event) => normalizeEvent(event, this.config.attributePolicy)),
    };
  }
}

const decodeTraceId = Schema.decodeUnknownOption(TraceId);
const decodeSpanId = Schema.decodeUnknownOption(SpanId);

function snapshotParent(parent: Option.Option<Tracer.AnySpan>): Option.Option<Tracer.AnySpan> {
  if (Option.isNone(parent)) return Option.none();
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

function safeLinks(links: ReadonlyArray<Tracer.SpanLink>): Array<Tracer.SpanLink> {
  const output: Array<Tracer.SpanLink> = [];
  try {
    for (const link of links.slice(0, 128)) {
      const parent = snapshotParent(Option.some(link.span));
      if (Option.isSome(parent)) output.push({ span: parent.value, attributes: link.attributes });
    }
  } catch {
    return output;
  }
  return output;
}

interface RawEvent {
  readonly name: string;
  readonly startTime: bigint;
  readonly attributes: Record<string, unknown>;
  readonly sequence: number;
}

function normalizeEvent(
  event: RawEvent,
  policy: ObservabilityConfig["Service"]["attributePolicy"],
): SpanEvent {
  const level = readLogLevel(event.attributes["effect.logLevel"]);
  return {
    name: safeName(event.name, "event"),
    timestamp: timestamp(event.startTime),
    ...(level === undefined ? {} : { level }),
    attributes: normalizeAttributes(event.attributes, policy),
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

function classifyOutcome(exit: Exit.Exit<unknown, unknown>): SpanOutcome {
  if (Exit.isSuccess(exit)) return "success";
  if (exit.cause.reasons.some(Cause.isDieReason)) return "defect";
  if (exit.cause.reasons.some(Cause.isInterruptReason)) return "interrupted";
  return "failure";
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

export type { SafeAttributes };
