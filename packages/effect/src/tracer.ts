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
    this.parent = options.root ? Option.none() : validParent(options.parent);
    this.annotations = options.annotations;
    this.sampled = options.sampled;
    this.kind = options.kind;
    this.links = [...options.links];
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
    const completed = this.complete(startTime, endTime, exit);
    try {
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
      operation: this.name,
      kind: this.kind,
      outcome: classifyOutcome(exit),
      startTime: timestamp(startTimeNanos),
      endTime: timestamp(endTimeNanos),
      durationMs: Number(endTimeNanos - startTimeNanos) / 1_000_000,
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

function validParent(parent: Option.Option<Tracer.AnySpan>): Option.Option<Tracer.AnySpan> {
  return Option.filter(
    parent,
    (candidate) =>
      Option.isSome(decodeTraceId(candidate.traceId)) &&
      Option.isSome(decodeSpanId(candidate.spanId)),
  );
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
    name: String(normalizeValue(event.name)).slice(0, 256) || "event",
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
  return new Date(Number(nanos / 1_000_000n)).toISOString();
}

export type { SafeAttributes };
