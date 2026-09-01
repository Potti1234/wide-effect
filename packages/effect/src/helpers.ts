import { normalizeAttributes, type LogLevel } from "@wide-effect/core";
import { Clock, Effect, Option, Schema, type Tracer } from "effect";
import { ObservabilityConfig } from "./config.js";
import { annotateFailure, type FailureAnnotation } from "./failure.js";

export const retentionAttributeKeys = {
  important: "wide.sampling.important",
  reason: "wide.sampling.reason",
  audit: "wide.audit",
} as const;

const RetentionReason = Schema.String.check(Schema.isMaxLength(128));

export interface WideSpanOptions<E> {
  readonly attributes?: Readonly<Record<string, unknown>>;
  readonly kind?: Tracer.SpanKind;
  readonly important?: boolean;
  readonly audit?: boolean;
  readonly reason?: string;
  readonly failure?: (error: E) => FailureAnnotation;
}

/** Creates exactly one Effect-owned span and enriches it without executing the workflow. */
export function withWideSpan<E = never>(operation: string, options: WideSpanOptions<E> = {}) {
  return <A, R>(effect: Effect.Effect<A, E, R>) => {
    const summarizeFailure = options.failure;
    return Effect.gen(function* () {
      if (options.attributes !== undefined) yield* annotate(options.attributes);
      if (options.audit) yield* markAudit(options.reason);
      else if (options.important) yield* markImportant(options.reason);
      return yield* effect.pipe(
        Effect.tapError((error) =>
          summarizeFailure === undefined
            ? Effect.void
            : Effect.suspend(() => annotateFailure(summarizeFailure(error))).pipe(
                Effect.catchDefect(() => Effect.void),
              ),
        ),
      );
    }).pipe(Effect.withSpan(operation, options.kind === undefined ? {} : { kind: options.kind }));
  };
}

export const annotate = Effect.fnUntraced(function* (
  attributes: Readonly<Record<string, unknown>>,
) {
  const config = yield* ObservabilityConfig;
  yield* Effect.annotateCurrentSpan(normalizeAttributes(attributes, config.attributePolicy));
});

export const event = Effect.fnUntraced(function* (
  name: string,
  attributes: Readonly<Record<string, unknown>> = {},
  level?: LogLevel,
) {
  const current = yield* Effect.option(Effect.currentSpan);
  if (Option.isNone(current)) return;
  const config = yield* ObservabilityConfig;
  const time = yield* Clock.currentTimeNanos;
  current.value.event(name, time, {
    ...normalizeAttributes(attributes, config.attributePolicy),
    ...(level === undefined ? {} : { "effect.logLevel": level.toUpperCase() }),
  });
});

export const markImportant = Effect.fnUntraced(function* (reason?: string) {
  const decodedReason = Schema.decodeUnknownOption(RetentionReason)(reason);
  yield* Effect.annotateCurrentSpan({
    [retentionAttributeKeys.important]: true,
    ...(Option.isSome(decodedReason)
      ? { [retentionAttributeKeys.reason]: decodedReason.value }
      : {}),
  });
});

export const markAudit = Effect.fnUntraced(function* (reason?: string) {
  yield* markImportant(reason);
  yield* Effect.annotateCurrentSpan(retentionAttributeKeys.audit, true);
});
