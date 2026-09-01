import { FailureSummary, type FailureSummary as FailureSummaryType } from "@wide-effect/core";
import { Effect, Option, Schema } from "effect";

export const failureAttributeKeys = {
  category: "wide.failure.category",
  code: "wide.failure.code",
  retryable: "wide.failure.retryable",
  summary: "wide.failure.summary",
} as const;

const FailureAnnotation = Schema.Struct({
  category: FailureSummary.fields.category,
  code: FailureSummary.fields.code,
  retryable: FailureSummary.fields.retryable,
  summary: FailureSummary.fields.summary,
});

export interface FailureAnnotation extends Schema.Schema.Type<typeof FailureAnnotation> {}

/** Adds explicitly sanitized failure metadata to the current Effect span, if one exists. */
export const annotateFailure = Effect.fnUntraced(function* (input: FailureAnnotation) {
  const decoded = Schema.decodeUnknownOption(FailureAnnotation)(input);
  if (Option.isNone(decoded)) return;
  const value = decoded.value;
  yield* Effect.annotateCurrentSpan({
    ...(value.category === undefined ? {} : { [failureAttributeKeys.category]: value.category }),
    ...(value.code === undefined ? {} : { [failureAttributeKeys.code]: value.code }),
    ...(value.retryable === undefined ? {} : { [failureAttributeKeys.retryable]: value.retryable }),
    ...(value.summary === undefined ? {} : { [failureAttributeKeys.summary]: value.summary }),
  });
});

export function readFailureAnnotation(
  attributes: Record<string, unknown>,
): Omit<FailureSummaryType, "type"> {
  const candidate = {
    ...(attributes[failureAttributeKeys.category] === undefined
      ? {}
      : { category: attributes[failureAttributeKeys.category] }),
    ...(attributes[failureAttributeKeys.code] === undefined
      ? {}
      : { code: attributes[failureAttributeKeys.code] }),
    ...(attributes[failureAttributeKeys.retryable] === undefined
      ? {}
      : { retryable: attributes[failureAttributeKeys.retryable] }),
    ...(attributes[failureAttributeKeys.summary] === undefined
      ? {}
      : { summary: attributes[failureAttributeKeys.summary] }),
  };
  for (const key of Object.values(failureAttributeKeys)) delete attributes[key];
  return Option.getOrElse(Schema.decodeUnknownOption(FailureAnnotation)(candidate), () => ({}));
}
