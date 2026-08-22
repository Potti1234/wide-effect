import type { FlowSpan } from "@wide-effect/core";
import { TraceAssertionError } from "./trace-tree.js";

export interface RedactionExpectation {
  readonly forbiddenValues: ReadonlyArray<string>;
  readonly forbiddenKey?: RegExp;
}

const defaultForbiddenKey =
  /(authorization|cookie|credential|password|secret|token|api.?key|private.?key|prompt|terminal|source|content|output)/i;

export function assertRedacted(
  spans: ReadonlyArray<FlowSpan>,
  expectation: RedactionExpectation,
): void {
  const forbiddenKey = expectation.forbiddenKey ?? defaultForbiddenKey;
  for (const span of spans) {
    const encoded = JSON.stringify(span);
    for (const forbidden of expectation.forbiddenValues) {
      if (forbidden.length > 0 && encoded.includes(forbidden)) {
        throw new TraceAssertionError(`${span.operation}: found forbidden value in completed span`);
      }
    }
    inspect(
      span.attributes,
      `${span.operation}.attributes`,
      forbiddenKey,
      expectation.forbiddenValues,
    );
    for (let index = 0; index < span.events.length; index++) {
      const event = span.events[index];
      if (!event) continue;
      inspect(
        event.attributes,
        `${span.operation}.events[${index}].attributes`,
        forbiddenKey,
        expectation.forbiddenValues,
      );
    }
  }
}

function inspect(
  value: unknown,
  path: string,
  forbiddenKey: RegExp,
  forbiddenValues: ReadonlyArray<string>,
): void {
  if (typeof value === "string") {
    for (const forbidden of forbiddenValues) {
      if (forbidden.length > 0 && value.includes(forbidden)) {
        throw new TraceAssertionError(`${path}: found forbidden value`);
      }
    }
    return;
  }
  if (value === null || typeof value !== "object") return;
  for (const [key, nested] of Object.entries(value)) {
    forbiddenKey.lastIndex = 0;
    if (forbiddenKey.test(key) && nested !== "[REDACTED]") {
      throw new TraceAssertionError(`${path}.${key}: sensitive key was not redacted`);
    }
    inspect(nested, `${path}.${key}`, forbiddenKey, forbiddenValues);
  }
}
