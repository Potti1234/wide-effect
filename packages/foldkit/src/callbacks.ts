import type { Option } from "effect";
import type { Runtime } from "foldkit";

export type SanitizedRuntimeIncident =
  | Readonly<{ kind: "Crash"; messageTag?: string }>
  | Readonly<{
      kind: "Slow";
      phase: string;
      durationMs: number;
      thresholdMs: number;
      messageTag?: string;
    }>;
export const composeCrashReport =
  <Model, Message extends Readonly<{ _tag: string }>>(
    report: (incident: SanitizedRuntimeIncident) => void,
    delegate?: (context: Runtime.CrashContext<Model, Message>) => void,
  ) =>
  (context: Runtime.CrashContext<Model, Message>): void => {
    report({ kind: "Crash", ...messageTag(context.message) });
    delegate?.(context);
  };
export const composeSlowReport =
  <Model, Message extends Readonly<{ _tag: string }>>(
    report: (incident: SanitizedRuntimeIncident) => void,
    delegate?: (context: Runtime.SlowContext<Model, Message>) => void,
  ) =>
  (context: Runtime.SlowContext<Model, Message>): void => {
    report({
      kind: "Slow",
      phase: context._tag,
      durationMs: context.durationMs,
      thresholdMs: context.thresholdMs,
      ...slowMessageTag(context),
    });
    delegate?.(context);
  };
const messageTag = <Message extends Readonly<{ _tag: string }>>(
  message: Option.Option<Message>,
): Readonly<{ messageTag?: string }> =>
  message._tag === "Some" ? { messageTag: message.value._tag } : {};
const slowMessageTag = <Model, Message extends Readonly<{ _tag: string }>>(
  context: Runtime.SlowContext<Model, Message>,
): Readonly<{ messageTag?: string }> =>
  context._tag === "Update"
    ? { messageTag: context.message._tag }
    : context._tag === "SubscriptionDependencies"
      ? {}
      : messageTag(context.message);
