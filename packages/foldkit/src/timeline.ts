import { SafeAttributes } from "@wide-effect/core";
import { Effect, Schema } from "effect";

const Name = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const SafePath = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
export const FoldkitCommandRecord = Schema.Struct({
  name: Name,
  attributes: SafeAttributes,
  outcome: Schema.optionalKey(Schema.Literals(["success", "failure", "interrupted"])),
  durationMs: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
});
export const FoldkitMountRecord = Schema.Struct({ name: Name, attributes: SafeAttributes });
export const FoldkitTimelineEntry = Schema.Struct({
  index: Schema.Number.check(Schema.isInt()),
  timestamp: Schema.Finite,
  messageTag: Name,
  interactionId: Schema.optionalKey(
    Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
  ),
  message: SafeAttributes,
  routeTransition: Schema.optionalKey(SafeAttributes),
  commands: Schema.Array(FoldkitCommandRecord).check(Schema.isMaxLength(64)),
  changedPaths: Schema.Array(SafePath).check(Schema.isMaxLength(256)),
  mountStarts: Schema.Array(FoldkitMountRecord).check(Schema.isMaxLength(64)),
  mountEnds: Schema.Array(FoldkitMountRecord).check(Schema.isMaxLength(64)),
});
export type FoldkitTimelineEntry = typeof FoldkitTimelineEntry.Type;
export const FoldkitTimeline = Schema.Array(FoldkitTimelineEntry);

export interface TimelineLimits {
  readonly maxEntries: number;
  readonly maxBytes: number;
  readonly maxAgeMs: number;
}
export const defaultTimelineLimits: TimelineLimits = {
  maxEntries: 100,
  maxBytes: 256 * 1024,
  maxAgeMs: 15 * 60 * 1_000,
};
export interface TimelineBuffer {
  /** Accepts only an already sanitized public timeline entry. */
  readonly append: (entry: FoldkitTimelineEntry) => void;
  readonly recordCommandOutcome: (outcome: FoldkitCommandOutcome) => void;
  readonly clear: () => void;
  readonly snapshot: (now?: number) => ReadonlyArray<FoldkitTimelineEntry>;
}
export interface FoldkitCommandOutcome {
  readonly interactionId: string;
  readonly name: string;
  readonly outcome: "success" | "failure" | "interrupted";
  readonly durationMs: number;
}

export const makeTimelineBuffer = (
  limits: TimelineLimits = defaultTimelineLimits,
): TimelineBuffer => {
  validateLimits(limits);
  let entries: ReadonlyArray<FoldkitTimelineEntry> = [];
  const prune = (now: number) => {
    entries = entries.filter((entry) => now - entry.timestamp <= limits.maxAgeMs);
    while (entries.length > limits.maxEntries || encodedBytes(entries) > limits.maxBytes)
      entries = entries.slice(1);
  };
  return {
    append: (entry) => {
      const decoded = Schema.decodeUnknownSync(FoldkitTimelineEntry, { onExcessProperty: "error" })(
        entry,
      );
      entries = [...entries, decoded];
      prune(decoded.timestamp);
    },
    recordCommandOutcome: (outcome) => {
      for (let entryIndex = entries.length - 1; entryIndex >= 0; entryIndex -= 1) {
        const entry = entries[entryIndex];
        if (entry?.interactionId !== outcome.interactionId) continue;
        const commandIndex = entry.commands.findIndex(
          (command) => command.name === outcome.name && command.outcome === undefined,
        );
        if (commandIndex < 0) continue;
        const commands = entry.commands.map((command, index) =>
          index === commandIndex
            ? { ...command, outcome: outcome.outcome, durationMs: Math.max(0, outcome.durationMs) }
            : command,
        );
        entries = entries.map((candidate, index) =>
          index === entryIndex ? { ...candidate, commands } : candidate,
        );
        prune(entry.timestamp);
        return;
      }
    },
    clear: () => {
      entries = [];
    },
    snapshot: (now = Date.now()) => {
      prune(now);
      return entries;
    },
  };
};

export interface TimelineTransport<E = never, R = never> {
  /** The transport can only receive the decoded, sanitized timeline contract. */
  readonly upload: (timeline: ReadonlyArray<FoldkitTimelineEntry>) => Effect.Effect<void, E, R>;
}
export const uploadTimeline = <E, R>(
  transport: TimelineTransport<E, R>,
  timeline: ReadonlyArray<FoldkitTimelineEntry>,
): Effect.Effect<void, E | Schema.SchemaError, R> =>
  Schema.decodeUnknownEffect(FoldkitTimeline)(timeline).pipe(
    Effect.flatMap((safe) => transport.upload(safe)),
  );

const encodedBytes = (value: unknown): number =>
  new TextEncoder().encode(JSON.stringify(value)).byteLength;
function validateLimits(limits: TimelineLimits): void {
  if (
    !Number.isInteger(limits.maxEntries) ||
    limits.maxEntries < 1 ||
    !Number.isInteger(limits.maxBytes) ||
    limits.maxBytes < 2 ||
    !Number.isFinite(limits.maxAgeMs) ||
    limits.maxAgeMs < 0
  )
    throw new RangeError("timeline limits must be positive and finite");
}
