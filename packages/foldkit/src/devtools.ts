import { isSafeAttributeKey, normalizeAttributes, type SafeAttributes } from "@wide-effect/core";
import { Effect, Stream, SubscriptionRef } from "effect";
import {
  __setDevToolsOverlay,
  latestEntryIndex,
  type CommandRecord,
  type DevToolsOverlay,
  type MountRecord,
} from "foldkit/devtools-host";
import type { TimelineBuffer } from "./timeline.js";
import type { InteractionRegistry } from "./application.js";

export interface DevToolsObserverOptions {
  readonly buffer: TimelineBuffer;
  readonly summarizeMessage?: (message: unknown, tag: string) => Readonly<Record<string, unknown>>;
  readonly summarizeCommand?: (command: CommandRecord) => Readonly<Record<string, unknown>>;
  readonly summarizeMount?: (mount: MountRecord) => Readonly<Record<string, unknown>>;
  readonly summarizeRouteTransition?: (
    message: unknown,
    tag: string,
  ) => Readonly<Record<string, unknown>> | undefined;
  readonly allowedSummaryPrefixes?: ReadonlyArray<string>;
  readonly excludeFromHistory?: ReadonlyArray<string>;
  readonly interactions?: InteractionRegistry;
}
/** Observes only newly appended public history entries and never reads Store model fields. */
export const makeDevToolsObserver =
  (options: DevToolsObserverOptions): DevToolsOverlay =>
  (store) =>
    Effect.gen(function* () {
      let observedThrough = latestEntryIndex(yield* SubscriptionRef.get(store.stateRef));
      yield* SubscriptionRef.changes(store.stateRef).pipe(
        Stream.runForEach((state) =>
          Effect.sync(() => {
            const end = latestEntryIndex(state);
            if (end <= observedThrough) return;
            const first = Math.max(observedThrough + 1, state.startIndex);
            for (let index = first; index <= end; index += 1) {
              const entry = state.entries[index - state.startIndex];
              if (entry === undefined || options.excludeFromHistory?.includes(entry.tag)) continue;
              const interactionId = options.interactions?.lookup(entry.message);
              const routeTransition = options.summarizeRouteTransition?.(entry.message, entry.tag);
              options.buffer.append({
                index,
                timestamp: entry.timestamp,
                messageTag: boundedName(entry.tag),
                ...(interactionId === undefined ? {} : { interactionId }),
                message: summarize(options.summarizeMessage?.(entry.message, entry.tag), options),
                ...(routeTransition === undefined
                  ? {}
                  : { routeTransition: summarize(routeTransition, options) }),
                commands: entry.commands.slice(0, 64).map((item) => ({
                  name: boundedName(item.name),
                  attributes: summarize(options.summarizeCommand?.(item), options),
                })),
                changedPaths: [...entry.diff.changedPaths].filter(isSafeAttributeKey).slice(0, 256),
                mountStarts: entry.mountStarts.slice(0, 64).map((item) => ({
                  name: boundedName(item.name),
                  attributes: summarize(options.summarizeMount?.(item), options),
                })),
                mountEnds: entry.mountEnds.slice(0, 64).map((item) => ({
                  name: boundedName(item.name),
                  attributes: summarize(options.summarizeMount?.(item), options),
                })),
              });
            }
            observedThrough = end;
          }),
        ),
      );
    });
export const composeDevToolsOverlays =
  (observer: DevToolsOverlay, visual?: DevToolsOverlay): DevToolsOverlay =>
  (store, position, mode, banner) =>
    Effect.all(
      [
        observer(store, position, mode, banner),
        ...(visual ? [visual(store, position, mode, banner)] : []),
      ],
      { concurrency: "unbounded", discard: true },
    );
/** Last registration wins in Foldkit 0.144; call after Vite registration. */
export const registerDevToolsObserver = (
  observer: DevToolsOverlay,
  visual?: DevToolsOverlay,
): void => {
  __setDevToolsOverlay(composeDevToolsOverlays(observer, visual));
};
const summarize = (
  value: Readonly<Record<string, unknown>> | undefined,
  options: DevToolsObserverOptions,
): SafeAttributes =>
  normalizeAttributes(value ?? {}, { allowedPrefixes: options.allowedSummaryPrefixes ?? [] });
const boundedName = (value: string): string =>
  value.length === 0 ? "Unknown" : [...value].slice(0, 256).join("");
