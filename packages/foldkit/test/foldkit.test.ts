import { expect } from "@effect/vitest";
import { describe, it } from "vitest";
import { Effect, HashMap, HashSet, Option, SubscriptionRef } from "effect";
import type { DevToolsStore, StoreState } from "foldkit/devtools-host";
import {
  composeCrashReport,
  composeDevToolsOverlays,
  injectCommandTraceHeaders,
  instrumentApplication,
  makeDevToolsObserver,
  makeInteractionRegistry,
  makeTimelineBuffer,
  uploadTimeline,
} from "../src/index.js";
import { parseSpanId, parseTraceId, type TraceCarrier } from "@wide-effect/core";

describe("Foldkit application instrumentation", () => {
  it("keeps update pure and annotates commands without changing their names", async () => {
    const message = { _tag: "Clicked", id: "safe", token: "never" } as const;
    let calls = 0;
    const original = {
      init: () => [0, []] as const,
      update: (model: number, received: typeof message) => {
        calls += 1;
        return [model + 1, [{ name: "Api.load", effect: Effect.succeed(received) }]] as const;
      },
    };
    const interactions = makeInteractionRegistry();
    const wrapped = instrumentApplication(original, {
      interactionId: () => "interaction-1",
      interactions,
      summarizeMessage: ({ id, token }) => ({ id, token }),
      allowedSummaryPrefixes: ["id", "token"],
    });
    const [next, commands] = wrapped.update(1, message);
    expect(next).toBe(2);
    expect(calls).toBe(1);
    expect(commands[0]?.name).toBe("Api.load");
    expect(interactions.lookup(message)).toBe("interaction-1");
    expect(await Effect.runPromise(commands[0]!.effect)).toBe(message);
  });
});

describe("sanitized timeline", () => {
  it("runs both the timeline observer and optional visual overlay", async () => {
    const stateRef = Effect.runSync(SubscriptionRef.make(state([])));
    const store = mockStore(stateRef);
    const mounted: string[] = [];
    const composite = composeDevToolsOverlays(
      () =>
        Effect.sync(() => {
          mounted.push("observer");
        }),
      () =>
        Effect.sync(() => {
          mounted.push("visual");
        }),
    );
    await Effect.runPromise(
      composite(store, "BottomRight", "TimeTravel", Option.none()).pipe(Effect.scoped),
    );
    expect(mounted.toSorted()).toEqual(["observer", "visual"]);
  });

  it("observes only new entries, exclusions and allowed summaries", async () => {
    const buffer = makeTimelineBuffer({ maxEntries: 5, maxBytes: 10_000, maxAgeMs: 1_000 });
    const initial = state([]);
    const stateRef = Effect.runSync(SubscriptionRef.make(initial));
    const store = mockStore(stateRef);
    const observer = makeDevToolsObserver({
      buffer,
      excludeFromHistory: ["Tick"],
      summarizeMessage: (message) => message as Record<string, unknown>,
      allowedSummaryPrefixes: ["id", "token"],
    });
    const program = Effect.gen(function* () {
      yield* observer(store, "BottomRight", "Inspect", Option.none()).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      yield* SubscriptionRef.set(
        stateRef,
        state([
          entry("Tick", { id: "skip" }, 10),
          entry("Clicked", { id: "kept", token: "secret" }, 11),
        ]),
      );
      yield* Effect.yieldNow;
    }).pipe(Effect.scoped);
    await Effect.runPromise(program);
    expect(buffer.snapshot(11)).toEqual([
      expect.objectContaining({ messageTag: "Clicked", message: { id: "kept" } }),
    ]);
  });

  it("enforces count, byte and age limits and uploads only decoded entries", async () => {
    const buffer = makeTimelineBuffer({ maxEntries: 2, maxBytes: 700, maxAgeMs: 10 });
    for (let index = 0; index < 3; index += 1) buffer.append(safeEntry(index, index));
    expect(buffer.snapshot(2).map(({ index }) => index)).toEqual([1, 2]);
    expect(buffer.snapshot(20)).toEqual([]);
    let uploaded: unknown;
    buffer.append(safeEntry(3, 20));
    buffer.append({
      ...safeEntry(4, 20),
      interactionId: "i-1",
      commands: [{ name: "Api.load", attributes: {} }],
    });
    buffer.recordCommandOutcome({
      interactionId: "i-1",
      name: "Api.load",
      outcome: "success",
      durationMs: 4,
    });
    expect(buffer.snapshot(20).at(-1)?.commands[0]).toMatchObject({
      outcome: "success",
      durationMs: 4,
    });
    await Effect.runPromise(
      uploadTimeline(
        {
          upload: (value) =>
            Effect.sync(() => {
              uploaded = value;
            }),
        },
        buffer.snapshot(20),
      ),
    );
    expect(uploaded).toEqual(buffer.snapshot(20));
    await expect(
      Effect.runPromise(
        uploadTimeline({ upload: () => Effect.fail("offline") }, buffer.snapshot(20)),
      ),
    ).rejects.toBe("offline");
  });
});

describe("safe runtime boundaries", () => {
  it("reports crashes without passing Model to the sanitized reporter", () => {
    const incidents: unknown[] = [];
    const report = composeCrashReport<object, { readonly _tag: string }>((incident) =>
      incidents.push(incident),
    );
    report({
      error: new Error("boom"),
      model: { secret: "model" },
      message: Option.some({ _tag: "Clicked" }),
    });
    expect(incidents).toEqual([{ kind: "Crash", messageTag: "Clicked" }]);
  });

  it("injects only for exact allowed origins", () => {
    const carrier: TraceCarrier = {
      traceparent: {
        version: "00",
        traceId: parseTraceId("a".repeat(32)),
        parentId: parseSpanId("b".repeat(16)),
        traceFlags: "01",
      },
    };
    const blocked = injectCommandTraceHeaders(
      new Request("https://api.example.test.evil/path"),
      carrier,
      { currentOrigin: "https://app.example.test", allowedOrigins: ["https://api.example.test"] },
    );
    const allowed = injectCommandTraceHeaders(
      new Request("https://api.example.test/path"),
      carrier,
      { currentOrigin: "https://app.example.test", allowedOrigins: ["https://api.example.test"] },
    );
    expect(blocked.headers.get("traceparent")).toBeNull();
    expect(allowed.headers.get("traceparent")).toContain("-bbbbbbbbbbbbbbbb-");
  });
});

const entry = (tag: string, message: unknown, timestamp: number) => ({
  tag,
  message,
  timestamp,
  commands: [],
  mountStarts: [],
  mountEnds: [],
  isModelChanged: true,
  diff: { changedPaths: HashSet.make("screen"), affectedPaths: HashSet.empty<string>() },
});
const state = (entries: ReadonlyArray<ReturnType<typeof entry>>): StoreState => ({
  entries,
  keyframes: HashMap.empty(),
  maybeInitModel: Option.none(),
  initCommands: [],
  initMountStarts: [],
  startIndex: 0,
  isPaused: false,
  pausedAtIndex: -1,
  maybeLatestModel: Option.none(),
});
const mockStore = (stateRef: DevToolsStore["stateRef"]): DevToolsStore => ({
  stateRef,
  recordInit: () => Effect.void,
  recordMessage: () => Effect.void,
  updateLatestModel: () => Effect.void,
  attachRenderedMounts: () => Effect.void,
  getModelAtIndex: () => Effect.succeed(undefined),
  getMessageAtIndex: () => Effect.succeed(Option.none()),
  getDiffAtIndex: () =>
    Effect.succeed({ changedPaths: HashSet.empty(), affectedPaths: HashSet.empty() }),
  jumpTo: () => Effect.succeed(undefined),
  resume: Effect.void,
  clear: Effect.void,
});
const safeEntry = (index: number, timestamp: number) => ({
  index,
  timestamp,
  messageTag: "Clicked",
  message: {},
  commands: [],
  changedPaths: [],
  mountStarts: [],
  mountEnds: [],
});
