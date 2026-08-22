import { parseSpanId, parseTraceId, type FlowSpan } from "@wide-effect/core";
import { expect } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { describe, it } from "@effect/vitest";
import {
  fanOutExporter,
  makeConsoleSpanExporter,
  makeInMemorySpanExporter,
  SpanExporterError,
  type SpanExporter,
} from "../src/index.js";
import { makeNdjsonSpanExporter } from "../src/server.js";

const span: FlowSpan = {
  schemaVersion: 1,
  traceId: parseTraceId("0123456789abcdef0123456789abcdef"),
  spanId: parseSpanId("0123456789abcdef"),
  service: { name: "test", version: "1", environment: "test" },
  operation: "Exporter.write",
  kind: "internal",
  outcome: "success",
  startTime: "2026-08-22T20:00:00.000Z",
  endTime: "2026-08-22T20:00:00.010Z",
  durationMs: 10,
  attributes: { "test.safe": "yes" },
  events: [],
};

describe("span exporters", () => {
  it.effect("isolates fan-out failures while reporting every failed child", () =>
    Effect.gen(function* () {
      const memory = yield* makeInMemorySpanExporter("healthy");
      let attempted = 0;
      const failed = failingExporter("failed", () => attempted++);
      const exporter = fanOutExporter("all", [failed, memory.exporter]);
      const exit = yield* Effect.exit(exporter.export([span]));
      expect(exit._tag).toBe("Failure");
      expect(attempted).toBe(1);
      expect(yield* memory.spans).toEqual([span]);
      const error = yield* Effect.flip(exporter.flush);
      expect(error).toBeInstanceOf(SpanExporterError);
      expect(error.failures.map((failure) => failure.exporter)).toEqual(["failed"]);
      const shutdownError = yield* Effect.flip(exporter.shutdown);
      expect(shutdownError.failures.map((failure) => failure.exporter)).toEqual(["failed"]);
      const closedHealthy = yield* Effect.flip(memory.exporter.export([span]));
      expect(closedHealthy.message).toBe("exporter is shut down");
    }),
  );

  it.effect(
    "bounds fan-out defects, synchronous throws, and hangs without leaking child errors",
    () =>
      Effect.gen(function* () {
        const malicious = new SpanExporterError({
          exporter: "evil\nexporter",
          operation: "export",
          message: "Bearer never-leak",
          failures: [],
        });
        const exporters: readonly [SpanExporter, ...ReadonlyArray<SpanExporter>] = [
          {
            name: "throws",
            export: () => {
              throw new Error("synchronous secret");
            },
            flush: Effect.void,
            shutdown: Effect.void,
          },
          {
            name: "defects",
            export: () => Effect.die("defect secret"),
            flush: Effect.void,
            shutdown: Effect.void,
          },
          {
            name: "hangs",
            export: () => Effect.never,
            flush: Effect.void,
            shutdown: Effect.void,
          },
          {
            name: "malicious",
            export: () => Effect.fail(malicious),
            flush: Effect.void,
            shutdown: Effect.void,
          },
        ];
        const error = yield* Effect.flip(
          fanOutExporter("all", exporters, { attemptTimeoutMs: 0 }).export([span]),
        );
        expect(error.failures).toHaveLength(4);
        expect(JSON.stringify(error)).not.toContain("never-leak");
        expect(JSON.stringify(error)).not.toContain("secret");
      }),
  );

  it.effect("bounds and validates in-memory storage", () =>
    Effect.gen(function* () {
      const memory = yield* makeInMemorySpanExporter({ capacity: 2, overflow: "drop-oldest" });
      yield* memory.exporter.export([
        span,
        { ...span, spanId: parseSpanId("1111111111111111") },
        { ...span, spanId: parseSpanId("2222222222222222") },
      ]);
      expect((yield* memory.spans).map((item) => item.spanId)).toEqual([
        parseSpanId("1111111111111111"),
        parseSpanId("2222222222222222"),
      ]);
      expect(yield* memory.dropped).toBe(1);
      const forged = { ...span, attributes: { authorization: "Bearer never-store" } } as FlowSpan;
      yield* Effect.flip(memory.exporter.export([forged]));
      expect(JSON.stringify(yield* memory.spans)).not.toContain("never-store");
    }),
  );

  it.effect("serializes schema-safe JSON without Effect logger recursion", () =>
    Effect.gen(function* () {
      const lines: Array<string> = [];
      const exporter = yield* makeConsoleSpanExporter({ write: (line) => lines.push(line) });
      yield* exporter.export([span]);
      expect(lines).toHaveLength(1);
      expect(JSON.parse(lines[0] ?? "")).toEqual(span);

      const forged = {
        ...span,
        attributes: { authorization: "Bearer never-export" },
      } as FlowSpan;
      const error = yield* Effect.flip(exporter.export([forged]));
      expect(error._tag).toBe("SpanExporterError");
      expect(JSON.stringify(error)).not.toContain("never-export");
      expect(lines.join("\n")).not.toContain("never-export");
    }),
  );

  it.effect("appends one NDJSON record per span and rejects writes after shutdown", () => {
    const writes: Array<{
      readonly path: string;
      readonly data: string;
      readonly options: { readonly flag?: FileSystem.OpenFlag; readonly mode?: number } | undefined;
    }> = [];
    const fileSystem = FileSystem.makeNoop({
      writeFileString: (path, data, options) =>
        Effect.sync(() => {
          writes.push({ path, data, options });
        }),
    });
    return Effect.gen(function* () {
      const exporter = yield* makeNdjsonSpanExporter({ path: "telemetry/spans.ndjson" });
      yield* exporter.export([span, { ...span, spanId: parseSpanId("fedcba9876543210") }]);
      yield* exporter.flush;
      expect(writes).toHaveLength(1);
      expect(writes[0]?.path).toBe("telemetry/spans.ndjson");
      expect(writes[0]?.options?.flag).toBe("a");
      expect(writes[0]?.options?.mode).toBe(0o600);
      const records = writes[0]?.data.trimEnd().split("\n").map(JSON.parse);
      expect(records).toHaveLength(2);
      expect(records?.[0]).toEqual(span);
      yield* exporter.shutdown;
      yield* exporter.shutdown;
      const error = yield* Effect.flip(exporter.export([span]));
      expect(error.operation).toBe("export");
      const flushError = yield* Effect.flip(exporter.flush);
      expect(flushError.operation).toBe("flush");
      expect(writes).toHaveLength(1);
    }).pipe(Effect.provideService(FileSystem.FileSystem, fileSystem));
  });

  it.effect("serializes concurrent NDJSON appends", () => {
    let active = 0;
    let maximumActive = 0;
    const writes: Array<string> = [];
    const fileSystem = FileSystem.makeNoop({
      writeFileString: (_path, data) =>
        Effect.gen(function* () {
          active++;
          maximumActive = Math.max(maximumActive, active);
          yield* Effect.yieldNow;
          writes.push(data);
          active--;
        }),
    });
    return Effect.gen(function* () {
      const exporter = yield* makeNdjsonSpanExporter({ path: "telemetry/spans.ndjson" });
      yield* Effect.all(
        [
          exporter.export([span]),
          exporter.export([{ ...span, spanId: parseSpanId("fedcba9876543210") }]),
        ],
        { concurrency: "unbounded" },
      );
      expect(maximumActive).toBe(1);
      expect(writes).toHaveLength(2);
      expect(writes.flatMap((data) => data.trimEnd().split("\n"))).toHaveLength(2);
    }).pipe(Effect.provideService(FileSystem.FileSystem, fileSystem));
  });
});

function failingExporter(name: string, attempted: () => void): SpanExporter {
  const failure = (operation: "export" | "flush" | "shutdown") =>
    Effect.sync(attempted).pipe(
      Effect.andThen(
        Effect.fail(
          new SpanExporterError({
            exporter: name,
            operation,
            message: "expected failure",
            failures: [],
          }),
        ),
      ),
    );
  return {
    name,
    export: () => failure("export"),
    flush: failure("flush"),
    shutdown: failure("shutdown"),
  };
}
