import { Effect, FileSystem } from "effect";
import {
  exporterError,
  makeExporterLifecycle,
  safeErrorMessage,
  serializeSpan,
  type SpanExporter,
  SpanExporterError,
} from "./exporter.js";

export interface NdjsonSpanExporterOptions {
  readonly path: string;
  readonly name?: string;
  readonly mode?: number;
}

/** Server-only serialized append exporter. Completion does not imply an fsync durability guarantee. */
export const makeNdjsonSpanExporter = (
  options: NdjsonSpanExporterOptions,
): Effect.Effect<SpanExporter, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const name = options.name ?? "ndjson";
    const lifecycle = yield* makeExporterLifecycle(name);
    return {
      name,
      export: (batch) =>
        lifecycle.run(
          "export",
          Effect.forEach(batch, serializeSpan).pipe(
            Effect.map((lines) => `${lines.join("\n")}\n`),
            Effect.flatMap((data) =>
              fileSystem.writeFileString(options.path, data, {
                flag: "a",
                mode: options.mode ?? 0o600,
              }),
            ),
            Effect.mapError((cause) =>
              cause instanceof SpanExporterError
                ? cause
                : exporterError(
                    name,
                    "export",
                    safeErrorMessage(cause, "filesystem append failed"),
                  ),
            ),
          ),
        ),
      flush: lifecycle.run("flush", Effect.void),
      shutdown: lifecycle.shutdown,
    };
  });
