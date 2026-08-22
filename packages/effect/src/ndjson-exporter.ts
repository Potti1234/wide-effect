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

/** Server-only append exporter. Each successful export is durable at the FileSystem boundary. */
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
        lifecycle.ensureOpen("export").pipe(
          Effect.andThen(Effect.forEach(batch, serializeSpan)),
          Effect.map((lines) => `${lines.join("\n")}\n`),
          Effect.flatMap((data) =>
            fileSystem.writeFileString(options.path, data, {
              flag: "a",
              ...(options.mode === undefined ? {} : { mode: options.mode }),
            }),
          ),
          Effect.mapError((cause) =>
            cause instanceof SpanExporterError
              ? cause
              : exporterError(name, "export", safeErrorMessage(cause, "filesystem append failed")),
          ),
        ),
      flush: lifecycle.ensureOpen("flush"),
      shutdown: lifecycle.shutdown,
    };
  });
