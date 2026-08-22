import { Effect } from "effect";
import {
  exporterError,
  makeExporterLifecycle,
  safeErrorMessage,
  serializeSpan,
  type SpanExporter,
} from "./exporter.js";

export interface ConsoleSpanExporterOptions {
  readonly name?: string;
  /** Direct writer deliberately avoids Effect logging and recursive span events. */
  readonly write?: (line: string) => void;
}

export const makeConsoleSpanExporter = (
  options: ConsoleSpanExporterOptions = {},
): Effect.Effect<SpanExporter> =>
  Effect.gen(function* () {
    const name = options.name ?? "console";
    const write = options.write ?? ((line: string) => console.log(line));
    const lifecycle = yield* makeExporterLifecycle(name);
    return {
      name,
      export: (batch) =>
        lifecycle.ensureOpen("export").pipe(
          Effect.andThen(Effect.forEach(batch, serializeSpan)),
          Effect.flatMap((lines) =>
            Effect.try({
              try: () => lines.forEach(write),
              catch: (cause) =>
                exporterError(name, "export", safeErrorMessage(cause, "console write failed")),
            }),
          ),
        ),
      flush: lifecycle.ensureOpen("flush"),
      shutdown: lifecycle.shutdown,
    };
  });
