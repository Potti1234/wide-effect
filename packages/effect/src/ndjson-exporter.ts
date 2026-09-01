import { Clock, Duration, Effect, FileSystem, Ref, Schema } from "effect";
import {
  exporterError,
  makeExporterLifecycle,
  safeErrorMessage,
  serializeSpan,
  type SpanExporter,
  SpanExporterError,
} from "./exporter.js";

export interface NdjsonSpanExporterOptions {
  readonly path?: string;
  readonly directory?: string;
  readonly baseFilename?: string;
  readonly name?: string;
  readonly mode?: number;
  readonly directoryMode?: number;
  readonly maxFileBytes?: number;
  readonly rotationInterval?: Duration.Input;
  readonly maxRetainedFiles?: number;
  readonly maxRetainedBytes?: number;
  readonly fsync?: "never" | "on-flush" | "each-batch";
}

interface ResolvedOptions {
  readonly directory: string;
  readonly baseFilename: string;
  readonly path: string;
  readonly name: string;
  readonly mode: number;
  readonly directoryMode: number;
  readonly maxFileBytes: number;
  readonly rotationIntervalMs?: number;
  readonly maxRetainedFiles: number;
  readonly maxRetainedBytes: number;
  readonly fsync: "never" | "on-flush" | "each-batch";
}

interface WriterState {
  readonly initialized: boolean;
  readonly activeBytes: number;
  readonly openedAt: number;
  readonly sequence: number;
}

const DecodedOptions = Schema.Struct({
  directory: Schema.String,
  baseFilename: Schema.String,
  path: Schema.String,
  name: Schema.String,
  mode: Schema.Number,
  directoryMode: Schema.Number,
  maxFileBytes: Schema.Number,
  rotationIntervalMs: Schema.optional(Schema.Number),
  maxRetainedFiles: Schema.Number,
  maxRetainedBytes: Schema.Number,
  fsync: Schema.Literals(["never", "on-flush", "each-batch"]),
});

/** Server-only bounded NDJSON exporter with serialized writer ownership. */
export const makeNdjsonSpanExporter = (
  input: NdjsonSpanExporterOptions,
): Effect.Effect<SpanExporter, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const options = resolveOptions(input);
    const lifecycle = yield* makeExporterLifecycle(options.name);
    const state = yield* Ref.make<WriterState>({
      initialized: false,
      activeBytes: 0,
      openedAt: yield* Clock.currentTimeMillis,
      sequence: 0,
    });

    const initialize = Effect.gen(function* () {
      const current = yield* Ref.get(state);
      if (current.initialized) return;
      yield* fileSystem.makeDirectory(options.directory, {
        recursive: true,
        mode: options.directoryMode,
      });
      const exists = yield* fileSystem.exists(options.path);
      let next = { ...current, initialized: true, openedAt: yield* Clock.currentTimeMillis };
      if (exists) {
        const info = yield* fileSystem.stat(options.path);
        if (BigInt(info.size) > 0n) next = yield* rotate(fileSystem, options, next);
      }
      yield* Ref.set(state, next);
    });

    const syncActive = fileSystem.open(options.path, { flag: "a", mode: options.mode }).pipe(
      Effect.flatMap((file) => file.sync),
      Effect.scoped,
    );

    const mapFailure = (operation: "export" | "flush", cause: unknown) =>
      cause instanceof SpanExporterError
        ? cause
        : exporterError(
            options.name,
            operation,
            safeErrorMessage(cause, `filesystem ${operation} failed`),
          );

    return {
      name: options.name,
      export: (batch) =>
        lifecycle.run(
          "export",
          Effect.gen(function* () {
            yield* initialize;
            const lines = yield* Effect.forEach(batch, (span) =>
              serializeSpan(span).pipe(Effect.map((line) => `${line}\n`)),
            );
            for (const line of lines) {
              const bytes = encodedBytes(line);
              if (bytes > options.maxFileBytes)
                return yield* Effect.fail(
                  exporterError(options.name, "export", "serialized span exceeds maxFileBytes"),
                );
              let current = yield* Ref.get(state);
              const timeRotationDue =
                options.rotationIntervalMs !== undefined &&
                (yield* Clock.currentTimeMillis) - current.openedAt >= options.rotationIntervalMs;
              if (
                current.activeBytes > 0 &&
                (current.activeBytes + bytes > options.maxFileBytes || timeRotationDue)
              )
                current = yield* rotate(fileSystem, options, current);
              yield* fileSystem.writeFileString(options.path, line, {
                flag: "a",
                mode: options.mode,
              });
              yield* fileSystem.chmod(options.path, options.mode);
              yield* Ref.set(state, { ...current, activeBytes: current.activeBytes + bytes });
            }
            if (options.fsync === "each-batch") yield* syncActive;
          }).pipe(Effect.mapError((cause) => mapFailure("export", cause))),
        ),
      flush: lifecycle.run(
        "flush",
        initialize.pipe(
          Effect.andThen(options.fsync === "never" ? Effect.void : syncActive),
          Effect.mapError((cause) => mapFailure("flush", cause)),
        ),
      ),
      shutdown: lifecycle.shutdown,
    };
  });

function rotate(
  fileSystem: FileSystem.FileSystem,
  options: ResolvedOptions,
  state: WriterState,
): Effect.Effect<WriterState, unknown> {
  return Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const prefix = `${options.baseFilename}.${String(now).padStart(13, "0")}.`;
    const existingSequence = (yield* fileSystem.readDirectory(options.directory)).reduce(
      (maximum, entry) => {
        if (!entry.startsWith(prefix)) return maximum;
        const parsed = Number(entry.slice(prefix.length));
        return Number.isSafeInteger(parsed) ? Math.max(maximum, parsed) : maximum;
      },
      0,
    );
    const sequence = Math.max(state.sequence, existingSequence) + 1;
    const archive = joinPath(options.directory, `${prefix}${String(sequence).padStart(6, "0")}`);
    yield* fileSystem.rename(options.path, archive);
    yield* enforceRetention(fileSystem, options);
    return { initialized: true, activeBytes: 0, openedAt: now, sequence };
  });
}

function enforceRetention(
  fileSystem: FileSystem.FileSystem,
  options: ResolvedOptions,
): Effect.Effect<void, unknown> {
  return Effect.gen(function* () {
    const entries = (yield* fileSystem.readDirectory(options.directory))
      .filter((entry) => entry.startsWith(`${options.baseFilename}.`))
      .sort();
    const archives = yield* Effect.forEach(entries, (entry) => {
      const path = joinPath(options.directory, entry);
      return fileSystem.stat(path).pipe(Effect.map((info) => ({ path, size: Number(info.size) })));
    });
    let totalBytes = archives.reduce((total, archive) => total + archive.size, 0);
    let retained = archives.length;
    for (const archive of archives) {
      if (retained <= options.maxRetainedFiles && totalBytes <= options.maxRetainedBytes) break;
      yield* fileSystem.remove(archive.path);
      retained--;
      totalBytes -= archive.size;
    }
  });
}

function resolveOptions(input: NdjsonSpanExporterOptions): ResolvedOptions {
  const legacy = input.path === undefined ? undefined : splitPath(input.path);
  const directory = input.directory ?? legacy?.directory;
  const baseFilename = input.baseFilename ?? legacy?.baseFilename;
  if (directory === undefined || baseFilename === undefined)
    throw new RangeError("directory and baseFilename, or path, are required");
  const rotationIntervalMs =
    input.rotationInterval === undefined ? undefined : Duration.toMillis(input.rotationInterval);
  const maxFileBytes = input.maxFileBytes ?? 67_108_864;
  const maxRetainedFiles = input.maxRetainedFiles ?? 5;
  const decoded = Schema.decodeUnknownSync(DecodedOptions, { onExcessProperty: "error" })({
    directory,
    baseFilename,
    path: joinPath(directory, baseFilename),
    name: input.name ?? "ndjson",
    mode: input.mode ?? 0o600,
    directoryMode: input.directoryMode ?? 0o700,
    maxFileBytes,
    ...(rotationIntervalMs === undefined ? {} : { rotationIntervalMs }),
    maxRetainedFiles,
    maxRetainedBytes: input.maxRetainedBytes ?? maxFileBytes * maxRetainedFiles,
    fsync: input.fsync ?? "never",
  });
  if (decoded.directory.length === 0 || decoded.directory.includes("\0"))
    throw new RangeError("directory must be a non-empty filesystem path");
  if (
    !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(decoded.baseFilename) ||
    decoded.baseFilename === "." ||
    decoded.baseFilename === ".."
  )
    throw new RangeError("baseFilename must be a safe filename");
  for (const [name, value, minimum] of [
    ["maxFileBytes", decoded.maxFileBytes, 1],
    ["maxRetainedFiles", decoded.maxRetainedFiles, 0],
    ["maxRetainedBytes", decoded.maxRetainedBytes, 0],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < minimum)
      throw new RangeError(`${name} is outside its supported range`);
  }
  if (
    decoded.rotationIntervalMs !== undefined &&
    (!Number.isSafeInteger(decoded.rotationIntervalMs) || decoded.rotationIntervalMs < 1)
  )
    throw new RangeError("rotationInterval must be positive");
  if (!Number.isInteger(decoded.mode) || decoded.mode < 0 || decoded.mode > 0o777)
    throw new RangeError("mode must be a valid permission mask");
  if (
    !Number.isInteger(decoded.directoryMode) ||
    decoded.directoryMode < 0 ||
    decoded.directoryMode > 0o777
  )
    throw new RangeError("directoryMode must be a valid permission mask");
  return {
    directory: decoded.directory,
    baseFilename: decoded.baseFilename,
    path: decoded.path,
    name: decoded.name,
    mode: decoded.mode,
    directoryMode: decoded.directoryMode,
    maxFileBytes: decoded.maxFileBytes,
    ...(decoded.rotationIntervalMs === undefined
      ? {}
      : { rotationIntervalMs: decoded.rotationIntervalMs }),
    maxRetainedFiles: decoded.maxRetainedFiles,
    maxRetainedBytes: decoded.maxRetainedBytes,
    fsync: decoded.fsync,
  };
}

function splitPath(path: string): { readonly directory: string; readonly baseFilename: string } {
  const normalized = path.replace(/\\/g, "/");
  const separator = normalized.lastIndexOf("/");
  return separator < 0
    ? { directory: ".", baseFilename: normalized }
    : {
        directory: normalized.slice(0, separator) || "/",
        baseFilename: normalized.slice(separator + 1),
      };
}

function joinPath(directory: string, filename: string): string {
  return directory === "/" ? `/${filename}` : `${directory.replace(/[\\/]$/, "")}/${filename}`;
}

function encodedBytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}
