import { type FlowSpan } from "@wide-effect/core";
import { exporterError, makeExporterLifecycle, type SpanExporter } from "@wide-effect/effect";
import { Clock, Effect, Option, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";
import { mapClickHouseRow } from "./mapping.js";

const Config = Schema.Struct({
  endpoint: Schema.String.check(
    Schema.makeFilter((value) => /^https?:\/\/[^\s]+$/.test(value) || "invalid endpoint"),
  ),
  username: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
  password: Schema.String.check(Schema.isMaxLength(8_192)),
  timeoutMs: Schema.Finite.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(300_000)),
});

export interface ClickHouseExporterOptions {
  readonly endpoint: string;
  readonly username: string;
  readonly password: string;
  readonly timeoutMs?: number;
}

const insertQuery = "INSERT INTO wide_effect_spans FORMAT JSONEachRow";

export const makeClickHouseSpanExporter = (
  input: ClickHouseExporterOptions,
): Effect.Effect<SpanExporter, ReturnType<typeof exporterError>, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const config = yield* Schema.decodeUnknownEffect(Config)({
      ...input,
      timeoutMs: input.timeoutMs ?? 10_000,
    }).pipe(
      Effect.mapError(() =>
        exporterError("clickhouse-local", "export", "invalid ClickHouse configuration"),
      ),
    );
    const client = yield* HttpClient.HttpClient;
    const lifecycle = yield* makeExporterLifecycle("clickhouse-local");
    return {
      name: "clickhouse-local",
      export: (batch) =>
        lifecycle.run(
          "export",
          Effect.gen(function* () {
            const ingestedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            const rows = yield* Effect.forEach(batch, (span: FlowSpan) =>
              mapClickHouseRow(span, ingestedAt),
            );
            const endpoint = new URL(config.endpoint);
            endpoint.searchParams.set("query", insertQuery);
            let request = HttpClientRequest.post(endpoint.toString()).pipe(
              HttpClientRequest.setHeader("x-clickhouse-user", config.username),
              HttpClientRequest.setHeader("x-clickhouse-key", config.password),
              HttpClientRequest.setHeader("content-type", "application/x-ndjson"),
            );
            request = HttpClientRequest.bodyText(
              request,
              `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`,
            );
            const response = yield* client
              .execute(request)
              .pipe(
                Effect.withTracerEnabled(false),
                Effect.flatMap(HttpClientResponse.filterStatusOk),
                Effect.timeoutOption(config.timeoutMs),
              );
            if (Option.isNone(response))
              return yield* Effect.fail(
                exporterError("clickhouse-local", "export", "ClickHouse insert timed out"),
              );
          }).pipe(
            Effect.mapError(() =>
              exporterError("clickhouse-local", "export", "ClickHouse insert failed"),
            ),
          ),
        ),
      flush: lifecycle.run("flush", Effect.void),
      shutdown: lifecycle.shutdown,
    };
  });
