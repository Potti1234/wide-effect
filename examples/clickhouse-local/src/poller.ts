import { Effect, Ref, Schema } from "effect";
import type { ClickHouseSpanRow } from "./mapping.js";

const MachineName = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z][A-Za-z0-9_.:-]*$/),
);
const BoundedNames = Schema.Array(MachineName).check(Schema.isMaxLength(32));
const BoundedOperations = Schema.Array(
  Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256)),
).check(Schema.isMaxLength(32));
const Statuses = Schema.Array(
  Schema.Finite.check(
    Schema.makeFilter((value) => Number.isInteger(value) || "status must be an integer"),
    Schema.isBetween({ minimum: 100, maximum: 599 }),
  ),
).check(Schema.isMaxLength(32));
const PositiveInteger = Schema.Finite.check(
  Schema.makeFilter((value) => Number.isInteger(value) || "must be an integer"),
  Schema.isGreaterThan(0),
);

export const AlertRule = Schema.Struct({
  id: MachineName,
  service: MachineName,
  operations: Schema.optionalKey(BoundedOperations),
  statusCodes: Schema.optionalKey(Statuses),
  failureCodes: Schema.optionalKey(BoundedNames),
  minimumOccurrences: PositiveInteger.check(Schema.isLessThanOrEqualTo(10_000)),
  windowSeconds: PositiveInteger.check(Schema.isLessThanOrEqualTo(86_400)),
  cooldownSeconds: PositiveInteger.check(Schema.isLessThanOrEqualTo(86_400)),
});
export type AlertRule = typeof AlertRule.Type;

export interface AlertCursor {
  readonly ingestedAt: string;
  readonly spanId: string;
}

export interface AlertSource {
  readonly read: (
    cursor: AlertCursor | undefined,
    lookbackMs: number,
  ) => Effect.Effect<ReadonlyArray<ClickHouseSpanRow>>;
}

export type IncidentState =
  | "discovered"
  | "claimed"
  | "triaging"
  | "ticketed"
  | "retry_wait"
  | "needs_human";

export interface IncidentRecord {
  readonly key: string;
  readonly state: IncidentState;
  readonly attempts: number;
  readonly retryAt?: number;
  readonly lastErrorCode?: string;
}

export interface AlertPoller {
  readonly pollOnce: Effect.Effect<void>;
  readonly incidents: Effect.Effect<ReadonlyArray<IncidentRecord>>;
  readonly cursor: Effect.Effect<AlertCursor | undefined>;
}

export interface PollerOptions {
  readonly lookbackMs: number;
  readonly retryDelayMs: number;
  readonly maxAttempts: number;
  readonly now: Effect.Effect<number>;
}

export const makeAlertPoller = (
  ruleInput: unknown,
  source: AlertSource,
  triage: (row: ClickHouseSpanRow) => Effect.Effect<void, string>,
  options: PollerOptions,
): Effect.Effect<AlertPoller, Schema.SchemaError> =>
  Effect.gen(function* () {
    const rule = yield* Schema.decodeUnknownEffect(AlertRule)(ruleInput, {
      onExcessProperty: "error",
    });
    const cursor = yield* Ref.make<AlertCursor | undefined>(undefined);
    const incidents = yield* Ref.make<ReadonlyMap<string, IncidentRecord>>(new Map());
    const lastTicketedAt = yield* Ref.make<number | undefined>(undefined);

    const claim = (key: string) =>
      Ref.modify(incidents, (records) => {
        if (records.has(key)) return [false, records] as const;
        const next = new Map(records);
        next.set(key, { key, state: "claimed", attempts: 0 });
        return [true, next] as const;
      });

    const update = (key: string, record: IncidentRecord) =>
      Ref.update(incidents, (records) => {
        const next = new Map(records);
        next.set(key, record);
        return next;
      });

    const dispatch = (key: string, row: ClickHouseSpanRow, previousAttempts: number) =>
      Effect.gen(function* () {
        const attempts = previousAttempts + 1;
        yield* update(key, { key, state: "triaging", attempts });
        const result = yield* Effect.exit(triage(row));
        if (result._tag === "Success") {
          yield* update(key, { key, state: "ticketed", attempts });
          yield* Ref.set(lastTicketedAt, yield* options.now);
          return;
        }
        if (attempts >= options.maxAttempts) {
          yield* update(key, {
            key,
            state: "needs_human",
            attempts,
            lastErrorCode: "triage.failed",
          });
          return;
        }
        yield* update(key, {
          key,
          state: "retry_wait",
          attempts,
          retryAt: (yield* options.now) + options.retryDelayMs,
          lastErrorCode: "triage.failed",
        });
      });

    const pollOnce = Effect.gen(function* () {
      const now = yield* options.now;
      const rows = (yield* source.read(yield* Ref.get(cursor), options.lookbackMs))
        .filter((row) => matches(rule, row))
        .toSorted(compareRows);
      const latestTime = Date.parse(rows.at(-1)?.ingested_at ?? "");
      const windowStart = latestTime - rule.windowSeconds * 1_000;
      const windowed = Number.isFinite(latestTime)
        ? rows.filter((row) => Date.parse(row.ingested_at) >= windowStart)
        : [];
      const unique = new Map(windowed.map((row) => [incidentKey(rule.id, row), row]));
      const thresholdMet = unique.size >= rule.minimumOccurrences;
      for (const [key, row] of unique) {
        if (!thresholdMet) break;
        const lastTicket = yield* Ref.get(lastTicketedAt);
        if (lastTicket !== undefined && now - lastTicket < rule.cooldownSeconds * 1_000) break;
        const claimed = yield* claim(key);
        if (claimed) yield* dispatch(key, row, 0);
      }
      const current = yield* Ref.get(incidents);
      for (const record of current.values()) {
        if (record.state !== "retry_wait" || record.retryAt === undefined || record.retryAt > now)
          continue;
        const row = unique.get(record.key);
        if (row !== undefined) yield* dispatch(record.key, row, record.attempts);
      }
      const latest = rows.at(-1);
      if (latest !== undefined)
        yield* Ref.set(cursor, { ingestedAt: latest.ingested_at, spanId: latest.span_id });
    });

    return {
      pollOnce,
      incidents: Ref.get(incidents).pipe(Effect.map((records) => [...records.values()])),
      cursor: Ref.get(cursor),
    };
  });

function matches(rule: AlertRule, row: ClickHouseSpanRow): boolean {
  return (
    row.service_name === rule.service &&
    (rule.operations === undefined || rule.operations.includes(row.operation)) &&
    (rule.statusCodes === undefined ||
      (row.http_status !== null && rule.statusCodes.includes(row.http_status))) &&
    (rule.failureCodes === undefined ||
      (row.failure_code !== null && rule.failureCodes.includes(row.failure_code)))
  );
}

function compareRows(left: ClickHouseSpanRow, right: ClickHouseSpanRow): number {
  return (
    left.ingested_at.localeCompare(right.ingested_at) || left.span_id.localeCompare(right.span_id)
  );
}

function incidentKey(ruleId: string, row: ClickHouseSpanRow): string {
  return `${ruleId}:${row.trace_id}:${row.span_id}`;
}
