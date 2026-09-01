import {
  decodeDiagnosticBundle,
  decodeFlowSpan,
  type DiagnosticBundle,
  type FlowSpan,
} from "@wide-effect/core";
import { Effect } from "effect";
import type { ClickHouseSpanRow } from "./mapping.js";

export interface TriageBounds {
  readonly maxSpans: number;
  readonly maxBytes: number;
  readonly windowMs: number;
}

export interface RepositoryRequest {
  readonly repository: string;
  readonly commit: string;
  readonly symbols: ReadonlyArray<string>;
}

export interface RepositoryReference {
  readonly filename: string;
  readonly symbol?: string;
  readonly patch?: string;
}

export class RepositoryAuthorizationError {
  readonly _tag = "RepositoryAuthorizationError";
}

export interface RepositorySource {
  readonly fetch: (
    request: RepositoryRequest,
  ) => Effect.Effect<ReadonlyArray<RepositoryReference>, RepositoryAuthorizationError>;
}

export interface SimulatedTicket {
  readonly incidentId: string;
  readonly service: string;
  readonly operation: string;
  readonly statusCode?: number;
  readonly failureCode?: string;
  readonly deploymentId?: string;
  readonly commitHash?: string;
  readonly traceCommand: string;
  readonly analysis: string;
  readonly owner: "implementation-agent" | "human-review";
}

export interface TriageResult {
  readonly bundle: DiagnosticBundle;
  readonly ticket: SimulatedTicket;
  readonly repositoryReferences: ReadonlyArray<RepositoryReference>;
}

export const buildTriageEvidence = (
  trigger: ClickHouseSpanRow,
  relatedRows: ReadonlyArray<ClickHouseSpanRow>,
  repository: RepositoryRequest,
  source: RepositorySource,
  bounds: TriageBounds,
): Effect.Effect<TriageResult, RepositoryAuthorizationError | Error> =>
  Effect.gen(function* () {
    validateBounds(bounds);
    const triggerTime = Date.parse(trigger.start_time);
    const ordered = relatedRows
      .filter(
        (row) =>
          row.trace_id === trigger.trace_id &&
          Math.abs(Date.parse(row.start_time) - triggerTime) <= bounds.windowMs,
      )
      .toSorted(compareRows);
    const selected: Array<{ readonly row: ClickHouseSpanRow; readonly span: FlowSpan }> = [];
    let bytes = 0;
    for (const row of ordered) {
      if (selected.length >= bounds.maxSpans) break;
      const span = decodeFlowSpan(JSON.parse(row.canonical_envelope));
      const nextBytes = new TextEncoder().encode(row.canonical_envelope).byteLength;
      if (bytes + nextBytes > bounds.maxBytes) break;
      selected.push({ row, span });
      bytes += nextBytes;
    }
    const triggerSpan = decodeFlowSpan(JSON.parse(trigger.canonical_envelope));
    const references = yield* source.fetch(repository);
    const owner: SimulatedTicket["owner"] =
      trigger.failure_retryable === false ? "implementation-agent" : "human-review";
    const bundle = decodeDiagnosticBundle({
      schemaVersion: 1,
      traceId: triggerSpan.traceId,
      ...(triggerSpan.interactionId === undefined
        ? {}
        : { interactionId: triggerSpan.interactionId }),
      ...(triggerSpan.requestId === undefined ? {} : { requestId: triggerSpan.requestId }),
      summary: {
        title: `${trigger.service_name}:${trigger.operation}`.slice(0, 2_048),
        outcome:
          trigger.http_status === null ? trigger.outcome : `http-status-${trigger.http_status}`,
      },
      timeline: selected.map(({ span }) => ({
        timestamp: span.startTime,
        operation: span.operation,
        summary: safeSpanSummary(span),
        attributes: {
          "triage.duration_ms": span.durationMs,
          ...(span.failure?.code === undefined ? {} : { "triage.failure.code": span.failure.code }),
        },
      })),
      context: {
        "triage.service": trigger.service_name,
        "triage.operation": trigger.operation,
        ...(trigger.http_status === null ? {} : { "triage.http.status": trigger.http_status }),
        ...(trigger.deployment_id === null
          ? {}
          : { "triage.deployment.id": trigger.deployment_id }),
        ...(trigger.commit_hash === null ? {} : { "triage.commit": trigger.commit_hash }),
      },
      evidence: selected
        .filter(({ span }) => span.failure !== undefined || httpStatus(span) !== undefined)
        .map(({ span }) => ({
          timestamp: span.endTime,
          operation: span.operation,
          summary: safeSpanSummary(span),
          attributes: {},
        })),
    });
    return {
      bundle,
      repositoryReferences: references.slice(0, 32).map(boundReference),
      ticket: {
        incidentId: `returned-400:${trigger.trace_id}:${trigger.span_id}`,
        service: trigger.service_name,
        operation: trigger.operation,
        ...(trigger.http_status === null ? {} : { statusCode: trigger.http_status }),
        ...(trigger.failure_code === null ? {} : { failureCode: trigger.failure_code }),
        ...(trigger.deployment_id === null ? {} : { deploymentId: trigger.deployment_id }),
        ...(trigger.commit_hash === null ? {} : { commitHash: trigger.commit_hash }),
        traceCommand: `SELECT canonical_envelope FROM wide_effect_spans FINAL WHERE trace_id = '${trigger.trace_id}'`,
        analysis:
          "Untrusted automation: review the retained status/failure evidence and authorized source references.",
        owner,
      },
    };
  }).pipe(
    Effect.mapError((error) =>
      error instanceof RepositoryAuthorizationError ? error : new Error("triage evidence invalid"),
    ),
  );

function safeSpanSummary(span: FlowSpan): string {
  const status = httpStatus(span);
  if (span.failure?.code !== undefined) return `failure-code=${span.failure.code}`;
  if (status !== undefined) return `http-status=${status}`;
  return `outcome=${span.outcome}`;
}

function httpStatus(span: FlowSpan): number | undefined {
  const value = span.attributes["http.response.status_code"];
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599
    ? value
    : undefined;
}

function compareRows(left: ClickHouseSpanRow, right: ClickHouseSpanRow): number {
  return (
    left.start_time.localeCompare(right.start_time) ||
    left.end_time.localeCompare(right.end_time) ||
    left.span_id.localeCompare(right.span_id)
  );
}

function boundReference(reference: RepositoryReference): RepositoryReference {
  return {
    filename: reference.filename.slice(0, 256),
    ...(reference.symbol === undefined ? {} : { symbol: reference.symbol.slice(0, 256) }),
    ...(reference.patch === undefined ? {} : { patch: reference.patch.slice(0, 16_384) }),
  };
}

function validateBounds(bounds: TriageBounds): void {
  if (!Number.isSafeInteger(bounds.maxSpans) || bounds.maxSpans < 1 || bounds.maxSpans > 512)
    throw new RangeError("maxSpans outside supported range");
  if (!Number.isSafeInteger(bounds.maxBytes) || bounds.maxBytes < 1 || bounds.maxBytes > 1_048_576)
    throw new RangeError("maxBytes outside supported range");
  if (!Number.isSafeInteger(bounds.windowMs) || bounds.windowMs < 0 || bounds.windowMs > 86_400_000)
    throw new RangeError("windowMs outside supported range");
}
