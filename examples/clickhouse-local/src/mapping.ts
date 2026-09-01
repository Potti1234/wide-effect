import { decodeFlowSpan, type FlowSpan } from "@wide-effect/core";
import { Effect } from "effect";

export interface ClickHouseSpanRow {
  readonly schema_version: number;
  readonly trace_id: string;
  readonly span_id: string;
  readonly parent_span_id: string | null;
  readonly segment_id: string | null;
  readonly service_name: string;
  readonly service_version: string;
  readonly environment: string;
  readonly deployment_id: string | null;
  readonly commit_hash: string | null;
  readonly region: string | null;
  readonly instance_id: string | null;
  readonly operation: string;
  readonly kind: string;
  readonly outcome: string;
  readonly start_time: string;
  readonly end_time: string;
  readonly duration_ms: number;
  readonly http_method: string | null;
  readonly http_route: string | null;
  readonly http_status: number | null;
  readonly failure_type: string | null;
  readonly failure_category: string | null;
  readonly failure_code: string | null;
  readonly failure_retryable: boolean | null;
  readonly interaction_id: string | null;
  readonly request_id: string | null;
  readonly ingested_at: string;
  readonly canonical_envelope: string;
}

export class ClickHouseMappingError {
  readonly _tag = "ClickHouseMappingError";
}

export function mapClickHouseRow(
  input: FlowSpan,
  ingestedAt: string,
): Effect.Effect<ClickHouseSpanRow, ClickHouseMappingError> {
  return Effect.try({
    try: () => {
      const span = decodeFlowSpan(input);
      const canonical = JSON.stringify(span);
      return {
        schema_version: span.schemaVersion,
        trace_id: span.traceId,
        span_id: span.spanId,
        parent_span_id: span.parentSpanId ?? null,
        segment_id: span.segmentId ?? null,
        service_name: span.service.name,
        service_version: span.service.version,
        environment: span.service.environment,
        deployment_id: span.service.deploymentId ?? null,
        commit_hash: span.service.commitHash ?? null,
        region: span.service.region ?? null,
        instance_id: span.service.instanceId ?? null,
        operation: span.operation,
        kind: span.kind,
        outcome: span.outcome,
        start_time: span.startTime,
        end_time: span.endTime,
        duration_ms: span.durationMs,
        http_method: safeString(span.attributes["http.request.method"], 32),
        http_route: safeString(span.attributes["http.route"], 256),
        http_status: safeStatus(span.attributes["http.response.status_code"]),
        failure_type: span.failure?.type ?? null,
        failure_category: span.failure?.category ?? null,
        failure_code: span.failure?.code ?? null,
        failure_retryable: span.failure?.retryable ?? null,
        interaction_id: span.interactionId ?? null,
        request_id: span.requestId ?? null,
        ingested_at: canonicalTimestamp(ingestedAt),
        canonical_envelope: canonical,
      };
    },
    catch: () => new ClickHouseMappingError(),
  });
}

function safeString(value: unknown, maximum: number): string | null {
  return typeof value === "string" && value.length > 0 && value.length <= maximum ? value : null;
}

function safeStatus(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599
    ? value
    : null;
}

function canonicalTimestamp(value: string): string {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value))
    throw new Error("invalid timestamp");
  return value;
}
