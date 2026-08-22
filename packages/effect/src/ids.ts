import {
  SpanId,
  TraceId,
  type SpanId as SpanIdType,
  type TraceId as TraceIdType,
} from "@wide-effect/core";
import { Context, Layer, Schema } from "effect";

export interface SpanIdGeneratorService {
  readonly nextTraceId: () => TraceIdType;
  readonly nextSpanId: () => SpanIdType;
}

export class SpanIdGenerator extends Context.Service<SpanIdGenerator, SpanIdGeneratorService>()(
  "@wide-effect/effect/SpanIdGenerator",
) {}

export const idGeneratorLayer = (generator: SpanIdGeneratorService) =>
  Layer.succeed(SpanIdGenerator, SpanIdGenerator.of(generator));

export const secureIdGeneratorLayer = Layer.sync(SpanIdGenerator, () =>
  SpanIdGenerator.of({
    nextTraceId: () => Schema.decodeUnknownSync(TraceId)(nonZeroRandomHex(16)),
    nextSpanId: () => Schema.decodeUnknownSync(SpanId)(nonZeroRandomHex(8)),
  }),
);

function nonZeroRandomHex(bytes: number): string {
  let value: string;
  do value = randomHex(bytes);
  while (/^0+$/.test(value));
  return value;
}

function randomHex(bytes: number): string {
  const buffer = new Uint8Array(bytes);
  crypto.getRandomValues(buffer);
  let output = "";
  for (const byte of buffer) output += byte.toString(16).padStart(2, "0");
  return output;
}
