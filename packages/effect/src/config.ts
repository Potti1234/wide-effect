import type { AttributePolicy, ServiceMetadata } from "@wide-effect/core";
import { Context, Layer } from "effect";

export interface ObservabilityConfigService {
  readonly service: ServiceMetadata;
  readonly attributePolicy: AttributePolicy;
}

export class ObservabilityConfig extends Context.Service<
  ObservabilityConfig,
  ObservabilityConfigService
>()("@wide-effect/effect/ObservabilityConfig") {}

export interface ObservabilityOptions {
  readonly service: ServiceMetadata;
  readonly attributePolicy?: AttributePolicy;
}

export const configLayer = (options: ObservabilityOptions) =>
  Layer.succeed(
    ObservabilityConfig,
    ObservabilityConfig.of({
      service: options.service,
      attributePolicy: options.attributePolicy ?? defaultAttributePolicy,
    }),
  );

const defaultAttributePolicy: AttributePolicy = {
  allowedPrefixes: ["effect.", "http.", "url.", "server.", "client.", "user_agent."],
};
