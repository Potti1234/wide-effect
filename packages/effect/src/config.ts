import { ServiceMetadata, type AttributePolicy } from "@wide-effect/core";
import { Context, Effect, Layer, Schema } from "effect";

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
  Layer.effect(
    ObservabilityConfig,
    Schema.decodeUnknownEffect(ServiceMetadata)(options.service).pipe(
      Effect.map((service) =>
        ObservabilityConfig.of({
          service,
          attributePolicy: options.attributePolicy ?? defaultAttributePolicy,
        }),
      ),
    ),
  );

const defaultAttributePolicy: AttributePolicy = {
  allowedPrefixes: ["effect.", "http.", "url.", "server.", "client.", "user_agent."],
};
