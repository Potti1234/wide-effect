import { ServiceMetadata, type AttributePolicy } from "@wide-effect/core";
import { Context, Effect, Layer, Schema } from "effect";

export interface ObservabilityConfigService {
  readonly service: ServiceMetadata;
  readonly attributePolicy: AttributePolicy;
  readonly sanitizeHttpPath?: ((path: string) => string) | undefined;
}

export class ObservabilityConfig extends Context.Service<
  ObservabilityConfig,
  ObservabilityConfigService
>()("@wide-effect/effect/ObservabilityConfig") {}

export interface ObservabilityOptions {
  readonly service: ServiceMetadata;
  readonly attributePolicy?: AttributePolicy;
  /** Programmatic opt-in for recording a sanitized `url.path`; never decoded from external data. */
  readonly sanitizeHttpPath?: ((path: string) => string) | undefined;
}

export const configLayer = (options: ObservabilityOptions) =>
  Layer.effect(
    ObservabilityConfig,
    Schema.decodeUnknownEffect(ServiceMetadata)(options.service).pipe(
      Effect.map((service) =>
        ObservabilityConfig.of({
          service,
          attributePolicy: withSanitizedPath(
            options.attributePolicy ?? defaultAttributePolicy,
            options.sanitizeHttpPath !== undefined,
          ),
          sanitizeHttpPath: options.sanitizeHttpPath,
        }),
      ),
    ),
  );

const defaultAttributePolicy: AttributePolicy = {
  allowedKeys: new Set([
    "http.request.method",
    "http.response.status_code",
    "http.route",
    "url.scheme",
    "server.address",
    "server.port",
  ]),
  allowedPrefixes: ["effect.", "foldkit.", "client."],
  deniedKeys: new Set([
    "url.full",
    "url.query",
    "url.path",
    "http.request.headers",
    "http.response.headers",
  ]),
};

function withSanitizedPath(policy: AttributePolicy, enabled: boolean): AttributePolicy {
  if (!enabled) return policy;
  const deniedKeys = new Set(policy.deniedKeys);
  deniedKeys.delete("url.path");
  return {
    ...policy,
    allowedKeys: new Set([...(policy.allowedKeys ?? []), "url.path"]),
    deniedKeys,
  };
}
