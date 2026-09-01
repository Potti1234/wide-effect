import {
  HttpOutcomePolicy,
  ServiceMetadata,
  type AttributePolicy,
  type HttpOutcomePolicy as HttpOutcomePolicyType,
} from "@wide-effect/core";
import { Context, Effect, Layer, Schema } from "effect";
import { failureAttributeKeys } from "./failure.js";

export interface ObservabilityConfigService {
  readonly service: ServiceMetadata;
  readonly attributePolicy: AttributePolicy;
  readonly sanitizeHttpPath?: ((path: string) => string) | undefined;
  readonly httpOutcomePolicy: HttpOutcomePolicyType;
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
  readonly httpOutcomePolicy?: HttpOutcomePolicyType | undefined;
}

export const configLayer = (options: ObservabilityOptions) =>
  Layer.effect(
    ObservabilityConfig,
    Effect.all({
      service: Schema.decodeUnknownEffect(ServiceMetadata)(options.service),
      httpOutcomePolicy: Schema.decodeUnknownEffect(HttpOutcomePolicy)(
        options.httpOutcomePolicy ?? defaultHttpOutcomePolicy,
      ),
    }).pipe(
      Effect.map(({ service, httpOutcomePolicy }) =>
        ObservabilityConfig.of({
          service,
          httpOutcomePolicy,
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

const defaultHttpOutcomePolicy: HttpOutcomePolicyType = {
  serverErrorRanges: [{ minimum: 500, maximum: 599 }],
  clientErrorRanges: [{ minimum: 500, maximum: 599 }],
};

function withSanitizedPath(policy: AttributePolicy, enabled: boolean): AttributePolicy {
  const deniedKeys = new Set(policy.deniedKeys);
  const reservedKeys = Object.values(failureAttributeKeys);
  for (const key of reservedKeys) deniedKeys.delete(key);
  if (enabled) deniedKeys.delete("url.path");
  return {
    ...policy,
    allowedKeys: new Set([
      ...(policy.allowedKeys ?? []),
      ...reservedKeys,
      ...(enabled ? ["url.path"] : []),
    ]),
    deniedKeys,
  };
}
