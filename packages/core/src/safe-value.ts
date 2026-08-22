import { Schema } from "effect";

export type SafeValue = string | number | boolean | null | ReadonlyArray<SafeValue> | SafeObject;
export interface SafeObject {
  readonly [key: string]: SafeValue;
}

export interface NormalizationLimits {
  readonly maxDepth: number;
  readonly maxEntries: number;
  readonly maxStringLength: number;
  readonly maxBytes: number;
}

export const defaultNormalizationLimits: NormalizationLimits = {
  maxDepth: 6,
  maxEntries: 100,
  maxStringLength: 2_048,
  maxBytes: 32_768,
};

const SafeValueInternal: Schema.Codec<SafeValue> = Schema.Union([
  Schema.String,
  Schema.Finite,
  Schema.Boolean,
  Schema.Null,
  Schema.Array(Schema.suspend((): Schema.Codec<SafeValue> => SafeValueInternal)),
  Schema.Record(
    Schema.String,
    Schema.suspend((): Schema.Codec<SafeValue> => SafeValueInternal),
  ),
]);

export const SafeValue = SafeValueInternal.check(
  Schema.makeFilter((value) => withinLimits(value) || "safe value exceeds canonical limits"),
);
export const SafeAttributes = Schema.Record(Schema.String, SafeValue).check(
  Schema.makeFilter(
    (attributes) => hasOnlySafeKeys(attributes) || "unsafe or sensitive attribute key",
  ),
);
export type SafeAttributes = typeof SafeAttributes.Type;

export interface AttributePolicy {
  readonly allowedKeys?: ReadonlySet<string>;
  readonly allowedPrefixes?: ReadonlyArray<string>;
}

export const MAX_ATTRIBUTE_KEY_BYTES = 256;

const sensitiveMarkers = [
  "authorization",
  "cookie",
  "credential",
  "password",
  "secret",
  "token",
  "apikey",
  "privatekey",
  "encryptedvalue",
  "prompt",
  "terminal",
  "source",
  "content",
  "output",
] as const;

export function normalizeValue(input: unknown, limits = defaultNormalizationLimits): SafeValue {
  validateLimits(limits);
  if (typeof input === "string") return boundedJsonString(input, limits);
  const normalized = visit(input, 0, limits, {
    bytes: 0,
    entries: 0,
    seen: new WeakSet(),
  });
  if (encodedBytes(normalized) <= limits.maxBytes) {
    return normalized;
  }
  return bounded(
    "[opaque:max-bytes]",
    { ...limits, maxBytes: Math.max(0, limits.maxBytes - 2) },
    {
      bytes: 0,
    },
  );
}

export function normalizeAttributes(
  input: unknown,
  policy: AttributePolicy,
  limits = defaultNormalizationLimits,
): SafeAttributes {
  const normalized = normalizeValue(input, limits);
  if (normalized === null || typeof normalized !== "object" || Array.isArray(normalized)) return {};
  return Object.fromEntries(
    Object.entries(normalized).filter(
      ([key]) =>
        isSafeAttributeKey(key) &&
        (policy.allowedKeys?.has(key) === true ||
          policy.allowedPrefixes?.some((prefix) => key.startsWith(prefix)) === true),
    ),
  );
}

export function isSafeAttributeKey(key: string): boolean {
  return (
    key.length > 0 &&
    new TextEncoder().encode(key).byteLength <= MAX_ATTRIBUTE_KEY_BYTES &&
    /^[A-Za-z][A-Za-z0-9_.:-]*$/.test(key) &&
    !isSensitiveKey(key)
  );
}

function hasOnlySafeKeys(value: SafeValue): boolean {
  if (value === null || typeof value !== "object") return true;
  if (Array.isArray(value)) return value.every(hasOnlySafeKeys);
  return Object.entries(value).every(
    ([key, nested]) => isSafeAttributeKey(key) && hasOnlySafeKeys(nested),
  );
}

function visit(
  input: unknown,
  depth: number,
  limits: NormalizationLimits,
  state: { bytes: number; entries: number; seen: WeakSet<object> },
): SafeValue {
  if (input === null || typeof input === "boolean") return input;
  if (typeof input === "string") return bounded(input, limits, state);
  if (typeof input === "number") return Number.isFinite(input) ? input : opaque("number", state);
  if (typeof input !== "object") return opaque(typeof input, state);
  if (depth >= limits.maxDepth) return opaque("max-depth", state);
  if (state.seen.has(input)) return opaque("cycle", state);
  state.seen.add(input);
  try {
    const descriptors = Object.getOwnPropertyDescriptors(input);
    if (Array.isArray(input)) {
      const output: SafeValue[] = [];
      for (let index = 0; index < input.length && state.entries < limits.maxEntries; index++) {
        state.entries++;
        const descriptor = descriptors[String(index)];
        output.push(
          descriptor && "value" in descriptor
            ? visit(descriptor.value, depth + 1, limits, state)
            : opaque("accessor", state),
        );
      }
      return output;
    }
    const prototype = Object.getPrototypeOf(input);
    if (prototype !== Object.prototype && prototype !== null) return opaque("object", state);
    const output: Record<string, SafeValue> = {};
    for (const key of Object.keys(descriptors).sort()) {
      if (state.entries >= limits.maxEntries || state.bytes >= limits.maxBytes) break;
      state.entries++;
      const keyBytes = new TextEncoder().encode(key).byteLength;
      if (state.bytes + keyBytes > limits.maxBytes) break;
      state.bytes += keyBytes;
      if (!isSafeAttributeKey(key)) {
        // Attribute names are exported as data too. Dropping the complete pair
        // prevents attacker-controlled keys from smuggling secrets.
        continue;
      } else {
        const descriptor = descriptors[key];
        output[key] =
          descriptor && "value" in descriptor
            ? visit(descriptor.value, depth + 1, limits, state)
            : opaque("accessor", state);
      }
    }
    return output;
  } catch {
    return opaque("uninspectable", state);
  }
}

function isSensitiveKey(key: string) {
  const canonical = key.toLowerCase().replaceAll(/[^a-z0-9]/g, "");
  return sensitiveMarkers.some((marker) => canonical.includes(marker));
}

function bounded(value: string, limits: NormalizationLimits, state: { bytes: number }) {
  const availableBytes = Math.max(0, limits.maxBytes - state.bytes);
  let output = "";
  let byteLength = 0;
  let characterCount = 0;
  for (const character of value) {
    if (characterCount >= limits.maxStringLength) break;
    const codePoint = character.codePointAt(0);
    if (codePoint !== undefined && codePoint >= 0xd800 && codePoint <= 0xdfff) continue;
    const characterBytes = new TextEncoder().encode(character).byteLength;
    if (byteLength + characterBytes > availableBytes) break;
    output += character;
    byteLength += characterBytes;
    characterCount++;
  }
  state.bytes += byteLength;
  return output;
}
function boundedJsonString(value: string, limits: NormalizationLimits): string {
  let output = "";
  let characterCount = 0;
  for (const character of value) {
    if (characterCount >= limits.maxStringLength) break;
    const codePoint = character.codePointAt(0);
    if (codePoint !== undefined && codePoint >= 0xd800 && codePoint <= 0xdfff) continue;
    const candidate = output + character;
    if (new TextEncoder().encode(JSON.stringify(candidate)).byteLength > limits.maxBytes) break;
    output = candidate;
    characterCount++;
  }
  return output;
}
function opaque(kind: string, state: { bytes: number }) {
  const output = `[opaque:${kind}]`;
  state.bytes += output.length;
  return output;
}
function withinLimits(value: SafeValue): boolean {
  let entries = 0;
  const stack: Array<{ value: SafeValue; depth: number }> = [{ value, depth: 0 }];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) break;
    if (current.depth > defaultNormalizationLimits.maxDepth) return false;
    if (
      typeof current.value === "string" &&
      current.value.length > defaultNormalizationLimits.maxStringLength
    )
      return false;
    if (current.value !== null && typeof current.value === "object") {
      const values = Array.isArray(current.value) ? current.value : Object.values(current.value);
      entries += values.length;
      if (entries > defaultNormalizationLimits.maxEntries) return false;
      for (const nested of values) stack.push({ value: nested, depth: current.depth + 1 });
    }
  }
  return (
    new TextEncoder().encode(JSON.stringify(value)).byteLength <=
    defaultNormalizationLimits.maxBytes
  );
}

function encodedBytes(value: SafeValue): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function validateLimits(limits: NormalizationLimits): void {
  if (
    !Number.isInteger(limits.maxDepth) ||
    limits.maxDepth < 0 ||
    !Number.isInteger(limits.maxEntries) ||
    limits.maxEntries < 0 ||
    !Number.isInteger(limits.maxStringLength) ||
    limits.maxStringLength < 0 ||
    !Number.isInteger(limits.maxBytes) ||
    limits.maxBytes < 2
  ) {
    throw new RangeError("normalization limits must be non-negative integers and maxBytes >= 2");
  }
}
