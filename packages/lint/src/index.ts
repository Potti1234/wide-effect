/**
 * The lint rules are shipped as ast-grep YAML files. This value gives tooling a
 * stable way to discover the package without importing a lint engine at runtime.
 */
export const ruleIds = [
  "wide-effect-no-dynamic-observability-name",
  "wide-effect-no-sensitive-observability-key",
  "wide-effect-no-high-cardinality-metric-label",
  "wide-effect-safe-effect-log-call",
] as const;

export type RuleId = (typeof ruleIds)[number];
