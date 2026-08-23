# @wide-effect/lint

Reusable [ast-grep](https://ast-grep.github.io/) rules for observability code. The package ships data
files, not a runtime lint plugin, so it works with the same repository-owned ast-grep setup used by
Fractal. Rules intentionally target only syntax whose meaning can be determined reliably.

## Enable the rules

Install `@wide-effect/lint` and `@ast-grep/cli`, then add the package directory to your root
`sgconfig.yml`:

```yaml
ruleDirs:
  - rules
  - node_modules/@wide-effect/lint/rules

testConfigs:
  - testDir: rule-tests
  - testDir: node_modules/@wide-effect/lint/rule-tests
```

Run `ast-grep scan && ast-grep test --skip-snapshot-tests`. If pnpm does not hoist the package, use
the path at which your package manager links `@wide-effect/lint`.

## Rules

### `wide-effect-no-dynamic-observability-name`

Requires static names for `event`, Effect log functions, and conventional logger methods. Move a
changing status out of the event name:

```ts
event("run.finished", { ...fields, status });
```

### `wide-effect-no-sensitive-observability-key`

Rejects exact, well-known secret/content keys inside `event`, `annotate`, `Effect.annotateLogs`, and
`Effect.annotateCurrentSpan`. Replace raw values with an allowlisted derived fact. The exact-key
policy avoids false positives such as `tokenCount`; applications may add stricter domain rules.

### `wide-effect-no-high-cardinality-metric-label`

Rejects literal ID, URL, path, and actor label names passed to Effect `Metric.tagged` and
`Metric.taggedWithLabels`. Put those values on spans and keep metric dimensions bounded.

## Deliberate omissions

Direct exporter construction is valid in composition roots, scripts, tests, and adapter packages;
ast-grep YAML has no portable consumer-configured root predicate. Uninstrumented HTTP calls likewise
require repository-specific knowledge of approved clients and boundaries. Shipping either as a
universal rule would create false positives, so consumers should implement those policies locally.

The rules match the documented call shapes rather than resolving TypeScript symbols. Aliased or
wrapped APIs should be covered by a local rule when a project standardizes those names.
