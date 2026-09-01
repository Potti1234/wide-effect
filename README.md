# wide-effect

Reusable wide-event observability for Effect v4 and Foldkit applications. A completed enriched Effect
span is the canonical event; structured logs become span events, while browser, server, and worker
boundaries share strict W3C trace context.

The packages are pre-1.0 and currently target Effect `4.0.0-rc.108` and Foldkit `0.144.0` exactly.
They provide decoded telemetry contracts, bounded delivery and tail sampling, exporter adapters,
Foldkit interaction diagnostics, deterministic testing tools, and reusable ast-grep rules. Fractal is
the first production-shaped validation target. LowkeySec is the intended second consumer once that
repository is explicitly placed in scope.

## Packages

- `@wide-effect/core`: schemas, identifiers, redaction, W3C carriers, sampling and diagnostics.
- `@wide-effect/effect`: Effect-native tracer, logging, delivery, exporters, and HTTP propagation.
- `@wide-effect/foldkit`: command correlation and bounded, sanitized DevTools timelines.
- `@wide-effect/testing`: deterministic IDs, collectors, trace assertions, and HTTP fixtures.
- `@wide-effect/lint`: portable ast-grep observability rules and fixtures.

Server-only exporters are available from `@wide-effect/effect/server`; the main entry point remains
browser-safe. Applications own authentication, persistence, retention, and domain-specific fields.

- [Executable implementation plan](docs/implementation-plan.md)
- [Compatibility policy](docs/compatibility.md)
- [API and configuration](docs/configuration.md)
- [Security model](docs/security.md)
- [Sampling and delivery](docs/sampling-and-delivery.md)
- [Operations](docs/operations.md)
- [Secure workflow recipes](docs/secure-workflows.md)
- [Migration guide](docs/migration.md)
- [Release process](docs/releasing.md)
- [Repository agent instructions](AGENTS.md)
