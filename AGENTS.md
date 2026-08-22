# wide-effect Agent Instructions

These instructions apply to the entire repository. Read them before every implementation or review task.

## Mission

wide-effect is a reusable observability library for Effect v4 and Foldkit applications. It provides
wide, context-rich traces without replacing Effect's execution model. A completed enriched Effect
span is the canonical wide event; logs become span events, metrics remain low-cardinality, and
browser, server, worker, and external calls share standard W3C trace context.

The executable roadmap and completion criteria live in
[docs/implementation-plan.md](docs/implementation-plan.md). Keep that plan accurate when discoveries
change the design or sequencing.

## Architecture

The intended packages are:

- `@wide-effect/core`: runtime-neutral schemas, identifiers, safe attributes, redaction, sampling
  decisions, and diagnostic contracts.
- `@wide-effect/effect`: Effect-native layers, spans, logger integration, HTTP propagation, buffering,
  and exporter interfaces.
- `@wide-effect/foldkit`: browser interaction correlation and sanitized Foldkit timeline capture.
- `@wide-effect/testing`: deterministic IDs, in-memory exporters, and trace assertions.
- `@wide-effect/lint`: universally valid observability lint rules and fixtures.

Keep application-specific fields, persistence schemas, authentication, retention, and diagnostic
interfaces in consuming applications unless two real consumers prove the abstraction reusable.
Browser entry points must never import Node or Bun modules.

## Effect rules

- Use Effect v4 services, layers, scopes, fibers, spans, loggers, HTTP middleware, and HTTP clients.
  Do not create a parallel ambient context or request lifecycle.
- Keep `effect` a peer dependency. Verify consumers resolve one compatible Effect installation.
- Compile and test against the consumer baseline `4.0.0-rc.108`. Vendored or reference source at a
  different prerelease, including beta.100, is evidence about design rather than proof of API
  compatibility. Confirm uncertain APIs against the installed rc.108 declarations and source.
- Execute Effects only at explicit runtime boundaries. Export composable Effects and Layers from
  library modules.
- Give public and non-trivial workflows stable domain-qualified names.
- Preserve typed failures. Exporters must not turn an otherwise successful business operation into a
  failure; expose delivery failures through health, counters, or explicit flush results.
- Use Effect Clock, Scope, Queue, Ref, Schedule, and TestClock rather than module globals, raw timers,
  or ad hoc mutable lifecycle state.

## Telemetry and trust

- Decode all external configuration, trace carriers, exporter input, persisted data, and uploaded
  diagnostics with Effect Schema before trusting them.
- Generate and validate W3C `traceparent`; support `tracestate` deliberately. Treat browser-supplied
  correlation as untrusted metadata, never identity or authorization.
- Redact before buffering, persistence, export, diagnostics, and test snapshots. Do not collect
  cookies, authorization headers, credentials, tokens, prompts, tool output, terminal output, source
  content, or arbitrary application state by default.
- Never export a full Foldkit Model. Capture Message tags, approved summaries, changed paths, command
  names/outcomes, route transitions, and bounded runtime metadata.
- Apply tail sampling at the exporter side after outcome and duration are known. Always retain
  configured errors and slow/important operations; make successful-flow sampling deterministic and
  testable.
- Every buffer must be bounded and have explicit overflow, retry, shutdown, and flush semantics.
- Keep metric labels bounded. Trace, user, request, run, task, URL, path, and arbitrary application IDs
  belong on spans, not metric dimensions.

## Foldkit rules

- Preserve The Elm Architecture: Model is serializable, update and view remain pure, Commands own
  effects, and Subscriptions own streams.
- Integrate through supported public seams. Prefer the public DevTools overlay/store seam for timeline
  observation and retain Foldkit's existing Command spans rather than wrapping every command in a
  competing lifecycle.
- Do not depend on Foldkit internals. Pin and test supported Foldkit versions and fail clearly when a
  required public seam changes.
- Browser propagation is opt-in by explicit allowed origin. Never attach trace headers to arbitrary or
  cross-origin requests by default.

## Testing and quality

- Test observable behavior through production boundaries. Use `@effect/vitest`, `it.effect`,
  deterministic IDs, TestClock, real `Request`/`Response` objects, and in-memory exporters.
- Test span trees, propagation, redaction, tail sampling, buffer overflow, exporter degradation,
  shutdown flushing, tree-shaking, and browser/server export separation.
- Every lint rule requires valid and invalid fixtures. Do not add rules that cannot distinguish the
  intended invariant reliably.
- Keep modules focused and small. Extract cohesive ownership instead of creating a general `utils.ts`.
- Do not weaken a type, schema, test, lint rule, or guardrail merely to make a check pass.

## Workflow and commits

1. Inspect relevant installed dependency source, tests, package exports, and consumer integration
   points before editing.
2. Implement one acceptance-criteria slice at a time and add its tests in the same slice.
3. Exercise packaged tarballs against Fractal and isolated consumer fixtures; filesystem workspace
   imports can hide broken exports or undeclared dependencies. Validate against LowkeySec when that
   repository is explicitly placed in scope.
4. Feed integration discoveries back into the library API and this repository's documentation before
   declaring a milestone stable.
5. Make small, reviewable commits matching the commit slices in the implementation plan. Do not mix
   unrelated cleanup or generated release artifacts into feature commits.

Before completing an implementation slice, run the focused checks and then the repository's full
format, lint, test, build, and package-consumer gates defined in `package.json`. Confirm `git status`
and preserve unrelated user changes.
