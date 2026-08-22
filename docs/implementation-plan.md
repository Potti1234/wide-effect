# wide-effect implementation plan

## Outcome

Build and prove a reusable Effect v4 and Foldkit observability toolkit whose canonical artifact is a
completed, enriched Effect span. It must correlate a browser interaction through API and worker calls,
produce bounded and redacted diagnostics, degrade safely when exporters fail, and be consumable as
versioned npm packages. Current completion means Fractal-proven and LowkeySec-ready; changing
LowkeySec itself requires that repository to be explicitly placed in scope.

The plan is executable in order. A phase is complete only when its listed evidence exists and its
acceptance criteria pass. Discoveries made in Fractal must be reflected back into this library rather
than patched only in the application.

## Fixed design decisions

1. Effect owns execution context and lifecycle. The library builds on Effect spans, loggers,
   `HttpMiddleware`, `HttpClient`, Layers, Scope, and fiber-local behavior; it does not implement a
   second ambient request logger.
2. One completed enriched span per meaningful service hop is the wide event. Child spans represent
   material boundaries. Effect logs inside spans become span events.
3. Strict W3C `traceparent` and deliberate, separately modeled `tracestate` forwarding provide
   cross-process propagation. Effect's current external-span representation does not carry
   `tracestate`; baggage is unsupported initially.
   Request/interaction IDs remain support lookup fields, not replacements for trace IDs.
4. Tail sampling runs in the exporter pipeline, when outcome and duration are known. Instrumentation
   always creates spans so a late decision can retain failures and slow operations.
5. Buffers are bounded. Overflow policy, retries, exporter isolation, flush, and shutdown are explicit
   configuration with observable counters.
6. Foldkit integration uses supported public application, `Command.mapEffect`, and DevTools
   overlay/store seams. It annotates Foldkit's existing Command spans rather than creating competing
   spans, and does not mutate user `update`, `view`, or private runtime internals.
7. Browser header injection requires an explicit allowed-origin policy. No blanket fetch patching.
8. Diagnostics use allowlisted summaries. A full Foldkit Model is never captured or exported.
9. Effect `4.0.0-rc.108` is the initial consumer compatibility target. Reference source at beta.100
   may inform intent, but every API must compile and run against rc.108.
10. Persistence schemas, application domain fields, authentication, support UI, and retention remain
    consumer-owned until repetition in Fractal and LowkeySec justifies extraction.

## Target repository

```text
wide-effect/
+- packages/
|  +- core/
|  +- effect/
|  +- foldkit/
|  +- testing/
|  `- lint/
+- examples/
|  +- effect-server/
|  +- foldkit-fullstack/
|  `- worker/
+- docs/
`- scripts/
```

Initial package names:

- `@wide-effect/core`
- `@wide-effect/effect`
- `@wide-effect/foldkit`
- `@wide-effect/testing`
- `@wide-effect/lint`

Use ESM, explicit exports, declaration files, source maps, and `sideEffects: false` where truthful.
Effect and Foldkit are peer dependencies in packages that expose their types. Browser and server
entry points must be statically separated.

## Phase 0 - workspace and compatibility harness

### Work

- Create the Bun workspace, shared strict TypeScript configuration, formatter, linter, Vitest setup,
  build pipeline, package validation, and Changesets prerelease configuration.
- Scaffold the five packages with public export maps but no speculative application abstractions.
- Pin development/test fixtures to Effect `4.0.0-rc.108` and Foldkit `0.144.0`, matching Fractal.
- Inspect the installed Foldkit 0.144 public exports and declarations before fixing the integration
  API; the vendored Foldkit reference is an older version and is architectural evidence only.
- Add a dependency check that fails if a packed consumer resolves multiple Effect installations.
- Add package-tarball smoke tests for Node/Bun server and browser entry points.
- Record the supported runtime and peer-version policy.

### Acceptance criteria

- `bun install`, format, lint, typecheck, test, and build pass from a fresh clone.
- Every package can be packed and installed into an isolated fixture without workspace resolution.
- Server imports do not enter browser bundles; test-only code does not enter production packages.
- The rc.108 compatibility fixture compiles without casts used to bridge version differences.
- CI runs all full gates on the supported platform matrix.

### Commit slice

`chore(repo): establish workspace and package compatibility gates`

## Phase 1 - canonical core contracts

### Work

- Define branded and Schema-decoded `TraceId`, `SpanId`, `InteractionId`, `RequestId`, W3C carrier,
  service metadata, operation kind, outcome, level, safe attribute values, span events, and completed
  `FlowSpan` envelope with `schemaVersion`.
- Define a versioned, bounded `DiagnosticBundle` contract organized into summary, timeline, context,
  and evidence.
- Implement schema-registered safe attributes plus redaction with recursive handling, cycles,
  depth/size limits, hostile getters/proxies, and standard sensitive-key rejection. Unsupported class
  instances and defects become opaque summaries rather than being traversed.
- Define deterministic sampling decision inputs and configuration schemas without implementing
  transport.
- Document semantic field naming and application namespaces such as `fractal.run.id`.

### Acceptance criteria

- Unknown carriers and persisted envelopes decode or fail with typed schema errors.
- Invalid trace identifiers, malformed carriers, unsafe attribute values, oversized values, and
  unsupported schema versions are rejected or sanitized according to documented policy.
- Redaction tests prove secrets cannot survive supported nested values, arrays, safe error summaries,
  or diagnostics, and hostile objects cannot execute getters, crash normalization, or expose values.
- Core imports no Foldkit, platform, database, OTLP, Node, Bun, or application modules.
- Golden schema fixtures demonstrate compatible encode/decode behavior.

### Commit slice

`feat(core): define versioned telemetry and diagnostic contracts`

## Phase 2 - testing foundation

### Work

- Provide a minimal in-memory completed-span collector, deterministic trace/ID generator, test layer,
  trace-tree builder, and assertions for operation, ancestry, attributes, events, outcome, and
  redaction.
- Add deterministic delivery/sampling helpers using TestClock and real Request/Response helpers.
- Keep assertions framework-light and expose Vitest conveniences through an explicit subpath.

### Acceptance criteria

- Implementation phases can test complete traces without a real exporter or runtime global.
- Assertions produce readable structural diffs and reject duplicate/missing spans.
- TestClock and deterministic IDs work in parallel tests without shared mutable state.
- Production entry points have no dependency on `@wide-effect/testing`.

### Commit slice

`feat(testing): add deterministic trace test utilities`

## Phase 3 - Effect-native observability foundation

### Work

- Build service/layer configuration for service identity, environment, deployment/version, ID
  generation, redaction, sampling, and exporters.
- Provide thin helpers for annotating the current span and adding named span events; retain direct
  Effect APIs for normal spans and stable `Effect.fn` operation names.
- Integrate Effect logging with tracing so structured logs inside a span become events while a
  configurable human-readable logger remains possible.
- Implement exactly one custom `Tracer.Tracer` and completed-span fan-out pipeline. This tracer owns
  span completion, event collection, normalization, and delivery; OTLP is an exporter adapter rather
  than a competing Effect tracer Layer.
- Define a typed exporter interface over completed `FlowSpan` batches plus explicit flush/shutdown.
- Implement in-memory, console, and append-only NDJSON exporters. Keep OTLP behind a server-only entry
  point and map canonical spans without making OTLP the domain model.
- Confirm rc.108 APIs against installed source/declarations. Add a compatibility note for any
  divergence from the vendored beta.100 reference.

### Acceptance criteria

- A named Effect workflow exports trace/span/parent IDs, duration, attributes, events, and typed exit
  outcome.
- Nested Effect fibers preserve or explicitly fork correct trace relationships.
- Concurrent child fibers, interruption, typed failures, defects, sampled=false parents, duration
  units, same-timestamp event ordering, and Exit mapping retain deterministic trace fidelity.
- Structured Effect logs become span events exactly once.
- Default, custom-human-logger, and OTLP-export configurations each install exactly one
  `Logger.tracerLogger` and exactly one tracer service.
- Exporter failure does not fail the business Effect and is observable through health/counters.
- Layers are scoped, composable, test-replaceable, and flush during graceful shutdown.
- Browser imports cannot load NDJSON, OTLP server adapters, filesystem, Node, or Bun modules.

### Commit slices

1. `feat(effect): add scoped tracing and logger layers`
2. `feat(effect): add exporter contract and built-in exporters`

## Phase 4 - bounded delivery and exporter-side tail sampling

### Work

- Implement a scoped bounded queue between span completion and exporter batches.
- Support explicit overflow policies, batch size/age, bounded retry schedules, jitter, concurrency,
  final flush timeout, and exporter isolation.
- Implement exporter-side tail sampling after span completion: retain configured failures, slow spans,
  important operations/actors, and deterministic samples of routine successes.
- Publish low-cardinality counters/gauges for accepted, sampled, exported, retried, dropped, overflowed,
  and flush-timeout spans.
- Make redaction occur before enqueueing so secrets never reside in the queue.
- Queue offers never block business fibers. Define ordering and drop-new/drop-old semantics; bound
  retry state; interrupt hung attempts with per-attempt timeouts; do not use `Logger.batched` as the
  span delivery queue.

### Acceptance criteria

- TestClock tests deterministically cover batch age, retry exhaustion, overflow, and shutdown timeout.
- Memory remains bounded under an exporter that never completes.
- Shutdown terminates at its configured timeout even when an exporter hangs or is uninterruptible.
- Failures and configured slow spans survive sampling; deterministic success decisions are repeatable.
- One failed exporter does not prevent another exporter from receiving the batch.
- No span buffered or exported contains pre-redaction values.

### Commit slice

`feat(effect): add bounded delivery and tail sampling`

## Phase 5 - W3C HTTP propagation

### Work

- Implement strict W3C carrier parsing, formatting, extraction, and injection.
- Configure and compose Effect's existing `HttpMiddleware.tracer` so it continues a valid remote
  parent, creates the single server span, records safe HTTP fields, and returns support IDs.
- Configure Effect's existing `HttpClient` tracing and propagation so it creates the single client
  span and injects context. Custom carrier helpers are limited to non-Effect/browser boundaries.
- Preserve validated `tracestate` in a separate scoped service/annotation and define exact forwarding
  semantics; do not imply that Effect's `ExternalSpan` contains it.
- Strip or replace malformed inbound correlation. Never trust baggage for auth or actor identity.
- Add explicit allowed-origin browser injection with same-origin as the safe default and exact-origin
  configuration for additional destinations.
- Document required CORS allowed/request/exposed headers.

### Acceptance criteria

- Real Request/Response tests prove browser -> server and server -> worker parent/trace continuity.
- Exactly one server span and one client span exist at each Effect HTTP boundary.
- Malformed, oversized, and adversarial headers cannot crash middleware or influence authorization.
- Strict parsing rejects all-zero IDs, invalid flags and hex, unsupported versions/extra fields,
  ambiguous multiple headers, excess whitespace/length, and invalid/duplicate/oversized tracestate
  members.
- Headers are absent for origins outside the allowlist and present for explicitly allowed origins.
- HTTP error, cancellation, defect, and success paths close spans with correct outcomes/status.
- Existing caller headers are handled according to a documented replace/continue policy.

### Commit slices

1. `feat(effect): add W3C server and client propagation`
2. `feat(effect): enforce safe browser origin propagation`

## Phase 6 - Foldkit integration

### Work

- Inspect Foldkit 0.144 public APIs and its DevTools overlay/store package. Document the supported public
  observation seam before implementation.
- Observe the public DevTools timeline/store to capture Message tags, approved payload summaries,
  route transitions, changed Model paths, and Command names/arguments. Join outcomes from completed
  Effect spans; DevTools entries do not contain span outcomes.
- Wrap application `init`/`update` at the configuration boundary and transform returned Commands with
  public `Command.mapEffect`, annotating the runtime's existing Command span with interaction/message
  correlation. User `update` and `view` remain pure.
- Provide a composite overlay and require production capture to use `show: "Always"` with Inspect mode;
  the visual overlay remains optional and development time travel remains delegated to Foldkit.
- Maintain a redacted bounded browser ring buffer with count, byte, and age limits.
- Provide opt-in sanitized diagnostic upload/export and explicit per-Message/per-field summarizers.
- Preserve development time travel and MCP DevTools behavior.

### Acceptance criteria

- User `update` and `view` remain pure and application Model/Message schemas remain unchanged.
- DevTools and time travel still work with instrumentation enabled.
- Command spans retain Foldkit names and correlate with the initiating interaction.
- Applications configure high-frequency `excludeFromHistory` tags before recording; sensitive payload
  fields do not enter the wide-effect buffer.
- wide-effect never reads, copies, summarizes, or uploads the Model. Its summarizers accept only
  Message/Command metadata and its upload API accepts only the sanitized timeline contract. Foldkit
  DevTools may still retain Model snapshots for its own time travel.
- Buffer limits and upload failure behavior are deterministic and tested in a browser-like runtime.
- Packed Foldkit compatibility fixtures fail at compile time if required `foldkit/devtools-host` or
  `Command.mapEffect` public exports/types change; no runtime fallback reaches into internals.

### Commit slices

1. `feat(foldkit): observe public timeline and command spans`
2. `feat(foldkit): add bounded sanitized diagnostics`

## Phase 7 - lint package

### Work

- Choose the lint engine/export format based on actual Fractal and LowkeySec tooling.
- Implement only mechanically reliable, reusable rules, initially covering static observability event
  names, sensitive field keys, high-cardinality metric labels, direct exporter use outside composition
  roots, and uninstrumented known HTTP boundaries.
- Provide valid/invalid fixtures for syntax variants and typed configuration examples.
- Keep application operation registries and directory-specific policies in consumers.

### Acceptance criteria

- Every rule has positive, negative, and edge-case fixtures.
- Rules produce actionable messages and documented safe replacements.
- Fractal can enable the rules without ignores after its migration.
- Rules do not forbid legitimate scripts, tests, or application-specific adapters unless configured.

### Commit slice

`feat(lint): enforce reusable observability invariants`

## Phase 8 - executable examples and package release proof

### Work

- Build an Effect API, worker, and Foldkit full-stack example demonstrating one correlated interaction.
- Demonstrate success, failure, slow-operation sampling, redaction, exporter outage, and shutdown flush.
- Pack all packages and install them into isolated examples from tarballs.
- Write API, configuration, security, CORS, sampling, operations, and migration documentation.
- Publish pinned `0.x` prereleases only after the packed examples pass.

### Acceptance criteria

- One example trace visibly joins browser interaction, API request, worker request, and nested operation.
- Secret fixtures are absent from console, files, memory exporter, and diagnostic bundle.
- Examples run from packed packages with no workspace-only imports.
- Public exports, declarations, source maps, licenses, provenance, and changelog are present.

### Commit slices

1. `docs(examples): demonstrate correlated full-stack traces`
2. `chore(release): validate packed prerelease artifacts`

## Phase 9 - Fractal migration and feedback loop

Fractal's own `AGENTS.md`, handbook, backend guide, and Foldkit guide are mandatory reading before
changes. Its vendored repositories remain read-only.

### Slice A: dependency and runtime foundation

- Install packed, exact-version wide-effect packages in API, worker, and web; verify one Effect rc.108
  resolution.
- Add observability configuration through `ApiConfig` and `WorkerConfig`.
- Compose observability once at API and worker roots. Remove API's double evlog initialization and the
  worker's uninitialized ambient evlog usage.
- Make startup messages static with structured fields.

Acceptance: both runtimes export correct service/version/environment metadata; exporter outage does
not prevent startup; focused tests and Fractal format/lint pass.

Commit: `feat(observability): install shared runtime layers`

### Slice B: typed persistence

- Implement a Fractal-owned PostgreSQL exporter using a new typed `observability_spans` schema with
  indexed trace/span/parent, operation, outcome, duration, service, time, schema version, and selected
  Fractal IDs.
- Use the library's bounded batches. Decide and document whether existing `evlog_events` are retained,
  migrated, or retired; generate migrations through Drizzle.
- Add retention and exporter health behavior without allowing telemetry writes to fail requests.

Acceptance: schema-decoded round trip, batching/backpressure/shutdown tests, trace lookup query plan,
and migration tests pass.

Commit: `feat(api): persist typed completed spans`

### Slice C: API boundaries and lifecycle

- Apply server middleware to the main HttpApi and raw auth routes.
- Bring the separately launched worker-control HTTP listener under scoped Layer/fiber ownership so it
  shares telemetry and shutdown lifecycle.
- Record safe route templates, actor IDs after trusted auth, status, duration, and typed failures.

Acceptance: real handler tests cover success/failure/malformed headers; both listeners shut down and
flush; no request state is module-global.

Commit: `feat(api): trace public and control HTTP boundaries`

### Slice D: worker propagation and migration

- Instrument the centralized worker `ControlPlaneClient` with the wide-effect `HttpClient` transform.
- Continue context in API control handlers and annotate worker/run/task IDs only after schema decoding.
- Replace direct evlog calls in worker startup, migrations, control, loop, and lease flows with Effect
  spans/logs/events. Capture context explicitly across remaining Promise callbacks or migrate them to
  scoped Effect fibers.
- Extract observability helpers instead of increasing worker-loop/control-service module budgets.

Acceptance: an integration test proves API -> control API -> worker ancestry; lease/retry/error traces
close correctly; secrets remain redacted.

Commit: `feat(worker): continue and enrich distributed traces`

### Slice E: Foldkit/browser correlation

- Add Foldkit instrumentation at the application entry, retaining current DevTools and MCP behavior.
- Instrument the centralized typed API client and the two raw auth fetch helpers.
- Configure exact API origins and required CORS headers; do not send trace context elsewhere.
- Capture a bounded sanitized timeline, never the whole Model.

Acceptance: Scene/program/browser-boundary tests prove interaction -> command -> request correlation,
origin restrictions, time travel compatibility, and secret exclusion.

Commit: `feat(web): correlate Foldkit interactions with backend traces`

### Slice F: AI diagnostic bundle

- Implement a Fractal-owned authenticated trace query/export boundary returning the shared versioned
  bundle.
- Render Effect operations, typed failures, Foldkit Message tags, commands, and critical path in a
  bounded semantic timeline.
- Apply authorization, retention, redaction, and size limits before returning data.

Acceptance: route/CLI tests prove authorization, schema validity, deterministic ordering, size limits,
and exclusion of cookies, tokens, secrets, workflow/source content, prompts, and terminal output.

Commit: `feat(diagnostics): export safe correlated trace bundles`

### Slice G: enforcement and cleanup

- Enable shared lint rules and broaden Fractal's existing no-console/no-interpolated-log rules across
  migrated API/worker boundaries with valid/invalid fixtures.
- Remove evlog code/dependencies after data-retention parity is resolved.
- Update Fractal handbook/focused guides and lower any module budgets made reducible by extraction.

Acceptance: no production evlog imports remain; no guardrail is ignored or narrowed; all Fractal gates
pass: `bun run format:check`, `bun run lint`, `bun test`, and `bun run build`.

Commit: `refactor(observability): enforce canonical telemetry path`

### Feedback requirement

After every Fractal slice:

1. Record integration friction and missing abstractions in wide-effect issues/plan notes.
2. Fix generally reusable behavior in wide-effect with focused tests and a package commit.
3. Pack a new exact prerelease and upgrade Fractal through the package boundary.
4. Keep Fractal-only semantics local.
5. Repeat the packed-package test before proceeding.

## Future second-consumer validation - LowkeySec

This is a release-hardening follow-up, not part of the current Fractal implementation scope. Perform
it when LowkeySec is explicitly placed in scope.

- Integrate the same packed packages at LowkeySec runtime and HTTP boundaries.
- Add only LowkeySec domain annotations, storage, retention, and diagnostic policy locally.
- Compare both applications' adapters. Extract only behavior that is genuinely identical.
- Use discoveries to finalize the first beta API and compatibility policy.

### Acceptance criteria

- LowkeySec compiles and passes its full repository gates with exact prerelease packages.
- No Fractal domain type, field, schema, table, route, or policy appears in public packages.
- At least one correlated production-shaped flow proves the API works outside Fractal.
- Any second-consumer API revision is applied back to Fractal before beta publication.

### Commit slices

- wide-effect: `refactor(api): incorporate second-consumer feedback`
- LowkeySec: one focused runtime, boundary, and enforcement commit per local convention

## Final completion audit

The project is complete only when current evidence proves all of the following:

- All five packages are published or packable, versioned, documented, and installable independently.
- Effect rc.108 and Foldkit 0.144 compatibility gates pass from tarballs.
- A trace joins Foldkit interaction, API, worker control call, worker workflow, and a nested external
  boundary with correct ancestry.
- Logs are attached as span events without duplication.
- Redaction precedes buffering and export; adversarial secret tests pass.
- Tail sampling, bounded buffering, overflow, retries, exporter isolation, and shutdown flush are
  deterministic and verified.
- Browser propagation is exact-origin constrained and CORS behavior is tested.
- Foldkit DevTools/time travel remains functional and no full Model export path exists.
- Fractal uses exact packed package versions and passes its complete quality gates. Isolated consumer
  fixtures prove that the packages do not depend on Fractal's workspace.
- Fractal has no remaining direct evlog path and its typed diagnostic bundle is authenticated,
  bounded, schema-valid, and AI-readable.
- Shared lint rules have fixtures and consumers enable them without guardrail exceptions.
- Git status in every repository contains only intentional changes, and requested commits are small,
  reviewable, and individually gated.
