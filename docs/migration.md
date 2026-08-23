# Migration guide

1. Inventory runtime roots, HTTP boundaries, current loggers/exporters, correlation fields, and
   shutdown ownership. Keep application persistence, authorization, retention, and domain schemas
   local.
2. Install exact prerelease versions and the documented Effect/Foldkit peers. Compose the config,
   ID, logger, tracer, sink/delivery, and exporter layers once per runtime.
3. Convert meaningful workflows to stable named Effect operations and spans. Replace interpolated log
   names with static events plus structured allowlisted attributes.
4. Add `w3cServerTracer` at server boundaries and use Effect's traced `HttpClient` plus
   `withTraceStatePropagation` for outbound calls. Use browser injection only through an exact origin
   policy and configure CORS as documented.
5. Instrument Foldkit at the application boundary and register its composite observer after the Vite
   overlay. Configure noisy Message tags in `excludeFromHistory`; never summarize Model state.
6. Enable shared lint rules, then add repository-specific rules for approved HTTP clients and exporter
   composition roots. Remove the old logging path only after equivalent retention and operational
   evidence exists.
7. Test success, typed failure, defect, interruption, slow retention, exporter outage, overflow,
   redaction, and graceful shutdown through production boundaries. Finally test packed tarballs rather
   than workspace imports.

During a staged migration, avoid installing two tracing loggers or tracers. If the legacy system must
remain temporarily, adapt completed wide-effect spans at the exporter boundary instead of duplicating
instrumentation inside business workflows.
