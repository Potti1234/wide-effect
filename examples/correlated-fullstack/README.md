# Correlated full-stack example

This runnable example demonstrates one trace across four application boundaries:

```text
Foldkit interaction -> browser request -> Effect API -> Effect worker -> nested operation
```

It deliberately runs the transports in memory so the result is deterministic, but uses real Web
`Request`/`Response` objects, Effect's built-in HTTP client/server tracing, and only public
wide-effect APIs. The same middleware and client transforms can be placed around live HTTP servers
and clients.

From the repository root:

```sh
bun install
bun run example:fullstack
```

The process prints the joined trace and fails if ancestry is broken or the secret fixture survives
normalization. Browser propagation is restricted to the exact API origin. The worker call continues
the API span through Effect's HTTP client propagation.

This example focuses on the successful correlated flow. Exporter outage, sampling, bounded delivery,
and shutdown behavior are covered by the executable package tests; production configuration is
documented in the package READMEs.
