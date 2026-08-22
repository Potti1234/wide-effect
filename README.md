# wide-effect

Reusable wide-event observability for Effect v4 and Foldkit applications.

The project is currently in the planning stage. No public implementation API is stable yet. The
initial compatibility targets are Effect `4.0.0-rc.108` and Foldkit `0.144.0`, followed by real-world
validation in Fractal. LowkeySec is the intended second consumer once it is explicitly placed in
scope.

wide-effect will use completed enriched Effect spans as canonical wide events, correlate browser,
server, and worker flows with W3C trace context, and provide bounded redacted diagnostics suitable for
human debugging and AI-assisted incident analysis.

- [Executable implementation plan](docs/implementation-plan.md)
- [Repository agent instructions](AGENTS.md)
