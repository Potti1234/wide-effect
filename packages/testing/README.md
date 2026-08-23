# @wide-effect/testing

Deterministic test support for wide-effect consumers.

Use the main entry point for an in-memory completed-span collector, deterministic identifiers, real
Request/Response fixtures, redaction checks, and trace-tree construction. `@wide-effect/testing/vitest`
adds Vitest-oriented assertions without placing Vitest in production package graphs.

The helpers are instance-scoped: create a fresh collector and ID source per test so parallel tests do
not share mutable state. Use Effect `TestClock` for delivery timing rather than wall-clock sleeps.

This package is test-only and must not be imported by application production entry points.
