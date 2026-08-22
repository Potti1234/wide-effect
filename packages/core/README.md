# @wide-effect/core

Runtime-neutral schemas and safe normalization for wide-effect telemetry.

Application attributes are opt-in through `normalizeAttributes` with exact keys or owned prefixes.
Unknown input is normalized before schema decoding: sensitive keys are redacted without reading their
values, accessors are not invoked, cycles and unsupported objects become opaque markers, and depth,
entry, string, and byte budgets are bounded. Baggage is intentionally unsupported.
