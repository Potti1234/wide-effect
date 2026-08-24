# @wide-effect/effect

## 0.0.1-alpha.4

### Patch Changes

- c048f63: Support the Effect and Foldkit versions used by LowKeySec. Foldkit devtools registration is now
  capability-detected so importing the package remains safe on versions that configure overlays through
  `devTools.overlay`.
- Updated dependencies [c048f63]
  - @wide-effect/core@0.0.1-alpha.4

## 0.0.1-alpha.3

### Patch Changes

- Make release formatting deterministic across operating systems.

## 0.0.1-alpha.2

### Patch Changes

- Synchronize the package suite for tag-driven releases.

## 0.0.1-alpha.1

### Patch Changes

- Preserve Effect server-adapter request metadata during W3C normalization and retain spans when
  framework-provided attributes contain unsafe keys, while excluding those keys and values.

## 0.0.1-alpha.0

### Patch Changes

- 7b969c1: Introduce the initial pre-1.0 contracts, Effect runtime, Foldkit integration, testing utilities, and
  observability lint rules, validated by the packed full-stack examples and Fractal integration gates.
- Updated dependencies [7b969c1]
  - @wide-effect/core@0.0.1-alpha.0

## Unreleased

- Initial Effect tracing, logging, bounded delivery, propagation, and exporter APIs.
