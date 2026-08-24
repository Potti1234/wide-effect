# @wide-effect/foldkit

## 0.0.1-alpha.5

### Patch Changes

- d43e12c: Use the Effect tagged-error API shared by beta.102 and current release candidates, and run packaged
  runtime imports against both the current toolchain and LowKeySec's pinned compatibility profile.
- Updated dependencies [d43e12c]
  - @wide-effect/core@0.0.1-alpha.5
  - @wide-effect/effect@0.0.1-alpha.5

## 0.0.1-alpha.4

### Patch Changes

- c048f63: Support the Effect and Foldkit versions used by LowKeySec. Foldkit devtools registration is now
  capability-detected so importing the package remains safe on versions that configure overlays through
  `devTools.overlay`.
- Updated dependencies [c048f63]
  - @wide-effect/core@0.0.1-alpha.4
  - @wide-effect/effect@0.0.1-alpha.4

## 0.0.1-alpha.3

### Patch Changes

- Make release formatting deterministic across operating systems.

## 0.0.1-alpha.2

### Patch Changes

- Synchronize the package suite for tag-driven releases.

## 0.0.1-alpha.1

### Patch Changes

- Updated dependencies
  - @wide-effect/effect@0.0.1-alpha.1

## 0.0.1-alpha.0

### Patch Changes

- 7b969c1: Introduce the initial pre-1.0 contracts, Effect runtime, Foldkit integration, testing utilities, and
  observability lint rules, validated by the packed full-stack examples and Fractal integration gates.
- Updated dependencies [7b969c1]
  - @wide-effect/core@0.0.1-alpha.0
  - @wide-effect/effect@0.0.1-alpha.0

## Unreleased

- Initial Foldkit command correlation and bounded sanitized timeline APIs.
