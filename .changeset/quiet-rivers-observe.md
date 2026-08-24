---
"@wide-effect/core": patch
"@wide-effect/effect": patch
"@wide-effect/foldkit": patch
"@wide-effect/lint": patch
"@wide-effect/testing": patch
---

Support the Effect and Foldkit versions used by LowKeySec. Foldkit devtools registration is now
capability-detected so importing the package remains safe on versions that configure overlays through
`devTools.overlay`.
