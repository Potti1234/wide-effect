# Compatibility policy

The initial compile and package-consumer baseline is:

- Effect `4.0.0-rc.108`
- Foldkit `0.144.0`
- Bun `1.3.14`
- TypeScript `5.9.x`

Packages expose Effect and Foldkit as peer dependencies so an application owns those runtimes. The
workspace pins the exact baseline as development dependencies and the package smoke gate installs
packed tarballs into an isolated consumer, imports every public entry point, and verifies that only
one Effect installation is present.

While Effect v4 is a prerelease, compatibility ranges remain exact. A new release candidate requires
an explicit compatibility change and a successful isolated package gate. Vendored or external source
from another beta or release candidate is design evidence only; installed declarations and runtime
behavior at the supported version are authoritative.
