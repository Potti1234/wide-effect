# Releasing

All public packages form one fixed Changesets group and always use the same version. A release tag
must be `v` followed by that exact package version, for example `v0.0.1-alpha.2` or `v1.0.0`.

## Prepare a release

1. Add Changesets with `bun changeset` as changes are merged.
2. Run `bun run release:version`. Changesets updates every package in the fixed group to the same
   version, updates internal package dependencies, and writes the changelogs.
3. Run `bun run check` and commit the generated release files.
4. Create and push an annotated tag matching the package version:

   ```sh
   git tag -a v0.0.1-alpha.2 -m "wide-effect v0.0.1-alpha.2"
   git push origin main v0.0.1-alpha.2
   ```

The `publish.yml` workflow rejects malformed tags, mismatched package versions, and mismatched
internal Wide Effect dependency versions. It runs the complete repository gate, publishes packages
in dependency order, uses the prerelease identifier as the npm tag (`alpha`, `beta`, or `rc`), and
uses `latest` only for stable versions.

## npm trusted publisher setup

Each npm package must trust the GitHub repository `Potti1234/wide-effect` and workflow
`publish.yml`, with the `npm publish` action allowed. The workflow uses GitHub OIDC and npm
provenance; it does not require an `NPM_TOKEN` secret.

Protect release tags in GitHub so only maintainers can create tags matching `v*`. Never reuse or
move a published release tag because npm versions are immutable.
