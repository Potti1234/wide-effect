const packageNames = ["core", "effect", "foldkit", "lint", "testing"] as const;

const tag = process.argv[2];

if (tag === undefined || !/^v\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)*)?$/.test(tag)) {
  throw new Error("Expected a release tag such as v0.0.1-alpha.2 or v1.0.0");
}

const expectedVersion = tag.slice(1);
const manifests = await Promise.all(
  packageNames.map(async (packageName) => {
    const manifest = await Bun.file(`packages/${packageName}/package.json`).json();
    return { packageName, manifest } as const;
  }),
);

for (const { packageName, manifest } of manifests) {
  if (manifest.version !== expectedVersion) {
    throw new Error(
      `Tag ${tag} does not match @wide-effect/${packageName}@${String(manifest.version)}`,
    );
  }

  for (const dependencyName of ["@wide-effect/core", "@wide-effect/effect"] as const) {
    const dependencyVersion = manifest.dependencies?.[dependencyName];
    if (dependencyVersion !== undefined && dependencyVersion !== expectedVersion) {
      throw new Error(
        `@wide-effect/${packageName} requires ${dependencyName}@${String(dependencyVersion)} instead of ${expectedVersion}`,
      );
    }
  }

  const corePeerVersion = manifest.peerDependencies?.["@wide-effect/core"];
  if (corePeerVersion !== undefined && corePeerVersion !== expectedVersion) {
    throw new Error(
      `@wide-effect/${packageName} requires @wide-effect/core@${String(corePeerVersion)} instead of ${expectedVersion}`,
    );
  }
}

console.log(`Verified ${tag}: all ${manifests.length} packages use ${expectedVersion}.`);
