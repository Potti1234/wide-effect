import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const packageDirectories = ["core", "effect", "foldkit", "testing", "lint"] as const;
const workspaceRoot = resolve(import.meta.dir, "..");
const temporaryRoot = await mkdtemp(join(tmpdir(), "wide-effect-package-smoke-"));
const tarballDirectory = join(temporaryRoot, "tarballs");
const consumerDirectory = join(temporaryRoot, "consumer");

try {
  await Promise.all([
    mkdir(tarballDirectory, { recursive: true }),
    mkdir(consumerDirectory, { recursive: true }),
  ]);

  const tarballs: Array<string> = [];
  for (const packageDirectory of packageDirectories) {
    const filesBefore = new Set(await readdir(tarballDirectory));
    await Bun.$`bun pm pack --destination ${tarballDirectory}`
      .cwd(join(workspaceRoot, "packages", packageDirectory))
      .quiet();
    const filename = (await readdir(tarballDirectory)).find(
      (file) => file.endsWith(".tgz") && !filesBefore.has(file),
    );
    if (!filename) throw new Error(`Packing ${packageDirectory} did not return a tarball`);
    tarballs.push(join(tarballDirectory, filename));
  }

  await writeFile(
    join(consumerDirectory, "package.json"),
    JSON.stringify({ name: "wide-effect-package-smoke", private: true, type: "module" }, null, 2),
  );
  await Bun.$`bun add ${tarballs} effect@4.0.0-rc.108 foldkit@0.144.0`.cwd(consumerDirectory);

  const source = packageDirectories
    .map((name) => `await import("@wide-effect/${name}");`)
    .concat('await import("effect");', 'await import("foldkit");')
    .join("\n");
  await writeFile(join(consumerDirectory, "smoke.mjs"), source);
  await Bun.$`bun smoke.mjs`.cwd(consumerDirectory);

  const effectInstallations = await findPackageInstallations(
    join(consumerDirectory, "node_modules"),
    "effect",
  );
  if (effectInstallations.length !== 1) {
    throw new Error(`Expected one Effect installation, found ${effectInstallations.length}`);
  }
} finally {
  await rm(temporaryRoot, { force: true, recursive: true });
}

async function findPackageInstallations(directory: string, packageName: string): Promise<string[]> {
  const installations: Array<string> = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.name === ".cache") continue;
    const path = join(directory, entry.name);
    if (entry.name === packageName) {
      const manifest = JSON.parse(await readFile(join(path, "package.json"), "utf8")) as {
        name?: unknown;
      };
      if (manifest.name === packageName) installations.push(path);
    }
    if (
      entry.name === "node_modules" ||
      entry.name.startsWith("@") ||
      directory.endsWith("@wide-effect")
    ) {
      installations.push(...(await findPackageInstallations(path, packageName)));
    }
  }
  return installations;
}
