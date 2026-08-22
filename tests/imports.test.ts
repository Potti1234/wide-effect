import { describe, expect, it } from "vitest";

const packageNames = [
  "@wide-effect/core",
  "@wide-effect/effect",
  "@wide-effect/foldkit",
  "@wide-effect/testing",
  "@wide-effect/lint",
] as const;

describe("workspace package exports", () => {
  for (const packageName of packageNames) {
    it(`imports ${packageName}`, async () => {
      await expect(import(packageName)).resolves.toBeTypeOf("object");
    });
  }
});
