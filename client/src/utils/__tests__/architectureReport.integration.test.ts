import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { createArchitectureReport } from "../architectureReport";
import { PDF_BRAND_ASSET_PATHS } from "../pdfBrand";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../");
const transparentPng =
  "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

describe("architecture report integration", () => {
  it("renders a real three-page jsPDF document in Node", () => {
    const doc = createArchitectureReport({
      costObsLockup: transparentPng,
      databricksMark: transparentPng,
    });

    expect(doc.getNumberOfPages()).toBe(3);
    expect(doc.internal.pageSize.getWidth()).toBeCloseTo(297, 0);
    expect(doc.internal.pageSize.getHeight()).toBeCloseTo(210, 0);
    const bytes = new Uint8Array(doc.output("arraybuffer"));
    expect(new TextDecoder().decode(bytes.slice(0, 4))).toBe("%PDF");
    expect(bytes.byteLength).toBeGreaterThan(10_000);
  });

  it("ships every PDF asset in source and compiled artifact trees", () => {
    for (const assetPath of Object.values(PDF_BRAND_ASSET_PATHS)) {
      const relativePath = assetPath.replace(/^\//, "");
      expect(existsSync(resolve(repoRoot, "client/public", relativePath))).toBe(true);
      expect(existsSync(resolve(repoRoot, "static", relativePath))).toBe(true);
    }
  });
});
