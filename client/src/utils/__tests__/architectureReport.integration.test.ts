import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  ARCHITECTURE_PDF_MIN_FONT_SIZE,
  createArchitectureReport,
  getArchitectureLayoutAudit,
} from "../architectureReport";
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

  it("audits real jsPDF geometry, card bounds, font floors, and text fitting", () => {
    const doc = createArchitectureReport({
      costObsLockup: transparentPng,
      databricksMark: transparentPng,
    });
    const audit = getArchitectureLayoutAudit(doc);

    expect(audit.violations).toEqual([]);
    expect(audit.overflows).toEqual([]);
    expect(audit.records.length).toBeGreaterThan(150);
    expect(new Set(audit.records.map((record) => record.page))).toEqual(new Set([1, 2, 3]));

    for (const record of audit.records) {
      const { bounds, container } = record;
      expect(bounds.x, record.label).toBeGreaterThanOrEqual(-0.2);
      expect(bounds.y, record.label).toBeGreaterThanOrEqual(-0.2);
      expect(bounds.x + bounds.width, record.label).toBeLessThanOrEqual(297.2);
      expect(bounds.y + bounds.height, record.label).toBeLessThanOrEqual(210.2);
      expect(bounds.x, record.label).toBeGreaterThanOrEqual(container.x - 0.2);
      expect(bounds.y, record.label).toBeGreaterThanOrEqual(container.y - 0.2);
      expect(bounds.x + bounds.width, record.label).toBeLessThanOrEqual(
        container.x + container.width + 0.2,
      );
      expect(bounds.y + bounds.height, record.label).toBeLessThanOrEqual(
        container.y + container.height + 0.2,
      );
      if (record.fontSize !== undefined) {
        expect(record.fontSize, record.label).toBeGreaterThanOrEqual(
          ARCHITECTURE_PDF_MIN_FONT_SIZE,
        );
      }
    }
  });

  it("ships every PDF asset in source and compiled artifact trees", () => {
    for (const assetPath of Object.values(PDF_BRAND_ASSET_PATHS)) {
      const relativePath = assetPath.replace(/^\//, "");
      expect(existsSync(resolve(repoRoot, "client/public", relativePath))).toBe(true);
      expect(existsSync(resolve(repoRoot, "static", relativePath))).toBe(true);
    }
  });
});
