import { readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "vitest";

describe("initial bundle boundaries", () => {
  it("keeps the report generator lazy and removes architecture generation", () => {
    const app = readFileSync("src/App.tsx", "utf8");

    expect(app).not.toMatch(/import\s+\{[^}]*generate(?:Architecture|Cost)Report[^}]*\}\s+from/);
    expect(app).toContain('await import("@/utils/pdfExport")');
    expect(app).not.toContain("architectureReport");
    expect(app).toContain("downloadArchitecturePdf");
    expect(readdirSync("../static/assets")).not.toEqual(
      expect.arrayContaining([expect.stringMatching(/^architectureReport-.*\.js$/)]),
    );
  });

  it("lazy-loads Settings and does not modulepreload PDF code", () => {
    const app = readFileSync("src/App.tsx", "utf8");
    const html = readFileSync("../static/index.html", "utf8");
    const entryPath = html.match(/<script type="module" crossorigin src="\/assets\/([^"]+)"/)?.[1];

    expect(app).not.toMatch(/import\s+\{\s*SettingsDialog\s*\}\s+from/);
    expect(app).toContain('import("@/components/SettingsDialog")');
    expect(html).not.toMatch(/rel="modulepreload"[^>]+vendor-pdf/);
    expect(entryPath).toBeTruthy();
    expect(readFileSync(`../static/assets/${entryPath}`, "utf8")).not.toMatch(
      /from["']\.\/vendor-pdf-/,
    );
  });
});
