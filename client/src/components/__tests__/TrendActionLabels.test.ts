import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const KPI_SURFACES = [
  "../SummaryCards.tsx",
  "../PlatformKPIsView.tsx",
  "../SQLWarehousing360.tsx",
  "../../pages/UsersGroups.tsx",
  "../AppsCostCenter.tsx",
  "../AIMLCostCenter.tsx",
  "../CloudCostsView.tsx",
  "../TaggingHub.tsx",
] as const;

describe("KPI card contracts", () => {
  it.each(KPI_SURFACES)("%s uses full-card actions with metric-specific names", (path) => {
    const source = readFileSync(new URL(path, import.meta.url), "utf8");
    const actionCount = (source.match(/\bonActivate=/g) ?? []).length;
    const labelCount = (source.match(/\bariaLabel=/g) ?? []).length;
    const labels = source.match(/ariaLabel="([^"]+)"/g) ?? [];

    expect(source).toContain("KPICard");
    expect(source).not.toContain("TrendAction");
    expect(actionCount).toBeGreaterThan(0);
    expect(labelCount).toBe(actionCount);
    for (const label of labels) {
      expect(label).toMatch(/ariaLabel="See .+ trend"/);
      expect(label).not.toBe('ariaLabel="See trend"');
    }
  });

  it("uses shared responsive layout, hover, and focus-visible rules", () => {
    const css = readFileSync(join(process.cwd(), "src/index.css"), "utf8");

    expect(css).toContain(".co-kpi-card__layout");
    expect(css).toContain("container-type: inline-size");
    expect(css).toContain("@container (max-width: 205px)");
    expect(css).toContain(".co-kpi-card--interactive:focus-visible");
    expect(css).toContain(".co-kpi-card--interactive:hover");
    expect(css).toMatch(/@container[\s\S]*?\.co-kpi-card__title--nowrap[\s\S]*?white-space: normal/);
  });
});
