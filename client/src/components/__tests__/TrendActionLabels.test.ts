import { readFileSync } from "node:fs";
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

describe("KPI trend action labels", () => {
  it.each(KPI_SURFACES)("%s gives every trend action a metric-specific name", (path) => {
    const source = readFileSync(new URL(path, import.meta.url), "utf8");
    const actions = source.match(/<TrendAction\b[\s\S]*?\/>/g) ?? [];

    expect(actions.length).toBeGreaterThan(0);
    for (const action of actions) {
      expect(action).toMatch(/\bariaLabel=/);
      expect(action).not.toContain('ariaLabel="See trend"');
    }
  });
});
