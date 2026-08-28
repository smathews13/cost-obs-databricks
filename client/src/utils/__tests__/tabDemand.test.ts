import { describe, expect, it } from "vitest";
import type { TabVisibility } from "@/components/SettingsDialog";
import { buildExportScopeKey, isTabDataRequested } from "../tabDemand";

const visibility: TabVisibility = {
  dbu: true,
  sql: true,
  infra: true,
  optimizer: true,
  kpis: true,
  aiml: false,
  apps: true,
  tagging: true,
  "users-groups": true,
};

describe("on-demand tab data", () => {
  it("loads only the active tab during normal dashboard use", () => {
    expect(isTabDataRequested("dbu", "dbu", false, visibility)).toBe(true);
    expect(isTabDataRequested("sql", "dbu", false, visibility)).toBe(false);
  });

  it("loads visible report tabs while export is open", () => {
    expect(isTabDataRequested("sql", "dbu", true, visibility)).toBe(true);
    expect(isTabDataRequested("aiml", "dbu", true, visibility)).toBe(false);
  });

  it("uses one stable cache key per report filter scope", () => {
    const first = buildExportScopeKey("2026-08-01", "2026-08-28", ["2", "1"], 3);
    expect(buildExportScopeKey("2026-08-01", "2026-08-28", ["1", "2"], 3)).toBe(first);
    expect(buildExportScopeKey("2026-08-01", "2026-08-28", ["1", "2"], 4)).not.toBe(first);
    expect(buildExportScopeKey("2026-08-02", "2026-08-28", ["1", "2"], 3)).not.toBe(first);
  });
});
