import { describe, expect, it } from "vitest";
import { QueryClient } from "@tanstack/react-query";
import type { TabVisibility } from "@/components/SettingsDialog";
import {
  buildExportScopeKey,
  cancelRunningSubmitAndPollForTab,
  isRunningSubmitAndPollQuery,
  isTabDataRequested,
} from "../tabDemand";

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
    const tabs = ["dbu", "sql"] as const;
    const first = buildExportScopeKey("2026-08-01", "2026-08-28", ["2", "1"], 3, [...tabs]);
    expect(buildExportScopeKey("2026-08-01", "2026-08-28", ["1", "2"], 3, ["sql", "dbu"])).toBe(first);
    expect(buildExportScopeKey("2026-08-01", "2026-08-28", ["1", "2"], 4, [...tabs])).not.toBe(first);
    expect(buildExportScopeKey("2026-08-02", "2026-08-28", ["1", "2"], 3, [...tabs])).not.toBe(first);
  });

  it("prepares again when a previously hidden tab becomes visible", () => {
    const before = buildExportScopeKey("2026-08-01", "2026-08-28", [], 0, ["dbu"]);
    const after = buildExportScopeKey("2026-08-01", "2026-08-28", [], 0, ["dbu", "apps"]);
    expect(after).not.toBe(before);
  });

  it("cancels only the previous tab's running submit-and-poll query", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const apps = client.getQueryCache().build(client, {
      queryKey: ["apps", "dashboard-bundle", "scope"],
      queryFn: ({ signal }) => new Promise((resolve, reject) => {
        signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")));
        void resolve;
      }),
    });
    const aiml = client.getQueryCache().build(client, {
      queryKey: ["aiml", "dashboard-bundle", "scope"],
      queryFn: async () => ({ ready: true }),
    });
    client.setQueryData(aiml.queryKey, { ready: true });
    const running = apps.fetch().catch(() => undefined);

    expect(isRunningSubmitAndPollQuery("apps", apps)).toBe(true);
    expect(isRunningSubmitAndPollQuery("aiml", aiml)).toBe(false);
    await cancelRunningSubmitAndPollForTab(client, "apps");
    await running;

    expect(apps.state.fetchStatus).toBe("idle");
    expect(client.getQueryData(aiml.queryKey)).toEqual({ ready: true });
  });
});
