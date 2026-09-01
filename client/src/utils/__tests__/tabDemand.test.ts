import { describe, expect, it } from "vitest";
import { QueryClient } from "@tanstack/react-query";
import {
  buildExportScopeKey,
  cancelExportPreparationQueries,
  cancelRunningSubmitAndPollForTab,
  clearTabDemandRefreshPhases,
  createTabDemandState,
  isTabDemandUnresolved,
  isTabProducerActive,
  isRunningSubmitAndPollQuery,
  isTabDataRequested,
  queueTabDemand,
  requeueTabDemand,
  settleTabDemand,
} from "../tabDemand";

describe("on-demand tab data", () => {
  it("clears waiting refresh phases after a refresh fails before fetching", () => {
    const phase = { apps: "waiting" as const, tagging: "fetching" as const };

    expect(clearTabDemandRefreshPhases(phase, ["apps"])).toEqual({
      tagging: "fetching",
    });
  });

  it("keeps nine rapidly visited tabs spinning while only two producers run", () => {
    const tabs = [
      "dbu", "sql", "infra", "optimizer", "kpis", "aiml", "apps", "tagging",
      "users-groups",
    ] as const;
    let state = createTabDemandState("scope-a", "dbu");

    for (const tab of tabs.slice(1)) {
      state = queueTabDemand(state, {
        scopeKey: "scope-a",
        currentTab: tab,
        visibleTabs: tabs,
      });
      expect(state.active.length).toBeLessThanOrEqual(2);
      expect(state.active).toContain(tab);
    }

    expect(tabs.every((tab) => isTabDemandUnresolved(state, tab))).toBe(true);
    expect(tabs.filter((tab) => isTabProducerActive(state, tab))).toHaveLength(2);

    while (state.active.length > 0) {
      state = settleTabDemand(state, [...state.active], "users-groups");
      expect(state.active.length).toBeLessThanOrEqual(2);
    }
    expect(state.queued).toEqual([]);
    expect(new Set(state.settled)).toEqual(new Set(tabs));
  });

  it("preempts the oldest background producer for the current tab", () => {
    const visible = ["dbu", "sql", "apps"] as const;
    let state = createTabDemandState("scope-a", "dbu");
    state = queueTabDemand(state, {
      scopeKey: "scope-a",
      currentTab: "sql",
      visibleTabs: visible,
    });
    state = queueTabDemand(state, {
      scopeKey: "scope-a",
      currentTab: "apps",
      visibleTabs: visible,
    });

    expect(state.active).toEqual(["sql", "apps"]);
    expect(state.queued).toEqual(["dbu"]);
    expect(state.active).toHaveLength(2);
    expect(isTabDemandUnresolved(state, "dbu")).toBe(true);
  });

  it("resets settlement and safely requeues visited visible tabs for a new scope", () => {
    const visible = ["dbu", "sql", "apps"] as const;
    let state = createTabDemandState("scope-a", "dbu");
    for (const tab of visible) {
      state = queueTabDemand(state, {
        scopeKey: "scope-a",
        currentTab: tab,
        visibleTabs: visible,
      });
    }
    while (state.active.length) {
      state = settleTabDemand(state, [...state.active], "apps");
    }

    state = queueTabDemand(state, {
      scopeKey: "scope-b",
      currentTab: "apps",
      visibleTabs: visible,
    });
    expect(state.settled).toEqual([]);
    expect(state.active[0]).toBe("apps");
    expect(state.active.length).toBe(2);
    expect(visible.every((tab) => isTabDemandUnresolved(state, tab))).toBe(true);
  });

  it("queues export-selected tabs through the same bounded producer budget", () => {
    const visible = ["dbu", "sql", "apps", "aiml"] as const;
    const state = queueTabDemand(
      createTabDemandState("scope-a", "dbu"),
      {
        scopeKey: "scope-a",
        currentTab: "dbu",
        visibleTabs: visible,
        exportTabs: ["sql", "apps", "aiml"],
      },
    );

    expect(state.active).toHaveLength(2);
    expect(state.queued).toHaveLength(2);
    expect(["sql", "apps", "aiml"].every((tab) => (
      state.active.includes(tab) || state.queued.includes(tab)
    ))).toBe(true);
  });

  it("requeues settled refreshes while preserving the two-producer cap", () => {
    const visible = ["dbu", "sql", "apps"] as const;
    let state = createTabDemandState("scope-a", "dbu");
    state = queueTabDemand(state, {
      scopeKey: "scope-a",
      currentTab: "sql",
      visibleTabs: visible,
    });
    state = settleTabDemand(state, ["dbu", "sql"], "apps");
    state = queueTabDemand(state, {
      scopeKey: "scope-a",
      currentTab: "apps",
      visibleTabs: visible,
    });

    state = requeueTabDemand(state, ["dbu", "sql", "apps"], "apps");

    expect(state.active[0]).toBe("apps");
    expect(state.active).toHaveLength(2);
    expect(state.queued).toHaveLength(1);
    expect(state.settled).toEqual([]);
    expect(visible.every((tab) => isTabDemandUnresolved(state, tab))).toBe(true);
  });

  it("requeues only stale settled tabs selected for export", () => {
    const visible = ["dbu", "sql", "apps", "aiml"] as const;
    let state = createTabDemandState("scope-a", "dbu");
    for (const tab of visible.slice(1)) {
      state = queueTabDemand(state, {
        scopeKey: "scope-a",
        currentTab: tab,
        visibleTabs: visible,
      });
    }
    while (state.active.length) {
      state = settleTabDemand(state, [...state.active], "dbu");
    }

    state = requeueTabDemand(state, ["sql", "apps"], "dbu");

    expect(state.active).toEqual(["sql", "apps"]);
    expect(state.active).toHaveLength(2);
    expect(state.settled).toEqual(expect.arrayContaining(["dbu", "aiml"]));
    expect(isTabDemandUnresolved(state, "dbu")).toBe(false);
    expect(isTabDemandUnresolved(state, "aiml")).toBe(false);
  });

  it.each(["cancel", "successful export"])(
    "removes export-only demand after %s without adding it to auto-refresh visits",
    () => {
      const visible = ["dbu", "apps", "aiml"] as const;
      let state = createTabDemandState("scope-a", "dbu");
      state = queueTabDemand(state, {
        scopeKey: "scope-a",
        currentTab: "dbu",
        visibleTabs: visible,
        exportTabs: ["apps", "aiml"],
      });
      state = requeueTabDemand(
        state,
        ["apps", "aiml"],
        "dbu",
        undefined,
        false,
      );

      expect(state.visited).toEqual(["dbu"]);
      expect([...state.active, ...state.queued]).toEqual(
        expect.arrayContaining(["apps", "aiml"]),
      );

      state = queueTabDemand(state, {
        scopeKey: "scope-a",
        currentTab: "dbu",
        visibleTabs: visible,
        exportTabs: [],
      });

      expect(state.visited).toEqual(["dbu"]);
      expect(state.active).toEqual(["dbu"]);
      expect(state.queued).toEqual([]);
    },
  );

  it("keeps active and previously visited tabs requested until they settle", () => {
    expect(isTabDataRequested("dbu", "dbu")).toBe(true);
    expect(isTabDataRequested("sql", "dbu")).toBe(false);
    expect(isTabDataRequested("sql", "dbu", [], new Set(["sql"]))).toBe(true);
    expect(isTabDataRequested("apps", "dbu", [], new Set(["sql"]))).toBe(false);
  });

  it("does not load report tabs when export merely opens", () => {
    expect(isTabDataRequested("sql", "dbu", [])).toBe(false);
    expect(isTabDataRequested("apps", "dbu", [])).toBe(false);
  });

  it("loads only tabs demanded by selected report sections", () => {
    expect(isTabDataRequested("sql", "dbu", ["sql", "apps"])).toBe(true);
    expect(isTabDataRequested("apps", "dbu", ["sql", "apps"])).toBe(true);
    expect(isTabDataRequested("infra", "dbu", ["sql", "apps"])).toBe(false);
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

  it("aborts export-owned pollers while preserving the active tab poller", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const makeRunning = (prefix: "apps" | "aiml" | "dbsql") => {
      const query = client.getQueryCache().build(client, {
        queryKey: [prefix, "dashboard-bundle", "scope"],
        queryFn: ({ signal }) => new Promise((resolve, reject) => {
          signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")));
          void resolve;
        }),
      });
      return { query, running: query.fetch().catch(() => undefined) };
    };
    const apps = makeRunning("apps");
    const aiml = makeRunning("aiml");
    const dbsql = makeRunning("dbsql");

    await cancelExportPreparationQueries(client, "apps");
    await Promise.all([aiml.running, dbsql.running]);

    expect(apps.query.state.fetchStatus).toBe("fetching");
    expect(aiml.query.state.fetchStatus).toBe("idle");
    expect(dbsql.query.state.fetchStatus).toBe("idle");
    await cancelExportPreparationQueries(client);
    await apps.running;
    expect(apps.query.state.fetchStatus).toBe("idle");
  });
});
