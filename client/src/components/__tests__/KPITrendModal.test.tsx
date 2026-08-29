import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { setActiveSourceLabels } from "@/hooks/useBillingData";
import { KPITrendModal } from "../KPITrendModal";
import { PLATFORM_KPI_TREND_KEYS } from "../PlatformKPIsView";

afterEach(() => {
  setActiveSourceLabels([]);
  vi.unstubAllGlobals();
});

describe("KPITrendModal query routing", () => {
  it.each(PLATFORM_KPI_TREND_KEYS)(
    "routes clickable platform KPI %s only to its platform trend",
    async (kpi) => {
      setActiveSourceLabels(["shared-west"]);
      const fetchMock = vi.fn(async () => new Response(JSON.stringify({
        kpi,
        granularity: "daily",
        data_points: [{ date: "2026-08-01", value: 1 }],
        summary: {
          period_start_value: 1,
          period_end_value: 1,
          change_amount: 0,
          change_percent: 0,
          min_value: 1,
          max_value: 1,
          avg_value: 1,
          trend: "flat",
        },
      }), { status: 200 }));
      vi.stubGlobal("fetch", fetchMock);
      const client = new QueryClient({
        defaultOptions: { queries: { retry: false } },
      });

      render(
        <QueryClientProvider client={client}>
          <KPITrendModal
            variant="platform"
            kpi={kpi}
            kpiLabel={kpi}
            isOpen
            onClose={vi.fn()}
            startDate="2026-08-01"
            endDate="2026-08-28"
            workspaceIds={["2", "1"]}
            queryKeyPrefix="kpis-platform-kpi-trend"
          />
        </QueryClientProvider>,
      );

      await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
      const url = new URL(String(fetchMock.mock.calls[0][0]), "https://example.test");
      expect(url.pathname).toBe("/api/billing/platform-kpi-trend");
      expect(url.searchParams.get("kpi")).toBe(kpi);
      expect(url.searchParams.get("workspace_ids")).toBe("1,2");
      expect(url.searchParams.getAll("source_labels")).toEqual(["shared-west"]);
      expect(url.searchParams.get("tab")).toBe("kpis");
    },
  );
});
