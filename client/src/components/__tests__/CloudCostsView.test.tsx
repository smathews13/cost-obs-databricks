import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { applyInfraPricing, getCloudInstanceFamily } from "@/utils/cloudCosts";
import { CloudCostsView } from "../CloudCostsView";

vi.mock("../KPITrendModal", () => ({
  KPITrendModal: ({ kpi }: { kpi: string }) => <div data-testid="cloud-selected-kpi">{kpi}</div>,
}));

const EMPTY_COSTS = {
  clusters: [],
  instance_families: [],
  total_estimated_cost: 0,
  total_databricks_spend: 0,
  total_dbu_hours: 0,
  start_date: "2026-08-01",
  end_date: "2026-08-28",
};

function renderView(extraProps: Partial<React.ComponentProps<typeof CloudCostsView>> = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <CloudCostsView
        data={EMPTY_COSTS}
        isLoading={false}
        timeseriesData={undefined}
        timeseriesLoading={false}
        host={null}
        {...extraProps}
      />
    </QueryClientProvider>,
  );
}

describe("CloudCostsView empty-data controls", () => {
  beforeEach(() => localStorage.clear());

  it("opens the integration wizard even when no estimate rows exist", async () => {
    renderView();

    await userEvent.click(screen.getByRole("button", { name: /integrate cloud costs/i }));

    expect(screen.getByText(/choose the cloud environment/i)).toBeInTheDocument();
  });

  it("allows the Actual Costs view to explain its unconfigured state", async () => {
    renderView();

    await userEvent.click(screen.getByRole("button", { name: "Actual Costs" }));

    expect(screen.getByText(/actual cloud costs are not connected yet/i)).toBeInTheDocument();
  });

  it("opens the actual provider selected in the multi-cloud switcher", async () => {
    renderView({
      actualData: { available: true, start_date: "2026-08-01", end_date: "2026-08-28" },
      azureActualData: { available: true, start_date: "2026-08-01", end_date: "2026-08-28" },
    });

    await userEvent.click(screen.getByRole("button", { name: /Azure/ }));
    await userEvent.click(screen.getByRole("button", { name: "Actual Costs" }));

    expect(screen.getByText("Actual Azure Infrastructure Cost")).toBeInTheDocument();
    for (const image of screen.getAllByRole("img", { name: "Azure" })) {
      expect(image).toHaveAttribute("src", expect.stringContaining("azure-128.png"));
    }
  });

  it("keeps an actual-cost total visible when optional detail is partial", async () => {
    renderView({
      gcpActualData: {
        available: true,
        availability: "partial",
        partial_reasons: { by_service: "SQL_OVERLOADED" },
        summary: {
          available: true,
          total_cost: 125,
          start_date: "2026-08-01",
          end_date: "2026-08-28",
        },
        start_date: "2026-08-01",
        end_date: "2026-08-28",
      },
    });

    await userEvent.click(screen.getByRole("button", { name: "Actual Costs" }));
    expect(screen.getByText(/Some actual-cost details are temporarily unavailable/i)).toBeInTheDocument();
    expect(screen.getByText("$125")).toBeInTheDocument();
  });

  it("loads only visible provider logos and uses the optimized Azure asset", () => {
    const { container } = renderView({
      actualData: { available: true, start_date: "2026-08-01", end_date: "2026-08-28" },
      azureActualData: { available: true, start_date: "2026-08-01", end_date: "2026-08-28" },
    });

    expect(container.querySelector('[aria-hidden="true"] img')).toBeNull();
    expect(screen.getByRole("img", { name: "Azure" })).toHaveAttribute(
      "src",
      expect.stringContaining("azure-128.png"),
    );
  });

  it("parses GCP machine families the same way as the backend", () => {
    expect(getCloudInstanceFamily("n2-standard-4", "GCP")).toBe("n2");
    expect(getCloudInstanceFamily("Standard_D8s_v3", "AZURE")).toBe("Standard_D");
    expect(getCloudInstanceFamily("m6i.xlarge", "AWS")).toBe("m6i");
  });

  it.each([
    ["Databricks Compute Spend", "infra_cost"],
    ["Total Cluster DBUs", "infra_dbu_hours"],
    ["Active Clusters", "infra_clusters"],
    ["Databricks Spend / Cluster", "avg_cost_per_cluster"],
  ])("opens the %s trend from the full card with a backend-compatible key", async (title, kpi) => {
    const clusters = [{
      cluster_id: "cluster-1",
      cluster_name: "Cluster 1",
      driver_instance_type: "m5.xlarge",
      worker_instance_type: "m5.xlarge",
      cluster_source: "UI",
      workspace_id: "123",
      state: null,
      total_dbu_hours: 10,
      databricks_spend: 15,
      days_active: 1,
      percentage: 100,
    }];
    const data = {
      ...EMPTY_COSTS,
      cloud: "AWS",
      cloud_display_name: "AWS",
      clusters,
      total_dbu_hours: 10,
      total_cluster_count: 1,
      billing_summary: {
        databricks_compute_spend: 15,
        avg_clusters_per_day: 1,
        avg_databricks_spend_per_cluster: 15,
        days_in_range: 28,
      },
    };
    renderView({
      data,
      infraData: { ...data, available: true, availability: "available" },
      startDate: "2026-08-01",
      endDate: "2026-08-28",
    });

    const card = screen.getByRole("button", { name: `See ${title} trend` });
    expect(card).toHaveClass("co-kpi-card");
    expect(card.querySelector("button")).toBeNull();
    await userEvent.click(card);
    expect(screen.getByTestId("cloud-selected-kpi")).toHaveTextContent(kpi);
  });

  it("keeps all cloud KPI cards static when their numeric values are zero", () => {
    const data = {
      ...EMPTY_COSTS,
      cloud: "AWS",
      cloud_display_name: "AWS",
      clusters: [{
        cluster_id: "cluster-zero",
        cluster_name: "Zero cluster",
        driver_instance_type: "m5.xlarge",
        worker_instance_type: "m5.xlarge",
        cluster_source: "UI",
        workspace_id: "123",
        state: null,
        total_dbu_hours: 0,
        databricks_spend: 0,
        days_active: 1,
        percentage: 0,
      }],
      total_cluster_count: 0,
      billing_summary: {
        databricks_compute_spend: 0,
        avg_clusters_per_day: 0,
        avg_databricks_spend_per_cluster: 0,
        days_in_range: 28,
      },
    };
    renderView({
      data,
      infraData: { ...data, available: true, availability: "available" },
      startDate: "2026-08-01",
      endDate: "2026-08-28",
    });

    for (const title of [
      "Databricks Compute Spend",
      "Total Cluster DBUs",
      "Active Clusters",
      "Databricks Spend / Cluster",
    ]) {
      expect(screen.queryByRole("button", { name: `See ${title} trend` })).not.toBeInTheDocument();
      expect(screen.getByText(title).closest(".co-kpi-card")?.tagName).toBe("DIV");
    }
    expect(screen.queryByText("See trend")).not.toBeInTheDocument();
  });

  it.each([
    ["Databricks Compute Spend", "infra_cost"],
    ["Databricks Spend / Cluster", "avg_cost_per_cluster"],
  ])("keeps positive %s trends available when cluster metadata is missing", async (title, kpi) => {
    const data = {
      ...EMPTY_COSTS,
      billing_summary: {
        databricks_compute_spend: 75,
        avg_clusters_per_day: 3,
        avg_databricks_spend_per_cluster: 25,
        days_in_range: 28,
      },
    };
    renderView({
      data,
      infraData: {
        ...data,
        available: true,
        availability: "partial",
        reason: "cluster_detail_unavailable",
      },
      startDate: "2026-08-01",
      endDate: "2026-08-28",
    });

    await userEvent.click(screen.getByRole("button", { name: `See ${title} trend` }));
    expect(screen.getByTestId("cloud-selected-kpi")).toHaveTextContent(kpi);
  });

  it("keeps a zero fallback spend-per-cluster card static", () => {
    const data = {
      ...EMPTY_COSTS,
      billing_summary: {
        databricks_compute_spend: 75,
        avg_clusters_per_day: 0,
        avg_databricks_spend_per_cluster: 0,
        days_in_range: 28,
      },
    };
    renderView({
      data,
      infraData: { ...data, available: true, availability: "partial" },
      startDate: "2026-08-01",
      endDate: "2026-08-28",
    });

    expect(screen.queryByRole("button", {
      name: "See Databricks Spend / Cluster trend",
    })).not.toBeInTheDocument();
  });

  it("uses compact instance metadata tooltips, a singular workspace filter, and a nowrap spend header", async () => {
    const clusters = ["123", "456"].map((workspaceId, index) => ({
      cluster_id: `cluster-${index + 1}`,
      cluster_name: `Cluster ${index + 1}`,
      driver_instance_type: "m5.xlarge",
      worker_instance_type: "m5.2xlarge",
      cluster_source: "UI",
      workspace_id: workspaceId,
      state: null,
      total_dbu_hours: 10,
      databricks_spend: 15,
      days_active: 1,
      percentage: 50,
    }));
    const data = {
      ...EMPTY_COSTS,
      cloud: "AWS",
      cloud_display_name: "AWS",
      clusters,
      total_databricks_spend: 30,
      total_dbu_hours: 20,
      total_cluster_count: 2,
    };
    renderView({
      data,
      infraData: { ...data, available: true, availability: "available" },
      workspaceNameMap: { "123": "Workspace A", "456": "Workspace B" },
    });

    expect(screen.getByRole("button", { name: "Workspace" })).toBeVisible();
    const spendHeader = screen.getByRole("columnheader", { name: /DBU Spend/i });
    expect(spendHeader).toHaveClass("whitespace-nowrap");

    const driverInfo = screen.getAllByRole("button", { name: "About driver instance type" })[0];
    expect(driverInfo.closest("td")?.querySelector("a")).toBeNull();
    await userEvent.click(driverInfo);
    expect(screen.getByRole("tooltip")).toHaveTextContent(/D means driver node/);
    expect(screen.getByRole("tooltip")).toHaveTextContent(/historical, deleted, or inaccessible/);

    await userEvent.click(screen.getAllByRole("button", { name: "About worker instance type" })[0]);
    expect(screen.getByRole("tooltip")).toHaveTextContent(/W means worker nodes/);
  });

  it("applies account pricing to every DBU list-price spend value exactly once", () => {
    const priced = applyInfraPricing({
      ...EMPTY_COSTS,
      cloud: "AWS",
      cloud_display_name: "AWS",
      clusters: [{
        cluster_id: "cluster-1",
        cluster_name: "Cluster 1",
        driver_instance_type: "m5.xlarge",
        worker_instance_type: "m5.xlarge",
        cluster_source: "UI",
        total_dbu_hours: 10,
        databricks_spend: 100,
        estimated_cost: null,
        days_active: 1,
        percentage: 100,
      }],
      total_databricks_spend: 100,
      billing_summary: {
        databricks_compute_spend: 100,
        avg_databricks_spend_per_cluster: 25,
      },
    }, 0.8);

    expect(priced?.clusters[0].databricks_spend).toBe(80);
    expect(priced?.total_databricks_spend).toBe(80);
    expect(priced?.billing_summary?.databricks_compute_spend).toBe(80);
    expect(priced?.billing_summary?.avg_databricks_spend_per_cluster).toBe(20);
    expect(applyInfraPricing(priced, 1)).toBe(priced);
  });

  it("shows a failed cluster query as unavailable instead of true zero", () => {
    renderView({
      infraData: {
        ...EMPTY_COSTS,
        cloud: "GCP",
        cloud_display_name: "GCP",
        available: false,
        availability: "unavailable",
        error: "[INSUFFICIENT_PERMISSIONS] denied",
        error_kind: "permission",
        reason: "permission_denied",
        reason_detail: "Classic cluster metadata could not be queried, so zero usage cannot be confirmed.",
      },
    });

    expect(screen.getByText("Cloud cost permissions are missing")).toBeInTheDocument();
    expect(screen.queryByText("No classic cluster infrastructure to estimate")).not.toBeInTheDocument();
  });

  it("shows actionable full-tab recovery only after required retries fail", () => {
    renderView({ data: undefined, infraData: undefined, loadError: "503: SQL capacity is full" });

    expect(screen.getByText("Cloud cost data could not be loaded")).toBeInTheDocument();
    expect(screen.getByText(/after automatic retries/i)).toBeInTheDocument();
    expect(screen.getByText(/refresh Cloud Costs/i)).toBeInTheDocument();
  });

  it("keeps authoritative totals visible when optional cluster detail fails", () => {
    const partial = {
      ...EMPTY_COSTS,
      cloud: "GCP",
      cloud_display_name: "GCP",
      available: true,
      availability: "partial" as const,
      reason: "cluster_detail_unavailable",
      reason_detail: "DBU totals are available, but cluster detail is temporarily unavailable.",
      billing_summary: {
        databricks_compute_spend: 240,
        total_dbu_hours: 120,
        total_cluster_count: 40,
        avg_clusters_per_day: 8,
        avg_databricks_spend_per_cluster: 30,
        days_in_range: 28,
      },
    };
    renderView({ data: partial, infraData: partial });

    expect(screen.getByText("Partial Cloud Costs data")).toBeInTheDocument();
    expect(screen.getByText("$240")).toBeInTheDocument();
    expect(screen.queryByText("Cloud cost data could not be loaded")).not.toBeInTheDocument();
  });

  it("shows the honest serverless-only empty state", () => {
    renderView({
      infraData: {
        ...EMPTY_COSTS,
        cloud: "GCP",
        cloud_display_name: "GCP",
        available: true,
        availability: "empty",
        reason: "serverless_only",
        reason_detail: "Usage exists, but it is serverless and has no classic cluster_id or VM metadata.",
      },
    });

    expect(screen.getByText("Only serverless usage was found")).toBeInTheDocument();
    expect(screen.getByText(/has no classic cluster_id/i)).toBeInTheDocument();
  });

  it("shows authoritative DBU spend instead of a fabricated VM cost", () => {
    const reasonDetail =
      "1 of 2 classic cluster rows had incomplete driver or worker instance metadata. DBU spend is still included.";
    renderView({
      data: {
        ...EMPTY_COSTS,
        clusters: [
          {
            cluster_id: "cluster-1",
            cluster_name: "Priced cluster",
            driver_instance_type: "m5.xlarge",
            worker_instance_type: "m5.xlarge",
            cluster_source: "UI",
            workspace_id: "123",
            state: null,
            total_dbu_hours: 10,
            databricks_spend: 7.5,
            days_active: 1,
            percentage: 100,
          },
        ],
        total_estimated_cost: null,
        total_databricks_spend: 7.5,
        total_dbu_hours: 10,
      },
      infraData: {
        cloud: "AWS",
        cloud_display_name: "AWS",
        clusters: [],
        instance_families: [],
        total_estimated_cost: null,
        total_databricks_spend: 7.5,
        total_dbu_hours: 10,
        start_date: "2026-08-01",
        end_date: "2026-08-28",
        available: true,
        availability: "partial",
        reason: "metadata_partial",
        reason_detail: reasonDetail,
      },
    });

    expect(screen.getByText("Cluster metadata is partial")).toBeInTheDocument();
    expect(screen.getByText(reasonDetail)).toBeInTheDocument();
    expect(screen.getByText("Priced cluster")).toBeInTheDocument();
    expect(screen.getByRole("columnheader", { name: /DBU Spend/i })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: /VM Cost/i })).not.toBeInTheDocument();
    expect(screen.getAllByText("$7.5")).toHaveLength(3);
  });

  it("uses full-set aggregates while labeling truncated cluster detail", () => {
    const clusters = [1, 2].map((number) => ({
      cluster_id: `cluster-${number}`,
      cluster_name: `Cluster ${number}`,
      driver_instance_type: "m5.xlarge",
      worker_instance_type: "m5.xlarge",
      cluster_source: "UI",
      workspace_id: "123",
      state: null,
      total_dbu_hours: 10,
      databricks_spend: 15,
      days_active: 1,
      percentage: 0.75,
    }));
    const infraData = {
      ...EMPTY_COSTS,
      cloud: "AWS",
      cloud_display_name: "AWS",
      clusters,
      total_databricks_spend: 2_000,
      total_dbu_hours: 1_500,
      total_cluster_count: 101,
      detail_limit: 100,
      detail_truncated: true,
      billing_summary: {
        databricks_compute_spend: 2_000,
        avg_clusters_per_day: 20,
        avg_databricks_spend_per_cluster: 100,
        days_in_range: 28,
      },
    };

    renderView({ data: infraData, infraData });

    expect(screen.getByText("$2,000")).toBeInTheDocument();
    expect(screen.getByText("1.5K")).toBeInTheDocument();
    expect(screen.getByText(/showing top 100 of 101 clusters/i)).toBeInTheDocument();
    expect(screen.getByText(/top 100 detail subtotal/i)).toBeInTheDocument();
    expect(screen.getByText("$30")).toBeInTheDocument();
  });

  it("sorts cluster columns from keyboard-accessible header buttons", async () => {
    const clusters = [{
      cluster_id: "cluster-1",
      cluster_name: "Cluster 1",
      driver_instance_type: "m5.xlarge",
      worker_instance_type: "m5.xlarge",
      cluster_source: "UI",
      workspace_id: "123",
      state: null,
      total_dbu_hours: 10,
      databricks_spend: 15,
      days_active: 1,
      percentage: 100,
    }];
    const data = {
      ...EMPTY_COSTS,
      cloud: "AWS",
      cloud_display_name: "AWS",
      clusters,
      instance_families: [],
    };
    renderView({ data, infraData: { ...data, available: true, availability: "available" } });

    const header = screen.getByRole("columnheader", { name: "Cluster" });
    expect(header).toHaveAttribute("aria-sort", "none");
    const sortButton = screen.getByRole("button", { name: "Cluster" });
    sortButton.focus();
    await userEvent.keyboard("{Enter}");
    expect(header).toHaveAttribute("aria-sort", "descending");
    await userEvent.keyboard(" ");
    expect(header).toHaveAttribute("aria-sort", "ascending");
  });
});
