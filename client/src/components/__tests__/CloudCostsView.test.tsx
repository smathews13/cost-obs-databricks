import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it } from "vitest";
import { applyInfraPricing, getCloudInstanceFamily } from "@/utils/cloudCosts";
import { CloudCostsView } from "../CloudCostsView";

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
  });

  it("parses GCP machine families the same way as the backend", () => {
    expect(getCloudInstanceFamily("n2-standard-4", "GCP")).toBe("n2");
    expect(getCloudInstanceFamily("Standard_D8s_v3", "AZURE")).toBe("Standard_D");
    expect(getCloudInstanceFamily("m6i.xlarge", "AWS")).toBe("m6i");
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
});
