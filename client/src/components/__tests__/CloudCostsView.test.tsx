import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it } from "vitest";
import { getCloudInstanceFamily } from "@/utils/cloudCosts";
import { CloudCostsView } from "../CloudCostsView";

const EMPTY_COSTS = {
  clusters: [],
  instance_families: [],
  total_estimated_cost: 0,
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

  it("shows valid estimates with a partial metadata warning", () => {
    const reasonDetail =
      "1 of 2 classic cluster rows were omitted because driver or worker instance metadata was unavailable.";
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
            estimated_aws_cost: 7.5,
            days_active: 1,
            percentage: 100,
          },
        ],
        total_estimated_cost: 7.5,
        total_dbu_hours: 10,
      },
      infraData: {
        cloud: "AWS",
        cloud_display_name: "AWS",
        clusters: [],
        instance_families: [],
        total_estimated_cost: 7.5,
        total_dbu_hours: 10,
        start_date: "2026-08-01",
        end_date: "2026-08-28",
        available: true,
        availability: "partial",
        reason: "metadata_partial",
        reason_detail: reasonDetail,
      },
    });

    expect(screen.getByText("Infrastructure estimate is partial")).toBeInTheDocument();
    expect(screen.getByText(reasonDetail)).toBeInTheDocument();
    expect(screen.getByText("Priced cluster")).toBeInTheDocument();
  });
});
