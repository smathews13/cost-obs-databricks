import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import type { AppsApp, AppsDashboardBundle } from "@/types/billing";
import { getAppFallbackColor } from "@/utils/apps";
import { AppsCostCenter } from "../AppsCostCenter";

const metadataApp: AppsApp = {
  app_id: "app-123",
  app_name: "Metadata App",
  app_url: "https://metadata-app.example.databricksapps.com",
  total_dbus: 42,
  total_spend: 123.45,
  workspace_count: 1,
  workspace_names: ["1001"],
  workspaces: [{ id: "1001", name: "Customer Workspace" }],
  days_active: 7,
  last_usage_date: "2026-08-27",
  percentage: 100,
  is_registered: true,
  status: "active",
  metadata: {
    availability: "available",
    description: "Tracks safe customer application metadata.",
    creator: "owner@example.com",
    updater: "editor@example.com",
    update_time: "2026-08-27T12:30:00Z",
    compute_size: "MEDIUM",
    compute_status: { state: "ACTIVE", message: "Compute is active", instances: 2 },
    app_status: { state: "RUNNING", message: "Application is running", instances: 2 },
    deployment: {
      deployment_id: "deployment-1",
      state: "SUCCEEDED",
      message: "Deployment succeeded",
      creator: "deployer@example.com",
      create_time: "2026-08-27T12:00:00Z",
      update_time: "2026-08-27T12:20:00Z",
      mode: "SNAPSHOT",
      pending: false,
    },
    source_code_path: "/Workspace/Users/owner@example.com/metadata-app",
    git: {
      repository_url: "https://github.example.com/customer/metadata-app",
      branch: "main",
      tag: "",
      commit: "abc123def4567890",
      source_code_path: "app",
    },
    thumbnail_url: "/api/apps/thumbnail?app_id=app-123",
  },
  resource_bindings: [
    { name: "analytics-warehouse", type: "SQL_WAREHOUSE", description: "Can use" },
  ],
  sku_breakdown: [
    { sku_name: "APPS_SERVERLESS_COMPUTE", total_dbus: 42, total_spend: 123.45, percentage: 100 },
  ],
};

const historicalApp: AppsApp = {
  ...metadataApp,
  app_id: "deleted-app-id",
  app_name: "deleted-app-id",
  app_url: "",
  is_registered: false,
  status: "inactive",
  metadata: {
    availability: "unavailable",
    description: "",
    creator: "",
    updater: "",
    compute_size: "",
    compute_status: null,
    app_status: null,
    deployment: null,
    source_code_path: "",
    git: null,
    thumbnail_url: null,
  },
  resource_bindings: [],
};

function bundle(apps: AppsApp[]): AppsDashboardBundle {
  const activeCount = apps.filter(app => app.status === "active").length;
  return {
    summary: {
      total_dbus: 42,
      total_spend: 123.45,
      app_count: apps.length,
      active_app_count: activeCount,
      workspace_count: 1,
      days_in_range: 30,
      avg_daily_spend: 4.12,
      avg_cost_per_app: 4.12,
    },
    apps: {
      apps,
      total_spend: 123.45,
      total_app_count: apps.length,
      active_count: activeCount,
      inactive_count: apps.filter(app => app.status === "inactive").length,
      active_window: {
        start_date: "2026-08-24",
        end_date: "2026-08-30",
        days: 7,
        definition: "Currently registered apps with positive Apps compute usage",
      },
      inactive_summary: { count: 0, total_spend: 0, total_dbus: 0, percentage: 0 },
      unregistered_summary: {
        count: apps.filter(app => !app.is_registered).length,
        total_spend: 0,
        total_dbus: 0,
        percentage: 0,
      },
    },
    timeseries: { timeseries: [], categories: ["Total"] },
    connected_artifacts: [],
    workspaces: [{ id: "1001", name: "Customer Workspace" }],
    active_only: false,
    start_date: "2026-08-01",
    end_date: "2026-08-30",
  };
}

function renderApps(apps: AppsApp[]) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <AppsCostCenter
        data={bundle(apps)}
        isLoading={false}
        host="https://workspace.example.com"
        startDate="2026-08-01"
        endDate="2026-08-30"
      />
    </QueryClientProvider>,
  );
}

describe("AppsCostCenter metadata detail", () => {
  it("uses the same active count contract for the KPI and status breakdown", () => {
    renderApps([metadataApp, historicalApp]);

    expect(screen.getByTestId("active-apps-kpi-value")).toHaveTextContent("1");
    expect(screen.getByTestId("active-apps-breakdown-count")).toHaveTextContent("1 Active");
    expect(screen.getByText("last 7 days · 1 workspace")).toBeVisible();
  });

  it("falls back to deterministic identity-colored initials when thumbnail loading fails", () => {
    renderApps([metadataApp]);
    fireEvent.error(screen.getByAltText("Metadata App icon"));

    const fallback = screen.getByLabelText("Metadata App fallback icon");
    expect(fallback).toHaveTextContent("MA");
    expect(fallback).toHaveStyle({
      backgroundColor: getAppFallbackColor(metadataApp.app_id),
    });
  });

  it("shows safe app, compute, deployment, workspace, and resource metadata", () => {
    renderApps([metadataApp]);

    expect(screen.getByAltText("Metadata App icon")).toHaveAttribute(
      "src",
      "/api/apps/thumbnail?app_id=app-123",
    );
    fireEvent.click(screen.getByText("Metadata App"));

    expect(screen.getByText("Tracks safe customer application metadata.")).toBeInTheDocument();
    expect(screen.getByText("App: Running")).toBeInTheDocument();
    expect(screen.getByText("Compute: Active")).toBeInTheDocument();
    expect(screen.getByText("Deploy: Succeeded")).toBeInTheDocument();
    expect(screen.getByText("owner")).toBeInTheDocument();
    expect(screen.getByText("Customer Workspace")).toBeInTheDocument();
    expect(screen.getByText("abc123def456")).toBeInTheDocument();
    expect(screen.getByText("analytics-warehouse")).toBeInTheDocument();
    expect(screen.getByText("APPS_SERVERLESS_COMPUTE")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Manage App →" })).toHaveAttribute(
      "href",
      "https://workspace.example.com/apps-v2/app/Metadata%20App/overview",
    );
    expect(screen.getByRole("link", { name: "Source Repository →" })).toHaveAttribute(
      "href",
      "https://github.example.com/customer/metadata-app",
    );
  });

  it("keeps billing detail and explains unavailable historical apps", () => {
    renderApps([historicalApp]);
    fireEvent.click(screen.getByText("deleted-app-id"));

    expect(screen.getAllByText(/\$123/).length).toBeGreaterThan(0);
    expect(screen.getByText(/no longer available in the Apps registry/i)).toBeInTheDocument();
    expect(screen.getByText(/Historical billing record: app deleted or inaccessible/i)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Live App Endpoint →" })).not.toBeInTheDocument();
  });

  it("derives selected detail from the current scoped bundle and closes stale detail", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const view = render(
      <QueryClientProvider client={client}>
        <AppsCostCenter data={bundle([metadataApp])} isLoading={false} />
      </QueryClientProvider>,
    );
    fireEvent.click(screen.getByText("Metadata App"));
    expect(screen.getByText("Tracks safe customer application metadata.")).toBeVisible();

    view.rerender(
      <QueryClientProvider client={client}>
        <AppsCostCenter data={bundle([])} isLoading={false} />
      </QueryClientProvider>,
    );

    expect(screen.queryByText("Tracks safe customer application metadata.")).not.toBeInTheDocument();
    expect(await screen.findByRole("status")).toHaveTextContent("no longer available in the current date, workspace, or source scope");
  });

  it("labels connected resources as registry-wide metadata", () => {
    const data = bundle([metadataApp]);
    data.connected_artifacts = [{
      app_id: "app-123",
      app_name: "Metadata App",
      artifact_name: "warehouse-1",
      artifact_type: "SQL_WAREHOUSE",
      artifact_description: "Can use",
    }];
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <AppsCostCenter data={data} isLoading={false} />
      </QueryClientProvider>,
    );

    expect(screen.getByText(/account-wide, not filtered by date, workspace, or source/i)).toBeVisible();
  });

  it("links a service principal only when the server supplies its workspace object ID", () => {
    const data = bundle([metadataApp]);
    data.connected_artifacts = [{
      app_id: "app-123",
      app_name: "Metadata App",
      artifact_name: "app-run-as",
      artifact_type: "SERVICE_PRINCIPAL",
      artifact_description: "Run-as identity",
      artifact_id: "123456789",
    }];

    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <AppsCostCenter
          data={data}
          isLoading={false}
          host="https://workspace.example.com/"
        />
      </QueryClientProvider>,
    );

    expect(screen.getByRole("link", { name: "app-run-as" })).toHaveAttribute(
      "href",
      "https://workspace.example.com/settings/identity-and-access/service-principals/123456789",
    );
    expect(screen.getByTestId("service-principal-external-link")).toBeInTheDocument();
  });

  it("keeps service-principal display names plain when no valid object ID is present", () => {
    const data = bundle([metadataApp]);
    data.connected_artifacts = [
      {
        app_id: "app-123",
        app_name: "Metadata App",
        artifact_name: "display-name-only",
        artifact_type: "SERVICE_PRINCIPAL",
        artifact_description: "Run-as identity",
      },
      {
        app_id: "app-123",
        app_name: "Metadata App",
        artifact_name: "client-id-is-not-a-workspace-id",
        artifact_type: "SERVICE_PRINCIPAL",
        artifact_description: "Run-as identity",
        artifact_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
      },
    ];

    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <AppsCostCenter data={data} isLoading={false} host="https://workspace.example.com" />
      </QueryClientProvider>,
    );

    expect(screen.getByText("display-name-only").closest("a")).toBeNull();
    expect(screen.getByText("client-id-is-not-a-workspace-id").closest("a")).toBeNull();
    expect(screen.queryByTestId("service-principal-external-link")).not.toBeInTheDocument();
  });
});
