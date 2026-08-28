import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { InteractiveBreakdown } from "../InteractiveBreakdown";
import { PipelineObjectsTable } from "../PipelineObjectsTable";
import { SpendChart } from "../SpendChart";
import type { PipelineObjectsResponse } from "@/types/billing";

describe("empty dashboard panels", () => {
  it("hides spend, interactive compute, and pipeline panels with no source rows", () => {
    const { container: spend } = render(<SpendChart data={{ timeseries: [], categories: [] }} isLoading={false} />);
    const { container: interactive } = render(
      <InteractiveBreakdown
        data={{ items: [], total_spend: 0, start_date: "2026-01-01", end_date: "2026-01-31" }}
        isLoading={false}
        host={null}
      />,
    );
    const { container: pipelines } = render(
      <PipelineObjectsTable
        data={{ objects: [], total_spend: 0, start_date: "2026-01-01", end_date: "2026-01-31" }}
        isLoading={false}
        host={null}
      />,
    );

    expect(spend).toBeEmptyDOMElement();
    expect(interactive).toBeEmptyDOMElement();
    expect(pipelines).toBeEmptyDOMElement();
  });

  it("hides a pipeline panel whose only rows are unresolved historical jobs", () => {
    const data: PipelineObjectsResponse = {
      objects: [{
        object_type: "Job",
        object_id: "123",
        object_name: "123",
        owner: null,
        workspace_id: "456",
        object_state: null,
        total_dbus: 0,
        total_spend: 0,
        total_runs: 0,
        percentage: 0,
      }],
      total_spend: 0,
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    };

    const { container } = render(<PipelineObjectsTable data={data} isLoading={false} host={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("keeps a populated pipeline panel visible when a user search has no matches", () => {
    const data: PipelineObjectsResponse = {
      objects: [{
        object_type: "Job",
        object_id: "123",
        object_name: "daily-refresh",
        owner: "owner@example.com",
        workspace_id: "456",
        object_state: "RUNNING",
        total_dbus: 2,
        total_spend: 1,
        total_runs: 1,
        percentage: 100,
      }],
      total_spend: 1,
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    };

    render(<PipelineObjectsTable data={data} isLoading={false} host={null} />);
    fireEvent.change(screen.getByPlaceholderText("Search ETLs..."), { target: { value: "missing" } });

    expect(screen.getByText(/No jobs or pipelines match the current filters/i)).toBeInTheDocument();
  });
});
