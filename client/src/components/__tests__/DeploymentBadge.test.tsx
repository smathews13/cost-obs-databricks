import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DeploymentBadge } from "../DeploymentBadge";
import {
  abbreviateCommit,
  formatDeploymentBadgeDate,
  formatDeploymentTimestamp,
  type DeploymentMetadata,
} from "@/utils/deploymentMetadata";

const metadata: DeploymentMetadata = {
  deployed_at: "2026-08-30T21:29:46Z",
  deployer: "deployer@example.com",
  commit_sha: "ed86035f1234567890",
  available: true,
  source: "databricks_apps_api",
};

describe("DeploymentBadge", () => {
  it("formats a compact date and adds the year only when it disambiguates", () => {
    expect(formatDeploymentBadgeDate(
      metadata.deployed_at,
      new Date("2026-12-01T00:00:00Z"),
      "UTC",
    )).toBe("Aug 30");
    expect(formatDeploymentBadgeDate(
      metadata.deployed_at,
      new Date("2027-01-01T00:00:00Z"),
      "UTC",
    )).toBe("Aug 30, 2026");
    expect(abbreviateCommit(metadata.commit_sha)).toBe("ed86035f");
  });

  it("shows authoritative details with UTC and local timezone context", () => {
    const formatted = formatDeploymentTimestamp(
      metadata.deployed_at,
      "America/Denver",
    );

    expect(formatted).toContain("Aug 30, 2026, 9:29:46 PM UTC");
    expect(formatted).toMatch(/Aug 30, 2026, 3:29:46 PM (MDT|GMT-6)/);
  });

  it("shares one tooltip between pointer hover and keyboard focus", () => {
    render(<DeploymentBadge metadata={metadata} />);

    const trigger = screen.getByRole("button", { name: /deployment information: aug 30/i });
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    fireEvent.mouseEnter(trigger);
    const tooltip = screen.getByRole("tooltip");
    expect(trigger).toHaveAttribute("aria-describedby", tooltip.id);
    expect(tooltip).toHaveTextContent("by deployer@example.com");
    expect(tooltip).toHaveTextContent("commit ed86035f");

    act(() => trigger.focus());
    expect(trigger).toHaveFocus();
  });

  it("keeps the full date on wide rails and collapses to an accessible icon trigger", () => {
    render(<DeploymentBadge metadata={metadata} />);

    const trigger = screen.getByRole("button", { name: /deployment information: aug 30/i });
    const date = screen.getByTestId("deployment-badge-date");

    expect(date).toHaveTextContent("Aug 30");
    expect(date).toHaveClass("hidden", "lg:inline");
    expect(trigger).toHaveClass(
      "h-[22px]",
      "rounded-[4px]",
      "text-[10px]",
      "w-[88px]",
      "rail-status-badge",
      "font-bold",
    );
    expect(trigger.className).not.toContain("border-");
    expect(screen.getByTestId("deployment-status-dot")).toBeVisible();
    expect(trigger).not.toHaveAttribute("aria-describedby");
    act(() => trigger.focus());
    expect(trigger).toHaveAttribute("aria-describedby", screen.getByRole("tooltip").id);
  });

  it("labels a missing deployment timestamp without fabricating other fields", () => {
    render(<DeploymentBadge metadata={{
      deployed_at: null,
      deployer: null,
      commit_sha: null,
      available: false,
      source: "unavailable",
    }} />);

    const trigger = screen.getByRole("button", { name: /deployment information: deploy info/i });
    act(() => trigger.focus());
    expect(screen.getByRole("tooltip")).toHaveTextContent("Deployment date unavailable");
    expect(screen.queryByText(/\bby\b/)).not.toBeInTheDocument();
    expect(screen.queryByText(/\bcommit\b/)).not.toBeInTheDocument();
  });

  it("does not crash when an older deployment response omits its source", () => {
    render(<DeploymentBadge metadata={{
      deployed_at: null,
      deployer: null,
      commit_sha: null,
      available: false,
    } as DeploymentMetadata} />);

    expect(screen.getByRole("button", {
      name: /deployment information: deploy info/i,
    })).toBeVisible();
  });

  it("labels process start as an approximation rather than deployment time", () => {
    render(<DeploymentBadge metadata={{
      ...metadata,
      deployer: null,
      commit_sha: null,
      source: "process_start_approximate_restart",
    }} />);

    act(() => screen.getByRole("button", { name: /deployment information/i }).focus());
    expect(screen.getByRole("tooltip")).toHaveTextContent(
      /server process started approximately.*may reflect a restart, not deployment/i,
    );
    expect(screen.getByRole("tooltip")).not.toHaveTextContent(/^Deployed /);
  });
});
