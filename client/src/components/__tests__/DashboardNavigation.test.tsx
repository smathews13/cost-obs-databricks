import { useState } from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { DashboardTabNavigation } from "@/App";
import type { TabVisibility } from "@/utils/settingsHydration";

const visibility: TabVisibility = {
  dbu: true,
  sql: true,
  aiml: false,
  apps: true,
  tagging: false,
  "users-groups": true,
  kpis: true,
  infra: true,
  optimizer: true,
};

function NavigationHarness() {
  const [activeTab, setActiveTab] = useState<keyof TabVisibility>("dbu");
  return (
    <>
      <DashboardTabNavigation
        activeTab={activeTab}
        visibility={visibility}
        onChange={setActiveTab}
      />
      <section
        id={`dashboard-panel-${activeTab}`}
        role="tabpanel"
        aria-labelledby={`dashboard-tab-${activeTab}`}
      >
        {activeTab} content
      </section>
    </>
  );
}

describe("dashboard tab navigation", () => {
  it("exposes only visible views with linked tab and panel semantics", () => {
    render(<NavigationHarness />);

    const tablist = screen.getByRole("tablist", { name: "Dashboard views" });
    const selected = screen.getByRole("tab", { name: "DBU Overview" });
    expect(tablist).toContainElement(selected);
    expect(selected).toHaveAttribute("aria-selected", "true");
    expect(selected).toHaveAttribute("aria-controls", "dashboard-panel-dbu");
    expect(selected).toHaveAttribute("tabindex", "0");
    expect(screen.getByRole("tab", { name: "SQL" })).toHaveAttribute("tabindex", "-1");
    expect(screen.queryByRole("tab", { name: "AI/ML" })).not.toBeInTheDocument();
    expect(screen.queryByRole("tab", { name: "Tagging" })).not.toBeInTheDocument();
    expect(screen.getByRole("tabpanel")).toHaveAttribute("aria-labelledby", selected.id);
  });

  it("uses a roving tab stop for arrows, Home, and End", async () => {
    render(<NavigationHarness />);
    const dbu = screen.getByRole("tab", { name: "DBU Overview" });
    dbu.focus();

    await userEvent.keyboard("{ArrowRight}");
    const sql = screen.getByRole("tab", { name: "SQL" });
    expect(sql).toHaveFocus();
    expect(sql).toHaveAttribute("aria-selected", "true");
    expect(dbu).toHaveAttribute("tabindex", "-1");
    expect(screen.getByRole("tabpanel")).toHaveTextContent("sql content");

    await userEvent.keyboard("{End}");
    const optimize = screen.getByRole("tab", { name: "Optimize" });
    expect(optimize).toHaveFocus();
    expect(optimize).toHaveAttribute("aria-selected", "true");

    await userEvent.keyboard("{Home}");
    expect(screen.getByRole("tab", { name: "DBU Overview" })).toHaveFocus();
    await userEvent.keyboard("{ArrowLeft}");
    expect(screen.getByRole("tab", { name: "Optimize" })).toHaveFocus();
  });
});
