import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { TabVisibility } from "../SettingsDialog";
import { ExportDialog } from "../ExportDialog";

const visibility: TabVisibility = {
  dbu: true,
  sql: true,
  infra: true,
  optimizer: true,
  kpis: true,
  aiml: true,
  apps: true,
  tagging: true,
  "users-groups": true,
};

describe("ExportDialog report data loading", () => {
  it("waits for on-demand report queries before exporting", () => {
    render(
      <ExportDialog
        isOpen
        onClose={vi.fn()}
        onExport={vi.fn()}
        tabVisibility={visibility}
        dataLoading
      />,
    );

    const button = screen.getByRole("button", { name: "Preparing report data" });
    expect(button).toBeDisabled();
  });
});
