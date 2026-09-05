import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { CloudIntegrationWizard } from "../CloudIntegrationWizard";

function WizardHarness({
  onClose = vi.fn(),
  awsActualAvailable = false,
  azureActualAvailable = false,
  gcpActualAvailable = false,
}: {
  onClose?: () => void;
  awsActualAvailable?: boolean;
  azureActualAvailable?: boolean;
  gcpActualAvailable?: boolean;
}) {
  const [cloud, setCloud] = useState<"azure" | "aws" | "gcp" | null>(null);
  const [expandedStep, setExpandedStep] = useState<number | null>(null);
  return (
    <CloudIntegrationWizard
      show
      onClose={onClose}
      wizardCloud={cloud}
      setWizardCloud={setCloud}
      wizardExpandedStep={expandedStep}
      setWizardExpandedStep={setExpandedStep}
      viewingIntegration={null}
      cloudIntegrations={[]}
      addIntegration={vi.fn()}
      isAzure={false}
      isGCP
      awsActualAvailable={awsActualAvailable}
      azureActualAvailable={azureActualAvailable}
      gcpActualAvailable={gcpActualAvailable}
    />
  );
}

describe("CloudIntegrationWizard", () => {
  it("supports the provider picker, accordion, and completion checkbox", async () => {
    const user = userEvent.setup();
    render(<WizardHarness />);

    const dialog = screen.getByRole("dialog", { name: /integrate cloud environment costs/i });
    expect(dialog).toHaveAttribute("aria-modal", "true");

    await user.click(screen.getByRole("button", { name: /configure google cloud cost integration/i }));
    expect(screen.getByRole("status")).toHaveTextContent("Backend not connected yet");
    expect(screen.getByRole("button", { name: "Save setup checklist" })).toBeVisible();

    const step = screen.getByRole("button", { name: /open gcp console/i });
    expect(step).toHaveAttribute("aria-expanded", "false");
    await user.click(step);
    expect(step).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("region")).toHaveTextContent("GCP Console");

    const checkbox = screen.getByRole("checkbox", { name: /mark complete: open gcp console/i });
    expect(checkbox).toHaveAttribute("aria-checked", "false");
    await user.click(checkbox);
    expect(screen.getByRole("checkbox", { name: /mark incomplete: open gcp console/i })).toHaveAttribute("aria-checked", "true");
  });

  it("reports backend GCP readiness without claiming the checklist configured it", async () => {
    render(<WizardHarness gcpActualAvailable />);
    await userEvent.click(screen.getByRole("button", { name: /configure google cloud cost integration/i }));

    expect(screen.getByRole("status")).toHaveTextContent("Backend connected");
    expect(screen.getByRole("status")).toHaveTextContent(
      "it does not configure credentials",
    );
  });

  it("reports Azure backend readiness with the same truthful checklist contract", async () => {
    render(<WizardHarness azureActualAvailable />);
    await userEvent.click(screen.getByRole("button", {
      name: /configure microsoft azure cost integration/i,
    }));

    expect(screen.getByRole("status")).toHaveTextContent("Backend connected");
    expect(screen.getByRole("status")).toHaveTextContent(
      "Saving this checklist records progress only",
    );
    expect(screen.getByRole("button", { name: "Save setup checklist" })).toBeVisible();
  });

  it("dismisses on Escape and outside interaction", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(<WizardHarness onClose={onClose} />);

    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalledTimes(1);

    const dialog = screen.getByRole("dialog");
    const backdrop = dialog.parentElement?.parentElement;
    expect(backdrop).not.toBeNull();
    fireEvent.mouseDown(backdrop!);
    expect(onClose).toHaveBeenCalledTimes(2);
  });
});
