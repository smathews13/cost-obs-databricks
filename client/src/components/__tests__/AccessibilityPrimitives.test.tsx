import { useState } from "react";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { Dialog } from "../ui/Dialog";
import { InfoPopover } from "../ui/InfoPopover";
import { KPICard } from "../ui/KPICard";
import { SortableHeader } from "../ui/SortableHeader";

function DialogHarness() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)}>Open details</button>
      <Dialog open={open} onClose={() => setOpen(false)} title="Cost details">
        <button>First action</button>
        <button>Last action</button>
      </Dialog>
    </>
  );
}

function LoadingDialogHarness({ onClose }: { onClose: (loaded: boolean) => void }) {
  const [open, setOpen] = useState(false);
  const [loaded, setLoaded] = useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)}>Open loading details</button>
      <Dialog
        open={open}
        onClose={() => {
          onClose(loaded);
          setOpen(false);
        }}
        title="Loading details"
      >
        <button onClick={() => setLoaded(true)}>Load data</button>
        {loaded && <p>Data loaded</p>}
      </Dialog>
    </>
  );
}

describe("shared accessibility primitives", () => {
  it("labels, traps, closes, and returns focus for dialogs", async () => {
    const user = userEvent.setup();
    render(<DialogHarness />);
    const opener = screen.getByRole("button", { name: "Open details" });

    await user.click(opener);

    const dialog = screen.getByRole("dialog", { name: "Cost details" });
    const close = screen.getByRole("button", { name: "Close dialog" });
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(close).toHaveFocus();

    await user.tab({ shift: true });
    expect(screen.getByRole("button", { name: "Last action" })).toHaveFocus();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
    expect(document.body.style.overflow).toBe("");

    await user.click(opener);
    const reopened = screen.getByRole("dialog", { name: "Cost details" });
    await user.click(reopened.parentElement!.parentElement!);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(opener).toHaveFocus();
  });

  it("preserves dialog focus and return focus through data rerenders", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(<LoadingDialogHarness onClose={onClose} />);
    const opener = screen.getByRole("button", { name: "Open loading details" });

    await user.click(opener);
    const loadData = screen.getByRole("button", { name: "Load data" });
    await user.click(loadData);

    expect(screen.getByText("Data loaded")).toBeVisible();
    expect(loadData).toHaveFocus();

    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalledWith(true);
    expect(opener).toHaveFocus();
  });

  it("opens metric help from keyboard or click and closes with Escape", async () => {
    const user = userEvent.setup();
    render(<InfoPopover text="How this metric is calculated" />);
    const trigger = screen.getByRole("button", { name: "More information" });

    await user.tab();
    expect(trigger).toHaveAttribute("aria-expanded", "true");
    expect(trigger).toHaveAttribute(
      "aria-describedby",
      screen.getByRole("tooltip").id,
    );

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();

    await user.click(trigger);
    expect(screen.getByRole("tooltip")).toHaveTextContent("How this metric is calculated");
  });

  it("moves an above-placement popover below when the viewport would clip it", async () => {
    const rectSpy = vi.spyOn(HTMLElement.prototype, "getBoundingClientRect")
      .mockReturnValue({
        top: 4,
        bottom: 20,
        left: 100,
        right: 116,
        width: 16,
        height: 80,
        x: 100,
        y: 4,
        toJSON: () => ({}),
      });
    render(<InfoPopover size="compact" text="Compact methodology" />);

    await userEvent.click(screen.getByRole("button", { name: "More information" }));
    expect(screen.getByRole("tooltip")).toHaveStyle({
      top: "28px",
      transform: "translateX(-50%)",
    });
    rectSpy.mockRestore();
  });

  it("sorts from a real header button and reports aria-sort", async () => {
    const user = userEvent.setup();
    const onSort = vi.fn();
    const { rerender } = render(
      <table>
        <thead>
          <tr>
            <SortableHeader
              field="spend"
              activeField="name"
              direction="desc"
              onSort={onSort}
            >
              Spend
            </SortableHeader>
          </tr>
        </thead>
      </table>,
    );

    expect(screen.getByRole("columnheader", { name: "Spend" }))
      .toHaveAttribute("aria-sort", "none");
    await user.tab();
    await user.keyboard("{Enter}");
    expect(onSort).toHaveBeenCalledWith("spend");

    rerender(
      <table>
        <thead>
          <tr>
            <SortableHeader
              field="spend"
              activeField="spend"
              direction="asc"
              onSort={onSort}
            >
              Spend
            </SortableHeader>
          </tr>
        </thead>
      </table>,
    );
    expect(screen.getByRole("columnheader", { name: "Spend" }))
      .toHaveAttribute("aria-sort", "ascending");
  });

  it("makes the full KPI card the only trend button", async () => {
    const user = userEvent.setup();
    const activate = vi.fn();
    const { rerender } = render(
      <KPICard
        title="Total Spend"
        value="$100"
        subtitle="over 30 days"
        infoText="List-price spend in the selected period."
        onActivate={activate}
        ariaLabel="See Total Spend trend"
      />,
    );

    const card = screen.getByRole("button", { name: "See Total Spend trend" });
    expect(card).toHaveClass("co-kpi-card", "co-kpi-card--interactive");
    expect(card).toHaveTextContent("See trend");
    expect(card.querySelector("button")).toBeNull();

    card.focus();
    await user.keyboard("{Enter}");
    await user.keyboard(" ");
    expect(activate).toHaveBeenCalledTimes(2);

    rerender(<KPICard title="Total Spend" value="$100" />);
    expect(screen.queryByRole("button", { name: "See Total Spend trend" })).not.toBeInTheDocument();
    expect(screen.getByText("Total Spend").closest(".co-kpi-card")?.tagName).toBe("DIV");
    expect(screen.queryByText("See trend")).not.toBeInTheDocument();
  });
});
