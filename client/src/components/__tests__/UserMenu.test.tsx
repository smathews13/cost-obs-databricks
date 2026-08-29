import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { UserMenu } from "../UserMenu";

const props = {
  name: "Samuel Mathews",
  email: "samuel.a.mathews@gmail.com",
  isAdmin: true,
  workspaceHost: "dbc-example.cloud.databricks.com",
};

describe("UserMenu", () => {
  it("replaces opaque forwarded identity names with a readable email-derived name", async () => {
    const user = userEvent.setup();
    render(<UserMenu {...props} name="218942052477871@8259562572257417" />);

    await user.click(screen.getByRole("button", { name: /samuel\.a\.mathews@gmail\.com/i }));

    expect(await screen.findByText("Samuel A Mathews")).toBeInTheDocument();
    expect(screen.queryByText(/218942052477871/)).not.toBeInTheDocument();
  });

  it("shows workspace navigation, feedback, and logout actions", async () => {
    const user = userEvent.setup();
    render(<UserMenu {...props} />);

    const trigger = screen.getByRole("button", { name: /samuel\.a\.mathews@gmail\.com/i });
    expect(trigger).toHaveClass(
      "rail-user-trigger",
      "rail-control-border",
      "min-w-0",
      "border",
      "bg-white/[.07]",
    );
    expect(trigger.className).not.toContain("border-white");
    expect(trigger).toHaveAttribute("data-state", "closed");
    expect(screen.getByTestId("user-menu-silhouette")).toBeInTheDocument();
    expect(screen.getByText(props.email)).toHaveClass(
      "hidden",
      "min-[900px]:block",
      "min-[1280px]:max-w-[160px]",
    );
    await user.click(trigger);

    expect(trigger).toHaveAttribute("data-state", "open");
    expect(trigger).toHaveClass("bg-[#294A56]");
    expect(await screen.findByRole("menu", { name: "User menu" })).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: "Back to Apps" })).toHaveAttribute(
      "href",
      "https://dbc-example.cloud.databricks.com/apps",
    );
    expect(screen.getByRole("menuitem", { name: "Logout" })).toHaveAttribute(
      "href",
      "https://dbc-example.cloud.databricks.com",
    );
    expect(screen.getByText("Admin")).toBeInTheDocument();
  });

  it("supports arrow navigation through the feedback submenu", async () => {
    const user = userEvent.setup();
    render(<UserMenu {...props} />);
    const trigger = screen.getByRole("button", { name: /samuel\.a\.mathews@gmail\.com/i });

    await user.click(trigger);
    const backToApps = await screen.findByRole("menuitem", { name: "Back to Apps" });
    await waitFor(() => expect(backToApps).toHaveFocus());

    await user.keyboard("{ArrowDown}");
    const feedback = screen.getByRole("menuitem", { name: "Report Feedback" });
    expect(feedback).toHaveFocus();

    await user.keyboard("{ArrowRight}");
    const slack = await screen.findByRole("menuitem", { name: "Slack DM" });
    await waitFor(() => expect(slack).toHaveFocus());
    expect(screen.getByRole("menuitem", { name: "Email" })).toHaveAttribute(
      "href",
      "mailto:samuel.a.mathews@gmail.com?subject=cost-obs%20v1.2%20feedback",
    );

    await user.keyboard("{ArrowDown}");
    expect(screen.getByRole("menuitem", { name: "Email" })).toHaveFocus();
    await user.keyboard("{ArrowLeft}");
    expect(feedback).toHaveFocus();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("menu", { name: "User menu" })).not.toBeInTheDocument();
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it("dismisses on an outside click and returns focus", async () => {
    const user = userEvent.setup();
    render(<UserMenu {...props} />);
    const trigger = screen.getByRole("button", { name: /samuel\.a\.mathews@gmail\.com/i });

    await user.click(trigger);
    expect(await screen.findByRole("menu", { name: "User menu" })).toBeInTheDocument();

    fireEvent.mouseDown(document.body);
    expect(screen.queryByRole("menu", { name: "User menu" })).not.toBeInTheDocument();
    await waitFor(() => expect(trigger).toHaveFocus());
  });
});
