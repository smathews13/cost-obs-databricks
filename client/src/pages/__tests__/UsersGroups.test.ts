import { createElement } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { UserSpend } from "@/hooks/useBillingData";
import { useUsersGroupsBundle } from "@/hooks/useBillingData";
import UsersGroups from "../UsersGroups";
import { buildAnonymizedIdentityMap } from "@/utils/identity";

vi.mock("@/hooks/useBillingData", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/hooks/useBillingData")>();
  return { ...actual, useUsersGroupsBundle: vi.fn() };
});
vi.mock("@/components/KPITrendModal", () => ({
  KPITrendModal: (props: {
    kpi: string;
    queryKeyPrefix?: string;
    variant?: string;
  }) => createElement("div", {
    "data-testid": "users-trend-routing",
    "data-kpi": props.kpi,
    "data-prefix": props.queryKeyPrefix,
    "data-variant": props.variant,
  }),
}));

const user = (user_email: string, total_spend: number): UserSpend => ({
  user_email,
  total_spend,
  total_dbus: 0,
  active_days: 1,
  workspace_count: 1,
  percentage: 0,
  primary_product: "SQL",
  products: [],
});

describe("user anonymization", () => {
  beforeEach(() => {
    vi.mocked(useUsersGroupsBundle).mockReturnValue({
      data: {
        summary: {
          user_count: 2,
          workspace_count: 1,
          total_spend: 150,
          total_dbus: 15,
          avg_spend_per_user: 75,
          spend_growth_pct: 0,
        },
        top_users: [
          { ...user("human@example.com", 100), total_dbus: 10, percentage: 66.7, products: [{ product: "SQL", spend: 100 }] },
          { ...user("12345678-1234-1234-1234-123456789abc", 50), total_dbus: 5, percentage: 33.3, products: [{ product: "SQL", spend: 50 }] },
        ],
        user_growth: [],
      },
      isLoading: false,
    } as ReturnType<typeof useUsersGroupsBundle>);
  });

  it("replaces human identities by spend rank and preserves service principals", () => {
    const servicePrincipal = "12345678-1234-1234-1234-123456789abc";
    const result = buildAnonymizedIdentityMap([
      user("lower-spend@example.com", 25),
      user(servicePrincipal, 1000),
      user("higher-spend@example.com", 100),
    ]);

    expect(result.get("higher-spend@example.com")).toBe("User 1");
    expect(result.get("lower-spend@example.com")).toBe("User 2");
    expect(result.has(servicePrincipal)).toBe(false);
  });

  it("keeps human identities anonymized in details and product drilldowns", () => {
    render(createElement(UsersGroups, {
      startDate: "2026-08-01",
      endDate: "2026-08-28",
      dateRange: { startDate: "2026-08-01", endDate: "2026-08-28" },
      anonymizeUsers: true,
    }));

    fireEvent.click(screen.getAllByRole("button", { name: "Details" })[0]);
    expect(screen.getAllByText("User 1").length).toBeGreaterThan(0);
    expect(screen.queryByText("human@example.com")).not.toBeInTheDocument();
  });

  it("uses the stable anonymized rank in avatars and product users", () => {
    render(createElement(UsersGroups, {
      startDate: "2026-08-01",
      endDate: "2026-08-28",
      dateRange: { startDate: "2026-08-01", endDate: "2026-08-28" },
      anonymizeUsers: true,
    }));

    fireEvent.click(screen.getByRole("button", { name: /^SQL.*\$/ }));
    expect(screen.getAllByText("User 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("SP-12345").length).toBeGreaterThan(0);
    expect(screen.queryByText("human@example.com")).not.toBeInTheDocument();
    const leaderboardRow = screen.getAllByText("User 1")
      .map((node) => node.closest("tr"))
      .find((row): row is HTMLTableRowElement => row !== null);
    expect(leaderboardRow).toBeTruthy();
    expect(leaderboardRow!.querySelector("td:first-child .rounded-full")?.textContent).toBe("1");
  });

  it("routes every full-card user trend through users-groups-owned cache keys", () => {
    render(createElement(UsersGroups, {
      startDate: "2026-08-01",
      endDate: "2026-08-28",
      dateRange: { startDate: "2026-08-01", endDate: "2026-08-28" },
    }));

    for (const [title, kpi, prefix] of [
      ["Unique Active Users", "total_users", "users-groups-platform-kpi-trend"],
      ["User Spend", "avg_spend_per_user", "users-groups-kpi-trend"],
      ["Power Users", "power_user_spend", "users-groups-kpi-trend"],
      ["User Spend Growth", "user_spend", "users-groups-kpi-trend"],
    ]) {
      const card = screen.getByRole("button", { name: `See ${title} trend` });
      expect(card).toHaveClass("co-kpi-card");
      expect(card.querySelector("button")).toBeNull();
      fireEvent.click(card);
      expect(screen.getByTestId("users-trend-routing")).toHaveAttribute("data-kpi", kpi);
      expect(screen.getByTestId("users-trend-routing")).toHaveAttribute("data-prefix", prefix);
    }
  });
});
