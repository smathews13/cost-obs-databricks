import { describe, expect, it } from "vitest";
import type { UserSpend } from "@/hooks/useBillingData";
import { buildAnonymizedIdentityMap } from "../UsersGroups";

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
});
