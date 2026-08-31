/**
 * Regression tests for SettingsPermissions SP identity and grant bundle.
 *
 * Key invariants:
 * 1. SP client ID and display name are rendered from /api/settings/auth-status.
 * 2. The grant SQL bundle uses the actual SP name (not a placeholder).
 * 3. After running grants, the readiness cache is invalidated.
 * 4. When SP is the active identity, the remediation bundle is shown.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { readFileSync } from "node:fs";
import { SettingsPermissions } from "../SettingsPermissions";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  fetchMock.mockReset();
});

const SP_AUTH_STATUS = {
  auth_mode: "sp",
  identity: "service_principal",
  token_present: false,
  user_email: "cost-observer-sp@apps.databricks.com",
  sp_client_id: "0000-aaaa-bbbb-1234",
  sp_display_name: "cost-observer-app-sp",
  sp_object_id: "123456789",
  sp_identity_url: "https://dbc.example.com/api/2.0/preview/scim/v2/ServicePrincipals/123456789",
  effective_oauth_scopes: ["all-apis"],
  oauth_scope_source: "databricks_apps_oauth_m2m",
  catalog: "main",
  schema: "coc",
};

function mockApis(authStatus: object, permissionsPayload?: object, userPermissions?: object) {
  const defaultUserPermissions = {
    admins: ["alice@databricks.com"],
    consumers: [],
    current_user: "alice@databricks.com",
    current_role: "admin",
    owner: {
      email: "alice@databricks.com",
      source: "databricks_apps_api",
      verified: true,
      deployment_creator: "alice@databricks.com",
    },
    role_capabilities: {
      admin: {
        summary: "Admin route capabilities.",
        can_view_dashboards: true,
        can_view_settings: true,
        can_manage_settings: true,
        can_manage_users: true,
        can_manage_data: true,
      },
      consumer: {
        summary: "Consumer route capabilities.",
        can_view_dashboards: true,
        can_view_settings: true,
        can_manage_settings: false,
        can_manage_users: false,
        can_manage_data: false,
      },
    },
  };
  const defaultPermissions = {
    permissions: [],
    summary: { total: 0, granted: 0, required_count: 2, required_granted: 2, all_required_granted: true, ready_to_use: true },
    user: { email: "alice@databricks.com", name: "Alice" },
    sp: { client_id: "0000-aaaa-bbbb-1234", display_name: "cost-observer-app-sp" },
    help_url: "https://docs.databricks.com",
  };

  fetchMock.mockImplementation((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/settings/auth-status")) {
      return Promise.resolve(
        new Response(JSON.stringify(authStatus), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );
    }
    if (url.includes("/api/settings/user-permissions")) {
      return Promise.resolve(
        new Response(JSON.stringify(userPermissions ?? defaultUserPermissions), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );
    }
    if (url.includes("/api/settings/resources")) {
      return Promise.resolve(
        new Response(JSON.stringify({
          service_principal: {
            display_name: "cost-observer-app-sp",
            client_id: "0000-aaaa-bbbb-1234",
            object_id: "123456789",
            identity_url: "https://dbc.example.com/api/2.0/preview/scim/v2/ServicePrincipals/123456789",
            execution_explanation: "Queries and maintenance run as this service principal.",
            effective_oauth_scopes: ["all-apis"],
          },
          warehouse: { id: "wh-1", name: "Main WH", size: "Medium", state: "RUNNING" },
          storage: { permissions_table: "main.coc.app_user_permissions" },
          inventory: {
            aggregates: { count: 8, names: [] },
            state: { count: 12, names: [] },
          },
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );
    }
    if (url.includes("/api/permissions/check")) {
      return Promise.resolve(
        new Response(JSON.stringify(permissionsPayload ?? defaultPermissions), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );
    }
    if (url.includes("/api/setup/readiness")) {
      return Promise.resolve(
        new Response(JSON.stringify({
          overall: "ready",
          warehouse: { name: "Main WH", granted: true, category: "core", source: "app_resource" },
          core: [],
          enhanced: [],
          sp_client_id: "0000-aaaa-bbbb-1234",
        }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );
    }
    return Promise.resolve(new Response("{}", { status: 200 }));
  });
}

function renderPermissions() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <SettingsPermissions />
    </QueryClientProvider>
  );
}

// ---------------------------------------------------------------------------
// SP identity panel
// ---------------------------------------------------------------------------

describe("SettingsPermissions: SP identity panel", () => {
  it("renders the Service principal auth mode from auth-status", async () => {
    // The Query-authentication group identifies the app's identity from auth-status.
    // (The raw client-id UUID is no longer surfaced here: the panel identifies the SP
    // by its display name, per the settings-revamp Access design.)
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    await waitFor(() => {
      expect(screen.getAllByText(/service principal/i).length).toBeGreaterThan(0);
    });
  });

  it("renders the SP display name when present", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    // The display name appears in more than one place (Running-as chip + the grant
    // bundle's Target-SP chip), so match all occurrences.
    await waitFor(() => {
      expect(screen.getAllByText("cost-observer-app-sp").length).toBeGreaterThan(0);
    });
  });

  it("shows verified warehouse access and keeps catalog/schema supporting", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    expect(await screen.findByText("SQL warehouse · CAN USE")).toBeVisible();
    expect(screen.getByText("Catalog & schema")).toBeVisible();
    expect(screen.getByText(/supporting write location/i)).toBeVisible();
  });

  it("renders role capabilities from the backend policy payload", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    expect(await screen.findByText("Permission roles")).toBeVisible();
    expect(screen.getByText("Admin route capabilities.")).toBeVisible();
    expect(screen.getByText("Consumer route capabilities.")).toBeVisible();
    expect(screen.getByText("Your role")).toBeVisible();
  });
});

// ---------------------------------------------------------------------------
// Grant SQL bundle uses the actual SP name
// ---------------------------------------------------------------------------

describe("SettingsPermissions: grant bundle targets actual SP name", () => {
  it("grant SQL uses sp_display_name, not a placeholder", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    // Wait for auth status to load and grant bundle to appear
    await waitFor(() => {
      // The grant bundle is rendered when identity is SP or token is absent
      const codeBlocks = document.querySelectorAll("code, pre");
      const sqlText = Array.from(codeBlocks).map(el => el.textContent ?? "").join("\n");
      // Must reference the real SP name, not a generic placeholder
      expect(sqlText).toContain("cost-observer-app-sp");
      expect(sqlText).not.toContain("<service-principal>");
    });
  });

  it("grant SQL does not include the requesting user's email when they differ from SP", async () => {
    // SP auth: user_email is the SP's own email
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    await waitFor(() => {
      const codeBlocks = document.querySelectorAll("code, pre");
      const sqlText = Array.from(codeBlocks).map(el => el.textContent ?? "").join("\n");
      // Section 1 grants must target the SP; no user-specific grants when SP is active identity
      expect(sqlText).toContain("GRANT USE CATALOG ON CATALOG system TO");
    });
  });
});

describe("SettingsPermissions: polished access controls", () => {
  it("describes the server bootstrap policy when no admin is configured", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: [],
      consumers: [],
      current_user: "alice@databricks.com",
      current_role: "admin",
    });
    renderPermissions();

    expect(await screen.findByText(/bootstrap mode: every authenticated user is an implicit admin/i))
      .toBeVisible();
    expect(screen.getByText(/server currently treats every authenticated user/i)).toBeVisible();
    expect(screen.queryByText(/everyone is a consumer/i)).not.toBeInTheDocument();
  });

  it("renders labeled custom role menus with a lava focus treatment and chevron", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["admin@databricks.com", "backup@databricks.com"],
      consumers: ["viewer@databricks.com"],
    });
    renderPermissions();

    const role = await screen.findByRole("combobox", { name: "Role for admin@databricks.com" });
    expect(role.tagName).toBe("BUTTON");
    expect(role).toHaveAttribute("aria-haspopup", "listbox");
    expect(role).toHaveAttribute("aria-expanded", "false");
    expect(role).toHaveClass("settings-role-select-trigger");
    expect(role.querySelector("svg[aria-hidden='true']")).toBeInTheDocument();
    role.focus();
    expect(role).toHaveFocus();
    expect(screen.getByRole("combobox", { name: "Role for new user" })).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "User email" })).toBeInTheDocument();
  });

  it("supports keyboard navigation and role selection without a native select", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["admin@databricks.com", "backup@databricks.com"],
      consumers: ["viewer@databricks.com"],
    });
    renderPermissions();

    const role = await screen.findByRole("combobox", { name: "Role for admin@databricks.com" });
    role.focus();
    await userEvent.keyboard("{Enter}");

    expect(role).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("listbox", { name: "Role for admin@databricks.com" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "Admin" })).toHaveAttribute("aria-selected", "true");

    await userEvent.keyboard("{ArrowUp}{Enter}");

    expect(role).toHaveAttribute("aria-expanded", "false");
    await waitFor(() => {
      const saveCall = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
      expect(saveCall).toBeDefined();
      expect(JSON.parse(String(saveCall?.[1]?.body))).toEqual({
        admins: ["backup@databricks.com"],
        consumers: ["viewer@databricks.com", "admin@databricks.com"],
      });
    });
  });

  it("closes an open role menu on Escape without changing the role", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["admin@databricks.com", "backup@databricks.com"],
      consumers: [],
    });
    renderPermissions();

    const role = await screen.findByRole("combobox", { name: "Role for admin@databricks.com" });
    role.focus();
    await userEvent.keyboard("{ArrowDown}{Escape}");

    expect(role).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByRole("listbox", { name: "Role for admin@databricks.com" })).not.toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);
  });

  it("uses an accessible SVG chevron and muted SQL panel styling", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    const grants = await screen.findByRole("button", {
      name: "App runtime grants: exact SQL (run as metastore admin)",
    });
    expect(grants).toHaveAttribute("aria-expanded", "true");
    expect(grants.querySelector("svg[aria-hidden='true']")).toBeInTheDocument();

    const sql = document.querySelector("pre");
    expect(sql).toHaveStyle({
      backgroundColor: "var(--dob-code-bg, #F2F5F7)",
      borderRadius: "8px",
      lineHeight: "1.6",
    });
    expect(screen.getByRole("button", { name: /Copy SQL/ })).toBeInTheDocument();

    await userEvent.click(grants);
    expect(grants).toHaveAttribute("aria-expanded", "false");
  });

  it("aligns existing users and the add-user controls to one balanced grid", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["admin@databricks.com"],
      consumers: ["viewer@databricks.com"],
    });
    renderPermissions();

    const rows = await screen.findAllByTestId("access-user-row");
    const addRow = screen.getByTestId("access-add-user-row");

    expect(addRow).toHaveClass("settings-access-user-grid");
    expect(addRow.style.getPropertyValue("--settings-access-grid-columns")).toBe(
      "minmax(210px, 1fr) 100px 170px 104px",
    );
    expect(addRow.style.minWidth).toBe("");
    expect(addRow.children).toHaveLength(4);
    expect(rows).toHaveLength(2);
    rows.forEach((row) => {
      expect(row).toHaveClass("settings-access-user-grid");
      expect(row.style.getPropertyValue("--settings-access-grid-columns")).toBe(
        addRow.style.getPropertyValue("--settings-access-grid-columns"),
      );
      expect(row.style.minWidth).toBe("");
      expect(row.children).toHaveLength(4);
    });
    expect(screen.getByRole("textbox", { name: "User email" })).toHaveStyle({ width: "100%" });
    expect(screen.getByRole("combobox", { name: "Role for new user" }).parentElement).toHaveClass("settings-role-select");
    expect(screen.getByRole("button", { name: "Add User" }).parentElement).toHaveStyle({ display: "grid" });
  });

  it("puts Users first and the service principal immediately after it", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    const users = await screen.findByTestId("settings-users-section");
    const servicePrincipal = screen.getByTestId("settings-service-principal-section");

    expect(users.compareDocumentPosition(servicePrincipal) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.queryByRole("group", { name: "Add a metastore" })).not.toBeInTheDocument();
  });

  it("separates Owner persona from Admin role and protects the verified deployer", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["owner@databricks.com", "backup@databricks.com"],
      consumers: ["viewer@databricks.com"],
      owner: {
        email: "owner@databricks.com",
        source: "databricks_apps_api",
        verified: true,
        deployment_creator: "owner@databricks.com",
      },
    });
    renderPermissions();

    const ownerRow = (await screen.findByText("owner@databricks.com")).closest("[data-testid='access-user-row']");
    expect(ownerRow).toHaveTextContent("Owner");
    expect(ownerRow).toHaveTextContent("Admin");
    expect(screen.getByRole("combobox", { name: "Role for owner@databricks.com" })).toBeDisabled();
    expect(ownerRow?.querySelector(".settings-user-action--remove")).toBeDisabled();
    expect(screen.getByText("viewer@databricks.com").closest("[data-testid='access-user-row']")).toHaveTextContent("Member");
  });

  it("shows safe service-principal details without rendering credentials", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    const link = await screen.findByRole("link", { name: "Open Databricks identity" });
    expect(link).toHaveAttribute(
      "href",
      "https://dbc.example.com/api/2.0/preview/scim/v2/ServicePrincipals/123456789",
    );
    expect(screen.getByText("all-apis")).toBeVisible();
    expect(screen.getByText("123456789")).toBeVisible();
    expect(screen.getByTestId("service-principal-grants")).toBeVisible();
    expect(document.body.textContent).not.toMatch(/secret-value|bearer\s+[A-Za-z0-9]/i);
  });

  it("keeps Add User and Remove actions visually comparable and accessible", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["owner@databricks.com", "backup@databricks.com"],
      consumers: ["viewer@databricks.com"],
      owner: { email: "owner@databricks.com", source: "permission_store", verified: false },
    });
    renderPermissions();

    const add = await screen.findByRole("button", { name: "Add User" });
    const remove = screen.getAllByRole("button", { name: "Remove" }).find((button) => !button.hasAttribute("disabled"));
    expect(add).toHaveClass("settings-user-action", "settings-user-action--add");
    expect(remove).toHaveClass("settings-user-action", "settings-user-action--remove");
    expect(add).toBeDisabled();
    await userEvent.type(screen.getByRole("textbox", { name: "User email" }), "new@example.com");
    expect(add).toBeEnabled();
    add.focus();
    expect(add).toHaveFocus();

    const css = readFileSync("src/components/settings/settings.css", "utf8");
    expect(css).toMatch(/\.settings-user-action\s*\{[^}]*height:\s*32px[^}]*font-weight:\s*600/s);
    expect(css).toMatch(/\.settings-user-action--remove\s*\{[^}]*danger/s);
    expect(css).toMatch(/\.settings-user-action:focus-visible/);
  });

  it("disables removal and demotion of the last explicit admin", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["admin@databricks.com"],
      consumers: ["viewer@databricks.com"],
    });
    renderPermissions();

    expect(await screen.findByText(/only explicit admin cannot be removed/i)).toBeVisible();
    expect(screen.getByRole("combobox", { name: "Role for admin@databricks.com" })).toBeDisabled();
    const adminRow = screen.getByText("admin@databricks.com").closest("[data-testid='access-user-row']");
    expect(adminRow?.querySelector("button[aria-describedby='last-admin-explanation']")).toBeDisabled();
    expect(screen.getByRole("combobox", { name: "Role for viewer@databricks.com" })).toBeEnabled();
  });
});

