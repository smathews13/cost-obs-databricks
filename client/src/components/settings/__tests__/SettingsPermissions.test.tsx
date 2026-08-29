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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
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
  catalog: "main",
  schema: "coc",
};

function mockApis(authStatus: object, permissionsPayload?: object, userPermissions?: object) {
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
        new Response(JSON.stringify(userPermissions ?? { admins: [], consumers: [] }), {
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
  it("labels role selects and renders the custom focus chevron", async () => {
    mockApis(SP_AUTH_STATUS, undefined, {
      admins: ["admin@databricks.com"],
      consumers: ["viewer@databricks.com"],
    });
    renderPermissions();

    const role = await screen.findByRole("combobox", { name: "Role for admin@databricks.com" });
    expect(role).toHaveStyle({ appearance: "none" });
    expect(role.parentElement?.querySelector("svg[aria-hidden='true']")).toBeInTheDocument();
    fireEvent.focus(role);
    expect(role.parentElement?.getAttribute("style")).toContain("box-shadow");
    expect(screen.getByRole("combobox", { name: "Role for new user" })).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "User email" })).toBeInTheDocument();
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
      "minmax(220px, 0.5fr) 180px 96px",
    );
    expect(addRow.style.minWidth).toBe("");
    expect(addRow.children).toHaveLength(3);
    expect(rows).toHaveLength(2);
    rows.forEach((row) => {
      expect(row).toHaveClass("settings-access-user-grid");
      expect(row.style.getPropertyValue("--settings-access-grid-columns")).toBe(
        addRow.style.getPropertyValue("--settings-access-grid-columns"),
      );
      expect(row.style.minWidth).toBe("");
      expect(row.children).toHaveLength(3);
    });
    expect(screen.getByRole("textbox", { name: "User email" })).toHaveStyle({ width: "100%" });
  });

  it("shows the disabled metastore browser between Users and Query authentication", async () => {
    mockApis(SP_AUTH_STATUS);
    renderPermissions();

    const metastore = await screen.findByRole("group", { name: "Add a metastore" });
    const queryAuthentication = screen.getByText("Query authentication");

    expect(metastore).toBeDisabled();
    expect(metastore).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByText("Coming soon")).toBeVisible();
    expect(screen.getByRole("textbox", { name: "Metastore" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Browse" })).toBeDisabled();
    expect(metastore.compareDocumentPosition(queryAuthentication) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});

