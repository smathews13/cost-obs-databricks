import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const appSource = readFileSync(resolve(process.cwd(), "src/App.tsx"), "utf8");
const styles = readFileSync(resolve(process.cwd(), "src/index.css"), "utf8");
const html = readFileSync(resolve(process.cwd(), "index.html"), "utf8");

describe("account rail health polish", () => {
  it("uses one dark slate border token for rail controls and Export", () => {
    expect(styles).toContain("--rail-control-border: #315F70;");
    expect(styles).toContain(".rail-control-border");
    expect(appSource).toMatch(
      /aria-label="Export"[\s\S]{0,250}className="rail-control-border/,
    );
  });

  it("keeps rail actions together without a redundant Admin badge", () => {
    expect(appSource).not.toContain('{user.role === "admin" && (');
    expect(appSource).toContain('className="flex shrink-0 items-center gap-[4px]"');
    expect(appSource).not.toContain('className="-ml-[8px]');
  });

  it("wraps narrow chrome while keeping navigation horizontally controlled", () => {
    expect(appSource).toContain(
      'data-testid="account-rail" className="min-h-[52px] overflow-visible',
    );
    expect(appSource).toContain("min-w-0 flex-wrap items-center");
    expect(appSource).toContain(
      'className="order-last flex w-full min-w-0 items-center',
    );
    expect(appSource).toContain('className="flex flex-wrap items-center gap-4"');
    expect(appSource).toContain("overflow-x-auto overflow-y-hidden");
  });

  it("places deployment provenance beside the brand before account filters", () => {
    const badgeIndex = appSource.lastIndexOf("<DeploymentBadgeFromApi");
    const accountIndex = appSource.lastIndexOf('label="account ID"');
    const workspaceFilterIndex = appSource.lastIndexOf("<WorkspaceFilter");
    const servicePrincipalBadgeIndex = appSource.lastIndexOf(
      'label="service principal display name"',
    );

    expect(badgeIndex).toBeGreaterThan(0);
    expect(badgeIndex).toBeLessThan(accountIndex);
    expect(badgeIndex).toBeLessThan(workspaceFilterIndex);
    expect(workspaceFilterIndex).toBeLessThan(accountIndex);
    expect(accountIndex).toBeLessThan(servicePrincipalBadgeIndex);
    expect(appSource).not.toContain(">ACCOUNT</span>");
    expect(appSource).toContain('className="flex shrink-0 items-center gap-[4px]"');
  });

  it("uses the same direct hover-copy state for the account ID", () => {
    expect(appSource).not.toContain("<InfoPopover");
    expect(appSource).toContain("bg-green-500/20");
    expect(appSource).toContain("RAIL_STATUS_BADGE_CLASS");
    expect(appSource).toContain("h-[18px]");
    expect(appSource).toContain("rounded-[4px]");
    expect(appSource).toContain("px-[6px] text-[9px]");
    expect(appSource).toContain(
      'value={accountInfo?.account_id || accountInfo?.account_name || "Databricks account"}',
    );
    expect(appSource).toContain(
      'text={accountInfo?.account_name || "Databricks account"}',
    );
    expect(appSource).toContain('label="account ID"');
    expect(styles).not.toContain(".account-id-tooltip-content");
    expect(styles).not.toContain(".deployment-badge-tooltip");
  });

  it("sets the app font before React renders and prevents late font swaps", () => {
    expect(html).toContain('html, body, button, input, select, textarea, dialog');
    expect(html).toContain('button, input, select, textarea { font: inherit; }');
    expect(html).toContain('--sans: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;');
    expect(styles).not.toContain("@font-face");
    expect(styles).toMatch(/button,\s*input,\s*select,\s*textarea\s*\{\s*font: inherit;/);
  });

  it("gives only the healthy app, service-principal, and SQL dots a perceptible pulse", () => {
    expect(appSource.match(/<CopyableRailBadge/g)).toHaveLength(3);
    expect(appSource.match(/healthy-status-dot/g)).toHaveLength(2);
    expect(appSource).toContain(
      'warehouseStatus.status === "warm" ? "healthy-status-dot " : ""',
    );
    expect(styles).toMatch(
      /\.healthy-status-dot\s*\{[\s\S]*animation:\s*healthyStatusBlink 11s ease-out infinite/,
    );
    expect(styles).toContain("opacity: 0.35;");
    expect(styles).toContain("transform: scale(1.32);");
    expect(styles).toContain("box-shadow: 0 0 0 4px rgba(60, 214, 143, 0.32);");
    expect(styles).toMatch(
      /@media \(prefers-reduced-motion: reduce\)[\s\S]*\.healthy-status-dot[\s\S]*animation: none !important/,
    );
  });

  it("labels the SP name correctly and copies the same ID used by Settings", () => {
    expect(appSource).not.toContain('RAIL_COPY_BADGE_CLASS');
    expect(appSource.match(/RAIL_STATUS_BADGE_CLASS/g)?.length).toBeGreaterThanOrEqual(3);
    expect(appSource).not.toContain('trailing="ID"');
    expect(appSource).not.toContain('trailing="Name"');
    expect(appSource).toContain(
      'value={authStatus.sp_object_id || authStatus.sp_client_id || ""}',
    );
    expect(appSource).not.toContain(
      'value={authStatus.sp_user_name || authStatus.sp_client_id || ""}',
    );
  });
});
