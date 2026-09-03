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
    const accountIndex = appSource.lastIndexOf("<AccountIdentifier");
    const workspaceFilterIndex = appSource.lastIndexOf("<WorkspaceFilter");

    expect(badgeIndex).toBeGreaterThan(0);
    expect(badgeIndex).toBeLessThan(accountIndex);
    expect(badgeIndex).toBeLessThan(workspaceFilterIndex);
    expect(appSource).toContain('className="flex shrink-0 items-center gap-[4px]"');
  });

  it("exposes the full account ID with a keyboard-accessible tooltip", () => {
    expect(appSource).toContain('label="Show full account ID"');
    expect(appSource).toContain("<InfoPopover");
    expect(appSource).toContain("max-w-[210px]");
    expect(appSource).toContain("px-1 py-0.5 text-[8.5px]");
    expect(appSource).toContain('fontFamily: "var(--sans)"');
    expect(appSource).toContain('displayValue={accountInfo?.account_name || "Databricks account"}');
    expect(appSource).toContain('aria-label={copied ? "Account ID copied" : "Copy account ID"}');
    expect(appSource).not.toContain('title={accountInfo?.account_id');
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
    expect(appSource.match(/<CopyableRailBadge/g)).toHaveLength(2);
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
});
