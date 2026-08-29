import { afterEach, describe, expect, it, vi } from "vitest";
import { ARCHITECTURE_OVERVIEW } from "../architectureOverview";

const pdf = vi.hoisted(() => ({
  save: vi.fn(),
  addPage: vi.fn(),
  text: vi.fn(),
  pageText: [] as number[],
  textPositions: [] as Array<{ page: number; y: number }>,
}));

vi.mock("jspdf", () => ({
  jsPDF: class {
    internal = { pageSize: { width: 297, height: 210 } };
    private pages = 1;
    setFont = vi.fn();
    setFontSize = vi.fn();
    setTextColor = vi.fn();
    setFillColor = vi.fn();
    setDrawColor = vi.fn();
    setLineWidth = vi.fn();
    rect = vi.fn();
    roundedRect = vi.fn();
    line = vi.fn();
    triangle = vi.fn();
    circle = vi.fn();
    addImage = vi.fn();
    text = (...args: unknown[]) => {
      pdf.text(...args);
      pdf.pageText[this.pages] = (pdf.pageText[this.pages] ?? 0) + 1;
      if (typeof args[2] === "number") {
        pdf.textPositions.push({ page: this.pages, y: args[2] });
      }
    };
    save = pdf.save;
    addPage = () => {
      this.pages += 1;
      pdf.addPage();
    };
    getNumberOfPages = () => this.pages;
    setPage = vi.fn();
    splitTextToSize = (text: string) => [text];
  },
}));

vi.mock("../pdfBrand", () => ({
  DB_HEADER: [27, 49, 57],
  DB_ORANGE: [255, 54, 33],
  DB_ALT_ROW: [249, 247, 244],
  PDF_HAIRLINE: [228, 226, 221],
  PDF_BODY: [58, 56, 56],
  PDF_SLATE: [97, 135, 148],
  PDF_WHITE: [255, 255, 255],
  loadPdfBrandAssets: vi.fn().mockResolvedValue({ costObsLockup: "lockup", databricksMark: "mark" }),
  drawPdfMasthead: vi.fn(),
  addPdfFooters: vi.fn(),
}));

import { generateArchitectureReport } from "../architectureReport";

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
  pdf.pageText.length = 0;
  pdf.textPositions.length = 0;
});

describe("architecture report", () => {
  it("keeps customer-safe structured architecture content outside the renderer", () => {
    expect(ARCHITECTURE_OVERVIEW.title).toBe("Cost Observability Architecture");
    expect(ARCHITECTURE_OVERVIEW.components.map((item) => item.name)).toEqual(expect.arrayContaining([
      "React browser interface",
      "FastAPI application",
      "Databricks SQL Warehouse",
      "App-managed Delta layer",
    ]));
    expect(ARCHITECTURE_OVERVIEW.flowColumns.map((item) => item.title)).toEqual([
      "Browser / React",
      "FastAPI routes",
      "SQL Warehouse",
      "App-managed Delta",
      "Governed sources",
    ]);
    expect(ARCHITECTURE_OVERVIEW.dataFlow).not.toHaveLength(0);
    expect(ARCHITECTURE_OVERVIEW.securityGovernance).not.toHaveLength(0);
    expect(ARCHITECTURE_OVERVIEW.refreshPaths.map((item) => item.label)).toEqual([
      "Scheduled aggregate refresh",
      "Administrator full rebuild",
      "On-demand tab refresh",
    ]);

    const expectedTabs = [
      "DBU Overview",
      "SQL",
      "AI/ML",
      "Apps",
      "Tagging",
      "Users",
      "KPIs & Trends",
      "Cloud Costs",
      "Optimize",
    ];
    expect(ARCHITECTURE_OVERVIEW.tabLineage.map((item) => item.tab)).toEqual(expectedTabs);
    for (const lineage of ARCHITECTURE_OVERVIEW.tabLineage) {
      expect(lineage.uiComponents, `${lineage.tab} UI mapping`).not.toHaveLength(0);
      expect(lineage.apiRoutes, `${lineage.tab} API mapping`).not.toHaveLength(0);
      expect(lineage.managedData, `${lineage.tab} managed-data mapping`).not.toHaveLength(0);
      expect(lineage.sourceTables, `${lineage.tab} source mapping`).not.toHaveLength(0);
      expect(lineage.apiRoutes.every((route) => route.includes("/api/"))).toBe(true);
    }
    expect(
      ARCHITECTURE_OVERVIEW.tabLineage.find((item) => item.tab === "KPIs & Trends")
        ?.managedData,
    ).toEqual(expect.arrayContaining(["daily_usage_summary"]));

    const sourceGroups = Object.fromEntries(
      ARCHITECTURE_OVERVIEW.sourceTables.map((group) => [group.label, group]),
    );
    expect(sourceGroups["Core analytic system tables"].tables).toEqual([
      "system.billing.usage",
      "system.billing.list_prices",
      "system.query.history",
      "system.compute.clusters",
      "system.compute.warehouses",
      "system.compute.warehouse_events",
      "system.lakeflow.jobs",
      "system.lakeflow.pipelines",
      "system.lakeflow.job_run_timeline",
      "system.serving.served_entities",
      "system.access.workspaces_latest",
    ]);
    expect(sourceGroups["Permission and readiness probe"].tables).toEqual([
      "system.access.audit",
    ]);
    expect(sourceGroups["Permission and readiness probe"].note).toContain(
      "not an analytic input",
    );
    expect(sourceGroups["Durable app state and cache"].tables).toEqual(
      expect.arrayContaining([
        "app_settings",
        "app_schedule_settings",
        "app_refresh_log",
        "app_cloud_connections",
        "app_workspace_filter",
        "app_user_permissions",
        "app_mv_refresh_state",
      ]),
    );

    const sourceTables = ARCHITECTURE_OVERVIEW.sourceTables.flatMap(
      (group) => group.tables,
    );
    expect(new Set(sourceTables).size).toBe(sourceTables.length);
    expect(sourceTables).not.toEqual(
      expect.arrayContaining([
        "AWS Cost and Usage Reports",
        "Azure Cost Management exports",
        "GCP Cloud Billing exports",
      ]),
    );

    const content = JSON.stringify(ARCHITECTURE_OVERVIEW);
    expect(content).toContain("system.billing.usage");
    expect(content).toContain("app_response_cache");
    expect(content).not.toMatch(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    expect(content).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i);
    expect(content).not.toMatch(/sam\.mathews|astrolabe|azure-field-eng/i);
  });

  it("downloads a dated architecture PDF with populated pages", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-28T12:00:00Z"));

    await generateArchitectureReport();

    expect(pdf.save).toHaveBeenCalledWith("cost-observability-architecture-2026-08-28.pdf");
    expect(pdf.text).toHaveBeenCalledWith("Cost Observability Architecture", 14, 34);
    expect(pdf.text.mock.calls.length).toBeGreaterThan(20);
    const renderedText = JSON.stringify(pdf.text.mock.calls);
    expect(renderedText).toContain("Architecture at a glance");
    expect(renderedText).toContain("AUTHENTICATION + GOVERNANCE");
    expect(renderedText).toContain("SCHEDULED REFRESH");
    expect(renderedText).toContain("ON-DEMAND REFRESH");
    expect(renderedText).toContain("Tab-by-tab data lineage");
    for (const lineage of ARCHITECTURE_OVERVIEW.tabLineage) {
      expect(renderedText).toContain(lineage.tab);
    }
    const pageCount = pdf.addPage.mock.calls.length + 1;
    expect(pageCount).toBeGreaterThanOrEqual(4);
    const textCounts = Array.from({ length: pageCount }, (_, index) => pdf.pageText[index + 1] ?? 0);
    expect(textCounts.every((count) => count > 0)).toBe(true);
    expect(pdf.textPositions.every(({ y }) => y >= 0 && y <= 189)).toBe(true);
  });
});
