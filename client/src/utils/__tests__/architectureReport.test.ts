import { readFileSync, readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ARCHITECTURE_OVERVIEW } from "../architectureOverview";

interface TextRecord {
  page: number;
  text: string;
  x: number;
  y: number;
}

const pdf = vi.hoisted(() => ({
  save: vi.fn(),
  addPage: vi.fn(),
  setPage: vi.fn(),
  text: vi.fn(),
  records: [] as TextRecord[],
}));

vi.mock("jspdf", () => ({
  jsPDF: class {
    internal = { pageSize: { width: 297, height: 210 } };
    private pages = 1;
    private currentPage = 1;
    private fontSize = 9;
    setFont = vi.fn();
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
    saveGraphicsState = vi.fn();
    restoreGraphicsState = vi.fn();
    clip = vi.fn();
    discardPath = vi.fn();
    save = pdf.save;
    setFontSize = (size: number) => {
      this.fontSize = size;
    };
    getFontSize = () => this.fontSize;
    getTextWidth = (text: string) => text.length * this.fontSize * 0.18;
    text = (text: string | string[], x: number, y: number, ...rest: unknown[]) => {
      pdf.text(text, x, y, ...rest);
      pdf.records.push({
        page: this.currentPage,
        text: Array.isArray(text) ? text.join("\n") : text,
        x,
        y,
      });
    };
    addPage = () => {
      this.pages += 1;
      this.currentPage = this.pages;
      pdf.addPage();
    };
    getNumberOfPages = () => this.pages;
    setPage = (page: number) => {
      this.currentPage = page;
      pdf.setPage(page);
    };
    splitTextToSize = (text: string, width: number) => {
      const maxCharacters = Math.max(4, Math.floor(width / (this.fontSize * 0.18)));
      const words = text.split(/\s+/);
      const lines: string[] = [];
      let line = "";
      for (const word of words) {
        if (word.length > maxCharacters) {
          if (line) lines.push(line);
          for (let index = 0; index < word.length; index += maxCharacters) {
            lines.push(word.slice(index, index + maxCharacters));
          }
          line = "";
        } else if (!line) {
          line = word;
        } else if (`${line} ${word}`.length <= maxCharacters) {
          line = `${line} ${word}`;
        } else {
          lines.push(line);
          line = word;
        }
      }
      if (line) lines.push(line);
      return lines;
    };
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
  loadPdfBrandAssets: vi.fn().mockResolvedValue({
    costObsLockup: "lockup",
    databricksMark: "mark",
  }),
}));

import {
  ARCHITECTURE_PDF_BODY_BOTTOM,
  ARCHITECTURE_PDF_MIN_FONT_SIZE,
  ARCHITECTURE_PDF_PAGE_COUNT,
  fittedText,
  generateArchitectureReport,
} from "../architectureReport";
import { jsPDF } from "jspdf";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../");
const architectureMarkdownPath = resolve(repoRoot, "cost-obs-architecture.md");
const readmePath = resolve(repoRoot, "README.md");
const serverRoot = resolve(repoRoot, "server");
const forbiddenIdentifiers = [
  /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i,
  /[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i,
  /\b\d{12,16}\b/,
];

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
  pdf.records.length = 0;
});

describe("architecture source of truth", () => {
  it("contains exactly nine current tab mappings and the complete inventories", () => {
    expect(ARCHITECTURE_OVERVIEW.title).toBe("cost-obs — Architecture (v1.2)");
    expect(ARCHITECTURE_OVERVIEW.tabLineage.map((item) => item.tab)).toEqual([
      "DBU Overview",
      "SQL",
      "AI/ML",
      "Apps",
      "Tagging",
      "Users",
      "KPIs & Trends",
      "Cloud Costs",
      "Optimize",
    ]);
    expect(ARCHITECTURE_OVERVIEW.tabLineage).toHaveLength(9);
    for (const lineage of ARCHITECTURE_OVERVIEW.tabLineage) {
      expect(lineage.uiComponents, `${lineage.tab} UI mapping`).not.toHaveLength(0);
      expect(lineage.apiRoutes, `${lineage.tab} API mapping`).not.toHaveLength(0);
      expect(lineage.managedData, `${lineage.tab} managed-data mapping`).not.toHaveLength(0);
      expect(lineage.sourceTables, `${lineage.tab} source mapping`).not.toHaveLength(0);
      expect(lineage.apiRoutes.every((route) => route.startsWith("GET /api/"))).toBe(true);
    }

    const groups = Object.fromEntries(
      ARCHITECTURE_OVERVIEW.sourceTables.map((group) => [group.label, group]),
    );
    expect(groups["Core analytic system tables"].tables).toHaveLength(11);
    expect(groups["App-managed analytic tables"].tables).toHaveLength(8);
    expect(groups["Durable app state and cache"].tables).toHaveLength(13);
    expect(groups["Optional cloud billing sources"].tables).toHaveLength(3);
    expect(groups["Permission and readiness probe"].note).toContain("not an analytic input");

    const sourceTables = ARCHITECTURE_OVERVIEW.sourceTables.flatMap(
      (group) => group.tables,
    );
    expect(new Set(sourceTables).size).toBe(sourceTables.length);
  });

  it("keeps structured content free of customer identifiers", () => {
    const content = JSON.stringify(ARCHITECTURE_OVERVIEW);
    for (const pattern of forbiddenIdentifiers) {
      expect(content).not.toMatch(pattern);
    }
  });

  it("ships the customer-ready root markdown and links it from README", () => {
    const markdown = readFileSync(architectureMarkdownPath, "utf8");
    const readme = readFileSync(readmePath, "utf8");
    const requiredHeadings = [
      "# cost-obs — Architecture (v1.2)",
      "## Architecture at a glance",
      "## Components",
      "## Request & data flow",
      "## Authentication & governance",
      "## Refresh paths",
      "## Tab-by-tab data lineage",
      "## Source-table inventory",
    ];
    for (const heading of requiredHeadings) {
      expect(markdown).toContain(heading);
    }
    const tabRows = markdown.match(
      /^\| (DBU Overview|SQL|AI\/ML|Apps|Tagging|Users|KPIs & Trends|Cloud Costs|Optimize) \|/gm,
    );
    expect(tabRows).toHaveLength(9);
    expect(markdown).toContain("**App-managed aggregates (8)");
    expect(markdown).toContain("**Durable app state & cache (13):**");
    expect(markdown).toContain("Static design content only");
    for (const pattern of forbiddenIdentifiers) {
      expect(markdown).not.toMatch(pattern);
    }
    expect(readme).toContain(
      "[cost-obs v1.2 architecture specification](cost-obs-architecture.md)",
    );
  });

  it("keeps markdown, visible tabs, API routes, and table references in sync", () => {
    const markdown = readFileSync(architectureMarkdownPath, "utf8");
    const appSource = readFileSync(resolve(repoRoot, "client/src/App.tsx"), "utf8");
    const routerFiles: Record<string, string> = {
      billing: "billing.py",
      dbsql: "dbsql_base.py",
      aiml: "aiml.py",
      apps: "apps.py",
      tagging: "tagging.py",
      "users-groups": "users_groups.py",
      "aws-actual": "aws_actual.py",
      "azure-actual": "azure_actual.py",
      "gcp-actual": "gcp_actual.py",
      sql: "warehouse_health.py",
    };
    const serverText = readdirSync(serverRoot, { recursive: true })
      .filter((path) => String(path).endsWith(".py"))
      .map((path) => readFileSync(resolve(serverRoot, String(path)), "utf8"))
      .join("\n");

    for (const lineage of ARCHITECTURE_OVERVIEW.tabLineage) {
      expect(markdown).toContain(`| ${lineage.tab} |`);
      expect(appSource).toContain(lineage.tab);
      for (const route of lineage.apiRoutes) {
        const relative = route.replace(/^GET \/api\//, "");
        expect(markdown, `${lineage.tab} markdown route ${relative}`).toContain(relative);
        const normalizedRelative = relative.startsWith("sql/warehouse-health")
          ? relative.replace("sql/warehouse-health", "sql")
          : relative;
        const [prefix, ...suffixParts] = normalizedRelative.split("/");
        const routerFile = routerFiles[prefix];
        expect(routerFile, `router mapping for ${route}`).toBeTruthy();
        const suffix = `/${suffixParts.join("/")}`.replace(/\/$/, "");
        const routerSource = readFileSync(resolve(serverRoot, "routers", routerFile), "utf8");
        expect(routerSource, `implemented route ${route}`).toContain(
          `@router.get("${suffix}")`,
        );
      }
      for (const table of [...lineage.managedData, ...lineage.sourceTables]) {
        const tableName = table.split(" ")[0].replace(/[();]/g, "");
        if (tableName === "No") continue;
        expect(serverText, `server reference for ${tableName}`).toContain(tableName);
        expect(markdown, `${lineage.tab} markdown table ${tableName}`).toContain(
          tableName.replace(/^system\./, ""),
        );
      }
    }
  });
});

describe("three-page architecture PDF", () => {
  it("hard-limits fitted text and records overflow at the font floor", () => {
    const doc = new jsPDF();
    const overflows: number[] = [];
    const fitted = fittedText(
      doc,
      "A deliberately long architecture sentence that cannot fit inside a narrow two-line box without truncation.",
      18,
      8,
      2,
      ARCHITECTURE_PDF_MIN_FONT_SIZE,
      (result) => overflows.push(result.sourceLineCount),
    );

    expect(fitted.size).toBeGreaterThanOrEqual(ARCHITECTURE_PDF_MIN_FONT_SIZE);
    expect(fitted.lines).toHaveLength(2);
    expect(fitted.lines.at(-1)).toMatch(/\.\.\.$/);
    expect(fitted.overflow).toBe(true);
    expect(overflows).toEqual([fitted.sourceLineCount]);
  });

  it("renders the reference hierarchy on exactly three populated pages", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-28T12:00:00Z"));

    await generateArchitectureReport();

    expect(ARCHITECTURE_PDF_PAGE_COUNT).toBe(3);
    expect(pdf.addPage).toHaveBeenCalledTimes(2);
    expect(pdf.save).toHaveBeenCalledWith(
      "cost-observability-architecture-2026-08-28.pdf",
    );
    for (const page of [1, 2, 3]) {
      expect(pdf.records.some((record) => record.page === page)).toBe(true);
    }

    const text = pdf.records.map((record) => record.text).join("\n");
    for (const required of [
      "cost-obs — architecture",
      "Architecture at a glance",
      "AUTHENTICATION + GOVERNANCE",
      "SCHEDULED REFRESH",
      "ON-DEMAND REFRESH",
      "Request & data flow",
      "Authentication & governance",
      "Refresh paths",
      "COMPONENTS SUMMARY",
      "Tab-by-tab data lineage",
      "SOURCE / APP-MANAGED / CLOUD INVENTORIES",
    ]) {
      expect(text).toContain(required);
    }
    for (const lineage of ARCHITECTURE_OVERVIEW.tabLineage) {
      expect(text).toContain(lineage.tab);
    }
  });

  it("uses the exact v1.2 footer style and keeps body text inside the content area", async () => {
    await generateArchitectureReport();

    const footerLabels = pdf.records.filter((record) => record.y === 202);
    expect(footerLabels.map((record) => record.text)).toEqual([
      "cost-obs v1.2 · architecture",
      "1 / 3",
      "cost-obs v1.2 · architecture",
      "2 / 3",
      "cost-obs v1.2 · architecture · static design content only — no customer, account, workspace, or user identifiers",
      "3 / 3",
    ]);
    const bodyText = pdf.records.filter((record) => record.y !== 202);
    expect(
      bodyText.filter(
        (record) => record.y < 0 || record.y > ARCHITECTURE_PDF_BODY_BOTTOM,
      ),
    ).toEqual([]);
    expect(bodyText.filter((record) => record.x < 0 || record.x > 283)).toEqual([]);

    const renderedText = pdf.records.map((record) => record.text).join("\n");
    expect(renderedText).not.toContain("→");
    for (const pattern of forbiddenIdentifiers) {
      expect(renderedText).not.toMatch(pattern);
    }
  });
});
