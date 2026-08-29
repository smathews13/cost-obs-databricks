import { format } from "date-fns";
import { jsPDF } from "jspdf";
import {
  ARCHITECTURE_OVERVIEW,
  type ArchitectureComponent,
  type ArchitectureTabLineage,
} from "./architectureOverview";
import {
  DB_ALT_ROW,
  DB_HEADER,
  DB_ORANGE,
  loadPdfBrandAssets,
  PDF_BODY,
  PDF_HAIRLINE,
  PDF_SLATE,
  PDF_WHITE,
  type PdfBrandAssets,
} from "./pdfBrand";

const PAGE_COUNT = 3;
const MARGIN = 14;
const FOOTER_RULE_Y = 196;
const BODY_BOTTOM = 192;

type RGB = [number, number, number];

interface FittedText {
  lines: string[];
  size: number;
}

function setText(
  doc: jsPDF,
  size: number,
  color: RGB = PDF_BODY,
  style: "normal" | "bold" | "italic" = "normal",
): void {
  doc.setFont("helvetica", style);
  doc.setFontSize(size);
  doc.setTextColor(...color);
}

function fittedText(
  doc: jsPDF,
  text: string,
  width: number,
  preferredSize: number,
  maxLines: number,
  minimumSize = 4.2,
): FittedText {
  let size = preferredSize;
  doc.setFontSize(size);
  let lines = doc.splitTextToSize(text, width) as string[];
  while (lines.length > maxLines && size > minimumSize) {
    size = Math.max(minimumSize, size - 0.25);
    doc.setFontSize(size);
    lines = doc.splitTextToSize(text, width) as string[];
  }
  return { lines, size };
}

function sectionTitle(doc: jsPDF, title: string, x: number, y: number, width: number): void {
  setText(doc, 11.5, DB_HEADER, "bold");
  doc.text(title, x, y);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.setLineWidth(0.3);
  doc.line(x, y + 2.5, x + width, y + 2.5);
  doc.setFillColor(...DB_ORANGE);
  doc.rect(x, y + 2.1, 14, 0.8, "F");
}

function drawPageOneHeader(doc: jsPDF, assets: PdfBrandAssets): void {
  setText(doc, 18, DB_HEADER, "bold");
  doc.text("cost-obs — architecture", MARGIN, 15);
  doc.setFillColor(...DB_ORANGE);
  doc.roundedRect(80, 8.2, 18, 8.7, 1.4, 1.4, "F");
  setText(doc, 8, PDF_WHITE, "bold");
  doc.text("v1.2", 89, 14.1, { align: "center" });

  setText(doc, 8.4, PDF_SLATE);
  doc.text(
    "A Databricks App that turns governed platform usage and operational metadata into interactive cost analysis · August 2026",
    MARGIN,
    22.5,
  );
  setText(doc, 6.5, PDF_SLATE, "bold");
  doc.text("BUILT ON", 253, 10.6);
  doc.addImage(assets.databricksMark, "PNG", 270, 5.8, 9, 9);
  setText(doc, 7.2, DB_HEADER, "bold");
  doc.text("Databricks", 252.8, 18);

  setText(doc, 7.1, PDF_BODY);
  const intro =
    "The browser calls a FastAPI service, which executes governed SQL through a bound Databricks SQL Warehouse and serves results from system tables or app-managed Delta aggregates. This document contains static design information only — no customer, account, workspace, or user identifiers.";
  doc.text(doc.splitTextToSize(intro, 269) as string[], MARGIN, 28.5, { lineHeightFactor: 1.25 });
}

function architectureBox(
  doc: jsPDF,
  x: number,
  y: number,
  width: number,
  height: number,
  title: string,
  description: string,
  accent = false,
): void {
  doc.setFillColor(...(accent ? DB_HEADER : DB_ALT_ROW));
  doc.setDrawColor(...(accent ? DB_HEADER : PDF_HAIRLINE));
  doc.roundedRect(x, y, width, height, 2, 2, "FD");
  setText(doc, 9, accent ? PDF_WHITE : DB_HEADER, "bold");
  doc.text(doc.splitTextToSize(title, width - 8) as string[], x + 4, y + 8, {
    lineHeightFactor: 1.1,
  });
  setText(doc, 6.7, accent ? PDF_WHITE : PDF_BODY);
  const fitted = fittedText(doc, description, width - 8, 6.7, Math.floor((height - 18) / 3));
  setText(doc, fitted.size, accent ? PDF_WHITE : PDF_BODY);
  doc.text(fitted.lines, x + 4, y + 18, { lineHeightFactor: 1.2 });
}

function arrow(doc: jsPDF, x1: number, x2: number, y: number, top: string, bottom: string): void {
  doc.setDrawColor(...PDF_SLATE);
  doc.setFillColor(...PDF_SLATE);
  doc.setLineWidth(0.5);
  doc.line(x1, y, x2 - 2, y);
  doc.triangle(x2 - 4, y - 1.3, x2 - 4, y + 1.3, x2, y, "F");
  setText(doc, 5.5, PDF_SLATE, "bold");
  doc.text(top, (x1 + x2) / 2, y - 2.7, { align: "center" });
  setText(doc, 5.1, PDF_SLATE);
  doc.text(bottom, (x1 + x2) / 2, y + 4.2, { align: "center" });
}

function pathLane(doc: jsPDF, y: number, label: string, text: string, color: RGB): void {
  const width = 269;
  doc.setFillColor(...DB_ALT_ROW);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.roundedRect(MARGIN, y, width, 14, 1.5, 1.5, "FD");
  doc.setFillColor(...color);
  doc.roundedRect(MARGIN, y, 60, 14, 1.5, 1.5, "F");
  setText(doc, 6.6, PDF_WHITE, "bold");
  doc.text(label, MARGIN + 30, y + 8.2, { align: "center" });
  const fitted = fittedText(doc, text, 201, 7, 2, 6.2);
  setText(doc, fitted.size, PDF_BODY);
  doc.text(fitted.lines, MARGIN + 65, y + 5.8, { lineHeightFactor: 1.15 });
}

function drawPageOne(doc: jsPDF, assets: PdfBrandAssets): void {
  drawPageOneHeader(doc, assets);
  sectionTitle(doc, "Architecture at a glance", MARGIN, 43, 269);

  architectureBox(
    doc,
    14,
    50,
    50,
    67,
    "Browser · React 19",
    "Nine cost views · filters & reports · React Query cache · renders shaped JSON; report/PDF generation is local.",
    true,
  );
  architectureBox(
    doc,
    78,
    50,
    54,
    67,
    "FastAPI application",
    "Authenticated routes per tab · request validation & response shaping · serves cached bundles, else submits governed SQL · tab-scoped cache control.",
  );
  architectureBox(
    doc,
    147,
    50,
    52,
    67,
    "Databricks SQL Warehouse",
    "Bound app resource · governed SQL execution plane · parallel bundle queries · enforces CAN USE; executes all table reads & writes.",
  );
  architectureBox(
    doc,
    217,
    50,
    66,
    31,
    "App-managed Delta layer",
    "Eight aggregates · shared response cache · durable settings, refresh state & history.",
  );
  architectureBox(
    doc,
    217,
    86,
    66,
    31,
    "Governed sources",
    "System tables · optional cloud billing exports · optional shared aggregates — governed reads only.",
  );

  arrow(doc, 65, 77, 82, "REST", "same-origin");
  arrow(doc, 133, 146, 82, "SQL", "role checks");
  arrow(doc, 200, 216, 66, "read / write", "UC grants");
  arrow(doc, 200, 216, 102, "governed", "reads");

  pathLane(
    doc,
    127,
    "AUTHENTICATION + GOVERNANCE",
    "Apps session → FastAPI role checks → app service principal / setup user → Warehouse CAN USE + Unity Catalog grants.",
    DB_HEADER,
  );
  pathLane(
    doc,
    146,
    "SCHEDULED REFRESH",
    "Scheduler → service principal → incremental Delta MERGEs → refresh state & history → cache invalidation.",
    DB_ORANGE,
  );
  pathLane(
    doc,
    165,
    "ON-DEMAND REFRESH",
    "Tab refresh → scoped cache clear → active refetch. Administrator rebuild → full aggregate recreation.",
    PDF_SLATE,
  );
}

function numberedFlow(doc: jsPDF, x: number, y: number, width: number): void {
  let cursor = y;
  ARCHITECTURE_OVERVIEW.dataFlow.forEach((step, index) => {
    doc.setFillColor(...DB_ORANGE);
    doc.circle(x + 3, cursor - 1.1, 2.4, "F");
    setText(doc, 6.3, PDF_WHITE, "bold");
    doc.text(String(index + 1), x + 3, cursor, { align: "center" });
    const fitted = fittedText(doc, step, width - 11, 6.5, 3, 5.7);
    setText(doc, fitted.size);
    doc.text(fitted.lines, x + 9, cursor, { lineHeightFactor: 1.15 });
    cursor += Math.max(8.2, fitted.lines.length * 3.1 + 2);
  });
}

function governanceList(doc: jsPDF, x: number, y: number, width: number): void {
  const items = [
    "Databricks Apps authenticates the browser session and forwards user identity to the app.",
    "FastAPI roles protect settings mutations and rebuilds; user-scoped credentials are reserved for setup operations that need the caller's privileges.",
    "The app service principal performs dashboard SQL and owns managed tables, scheduled refreshes, and cache writes.",
    "Unity Catalog privileges govern table access; the bound SQL Warehouse separately enforces CAN USE.",
  ];
  let cursor = y;
  items.forEach((item) => {
    doc.setFillColor(...DB_ORANGE);
    doc.circle(x + 1.2, cursor - 1, 0.7, "F");
    const fitted = fittedText(doc, item, width - 5, 6.5, 3, 5.7);
    setText(doc, fitted.size);
    doc.text(fitted.lines, x + 4.5, cursor, { lineHeightFactor: 1.15 });
    cursor += Math.max(9, fitted.lines.length * 3.1 + 2.2);
  });
}

function refreshCard(
  doc: jsPDF,
  x: number,
  y: number,
  width: number,
  label: string,
  badge: string,
  body: string,
): void {
  doc.setFillColor(...DB_ALT_ROW);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.roundedRect(x, y, width, 44, 2, 2, "FD");
  doc.setFillColor(...DB_ORANGE);
  doc.rect(x, y, 2, 44, "F");
  setText(doc, 8, DB_HEADER, "bold");
  doc.text(label, x + 5, y + 7);
  setText(doc, 5.4, PDF_SLATE, "bold");
  doc.text(badge, x + width - 4, y + 7, { align: "right" });
  const fitted = fittedText(doc, body, width - 10, 6.2, 9, 5.2);
  setText(doc, fitted.size);
  doc.text(fitted.lines, x + 5, y + 13, { lineHeightFactor: 1.18 });
}

function componentCard(
  doc: jsPDF,
  component: ArchitectureComponent,
  x: number,
  y: number,
  width: number,
): void {
  doc.setFillColor(...DB_ALT_ROW);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.roundedRect(x, y, width, 14, 1.3, 1.3, "FD");
  setText(doc, 6.1, DB_HEADER, "bold");
  doc.text(component.name, x + 3, y + 4.5);
  const fitted = fittedText(doc, component.role, width - 6, 5.1, 2, 4.4);
  setText(doc, fitted.size, PDF_BODY);
  doc.text(fitted.lines, x + 3, y + 8.2, { lineHeightFactor: 1.05 });
}

function drawPageTwo(doc: jsPDF): void {
  sectionTitle(doc, "Request & data flow", 14, 17, 126);
  numberedFlow(doc, 14, 27, 126);

  sectionTitle(doc, "Authentication & governance", 153, 17, 130);
  governanceList(doc, 153, 27, 130);

  sectionTitle(doc, "Refresh paths", 14, 86, 269);
  refreshCard(
    doc,
    14,
    94,
    86,
    "Scheduled aggregate refresh",
    "nightly default",
    "Scheduler acquires the shared refresh lock → service principal incrementally MERGEs recent source partitions into eight Delta aggregates → watermarks & history persist, unified views rebuild, response caches invalidate. Weekly or monthly cadence selectable.",
  );
  refreshCard(
    doc,
    105.5,
    94,
    86,
    "Administrator full rebuild",
    "Settings → Config → Rebuild",
    "Admin check → locked background rebuild queued → every aggregate recreated from the configured history window → progress written to refresh state/history; all query & response caches clear on completion.",
  );
  refreshCard(
    doc,
    197,
    94,
    86,
    "On-demand tab refresh",
    "no rebuild",
    "React posts the tab name to /api/cache/clear → FastAPI invalidates only that tab's in-process and shared Delta response-cache entries → React Query refetches active routes. Aggregate tables are untouched.",
  );

  setText(doc, 6.2, PDF_SLATE, "bold");
  doc.text("COMPONENTS SUMMARY", 14, 148);
  const gap = 5;
  const width = (269 - gap * 2) / 3;
  ARCHITECTURE_OVERVIEW.components.forEach((component, index) => {
    componentCard(
      doc,
      component,
      14 + (index % 3) * (width + gap),
      153 + Math.floor(index / 3) * 18,
      width,
    );
  });
}

function compactRoutes(item: ArchitectureTabLineage): string {
  return item.apiRoutes.map((route) => route.replace(/^GET \/api\//, "")).join(" · ");
}

function compactTables(tables: string[]): string {
  return tables.map((table) => table.replace(/^system\./, "")).join(" · ");
}

function compactFallback(item: ArchitectureTabLineage): string {
  return [...(item.fallbacks ?? []), ...(item.optionalSources ?? [])].join(" ");
}

function drawCellText(
  doc: jsPDF,
  text: string,
  x: number,
  y: number,
  width: number,
  maxLines: number,
  bold = false,
): FittedText {
  const fitted = fittedText(doc, text || "—", width - 4, bold ? 5.5 : 5.1, maxLines, 4.1);
  setText(doc, fitted.size, bold ? DB_HEADER : PDF_BODY, bold ? "bold" : "normal");
  doc.text(fitted.lines, x + 2, y, { lineHeightFactor: 1.08 });
  return fitted;
}

function drawLineageTable(doc: jsPDF): number {
  const x = 14;
  const widths = [23, 65, 48, 55, 78];
  const headers = [
    "TAB",
    "FASTAPI ROUTES (GET /API/…)",
    "MANAGED DELTA / CACHE",
    "SOURCE SYSTEM TABLES",
    "FALLBACK & OPTIONAL ENRICHMENT",
  ];
  let cursorY = 30;
  let cursorX = x;

  doc.setFillColor(...DB_HEADER);
  doc.rect(x, 24, 269, 8, "F");
  headers.forEach((header, index) => {
    setText(doc, 4.9, PDF_WHITE, "bold");
    doc.text(header, cursorX + 2, 29.1);
    cursorX += widths[index] ?? 0;
  });
  cursorY = 36;

  ARCHITECTURE_OVERVIEW.tabLineage.forEach((item, rowIndex) => {
    const values = [
      item.tab,
      compactRoutes(item),
      compactTables(item.managedData),
      compactTables(item.sourceTables),
      compactFallback(item),
    ];
    const fitted = values.map((value, index) =>
      fittedText(doc, value || "—", (widths[index] ?? 0) - 4, index === 0 ? 5.5 : 5.1, 5, 4.1),
    );
    const lineCount = Math.max(...fitted.map((entry) => entry.lines.length));
    const rowHeight = Math.max(9.2, lineCount * 2.55 + 2.2);
    doc.setFillColor(...(rowIndex % 2 === 0 ? DB_ALT_ROW : PDF_WHITE));
    doc.rect(x, cursorY - 3.2, 269, rowHeight, "F");

    cursorX = x;
    values.forEach((value, index) => {
      drawCellText(doc, value, cursorX, cursorY, widths[index] ?? 0, 5, index === 0);
      cursorX += widths[index] ?? 0;
    });
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.line(x, cursorY - 3.2 + rowHeight, x + 269, cursorY - 3.2 + rowHeight);
    cursorY += rowHeight;
  });
  return cursorY;
}

function inventoryColumn(
  doc: jsPDF,
  x: number,
  y: number,
  width: number,
  heading: string,
  badge: string,
  values: string[],
  note?: string,
): void {
  setText(doc, 6.2, DB_HEADER, "bold");
  doc.text(heading, x, y);
  if (badge) {
    setText(doc, 4.7, DB_ORANGE, "bold");
    doc.text(badge, x + width, y, { align: "right" });
  }
  let cursor = y + 4;
  values.forEach((value) => {
    doc.setFont("courier", "normal");
    doc.setFontSize(4.8);
    doc.setTextColor(...PDF_BODY);
    doc.text(value, x, cursor);
    cursor += 2.2;
  });
  if (note) {
    const fitted = fittedText(doc, note, width, 4.5, 3, 4.1);
    setText(doc, fitted.size, PDF_SLATE, "italic");
    doc.text(fitted.lines, x, cursor + 0.8, { lineHeightFactor: 1.05 });
  }
}

function drawPageThree(doc: jsPDF): void {
  sectionTitle(doc, "Tab-by-tab data lineage", 14, 17, 269);
  const tableBottom = drawLineageTable(doc);
  const inventoryY = Math.min(Math.max(tableBottom + 4, 153), 158);
  setText(doc, 5.5, PDF_SLATE, "bold");
  doc.text("SOURCE / APP-MANAGED / CLOUD INVENTORIES", 14, inventoryY);

  const groups = Object.fromEntries(
    ARCHITECTURE_OVERVIEW.sourceTables.map((group) => [group.label, group]),
  );
  inventoryColumn(
    doc,
    14,
    inventoryY + 6,
    59,
    "Core analytic system tables",
    "",
    groups["Core analytic system tables"].tables,
    "Optional: system.billing.account_prices · probe-only: system.access.audit.",
  );
  inventoryColumn(
    doc,
    78,
    inventoryY + 6,
    54,
    "App-managed Delta",
    "8 AGGREGATES",
    groups["App-managed analytic tables"].tables,
  );
  inventoryColumn(
    doc,
    138,
    inventoryY + 6,
    65,
    "Durable app state & cache",
    "13 TABLES",
    groups["Durable app state and cache"].tables,
  );
  inventoryColumn(
    doc,
    210,
    inventoryY + 6,
    73,
    "Optional cloud billing sources",
    "",
    groups["Optional cloud billing sources"].tables,
    "Administrator-configured. GCP may be a federated BigQuery billing export or curated Delta table. Queried only for actual-cloud-cost views.",
  );
}

function addArchitectureFooters(doc: jsPDF): void {
  for (let page = 1; page <= PAGE_COUNT; page += 1) {
    doc.setPage(page);
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.setLineWidth(0.3);
    doc.line(MARGIN, FOOTER_RULE_Y, 283, FOOTER_RULE_Y);
    doc.setFillColor(...DB_ORANGE);
    doc.rect(MARGIN, FOOTER_RULE_Y - 0.4, 12, 0.8, "F");
    setText(doc, 6.5, PDF_SLATE);
    const suffix =
      page === PAGE_COUNT
        ? " · static design content only — no customer, account, workspace, or user identifiers"
        : "";
    doc.text(`cost-obs v1.2 · architecture${suffix}`, MARGIN, 202);
    setText(doc, 6.5, PDF_SLATE, "bold");
    doc.text(`${page} / ${PAGE_COUNT}`, 283, 202, { align: "right" });
  }
}

export function createArchitectureReport(assets: PdfBrandAssets): jsPDF {
  const doc = new jsPDF({ orientation: "landscape" });

  drawPageOne(doc, assets);
  doc.addPage();
  drawPageTwo(doc);
  doc.addPage();
  drawPageThree(doc);

  if (doc.getNumberOfPages() !== PAGE_COUNT) {
    throw new Error(`Architecture report must contain exactly ${PAGE_COUNT} pages`);
  }
  addArchitectureFooters(doc);
  return doc;
}

export async function generateArchitectureReport(): Promise<void> {
  const assets = await loadPdfBrandAssets();
  const doc = createArchitectureReport(assets);
  doc.save(`cost-observability-architecture-${format(new Date(), "yyyy-MM-dd")}.pdf`);
}

export const ARCHITECTURE_PDF_PAGE_COUNT = PAGE_COUNT;
export const ARCHITECTURE_PDF_BODY_BOTTOM = BODY_BOTTOM;
