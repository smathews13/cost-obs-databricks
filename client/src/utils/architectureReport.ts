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
const PAGE_WIDTH = 297;
const PAGE_HEIGHT = 210;
const FOOTER_RULE_Y = 196;
const BODY_BOTTOM = 192;
const MIN_FONT_SIZE = 6;
const POINT_TO_MM = 25.4 / 72;
const BOUNDS_TOLERANCE = 0.2;

type RGB = [number, number, number];

interface Bounds {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface FittedText {
  lines: string[];
  size: number;
  overflow: boolean;
  sourceLineCount: number;
}

export interface ArchitectureLayoutRecord {
  page: number;
  label: string;
  kind: "text" | "shape" | "image";
  bounds: Bounds;
  container: Bounds;
  fontSize?: number;
}

export interface ArchitectureTextOverflow {
  page: number;
  label: string;
  maxLines: number;
  sourceLineCount: number;
  renderedLines: string[];
}

export interface ArchitectureLayoutAudit {
  minimumFontSize: number;
  records: ArchitectureLayoutRecord[];
  overflows: ArchitectureTextOverflow[];
  violations: string[];
}

interface MutableArchitectureLayoutAudit extends ArchitectureLayoutAudit {
  currentPage: number;
  scopes: Array<{ label: string; bounds: Bounds }>;
}

const layoutAudits = new WeakMap<jsPDF, ArchitectureLayoutAudit>();

function contains(container: Bounds, bounds: Bounds): boolean {
  return (
    bounds.x >= container.x - BOUNDS_TOLERANCE &&
    bounds.y >= container.y - BOUNDS_TOLERANCE &&
    bounds.x + bounds.width <= container.x + container.width + BOUNDS_TOLERANCE &&
    bounds.y + bounds.height <= container.y + container.height + BOUNDS_TOLERANCE
  );
}

function currentScope(audit: MutableArchitectureLayoutAudit): { label: string; bounds: Bounds } {
  return (
    audit.scopes.at(-1) ?? {
      label: "page",
      bounds: { x: 0, y: 0, width: PAGE_WIDTH, height: PAGE_HEIGHT },
    }
  );
}

function recordLayout(
  audit: MutableArchitectureLayoutAudit,
  label: string,
  kind: ArchitectureLayoutRecord["kind"],
  bounds: Bounds,
  fontSize?: number,
): void {
  const scope = currentScope(audit);
  const pageBounds = { x: 0, y: 0, width: PAGE_WIDTH, height: PAGE_HEIGHT };
  const record = {
    page: audit.currentPage,
    label,
    kind,
    bounds,
    container: scope.bounds,
    ...(fontSize === undefined ? {} : { fontSize }),
  };
  audit.records.push(record);
  if (!contains(pageBounds, bounds)) {
    audit.violations.push(`${label} extends beyond page ${audit.currentPage}`);
  }
  if (!contains(scope.bounds, bounds)) {
    audit.violations.push(`${label} extends beyond ${scope.label} on page ${audit.currentPage}`);
  }
  if (fontSize !== undefined && fontSize < audit.minimumFontSize) {
    audit.violations.push(
      `${label} uses ${fontSize.toFixed(2)}pt below ${audit.minimumFontSize}pt`,
    );
  }
}

function within(
  audit: MutableArchitectureLayoutAudit,
  label: string,
  bounds: Bounds,
  render: () => void,
): void {
  const parent = currentScope(audit);
  if (!contains(parent.bounds, bounds)) {
    audit.violations.push(`${label} extends beyond ${parent.label} on page ${audit.currentPage}`);
  }
  audit.scopes.push({ label, bounds });
  try {
    render();
  } finally {
    audit.scopes.pop();
  }
}

function shape(
  audit: MutableArchitectureLayoutAudit,
  label: string,
  bounds: Bounds,
  render: () => void,
): void {
  recordLayout(audit, label, "shape", bounds);
  render();
}

function image(
  audit: MutableArchitectureLayoutAudit,
  label: string,
  bounds: Bounds,
  render: () => void,
): void {
  recordLayout(audit, label, "image", bounds);
  render();
}

function textBounds(
  doc: jsPDF,
  lines: string[],
  x: number,
  baselineY: number,
  size: number,
  lineHeightFactor: number,
  align: "left" | "center" | "right" = "left",
): Bounds {
  const width = Math.max(0, ...lines.map((line) => doc.getTextWidth(line)));
  const fontHeight = size * POINT_TO_MM;
  const lineHeight = fontHeight * lineHeightFactor;
  const left = align === "center" ? x - width / 2 : align === "right" ? x - width : x;
  return {
    x: left,
    y: baselineY - fontHeight * 0.82,
    width,
    height: fontHeight + Math.max(0, lines.length - 1) * lineHeight,
  };
}

function drawText(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  label: string,
  text: string | string[],
  x: number,
  y: number,
  options: {
    align?: "left" | "center" | "right";
    lineHeightFactor?: number;
  } = {},
): void {
  const lines = Array.isArray(text) ? text : [text];
  const size = doc.getFontSize();
  const lineHeightFactor = options.lineHeightFactor ?? 1.15;
  recordLayout(
    audit,
    label,
    "text",
    textBounds(doc, lines, x, y, size, lineHeightFactor, options.align),
    size,
  );
  doc.text(text, x, y, options);
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

function ellipsizeLine(doc: jsPDF, line: string, width: number): string {
  const suffix = "...";
  let candidate = line.trimEnd();
  while (candidate && doc.getTextWidth(`${candidate}${suffix}`) > width) {
    candidate = candidate.slice(0, -1).trimEnd();
  }
  return `${candidate}${suffix}`;
}

export function fittedText(
  doc: jsPDF,
  text: string,
  width: number,
  preferredSize: number,
  maxLines: number,
  minimumSize = MIN_FONT_SIZE,
  onOverflow?: (result: FittedText) => void,
): FittedText {
  if (maxLines < 1) throw new Error("fittedText maxLines must be at least 1");
  const safeText = text.replaceAll("→", ">").replaceAll("…", "...");
  const floor = Math.max(MIN_FONT_SIZE, minimumSize);
  let size = Math.max(preferredSize, floor);
  doc.setFontSize(size);
  let lines = doc.splitTextToSize(safeText, width) as string[];
  while (lines.length > maxLines && size > floor) {
    size = Math.max(floor, size - 0.25);
    doc.setFontSize(size);
    lines = doc.splitTextToSize(safeText, width) as string[];
  }
  const sourceLineCount = lines.length;
  const overflow = sourceLineCount > maxLines;
  if (overflow) {
    lines = lines.slice(0, maxLines);
    lines[maxLines - 1] = ellipsizeLine(doc, lines[maxLines - 1] ?? "", width);
  }
  const result = { lines, size, overflow, sourceLineCount };
  if (overflow) onOverflow?.(result);
  return result;
}

function fitForLayout(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  label: string,
  text: string,
  width: number,
  preferredSize: number,
  maxLines: number,
  minimumSize = MIN_FONT_SIZE,
): FittedText {
  return fittedText(doc, text, width, preferredSize, maxLines, minimumSize, (result) => {
    audit.overflows.push({
      page: audit.currentPage,
      label,
      maxLines,
      sourceLineCount: result.sourceLineCount,
      renderedLines: result.lines,
    });
  });
}

function sectionTitle(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  title: string,
  x: number,
  y: number,
  width: number,
): void {
  setText(doc, 11.5, DB_HEADER, "bold");
  drawText(doc, audit, `${title} heading`, title, x, y);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.setLineWidth(0.3);
  shape(
    audit,
    `${title} heading rule`,
    { x, y: y + 2.5, width, height: 0 },
    () => doc.line(x, y + 2.5, x + width, y + 2.5),
  );
  doc.setFillColor(...DB_ORANGE);
  shape(
    audit,
    `${title} heading accent`,
    { x, y: y + 2.1, width: 14, height: 0.8 },
    () => doc.rect(x, y + 2.1, 14, 0.8, "F"),
  );
}

function drawPageOneHeader(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  assets: PdfBrandAssets,
): void {
  setText(doc, 18, DB_HEADER, "bold");
  drawText(doc, audit, "report title", "cost-obs — architecture", MARGIN, 15);
  doc.setFillColor(...DB_ORANGE);
  shape(audit, "version badge", { x: 80, y: 8.2, width: 18, height: 8.7 }, () =>
    doc.roundedRect(80, 8.2, 18, 8.7, 1.4, 1.4, "F"),
  );
  setText(doc, 8, PDF_WHITE, "bold");
  within(audit, "version badge", { x: 80, y: 8.2, width: 18, height: 8.7 }, () => {
    drawText(doc, audit, "version", "v1.2", 89, 14.1, { align: "center" });
  });

  setText(doc, 8.4, PDF_SLATE);
  drawText(
    doc,
    audit,
    "report subtitle",
    "A Databricks App that turns governed platform usage and operational metadata into interactive cost analysis · August 2026",
    MARGIN,
    22.5,
  );
  setText(doc, 6.5, PDF_SLATE, "bold");
  drawText(doc, audit, "brand eyebrow", "BUILT ON", 253, 10.6);
  image(audit, "Databricks mark", { x: 270, y: 5.8, width: 9, height: 9 }, () =>
    doc.addImage(assets.databricksMark, "PNG", 270, 5.8, 9, 9),
  );
  setText(doc, 7.2, DB_HEADER, "bold");
  drawText(doc, audit, "Databricks wordmark", "Databricks", 252.8, 18);

  setText(doc, 7.1, PDF_BODY);
  const intro =
    "The browser calls a FastAPI service, which executes governed SQL through a bound Databricks SQL Warehouse and serves results from system tables or app-managed Delta aggregates. This document contains static design information only — no customer, account, workspace, or user identifiers.";
  const fitted = fitForLayout(doc, audit, "report introduction", intro, 269, 7.1, 2, 6.6);
  setText(doc, fitted.size, PDF_BODY);
  drawText(doc, audit, "report introduction", fitted.lines, MARGIN, 28.5, {
    lineHeightFactor: 1.25,
  });
}

function architectureBox(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  x: number,
  y: number,
  width: number,
  height: number,
  title: string,
  description: string,
  accent = false,
): void {
  const bounds = { x, y, width, height };
  shape(audit, `${title} card`, bounds, () => {
    doc.setFillColor(...(accent ? DB_HEADER : DB_ALT_ROW));
    doc.setDrawColor(...(accent ? DB_HEADER : PDF_HAIRLINE));
    doc.roundedRect(x, y, width, height, 2, 2, "FD");
  });
  within(audit, `${title} card`, bounds, () => {
    setText(doc, 8.2, accent ? PDF_WHITE : DB_HEADER, "bold");
    const titleFit = fitForLayout(doc, audit, `${title} title`, title, width - 8, 8.2, 2, 7.2);
    setText(doc, titleFit.size, accent ? PDF_WHITE : DB_HEADER, "bold");
    drawText(doc, audit, `${title} title`, titleFit.lines, x + 4, y + 7.5, {
      lineHeightFactor: 1.05,
    });
    setText(doc, 6.5, accent ? PDF_WHITE : PDF_BODY);
    const fitted = fitForLayout(
      doc,
      audit,
      `${title} description`,
      description,
      width - 8,
      6.5,
      5,
      6,
    );
    setText(doc, fitted.size, accent ? PDF_WHITE : PDF_BODY);
    drawText(doc, audit, `${title} description`, fitted.lines, x + 4, y + 17, {
      lineHeightFactor: 1.18,
    });
  });
}

function arrow(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  x1: number,
  x2: number,
  y: number,
  top: string,
  bottom: string,
): void {
  doc.setDrawColor(...PDF_SLATE);
  doc.setFillColor(...PDF_SLATE);
  doc.setLineWidth(0.5);
  shape(audit, `${top} connector`, { x: x1, y: y - 1.3, width: x2 - x1, height: 2.6 }, () => {
    doc.line(x1, y, x2 - 2, y);
    doc.triangle(x2 - 4, y - 1.3, x2 - 4, y + 1.3, x2, y, "F");
  });
  setText(doc, 6, PDF_SLATE, "bold");
  drawText(doc, audit, `${top} connector label`, top, (x1 + x2) / 2, y - 2.7, {
    align: "center",
  });
  setText(doc, 6, PDF_SLATE);
  drawText(doc, audit, `${bottom} connector label`, bottom, (x1 + x2) / 2, y + 4.6, {
    align: "center",
  });
}

function pathLane(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  y: number,
  label: string,
  text: string,
  color: RGB,
): void {
  const width = 269;
  const height = 16;
  const bounds = { x: MARGIN, y, width, height };
  shape(audit, `${label} lane`, bounds, () => {
    doc.setFillColor(...DB_ALT_ROW);
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.roundedRect(MARGIN, y, width, height, 1.5, 1.5, "FD");
    doc.setFillColor(...color);
    doc.roundedRect(MARGIN, y, 59, height, 1.5, 1.5, "F");
  });
  within(audit, `${label} lane`, bounds, () => {
    setText(doc, 6.4, PDF_WHITE, "bold");
    drawText(doc, audit, `${label} lane label`, label, MARGIN + 29.5, y + 9.2, {
      align: "center",
    });
    const fitted = fitForLayout(doc, audit, `${label} lane path`, text, 201, 6.7, 2, 6.2);
    setText(doc, fitted.size, PDF_BODY);
    drawText(doc, audit, `${label} lane path`, fitted.lines, MARGIN + 64, y + 6.4, {
      lineHeightFactor: 1.18,
    });
  });
}

function drawPageOne(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  assets: PdfBrandAssets,
): void {
  drawPageOneHeader(doc, audit, assets);
  sectionTitle(doc, audit, "Architecture at a glance", MARGIN, 43, 269);

  architectureBox(
    doc,
    audit,
    14,
    51,
    43,
    49,
    "Browser · React 19",
    "Nine cost views · filters and reports · React Query cache · local report/PDF generation.",
    true,
  );
  architectureBox(
    doc,
    audit,
    70,
    51,
    46,
    49,
    "FastAPI application",
    "Authenticated tab routes · validation and response shaping · cached bundles or governed SQL · tab cache control.",
  );
  architectureBox(
    doc,
    audit,
    130,
    51,
    51,
    49,
    "Databricks SQL Warehouse",
    "Bound app resource · governed SQL execution · parallel bundle queries · enforces CAN USE for all reads and writes.",
  );
  architectureBox(
    doc,
    audit,
    202,
    51,
    81,
    22,
    "App-managed Delta layer",
    "Eight aggregates · shared response cache · durable settings and refresh history.",
  );
  architectureBox(
    doc,
    audit,
    202,
    78,
    81,
    22,
    "Governed sources",
    "System tables · optional cloud exports · optional shared aggregates · governed reads only.",
  );

  arrow(doc, audit, 58, 69, 75.5, "REST", "same-origin");
  arrow(doc, audit, 117, 129, 75.5, "SQL", "role checks");
  arrow(doc, audit, 182, 201, 62, "read/write", "UC grants");
  arrow(doc, audit, 182, 201, 89, "governed", "reads");

  pathLane(
    doc,
    audit,
    113,
    "AUTHENTICATION + GOVERNANCE",
    "Apps session > FastAPI role checks > service principal / setup user > Warehouse CAN USE + Unity Catalog grants.",
    DB_HEADER,
  );
  pathLane(
    doc,
    audit,
    135,
    "SCHEDULED REFRESH",
    "Scheduler > service principal > incremental Delta MERGEs > refresh state and history > cache invalidation.",
    DB_ORANGE,
  );
  pathLane(
    doc,
    audit,
    157,
    "ON-DEMAND REFRESH",
    "Tab refresh > scoped cache clear > active refetch. Administrator rebuild > full aggregate recreation.",
    PDF_SLATE,
  );
}

function numberedFlow(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  x: number,
  y: number,
  width: number,
): void {
  let cursor = y;
  ARCHITECTURE_OVERVIEW.dataFlow.forEach((step, index) => {
    doc.setFillColor(...DB_ORANGE);
    shape(
      audit,
      `request step ${index + 1} marker`,
      { x: x + 0.6, y: cursor - 3.5, width: 4.8, height: 4.8 },
      () => doc.circle(x + 3, cursor - 1.1, 2.4, "F"),
    );
    setText(doc, 6.2, PDF_WHITE, "bold");
    drawText(doc, audit, `request step ${index + 1} number`, String(index + 1), x + 3, cursor, {
      align: "center",
    });
    const fitted = fitForLayout(
      doc,
      audit,
      `request step ${index + 1}`,
      step,
      width - 11,
      6.4,
      2,
      6,
    );
    setText(doc, fitted.size);
    drawText(doc, audit, `request step ${index + 1}`, fitted.lines, x + 9, cursor, {
      lineHeightFactor: 1.15,
    });
    cursor += Math.max(7.5, fitted.lines.length * 2.7 + 2.2);
  });
}

function governanceList(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  x: number,
  y: number,
  width: number,
): void {
  const items = [
    "Databricks Apps authenticates the browser session and forwards user identity to the app.",
    "FastAPI roles protect settings mutations and rebuilds; user-scoped credentials are reserved for setup operations that need the caller's privileges.",
    "The app service principal performs dashboard SQL and owns managed tables, scheduled refreshes, and cache writes.",
    "Unity Catalog privileges govern table access; the bound SQL Warehouse separately enforces CAN USE.",
  ];
  let cursor = y;
  items.forEach((item, index) => {
    doc.setFillColor(...DB_ORANGE);
    shape(
      audit,
      `governance item ${index + 1} marker`,
      { x: x + 0.5, y: cursor - 1.7, width: 1.4, height: 1.4 },
      () => doc.circle(x + 1.2, cursor - 1, 0.7, "F"),
    );
    const fitted = fitForLayout(
      doc,
      audit,
      `governance item ${index + 1}`,
      item,
      width - 5,
      6.4,
      2,
      6,
    );
    setText(doc, fitted.size);
    drawText(doc, audit, `governance item ${index + 1}`, fitted.lines, x + 4.5, cursor, {
      lineHeightFactor: 1.15,
    });
    cursor += Math.max(9, fitted.lines.length * 2.7 + 2.4);
  });
}

function refreshCard(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  x: number,
  y: number,
  width: number,
  label: string,
  badge: string,
  trigger: string,
  steps: string[],
): void {
  const height = 47;
  const bounds = { x, y, width, height };
  shape(audit, `${label} card`, bounds, () => {
    doc.setFillColor(...DB_ALT_ROW);
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.roundedRect(x, y, width, height, 2, 2, "FD");
    doc.setFillColor(...DB_ORANGE);
    doc.rect(x, y, 2, height, "F");
  });
  within(audit, `${label} card`, bounds, () => {
    doc.saveGraphicsState();
    doc.rect(x + 2.2, y + 0.5, width - 2.7, height - 1, null);
    doc.clip();
    doc.discardPath();
    try {
      setText(doc, 7.6, DB_HEADER, "bold");
      drawText(doc, audit, `${label} title`, label, x + 5, y + 7);
      setText(doc, 6, PDF_SLATE, "bold");
      drawText(doc, audit, `${label} badge`, badge, x + width - 4, y + 7, {
        align: "right",
      });
      setText(doc, 6.2, PDF_SLATE, "bold");
      drawText(doc, audit, `${label} trigger`, trigger, x + 5, y + 13);
      doc.setDrawColor(...PDF_HAIRLINE);
      shape(
        audit,
        `${label} divider`,
        { x: x + 5, y: y + 16, width: width - 10, height: 0 },
        () => doc.line(x + 5, y + 16, x + width - 5, y + 16),
      );

      steps.forEach((step, index) => {
        const baseline = y + 22 + index * 6;
        doc.setFillColor(...DB_ORANGE);
        shape(
          audit,
          `${label} step ${index + 1} marker`,
          { x: x + 5, y: baseline - 1.6, width: 1.4, height: 1.4 },
          () => doc.circle(x + 5.7, baseline - 0.9, 0.7, "F"),
        );
        setText(doc, 6.2, PDF_BODY);
        const fitted = fitForLayout(
          doc,
          audit,
          `${label} step ${index + 1}`,
          step,
          width - 14,
          6.2,
          1,
          6,
        );
        setText(doc, fitted.size, PDF_BODY);
        drawText(
          doc,
          audit,
          `${label} step ${index + 1}`,
          fitted.lines,
          x + 9,
          baseline,
        );
      });
    } finally {
      doc.restoreGraphicsState();
    }
  });
}

function componentCard(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  component: ArchitectureComponent,
  x: number,
  y: number,
  width: number,
): void {
  const height = 15;
  const bounds = { x, y, width, height };
  shape(audit, `${component.name} summary card`, bounds, () => {
    doc.setFillColor(...DB_ALT_ROW);
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.roundedRect(x, y, width, height, 1.3, 1.3, "FD");
  });
  within(audit, `${component.name} summary card`, bounds, () => {
    setText(doc, 6.2, DB_HEADER, "bold");
    drawText(doc, audit, `${component.name} summary title`, component.name, x + 3, y + 4.5);
    const fitted = fitForLayout(
      doc,
      audit,
      `${component.name} summary`,
      component.role,
      width - 6,
      6,
      2,
      6,
    );
    setText(doc, fitted.size, PDF_BODY);
    drawText(doc, audit, `${component.name} summary`, fitted.lines, x + 3, y + 8.7, {
      lineHeightFactor: 1.05,
    });
  });
}

function drawPageTwo(doc: jsPDF, audit: MutableArchitectureLayoutAudit): void {
  sectionTitle(doc, audit, "Request & data flow", 14, 17, 126);
  numberedFlow(doc, audit, 14, 27, 126);

  sectionTitle(doc, audit, "Authentication & governance", 153, 17, 130);
  governanceList(doc, audit, 153, 27, 130);

  sectionTitle(doc, audit, "Refresh paths", 14, 82, 269);
  refreshCard(
    doc,
    audit,
    14,
    90,
    86,
    "Scheduled aggregate refresh",
    "nightly default",
    "Nightly; weekly or monthly selectable",
    [
      "Acquire the shared refresh lock",
      "MERGE recent partitions into 8 aggregates",
      "Save watermarks/history; rebuild source views",
      "Invalidate shared response caches",
    ],
  );
  refreshCard(
    doc,
    audit,
    105.5,
    90,
    86,
    "Administrator full rebuild",
    "admin only",
    "Settings > Config > Rebuild",
    [
      "Verify app administrator; queue locked rebuild",
      "Recreate all aggregates for the history window",
      "Write progress to refresh state/history",
      "Clear query and response caches on completion",
    ],
  );
  refreshCard(
    doc,
    audit,
    197,
    90,
    86,
    "On-demand tab refresh",
    "no rebuild",
    "Refresh on the active dashboard tab",
    [
      "POST the tab name to /api/cache/clear",
      "Clear only that tab's process + shared cache",
      "React Query refetches the active routes",
      "Leave aggregate tables unchanged",
    ],
  );

  setText(doc, 6.2, PDF_SLATE, "bold");
  drawText(doc, audit, "components summary heading", "COMPONENTS SUMMARY", 14, 146);
  const gap = 5;
  const width = (269 - gap * 2) / 3;
  ARCHITECTURE_OVERVIEW.components.forEach((component, index) => {
    componentCard(
      doc,
      audit,
      component,
      14 + (index % 3) * (width + gap),
      151 + Math.floor(index / 3) * 19,
      width,
    );
  });
}

function compactRoutes(item: ArchitectureTabLineage): string {
  const aliases: Record<string, string> = {
    "billing/dashboard-bundle-fast": "billing:dashboard-fast",
    "billing/sku-breakdown": "billing:sku",
    "billing/interactive-breakdown": "billing:interactive",
    "billing/pipeline-objects": "billing:pipelines",
    "billing/kpi-trend": "billing:kpi",
    "billing/sql-breakdown": "billing:sql",
    "billing/platform-kpi-trend": "billing:platform-kpi",
    "billing/kpis-bundle": "billing:kpis",
    "billing/infra-bundle": "billing:infra",
    "dbsql/dashboard-bundle": "dbsql:dashboard",
    "dbsql/top-queries": "dbsql:top",
    "dbsql/top-queries-by-source": "dbsql:top-by-source",
    "dbsql/queries-by-user": "dbsql:by-user",
    "aiml/dashboard-bundle": "aiml:dashboard",
    "apps/dashboard-bundle": "apps:dashboard",
    "apps/kpi-trend": "apps:kpi",
    "tagging/dashboard-bundle": "tagging:dashboard",
    "tagging/top-objects-by-tag": "tagging:top-by-tag",
    "users-groups/bundle": "users-groups:bundle",
    "aws-actual/dashboard-bundle": "aws-actual:dashboard",
    "azure-actual/dashboard-bundle": "azure-actual:dashboard",
    "gcp-actual/dashboard-bundle": "gcp-actual:dashboard",
    "sql/warehouse-health": "sql:warehouse-health",
    "sql/warehouse-health/idle-time": "sql:warehouse-health/idle",
  };
  return item.apiRoutes
    .map((route) => route.replace(/^GET \/api\//, ""))
    .map((route) => aliases[route] ?? route)
    .join(" | ");
}

function compactTables(tables: string[]): string {
  return tables
    .map((table) => table.replace(/^system\./, ""))
    .map((table) =>
      table
        .replace("app_response_cache (bundle cache; analytic views are live)", "app_response_cache (live)")
        .replace(
          "app_response_cache (bundle cache; cluster and DBU analytics are live)",
          "app_response_cache (DBU/cluster live)",
        )
        .replace(
          "No Delta aggregate; results use a 30-minute in-process cache.",
          "No Delta; 30-min process cache",
        ),
    )
    .join(" | ");
}

function compactFallback(item: ArchitectureTabLineage): string {
  const summaries: Record<string, string> = {
    "DBU Overview":
      "Live billing scan if aggregates are empty; detail stays live. Optional: pipeline names + account prices.",
    SQL: "Live billing + query-history fallback. Per-query cost stays unavailable until its Delta table exists.",
    "AI/ML":
      "Billing-only views survive. Optional compute, serving, and workspace metadata enriches names.",
    Apps: "Live billing fallback. Apps API enriches names/resources; stale metadata is tolerated.",
    Tagging:
      "Totals fall back to live billing; untagged rows stay live. Optional compute/Lakeflow names.",
    Users: "Workspace SCIM APIs optionally enrich billing identities with group membership.",
    "KPIs & Trends":
      "Billing KPIs survive missing Lakeflow access; query-history trends remain live.",
    "Cloud Costs":
      "DBU/cluster views work without exports; currency costs require a configured cloud export.",
    Optimize:
      "Idle uptime can use billing intervals; rightsizing is unavailable without regional tables.",
  };
  return summaries[item.tab] ?? [...(item.fallbacks ?? []), ...(item.optionalSources ?? [])].join(" ");
}

function drawCellText(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  label: string,
  text: string,
  x: number,
  y: number,
  width: number,
  height: number,
  maxLines: number,
  bold = false,
): FittedText {
  const fitted = fitForLayout(
    doc,
    audit,
    label,
    text || "—",
    width - 4,
    bold ? 6.4 : 6,
    maxLines,
    6,
  );
  setText(doc, fitted.size, bold ? DB_HEADER : PDF_BODY, bold ? "bold" : "normal");
  within(audit, `${label} cell`, { x, y: y - 4.1, width, height }, () => {
    drawText(doc, audit, label, fitted.lines, x + 2, y, { lineHeightFactor: 1.04 });
  });
  return fitted;
}

function drawLineageTable(doc: jsPDF, audit: MutableArchitectureLayoutAudit): number {
  const x = 14;
  const widths = [22, 60, 50, 55, 82];
  const headers = [
    "TAB",
    "FASTAPI ROUTES (GET /API/...)",
    "MANAGED DELTA / CACHE",
    "SOURCE SYSTEM TABLES",
    "FALLBACK / OPTIONAL ENRICHMENT",
  ];
  const headerTop = 24;
  const headerHeight = 8;
  const rowHeight = 12.4;
  let cursorX = x;

  doc.setFillColor(...DB_HEADER);
  shape(audit, "lineage table header", { x, y: headerTop, width: 269, height: headerHeight }, () =>
    doc.rect(x, headerTop, 269, headerHeight, "F"),
  );
  headers.forEach((header, index) => {
    const width = widths[index] ?? 0;
    within(
      audit,
      `${header} header cell`,
      { x: cursorX, y: headerTop, width, height: headerHeight },
      () => {
        setText(doc, 6, PDF_WHITE, "bold");
        drawText(doc, audit, `${header} header`, header, cursorX + 2, 29.2);
      },
    );
    cursorX += widths[index] ?? 0;
  });

  ARCHITECTURE_OVERVIEW.tabLineage.forEach((item, rowIndex) => {
    const rowTop = headerTop + headerHeight + rowIndex * rowHeight;
    const baseline = rowTop + 4.1;
    const values = [
      item.tab,
      compactRoutes(item),
      compactTables(item.managedData),
      compactTables(item.sourceTables),
      compactFallback(item),
    ];
    doc.setFillColor(...(rowIndex % 2 === 0 ? DB_ALT_ROW : PDF_WHITE));
    shape(
      audit,
      `${item.tab} lineage row`,
      { x, y: rowTop, width: 269, height: rowHeight },
      () => doc.rect(x, rowTop, 269, rowHeight, "F"),
    );

    cursorX = x;
    values.forEach((value, index) => {
      drawCellText(
        doc,
        audit,
        `${item.tab} ${headers[index]}`,
        value,
        cursorX,
        baseline,
        widths[index] ?? 0,
        rowHeight,
        4,
        index === 0,
      );
      cursorX += widths[index] ?? 0;
    });
    doc.setDrawColor(...PDF_HAIRLINE);
    shape(
      audit,
      `${item.tab} lineage divider`,
      { x, y: rowTop + rowHeight, width: 269, height: 0 },
      () => doc.line(x, rowTop + rowHeight, x + 269, rowTop + rowHeight),
    );
  });
  return headerTop + headerHeight + ARCHITECTURE_OVERVIEW.tabLineage.length * rowHeight;
}

function inventoryColumn(
  doc: jsPDF,
  audit: MutableArchitectureLayoutAudit,
  x: number,
  y: number,
  width: number,
  heading: string,
  values: string[],
  note?: string,
): void {
  const height = 39;
  const bounds = { x, y, width, height };
  shape(audit, `${heading} inventory card`, bounds, () => {
    doc.setFillColor(...DB_ALT_ROW);
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.roundedRect(x, y, width, height, 1.3, 1.3, "FD");
  });
  within(audit, `${heading} inventory card`, bounds, () => {
    setText(doc, 6.3, DB_HEADER, "bold");
    drawText(doc, audit, `${heading} inventory title`, heading, x + 3, y + 5.4);
    let cursor = y + 10;
    values.forEach((value, index) => {
      doc.setFont("courier", "normal");
      doc.setFontSize(6);
      doc.setTextColor(...PDF_BODY);
      drawText(doc, audit, `${heading} item ${index + 1}`, value, x + 3, cursor, {
        lineHeightFactor: 1,
      });
      cursor += 2.25;
    });
    if (note) {
      const fitted = fitForLayout(
        doc,
        audit,
        `${heading} note`,
        note,
        width - 6,
        6,
        heading.startsWith("Core") ? 2 : 6,
        6,
      );
      setText(doc, fitted.size, PDF_SLATE, "italic");
      drawText(doc, audit, `${heading} note`, fitted.lines, x + 3, cursor + 1, {
        lineHeightFactor: 1.02,
      });
    }
  });
}

function drawPageThree(doc: jsPDF, audit: MutableArchitectureLayoutAudit): void {
  sectionTitle(doc, audit, "Tab-by-tab data lineage", 14, 17, 269);
  const tableBottom = drawLineageTable(doc, audit);
  const inventoryY = tableBottom + 5;
  setText(doc, 6, PDF_SLATE, "bold");
  drawText(
    doc,
    audit,
    "inventories heading",
    "SOURCE / APP-MANAGED / CLOUD INVENTORIES",
    14,
    inventoryY,
  );
  setText(doc, 6, PDF_SLATE, "italic");
  drawText(
    doc,
    audit,
    "public markdown cross-reference",
    "Full route and fallback detail: cost-obs-architecture.md",
    283,
    inventoryY,
    { align: "right" },
  );

  const groups = Object.fromEntries(
    ARCHITECTURE_OVERVIEW.sourceTables.map((group) => [group.label, group]),
  );
  inventoryColumn(
    doc,
    audit,
    14,
    inventoryY + 3,
    62,
    "Core analytic system tables",
    groups["Core analytic system tables"].tables,
    "Optional: system.billing.account_prices. Probe-only: system.access.audit.",
  );
  inventoryColumn(
    doc,
    audit,
    79,
    inventoryY + 3,
    51,
    "App-managed Delta (8)",
    groups["App-managed analytic tables"].tables,
  );
  inventoryColumn(
    doc,
    audit,
    133,
    inventoryY + 3,
    66,
    "Durable state & cache (13)",
    groups["Durable app state and cache"].tables,
  );
  inventoryColumn(
    doc,
    audit,
    202,
    inventoryY + 3,
    81,
    "Optional cloud billing sources",
    groups["Optional cloud billing sources"].tables,
    "Administrator-configured. GCP may be federated BigQuery or curated Delta. Used only for actual-cloud-cost views.",
  );
}

function addArchitectureFooters(doc: jsPDF, audit: MutableArchitectureLayoutAudit): void {
  for (let page = 1; page <= PAGE_COUNT; page += 1) {
    doc.setPage(page);
    audit.currentPage = page;
    within(
      audit,
      `page ${page} footer`,
      { x: 0, y: BODY_BOTTOM, width: PAGE_WIDTH, height: PAGE_HEIGHT - BODY_BOTTOM },
      () => {
        doc.setDrawColor(...PDF_HAIRLINE);
        doc.setLineWidth(0.3);
        shape(
          audit,
          `page ${page} footer rule`,
          { x: MARGIN, y: FOOTER_RULE_Y, width: 269, height: 0 },
          () => doc.line(MARGIN, FOOTER_RULE_Y, 283, FOOTER_RULE_Y),
        );
        doc.setFillColor(...DB_ORANGE);
        shape(
          audit,
          `page ${page} footer accent`,
          { x: MARGIN, y: FOOTER_RULE_Y - 0.4, width: 12, height: 0.8 },
          () => doc.rect(MARGIN, FOOTER_RULE_Y - 0.4, 12, 0.8, "F"),
        );
        setText(doc, 6.5, PDF_SLATE);
        const suffix =
          page === PAGE_COUNT
            ? " · static design content only — no customer, account, workspace, or user identifiers"
            : "";
        drawText(
          doc,
          audit,
          `page ${page} footer label`,
          `cost-obs v1.2 · architecture${suffix}`,
          MARGIN,
          202,
        );
        setText(doc, 6.5, PDF_SLATE, "bold");
        drawText(doc, audit, `page ${page} number`, `${page} / ${PAGE_COUNT}`, 283, 202, {
          align: "right",
        });
      },
    );
  }
}

export function createArchitectureReport(assets: PdfBrandAssets): jsPDF {
  const doc = new jsPDF({ orientation: "landscape" });
  const audit: MutableArchitectureLayoutAudit = {
    minimumFontSize: MIN_FONT_SIZE,
    records: [],
    overflows: [],
    violations: [],
    currentPage: 1,
    scopes: [],
  };

  within(audit, "page 1 body", { x: 0, y: 0, width: PAGE_WIDTH, height: BODY_BOTTOM }, () =>
    drawPageOne(doc, audit, assets),
  );
  doc.addPage();
  audit.currentPage = 2;
  within(audit, "page 2 body", { x: 0, y: 0, width: PAGE_WIDTH, height: BODY_BOTTOM }, () =>
    drawPageTwo(doc, audit),
  );
  doc.addPage();
  audit.currentPage = 3;
  within(audit, "page 3 body", { x: 0, y: 0, width: PAGE_WIDTH, height: BODY_BOTTOM }, () =>
    drawPageThree(doc, audit),
  );

  if (doc.getNumberOfPages() !== PAGE_COUNT) {
    throw new Error(`Architecture report must contain exactly ${PAGE_COUNT} pages`);
  }
  addArchitectureFooters(doc, audit);
  layoutAudits.set(doc, audit);
  if (audit.violations.length > 0) {
    throw new Error(`Architecture layout violations:\n${audit.violations.join("\n")}`);
  }
  return doc;
}

export function getArchitectureLayoutAudit(doc: jsPDF): ArchitectureLayoutAudit {
  const audit = layoutAudits.get(doc);
  if (!audit) throw new Error("No architecture layout audit is available for this document");
  return audit;
}

export async function generateArchitectureReport(): Promise<void> {
  const assets = await loadPdfBrandAssets();
  const doc = createArchitectureReport(assets);
  doc.save(`cost-observability-architecture-${format(new Date(), "yyyy-MM-dd")}.pdf`);
}

export const ARCHITECTURE_PDF_PAGE_COUNT = PAGE_COUNT;
export const ARCHITECTURE_PDF_BODY_BOTTOM = BODY_BOTTOM;
export const ARCHITECTURE_PDF_MIN_FONT_SIZE = MIN_FONT_SIZE;
