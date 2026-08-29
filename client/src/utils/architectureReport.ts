import { format } from "date-fns";
import { jsPDF } from "jspdf";
import {
  ARCHITECTURE_OVERVIEW,
  type ArchitectureRefreshPath,
  type ArchitectureSourceGroup,
  type ArchitectureTabLineage,
} from "./architectureOverview";
import {
  addPdfFooters,
  DB_ALT_ROW,
  DB_HEADER,
  DB_ORANGE,
  drawPdfMasthead,
  loadPdfBrandAssets,
  PDF_BODY,
  PDF_HAIRLINE,
  PDF_SLATE,
  PDF_WHITE,
  type PdfBrandAssets,
} from "./pdfBrand";

const MARGIN = 14;

function contentBottom(doc: jsPDF): number {
  return doc.internal.pageSize.height - 21;
}

function setBodyStyle(doc: jsPDF, size = 9): void {
  doc.setFont("helvetica", "normal");
  doc.setFontSize(size);
  doc.setTextColor(...PDF_BODY);
}

function addBrandedPage(doc: jsPDF, assets: PdfBrandAssets): number {
  doc.addPage();
  drawPdfMasthead(doc, assets);
  return 32;
}

function ensureRoom(doc: jsPDF, assets: PdfBrandAssets, y: number, height: number): number {
  return y + height > contentBottom(doc) ? addBrandedPage(doc, assets) : y;
}

function drawSectionHeading(doc: jsPDF, title: string, y: number): number {
  doc.setFont("helvetica", "bold");
  doc.setFontSize(13);
  doc.setTextColor(...DB_ORANGE);
  doc.text(title, MARGIN, y);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.setLineWidth(0.3);
  doc.line(MARGIN, y + 2.5, doc.internal.pageSize.width - MARGIN, y + 2.5);
  return y + 8;
}

function drawWrappedText(doc: jsPDF, text: string, x: number, y: number, width: number, size = 9): number {
  setBodyStyle(doc, size);
  const lines = doc.splitTextToSize(text, width) as string[];
  doc.text(lines, x, y);
  return y + lines.length * (size * 0.43) + 2;
}

function drawBulletList(
  doc: jsPDF,
  assets: PdfBrandAssets,
  items: string[],
  yStart: number,
): number {
  let y = yStart;
  const width = doc.internal.pageSize.width - MARGIN * 2 - 7;
  for (const item of items) {
    const lines = doc.splitTextToSize(item, width) as string[];
    const height = lines.length * 4.2 + 2;
    y = ensureRoom(doc, assets, y, height);
    doc.setFillColor(...DB_ORANGE);
    doc.circle(MARGIN + 1.5, y - 1.1, 0.8, "F");
    setBodyStyle(doc);
    doc.text(lines, MARGIN + 5, y);
    y += height;
  }
  return y;
}

function drawArrow(doc: jsPDF, x1: number, x2: number, y: number, label?: string): void {
  doc.setDrawColor(...PDF_SLATE);
  doc.setFillColor(...PDF_SLATE);
  doc.setLineWidth(0.5);
  doc.line(x1, y, x2 - 2, y);
  doc.triangle(x2 - 4, y - 1.4, x2 - 4, y + 1.4, x2, y, "F");
  if (label) {
    doc.setFont("helvetica", "normal");
    doc.setFontSize(6.7);
    doc.setTextColor(...PDF_SLATE);
    doc.text(label, (x1 + x2) / 2, y - 2, { align: "center" });
  }
}

function drawLayerBox(
  doc: jsPDF,
  x: number,
  y: number,
  width: number,
  title: string,
  lines: string[],
  accent = false,
): void {
  doc.setFillColor(...(accent ? DB_HEADER : DB_ALT_ROW));
  doc.setDrawColor(...(accent ? DB_HEADER : PDF_HAIRLINE));
  doc.roundedRect(x, y, width, 31, 2, 2, "FD");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(8.3);
  doc.setTextColor(...(accent ? PDF_WHITE : DB_HEADER));
  const titleLines = (doc.splitTextToSize(title, width - 5) as string[]).slice(0, 2);
  doc.text(titleLines, x + width / 2, y + 6, { align: "center" });
  doc.setFont("helvetica", "normal");
  doc.setFontSize(6.8);
  doc.setTextColor(...(accent ? PDF_WHITE : PDF_SLATE));
  const lineStart = y + (titleLines.length > 1 ? 15 : 12);
  doc.text(lines.slice(0, 3), x + width / 2, lineStart, {
    align: "center",
    lineHeightFactor: 1.25,
  });
}

function drawArchitectureDiagram(doc: jsPDF, y: number): number {
  const pageWidth = doc.internal.pageSize.width;
  const columns = ARCHITECTURE_OVERVIEW.flowColumns;
  const gap = 7;
  const width = (pageWidth - MARGIN * 2 - gap * (columns.length - 1)) / columns.length;
  columns.forEach((column, index) => {
    const x = MARGIN + index * (width + gap);
    drawLayerBox(doc, x, y, width, column.title, column.lines, index === 0);
    if (index < columns.length - 1) {
      const labels = ["REST", "SQL", "read / write", "governed reads"];
      drawArrow(doc, x + width + 0.7, x + width + gap - 0.7, y + 15.5, labels[index]);
    }
  });
  return y + 36;
}

function drawPathLane(
  doc: jsPDF,
  y: number,
  label: string,
  text: string,
  color: [number, number, number],
): number {
  const width = doc.internal.pageSize.width - MARGIN * 2;
  doc.setFillColor(...DB_ALT_ROW);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.roundedRect(MARGIN, y, width, 12, 1.5, 1.5, "FD");
  doc.setFillColor(...color);
  doc.roundedRect(MARGIN, y, 45, 12, 1.5, 1.5, "F");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(7.2);
  doc.setTextColor(...PDF_WHITE);
  doc.text(label, MARGIN + 22.5, y + 7.3, { align: "center" });
  doc.setFont("helvetica", "normal");
  doc.setFontSize(7.2);
  doc.setTextColor(...PDF_BODY);
  const lines = (doc.splitTextToSize(text, width - 50) as string[]).slice(0, 2);
  doc.text(lines, MARGIN + 49, y + 5.3, { lineHeightFactor: 1.2 });
  return y + 15;
}

function drawComponentCards(doc: jsPDF, assets: PdfBrandAssets, yStart: number): number {
  const overview = ARCHITECTURE_OVERVIEW;
  const gap = 5;
  const columns = 3;
  const width = (doc.internal.pageSize.width - MARGIN * 2 - gap * (columns - 1)) / columns;
  const rows = Math.ceil(overview.components.length / columns);
  let y = ensureRoom(doc, assets, yStart, rows * 25);
  overview.components.forEach((component, index) => {
    const col = index % columns;
    const row = Math.floor(index / columns);
    const x = MARGIN + col * (width + gap);
    const cardY = y + row * 25;
    doc.setFillColor(...DB_ALT_ROW);
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.roundedRect(x, cardY, width, 20, 2, 2, "FD");
    doc.setFont("helvetica", "bold");
    doc.setFontSize(7.8);
    doc.setTextColor(...DB_HEADER);
    doc.text(component.name, x + 3, cardY + 5.2);
    setBodyStyle(doc, 6.8);
    const lines = (doc.splitTextToSize(component.role, width - 6) as string[]).slice(0, 3);
    doc.text(lines, x + 3, cardY + 9.7, { lineHeightFactor: 1.15 });
  });
  return y + rows * 25;
}

function drawSourceGroup(
  doc: jsPDF,
  assets: PdfBrandAssets,
  group: ArchitectureSourceGroup,
  yStart: number,
): number {
  const rows = Math.ceil(group.tables.length / 2);
  const estimated = 9 + rows * 5 + (group.note ? 9 : 0);
  let y = ensureRoom(doc, assets, yStart, estimated);
  doc.setFont("helvetica", "bold");
  doc.setFontSize(10);
  doc.setTextColor(...DB_HEADER);
  doc.text(group.label, MARGIN, y);
  y += 5;

  const columnWidth = (doc.internal.pageSize.width - MARGIN * 2 - 5) / 2;
  group.tables.forEach((table, index) => {
    const col = index % 2;
    const row = Math.floor(index / 2);
    const x = MARGIN + col * (columnWidth + 5);
    doc.setFillColor(...DB_ALT_ROW);
    doc.roundedRect(x, y + row * 5 - 3.3, columnWidth, 4.2, 0.8, 0.8, "F");
    doc.setFont("courier", "normal");
    doc.setFontSize(7.2);
    doc.setTextColor(...PDF_BODY);
    doc.text(table, x + 2, y + row * 5);
  });
  y += rows * 5 + 1;
  if (group.note) {
    doc.setFont("helvetica", "italic");
    doc.setFontSize(7.5);
    doc.setTextColor(...PDF_SLATE);
    const lines = doc.splitTextToSize(group.note, doc.internal.pageSize.width - MARGIN * 2) as string[];
    doc.text(lines, MARGIN, y);
    y += lines.length * 3.5 + 2;
  }
  return y + 3;
}

function drawRefreshCard(
  doc: jsPDF,
  assets: PdfBrandAssets,
  path: ArchitectureRefreshPath,
  yStart: number,
): number {
  const width = doc.internal.pageSize.width - MARGIN * 2;
  const body = [path.trigger, ...path.steps];
  const lineCounts = body.map((line) => (doc.splitTextToSize(line, width - 13) as string[]).length);
  const height = 15 + lineCounts.reduce((sum, count) => sum + count * 3.5 + 1.2, 0);
  const y = ensureRoom(doc, assets, yStart, height);
  doc.setFillColor(...DB_ALT_ROW);
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.roundedRect(MARGIN, y, width, height - 3, 2, 2, "FD");
  doc.setFillColor(...DB_ORANGE);
  doc.rect(MARGIN, y, 2, height - 3, "F");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(9.2);
  doc.setTextColor(...DB_HEADER);
  doc.text(path.label, MARGIN + 6, y + 6);
  let cursor = y + 11;
  doc.setFont("helvetica", "italic");
  doc.setFontSize(7.3);
  doc.setTextColor(...PDF_SLATE);
  const triggerLines = doc.splitTextToSize(path.trigger, width - 13) as string[];
  doc.text(triggerLines, MARGIN + 6, cursor);
  cursor += triggerLines.length * 3.5 + 1.5;
  path.steps.forEach((step, index) => {
    const lines = doc.splitTextToSize(`${index + 1}. ${step}`, width - 13) as string[];
    setBodyStyle(doc, 7.4);
    doc.text(lines, MARGIN + 6, cursor);
    cursor += lines.length * 3.5 + 1.2;
  });
  return y + height;
}

function lineageRows(item: ArchitectureTabLineage): Array<[string, string[]]> {
  const rows: Array<[string, string[]]> = [
    ["UI / components", item.uiComponents],
    ["FastAPI routes", item.apiRoutes],
    ["Managed Delta / cache", item.managedData],
    ["Source system tables", item.sourceTables],
  ];
  if (item.fallbacks?.length) rows.push(["Fallback behavior", item.fallbacks]);
  if (item.optionalSources?.length) rows.push(["Optional sources / enrichment", item.optionalSources]);
  return rows;
}

function drawLineageCard(
  doc: jsPDF,
  assets: PdfBrandAssets,
  item: ArchitectureTabLineage,
  yStart: number,
): number {
  const width = doc.internal.pageSize.width - MARGIN * 2;
  const labelWidth = 42;
  const valueWidth = width - labelWidth - 8;
  const rows = lineageRows(item).map(([label, values]) => {
    const lines = doc.splitTextToSize(values.join("  |  "), valueWidth) as string[];
    return { label, lines, height: Math.max(8, lines.length * 3.6 + 3) };
  });
  const height = 12 + rows.reduce((sum, row) => sum + row.height, 0);
  const y = ensureRoom(doc, assets, yStart, height);
  doc.setFillColor(...DB_HEADER);
  doc.roundedRect(MARGIN, y, width, 10, 2, 2, "F");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(10);
  doc.setTextColor(...PDF_WHITE);
  doc.text(item.tab, MARGIN + 4, y + 6.6);

  let cursor = y + 12;
  rows.forEach((row, index) => {
    doc.setFillColor(...(index % 2 === 0 ? DB_ALT_ROW : PDF_WHITE));
    doc.rect(MARGIN, cursor - 2, width, row.height, "F");
    doc.setFont("helvetica", "bold");
    doc.setFontSize(7.2);
    doc.setTextColor(...DB_HEADER);
    doc.text(row.label, MARGIN + 3, cursor + 2);
    setBodyStyle(doc, 7.2);
    doc.text(row.lines, MARGIN + labelWidth, cursor + 2, { lineHeightFactor: 1.2 });
    cursor += row.height;
  });
  doc.setDrawColor(...PDF_HAIRLINE);
  doc.roundedRect(MARGIN, y, width, height - 2, 2, 2, "S");
  return y + height + 3;
}

export async function generateArchitectureReport(): Promise<void> {
  const assets = await loadPdfBrandAssets();
  const doc = new jsPDF({ orientation: "landscape" });
  const overview = ARCHITECTURE_OVERVIEW;
  const contentWidth = doc.internal.pageSize.width - MARGIN * 2;
  drawPdfMasthead(doc, assets);

  let y = 34;
  doc.setFont("helvetica", "bold");
  doc.setFontSize(22);
  doc.setTextColor(...DB_HEADER);
  doc.text(overview.title, MARGIN, y);
  y += 7;
  doc.setFont("helvetica", "normal");
  doc.setFontSize(8.5);
  doc.setTextColor(...PDF_SLATE);
  doc.text(`Generated ${format(new Date(), "MMMM d, yyyy")}`, MARGIN, y);
  y += 8;
  y = drawWrappedText(doc, overview.summary, MARGIN, y, contentWidth, 9.5) + 2;

  y = drawSectionHeading(doc, "Architecture at a glance", y);
  y = drawArchitectureDiagram(doc, y);
  y = drawPathLane(
    doc,
    y,
    "AUTHENTICATION + GOVERNANCE",
    "Databricks Apps session -> FastAPI role checks -> app service principal / setup user -> Warehouse CAN USE + Unity Catalog grants.",
    DB_HEADER,
  );
  y = drawPathLane(
    doc,
    y,
    "SCHEDULED REFRESH",
    "Scheduler -> service principal -> incremental Delta MERGEs -> refresh state and history -> cache invalidation.",
    DB_ORANGE,
  );
  y = drawPathLane(
    doc,
    y,
    "ON-DEMAND REFRESH",
    "Tab refresh -> scoped cache clear -> active API refetch. Administrator rebuild -> full aggregate recreation.",
    PDF_SLATE,
  );

  y = addBrandedPage(doc, assets);
  y = drawSectionHeading(doc, "Component overview", y);
  y = drawComponentCards(doc, assets, y);
  y = drawSectionHeading(doc, "Request and data flow", y + 2);
  y = drawBulletList(doc, assets, overview.dataFlow, y);

  y = ensureRoom(doc, assets, y + 3, 35);
  y = drawSectionHeading(doc, "Authentication and governance", y);
  y = drawBulletList(doc, assets, overview.securityGovernance, y);

  y = addBrandedPage(doc, assets);
  y = drawSectionHeading(doc, "Refresh paths", y);
  for (const path of overview.refreshPaths) {
    y = drawRefreshCard(doc, assets, path, y);
  }

  y = addBrandedPage(doc, assets);
  y = drawSectionHeading(doc, "Tab-by-tab data lineage", y);
  y = drawWrappedText(
    doc,
    "Each mapping traces the visible React view through its FastAPI routes and managed data layer to the exact governed source tables. Optional metadata and live fallback behavior are called out explicitly.",
    MARGIN,
    y,
    contentWidth,
    8.5,
  ) + 2;
  for (const item of overview.tabLineage) {
    y = drawLineageCard(doc, assets, item, y);
  }

  y = ensureRoom(doc, assets, y + 3, 28);
  y = drawSectionHeading(doc, "Source-table inventory", y);
  for (const group of overview.sourceTables) {
    y = drawSourceGroup(doc, assets, group, y);
  }

  addPdfFooters(doc, assets);
  doc.save(`cost-observability-architecture-${format(new Date(), "yyyy-MM-dd")}.pdf`);
}
