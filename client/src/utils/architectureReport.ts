import { format } from "date-fns";
import { jsPDF } from "jspdf";
import { ARCHITECTURE_OVERVIEW, type ArchitectureSourceGroup } from "./architectureOverview";
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
const CONTENT_BOTTOM = 276;

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
  return y + height > CONTENT_BOTTOM ? addBrandedPage(doc, assets) : y;
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

function drawArrow(doc: jsPDF, x: number, y1: number, y2: number): void {
  doc.setDrawColor(...PDF_SLATE);
  doc.setFillColor(...PDF_SLATE);
  doc.setLineWidth(0.6);
  doc.line(x, y1, x, y2 - 2);
  doc.triangle(x - 1.6, y2 - 4, x + 1.6, y2 - 4, x, y2, "F");
}

function drawLayerBox(
  doc: jsPDF,
  x: number,
  y: number,
  width: number,
  title: string,
  subtitle: string,
  accent = false,
): void {
  doc.setFillColor(...(accent ? DB_HEADER : DB_ALT_ROW));
  doc.setDrawColor(...(accent ? DB_HEADER : PDF_HAIRLINE));
  doc.roundedRect(x, y, width, 16, 2, 2, "FD");
  doc.setFont("helvetica", "bold");
  doc.setFontSize(9);
  doc.setTextColor(...(accent ? PDF_WHITE : DB_HEADER));
  doc.text(title, x + width / 2, y + 6, { align: "center" });
  doc.setFont("helvetica", "normal");
  doc.setFontSize(7.5);
  doc.setTextColor(...(accent ? PDF_WHITE : PDF_SLATE));
  doc.text(subtitle, x + width / 2, y + 11.5, { align: "center" });
}

function drawArchitectureDiagram(doc: jsPDF, y: number): number {
  const pageWidth = doc.internal.pageSize.width;
  const mainX = 35;
  const mainW = pageWidth - 70;
  drawLayerBox(doc, mainX, y, mainW, "React browser interface", "Authenticated Databricks Apps session", true);
  drawArrow(doc, pageWidth / 2, y + 16, y + 23);
  drawLayerBox(doc, mainX, y + 23, mainW, "FastAPI application", "API orchestration, validation, response shaping");
  drawArrow(doc, pageWidth / 2, y + 39, y + 46);
  drawLayerBox(doc, mainX, y + 46, mainW, "Databricks SQL Warehouse", "Governed SQL execution");
  drawArrow(doc, pageWidth / 2, y + 62, y + 69);

  const gap = 6;
  const sourceW = (pageWidth - MARGIN * 2 - gap) / 2;
  drawLayerBox(doc, MARGIN, y + 69, sourceW, "System + cloud sources", "Governed platform metadata and optional billing exports");
  drawLayerBox(doc, MARGIN + sourceW + gap, y + 69, sourceW, "App-managed Delta", "Aggregates, settings, refresh state, response cache");
  return y + 91;
}

function drawComponentCards(doc: jsPDF, assets: PdfBrandAssets, yStart: number): number {
  const overview = ARCHITECTURE_OVERVIEW;
  const pageWidth = doc.internal.pageSize.width;
  const gap = 6;
  const width = (pageWidth - MARGIN * 2 - gap) / 2;
  let y = ensureRoom(doc, assets, yStart, 91);
  for (let i = 0; i < overview.components.length; i++) {
    const col = i % 2;
    const row = Math.floor(i / 2);
    const x = MARGIN + col * (width + gap);
    const cardY = y + row * 28;
    doc.setFillColor(...DB_ALT_ROW);
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.roundedRect(x, cardY, width, 23, 2, 2, "FD");
    doc.setFont("helvetica", "bold");
    doc.setFontSize(8.5);
    doc.setTextColor(...DB_HEADER);
    doc.text(overview.components[i].name, x + 3, cardY + 5.5);
    setBodyStyle(doc, 7.5);
    const lines = (doc.splitTextToSize(overview.components[i].role, width - 6) as string[]).slice(0, 3);
    doc.text(lines, x + 3, cardY + 10.5);
  }
  return y + 84;
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

export async function generateArchitectureReport(): Promise<void> {
  const assets = await loadPdfBrandAssets();
  const doc = new jsPDF();
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
  y = drawSectionHeading(doc, "Component overview", y);
  y = drawComponentCards(doc, assets, y);

  y = ensureRoom(doc, assets, y + 5, 45);
  y = drawSectionHeading(doc, "Data flow and lineage", y);
  y = drawBulletList(doc, assets, overview.dataFlow, y);

  y = ensureRoom(doc, assets, y + 5, 45);
  y = drawSectionHeading(doc, "Security and governance", y);
  y = drawBulletList(doc, assets, overview.securityGovernance, y);

  y = ensureRoom(doc, assets, y + 5, 45);
  y = drawSectionHeading(doc, "Refresh and cache behavior", y);
  y = drawBulletList(doc, assets, overview.refreshCacheBehavior, y);

  y = ensureRoom(doc, assets, y + 5, 35);
  y = drawSectionHeading(doc, "Source-table inventory", y);
  for (const group of overview.sourceTables) {
    y = drawSourceGroup(doc, assets, group, y);
  }

  addPdfFooters(doc, assets);
  doc.save(`cost-observability-architecture-${format(new Date(), "yyyy-MM-dd")}.pdf`);
}
