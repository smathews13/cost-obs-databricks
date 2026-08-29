import { jsPDF } from "jspdf";

export const DB_HEADER: [number, number, number] = [27, 49, 57];
export const DB_ORANGE: [number, number, number] = [255, 54, 33];
export const DB_ALT_ROW: [number, number, number] = [249, 247, 244];
export const PDF_HAIRLINE: [number, number, number] = [228, 226, 221];
export const PDF_BODY: [number, number, number] = [58, 56, 56];
export const PDF_SLATE: [number, number, number] = [97, 135, 148];
export const PDF_WHITE: [number, number, number] = [255, 255, 255];

export interface PdfBrandAssets {
  costObsLockup: string;
  databricksMark: string;
}

export const PDF_BRAND_ASSET_PATHS = {
  costObsLockup: "/brand/costobs-lockup-white.svg",
  databricksMark: "/databricks.svg",
} as const;

/**
 * Rasterize a checked-in SVG without changing its geometry. jsPDF does not
 * natively embed SVG, so the browser converts the official asset to PNG.
 */
async function svgAssetToPng(path: string, width: number, height: number): Promise<string> {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`Unable to load PDF brand asset: ${path}`);
  const svg = await response.text();
  const blobUrl = URL.createObjectURL(new Blob([svg], { type: "image/svg+xml" }));

  try {
    const image = new Image();
    image.decoding = "async";
    await new Promise<void>((resolve, reject) => {
      image.onload = () => resolve();
      image.onerror = () => reject(new Error(`Unable to render PDF brand asset: ${path}`));
      image.src = blobUrl;
    });

    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    const context = canvas.getContext("2d");
    if (!context) throw new Error("Unable to initialize PDF brand rendering");
    context.drawImage(image, 0, 0, width, height);
    return canvas.toDataURL("image/png");
  } finally {
    URL.revokeObjectURL(blobUrl);
  }
}

export async function loadPdfBrandAssets(): Promise<PdfBrandAssets> {
  const [costObsLockup, databricksMark] = await Promise.all([
    svgAssetToPng(PDF_BRAND_ASSET_PATHS.costObsLockup, 880, 192),
    svgAssetToPng(PDF_BRAND_ASSET_PATHS.databricksMark, 192, 192),
  ]);
  return { costObsLockup, databricksMark };
}

export function drawPdfMasthead(doc: jsPDF, assets: PdfBrandAssets): void {
  const pageWidth = doc.internal.pageSize.width;
  doc.setFillColor(...DB_HEADER);
  doc.rect(0, 0, pageWidth, 22, "F");
  doc.addImage(assets.costObsLockup, "PNG", 14, 5.7, 43, 9.4);
  doc.addImage(assets.databricksMark, "PNG", pageWidth - 22, 6, 10, 10);
  doc.setFillColor(...DB_ORANGE);
  doc.rect(0, 22, pageWidth, 1.2, "F");
}

export function addPdfFooters(doc: jsPDF, assets: PdfBrandAssets): void {
  const pageCount = doc.getNumberOfPages();
  for (let i = 1; i <= pageCount; i++) {
    doc.setPage(i);
    const pageWidth = doc.internal.pageSize.width;
    const pageHeight = doc.internal.pageSize.height;
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.setLineWidth(0.3);
    doc.line(14, pageHeight - 14, pageWidth - 14, pageHeight - 14);
    doc.setFillColor(...DB_ORANGE);
    doc.rect(14, pageHeight - 14.4, 12, 0.8, "F");
    doc.setDrawColor(...PDF_HAIRLINE);
    doc.setLineWidth(0.2);
    doc.setFontSize(8);
    doc.setTextColor(...PDF_SLATE);
    doc.text(`Page ${i} of ${pageCount}`, pageWidth / 2, pageHeight - 10, { align: "center" });
    doc.addImage(assets.costObsLockup, "PNG", pageWidth - 36, pageHeight - 13, 22, 4.8);
  }
}
