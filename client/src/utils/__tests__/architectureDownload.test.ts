import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ARCHITECTURE_PDF_FILENAME,
  ARCHITECTURE_PDF_PATH,
  downloadArchitecturePdf,
} from "../architectureDownload";

const EXPECTED_SHA256 =
  "aade7cb46480fe0d30586557a06495c8e800c72c40f60f0331c1fa5990506812";
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../");
const sourcePdf = resolve(repoRoot, "client/public/reports/cost-obs-arch-1.2.pdf");
const builtPdf = resolve(repoRoot, "static/reports/cost-obs-arch-1.2.pdf");

beforeEach(() => {
  Object.defineProperty(URL, "createObjectURL", {
    configurable: true,
    value: () => "blob:test",
  });
  Object.defineProperty(URL, "revokeObjectURL", {
    configurable: true,
    value: () => {},
  });
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("canonical architecture PDF", () => {
  it("uses the stable same-origin path and customer filename", () => {
    expect(ARCHITECTURE_PDF_PATH).toBe("/reports/cost-obs-arch-1.2.pdf");
    expect(ARCHITECTURE_PDF_FILENAME).toBe("cost-obs-arch-1.2.pdf");
  });

  it.each([sourcePdf, builtPdf])(
    "ships exact PDF bytes at %s",
    (path) => {
      const bytes = readFileSync(path);
      expect(bytes.subarray(0, 5).toString("ascii")).toBe("%PDF-");
      expect(createHash("sha256").update(bytes).digest("hex")).toBe(EXPECTED_SHA256);
    },
  );

  it("downloads the fetched bytes and always revokes the object URL", async () => {
    const blob = new Blob(["exact-pdf-bytes"], { type: "application/pdf" });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(blob, { status: 200 }),
    );
    const createObjectUrl = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:architecture");
    const revokeObjectUrl = vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});

    await downloadArchitecturePdf();

    expect(fetchMock).toHaveBeenCalledWith("/reports/cost-obs-arch-1.2.pdf");
    expect(createObjectUrl).toHaveBeenCalledOnce();
    expect(click).toHaveBeenCalledOnce();
    expect(revokeObjectUrl).toHaveBeenCalledWith("blob:architecture");
    expect(document.querySelector('a[download="cost-obs-arch-1.2.pdf"]')).toBeNull();
  });

  it("rejects failed fetches without creating an object URL", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 503 }));
    const createObjectUrl = vi.spyOn(URL, "createObjectURL");

    await expect(downloadArchitecturePdf()).rejects.toThrow(
      "Architecture PDF download failed with status 503",
    );
    expect(createObjectUrl).not.toHaveBeenCalled();
  });
});
