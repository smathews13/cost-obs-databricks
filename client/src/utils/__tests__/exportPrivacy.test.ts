import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExportSections } from "@/utils/exportDemand";
import { generateCostCSV } from "../csvExport";
import {
  anonymizeExportIdentity,
  anonymizeExportPayload,
} from "../identity";
import { costReportContextLines } from "../pdfExport";

const sections: ExportSections = {
  summary: false,
  products: false,
  workspaces: false,
  skus: false,
  pipelines: false,
  interactive: false,
  query360: true,
  aiml: false,
  apps: true,
  tagging: false,
  users: true,
  platformKPIs: false,
  anomalies: false,
  awsCosts: false,
  optimize: false,
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("cost export identity privacy", () => {
  it("uses one deterministic anonymizer for every user-bearing field and preserves service principals", () => {
    const sp = "12345678-1234-1234-1234-123456789abc";
    const raw = {
      users: [{ user_email: "alice@example.com" }],
      query: {
        executed_by: "alice@example.com",
        owner: "bob@example.com",
      },
      app: {
        creator: "alice@example.com",
        updater: "carol@example.com",
        source_code_path: "/Workspace/Users/alice@example.com/app",
      },
      service_principal: { run_as: sp },
    };

    const safe = anonymizeExportPayload(raw, true);
    const serialized = JSON.stringify(safe);
    expect(serialized).not.toMatch(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    expect(safe.users[0].user_email).toBe(safe.query.executed_by);
    expect(safe.app.creator).toBe(safe.query.executed_by);
    expect(safe.service_principal.run_as).toBe(sp);
    expect(anonymizeExportIdentity("alice@example.com", true)).toBe(safe.query.executed_by);
  });

  it("does not write raw user emails into an anonymized spreadsheet", async () => {
    let workbook: Blob | undefined;
    Object.defineProperty(URL, "createObjectURL", { configurable: true, value: vi.fn((blob: Blob) => {
      workbook = blob as Blob;
      return "blob:test";
    }) });
    Object.defineProperty(URL, "revokeObjectURL", { configurable: true, value: vi.fn() });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});

    generateCostCSV({
      query360: {
        cost_by_user: {
          items: [{ user_email: "alice@example.com", total_spend: 12, query_count: 3 }],
        },
      },
      apps: { apps: { apps: [] } },
      users: {
        top_users: [{
          user_email: "bob@example.com",
          total_spend: 4,
          total_dbus: 2,
        }],
      },
    }, sections, { start: "2026-08-01", end: "2026-08-28" }, { ids: [] }, {
      anonymizeUsers: true,
      companyName: "Acme",
      sourceLabels: ["west"],
      cloudProvider: "azure",
    });

    expect(workbook).toBeDefined();
    const xml = await new Promise<string>((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(reader.error);
      reader.onload = () => resolve(String(reader.result ?? ""));
      reader.readAsText(workbook!);
    });
    expect(xml).not.toContain("alice@example.com");
    expect(xml).not.toContain("bob@example.com");
    expect(xml).toContain("Cloud Provider");
    expect(xml).toContain("AZURE");
    expect(xml).toContain("User ");
  });

  it("renders configured company, source scope, and the actual cloud in PDF context", () => {
    const lines = costReportContextLines({
      anonymizeUsers: false,
      companyName: "Acme Corp",
      sourceLabels: ["local", "shared-west"],
      cloudProvider: "gcp",
    });
    expect(lines).toEqual([
      "Company: Acme Corp",
      "Data source scope: local, shared-west",
      "Cloud provider: GCP",
    ]);
    expect(lines.join(" ")).not.toContain("AWS");
  });
});
