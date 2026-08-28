import { useState, useEffect, useCallback, useMemo } from "react";
import { createPortal } from "react-dom";
import type { TabVisibility } from "@/components/SettingsDialog";
import { C } from "@/theme";
import { CostObsMark } from "@/components/brand";
import { Spinner } from "@/components/Spinner";

export interface ExportSections {
  summary: boolean;
  products: boolean;
  workspaces: boolean;
  skus: boolean;
  pipelines: boolean;
  interactive: boolean;
  query360: boolean;
  aiml: boolean;
  apps: boolean;
  tagging: boolean;
  users: boolean;
  platformKPIs: boolean;
  anomalies: boolean;
  awsCosts: boolean;
  optimize: boolean;
}

export type ExportFormat = "pdf" | "csv";

interface ExportDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onExport: (sections: ExportSections, format: ExportFormat) => void;
  tabVisibility: TabVisibility;
  dataLoading?: boolean;
}

// Map export sections to the tab that owns them: order matches tab nav so the
// PDF reads in the same order as the app.
const sectionToTab: Record<keyof ExportSections, keyof TabVisibility | null> = {
  summary: "dbu",
  products: "dbu",
  workspaces: "dbu",
  skus: "dbu",
  pipelines: "dbu",
  interactive: "dbu",
  query360: "sql",
  aiml: "aiml",
  apps: "apps",
  tagging: "tagging",
  users: "users-groups",
  platformKPIs: "kpis",
  anomalies: "kpis",
  awsCosts: "infra",
  optimize: "optimizer",
};

const sectionLabels: Record<keyof ExportSections, { label: string; description: string }> = {
  summary: { label: "Executive Summary", description: "Total DBUs, spend, and key metrics" },
  products: { label: "Product Breakdown", description: "Spend by product category" },
  workspaces: { label: "Workspace Breakdown", description: "Top workspaces by spend" },
  skus: { label: "SKU Breakdown", description: "Spend by SKU/billing type" },
  pipelines: { label: "Jobs & Pipelines", description: "Top jobs and SDP pipelines" },
  interactive: { label: "Interactive Compute", description: "Notebook and cluster usage" },
  query360: { label: "Query", description: "SQL warehouse efficiency and query costs" },
  aiml: { label: "AI/ML", description: "Endpoints, models, ML clusters, and Agent Bricks" },
  apps: { label: "Apps", description: "Databricks Apps costs and connected artifacts" },
  tagging: { label: "Tagging", description: "Tag KPIs, coverage, top tags, untagged resources" },
  users: { label: "Users", description: "Top users by spend and product breakdown" },
  platformKPIs: { label: "Platform KPIs & Trends", description: "Platform-wide metrics and trends" },
  anomalies: { label: "Spend Anomalies", description: "Day-over-day spend changes" },
  awsCosts: { label: "Cloud Costs", description: "Estimated cloud infrastructure costs" },
  optimize: { label: "Optimize", description: "Warehouse rightsizing and idle-time opportunities" },
};

export function ExportDialog({ isOpen, onClose, onExport, tabVisibility, dataLoading = false }: ExportDialogProps) {
  const visibleSections = useMemo(() => {
    const result: ExportSections = {} as ExportSections;
    for (const key of Object.keys(sectionToTab) as Array<keyof ExportSections>) {
      const tab = sectionToTab[key];
      result[key] = tab === null || tabVisibility[tab];
    }
    return result;
  }, [tabVisibility]);

  const [sections, setSections] = useState<ExportSections>(visibleSections);
  const [format, setFormat] = useState<ExportFormat>("pdf");

  // Escape key handler
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === "Escape") onClose();
  }, [onClose]);

  // Reset sections when dialog opens to reflect current tab visibility
  useEffect(() => {
    if (isOpen) {
      setSections(visibleSections);
    }
  }, [isOpen, visibleSections]);

  useEffect(() => {
    if (isOpen) {
      document.addEventListener("keydown", handleKeyDown);
      document.body.style.overflow = "hidden";
    }
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = "";
    };
  }, [isOpen, handleKeyDown]);

  if (!isOpen) return null;

  const toggleSection = (key: keyof ExportSections) => {
    setSections((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const selectAll = () => {
    setSections({ ...visibleSections });
  };

  const selectNone = () => {
    const none: ExportSections = {} as ExportSections;
    for (const key of Object.keys(sectionToTab) as Array<keyof ExportSections>) none[key] = false;
    setSections(none);
  };

  const visibleKeys = (Object.keys(sectionToTab) as Array<keyof ExportSections>).filter((k) => visibleSections[k]);
  const selectedCount = visibleKeys.filter((k) => sections[k]).length;

  const handleExport = () => {
    if (dataLoading) return;
    onExport(sections, format);
    onClose();
  };

  return createPortal(
    <div
      className="animate-backdrop fixed inset-0 z-50 overflow-y-auto bg-black/40"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className="flex min-h-full items-center justify-center p-4 sm:p-6" onClick={(e) => e.target === e.currentTarget && onClose()}>
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="export-dialog-title"
          aria-describedby="export-dialog-description"
          className="animate-dialog relative flex h-[92vh] max-h-208 w-full max-w-4xl flex-col overflow-hidden"
          style={{
            background: C.card,
            border: "none",
            borderRadius: 8,
            boxShadow: "0 20px 60px rgba(0,0,0,0.25)",
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="px-6 py-5" style={{ background: C.navy, borderBottom: `1px solid ${C.hairline}` }}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 shrink-0 items-center justify-center">
                  <CostObsMark className="h-9 w-9" whiteOrbit />
                </div>
                <div>
                  <h3 id="export-dialog-title" className="text-lg font-bold leading-tight" style={{ color: C.white }}>
                    Export report
                  </h3>
                  <p id="export-dialog-description" className="mt-0.5 text-sm" style={{ color: C.muted }}>
                    Choose a format and the sections to include.
                  </p>
                </div>
              </div>
              <button
                type="button"
                onClick={onClose}
                aria-label="Close export dialog"
                className="flex h-9 w-9 items-center justify-center transition-colors focus-visible:outline-none focus-visible:shadow-(--focus)"
                style={{ color: C.white, borderRadius: 8 }}
                onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = "rgba(255,255,255,0.12)"; }}
                onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = "transparent"; }}
              >
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>

          {/* Format picker */}
          <div className="px-6 pb-4 pt-5" style={{ background: C.oatPage, borderBottom: `1px solid ${C.hairline}` }}>
            <div className="mb-2 text-xs font-bold uppercase tracking-[0.12em]" style={{ color: C.slate }}>
              File format
            </div>
            <div
              className="inline-flex overflow-hidden"
              role="group"
              aria-label="Export format"
              style={{ border: `1px solid ${C.hairline}`, borderRadius: 8, background: C.oatMed }}
            >
              {(["pdf", "csv"] as ExportFormat[]).map((f) => (
                <button
                  type="button"
                  key={f}
                  onClick={() => setFormat(f)}
                  aria-pressed={format === f}
                  className="min-w-20 px-4 py-2 text-sm font-semibold transition-colors focus-visible:z-10 focus-visible:outline-none focus-visible:shadow-(--focus)"
                  style={{
                    background: format === f ? C.card : "transparent",
                    color: format === f ? C.navy : C.slate,
                    borderLeft: f === "csv" ? `1px solid ${C.hairline}` : undefined,
                    boxShadow: format === f ? `inset 0 -2px 0 ${C.lava}` : "none",
                  }}
                >
                  {f.toUpperCase()}
                </button>
              ))}
            </div>
            {format === "csv" && (
              <p className="mt-2 text-xs leading-5" style={{ color: C.slate }}>
                Downloads a multi-sheet Excel workbook (.xls), with one sheet per app tab and the active workspace and date filters applied.
              </p>
            )}
          </div>

          {/* Content */}
          <div className="min-h-0 flex-1 overflow-y-auto px-6 py-4" style={{ background: C.card }}>
            {/* Quick actions */}
            <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="text-sm font-semibold" style={{ color: C.ink }}>Report sections</div>
                <div className="mt-0.5 text-xs" style={{ color: C.slate }}>
                  {selectedCount} of {visibleKeys.length} selected
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={selectAll}
                  className="px-2 py-1 text-sm font-semibold transition-colors focus-visible:outline-none focus-visible:shadow-(--focus)"
                  style={{ color: C.lava, borderRadius: 4 }}
                >
                  Select all
                </button>
                <button
                  type="button"
                  onClick={selectNone}
                  className="px-2 py-1 text-sm font-semibold transition-colors focus-visible:outline-none focus-visible:shadow-(--focus)"
                  style={{ color: C.lava, borderRadius: 4 }}
                >
                  Select none
                </button>
              </div>
            </div>

            {/* Sections */}
            <div className="grid gap-1.5 sm:grid-cols-2">
              {visibleKeys.map((key) => {
                const { label, description } = sectionLabels[key];
                const checked = sections[key];

                return (
                  <label
                    key={key}
                    className="flex cursor-pointer items-start gap-2.5 px-3 py-2.5 transition-colors"
                    style={{
                      background: checked ? C.coralTint : C.card,
                      border: `1px solid ${checked ? C.coralBrd : C.hairline}`,
                      borderLeft: `3px solid ${checked ? C.lava : C.hairline}`,
                      borderRadius: 8,
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSection(key)}
                      className="mt-0.5 h-4 w-4 shrink-0 rounded-[3px] focus-visible:outline-none focus-visible:shadow-(--focus)"
                      style={{ accentColor: C.lava }}
                    />
                    <div className="flex-1">
                      <div className="text-sm font-semibold leading-4" style={{ color: C.ink }}>{label}</div>
                      <div className="mt-0.5 truncate text-xs leading-4" style={{ color: C.slate }} title={description}>{description}</div>
                    </div>
                  </label>
                );
              })}
            </div>
          </div>

          {/* Footer */}
          <div
            className="flex flex-wrap items-center justify-between gap-3 px-6 py-4"
            style={{ background: C.oatPage, borderTop: `1px solid ${C.hairline}` }}
          >
            <span className="text-xs" style={{ color: C.slate }}>
              Active workspace and date filters are applied.
            </span>
            <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-sm font-semibold transition-colors focus-visible:outline-none focus-visible:shadow-(--focus)"
              style={{ color: C.ink, background: C.card, border: `1px solid ${C.hairline}`, borderRadius: 8 }}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleExport}
              disabled={selectedCount === 0 || dataLoading}
              aria-label={dataLoading ? "Preparing report data" : undefined}
              className="btn-brand inline-flex items-center gap-2 px-4 py-2 text-sm focus-visible:outline-none"
            >
              {dataLoading ? <Spinner size="xs" /> : (
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.293.707l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              )}
              {dataLoading
                ? "Preparing report data…"
                : `Export ${selectedCount} section${selectedCount !== 1 ? "s" : ""} as ${format.toUpperCase()}`}
            </button>
            </div>
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
}
