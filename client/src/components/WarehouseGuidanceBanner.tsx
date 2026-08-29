import { useState } from "react";
import { ExternalLink, RefreshCw, X } from "lucide-react";
import {
  WAREHOUSE_GUIDANCE_VERSION,
  classifyWarehouseSize,
  warehouseManagementHref,
  warehouseWarningDismissalKey,
  type WarehouseHealth,
} from "@/utils/warehouseGuidance";

interface WarehouseGuidanceBannerProps {
  warehouse: WarehouseHealth;
  workspaceHost: string | null | undefined;
  warningVersion?: string;
}

interface WarehouseHealthCheckBannerProps {
  isRetrying: boolean;
  onRetry: () => void;
}

export function WarehouseHealthCheckBanner({
  isRetrying,
  onRetry,
}: WarehouseHealthCheckBannerProps) {
  return (
    <div
      role="alert"
      className="flex items-center justify-between gap-4 border-b border-amber-200 bg-amber-50 px-4 py-2 text-amber-900"
    >
      <span className="text-xs font-medium">
        Warehouse status could not be verified. The app will keep loading data normally.
      </span>
      <button
        type="button"
        onClick={onRetry}
        disabled={isRetrying}
        className="inline-flex shrink-0 items-center gap-1.5 rounded border border-amber-300 px-2 py-1 text-xs font-semibold hover:bg-amber-100 disabled:cursor-wait disabled:opacity-60"
      >
        <RefreshCw size={12} className={isRetrying ? "animate-spin" : ""} aria-hidden="true" />
        {isRetrying ? "Checking…" : "Retry status check"}
      </button>
    </div>
  );
}

export function WarehouseGuidanceBanner({
  warehouse,
  workspaceHost,
  warningVersion = WAREHOUSE_GUIDANCE_VERSION,
}: WarehouseGuidanceBannerProps) {
  const warehouseId = warehouse.warehouse_id?.trim() ?? "";
  const dismissalKey = warehouseId
    ? warehouseWarningDismissalKey(warehouseId, warningVersion)
    : "";
  const [dismissed, setDismissed] = useState(
    () => Boolean(dismissalKey && localStorage.getItem(dismissalKey) === "1"),
  );

  if (
    dismissed
    || !warehouseId
    || classifyWarehouseSize(warehouse.warehouse_size) !== "below-medium"
  ) {
    return null;
  }

  const manageHref = warehouseManagementHref(workspaceHost, warehouseId);
  const typeLabel = warehouse.warehouse_type?.trim().toLowerCase();

  const dismiss = () => {
    localStorage.setItem(dismissalKey, "1");
    setDismissed(true);
  };

  return (
    <div
      role="alert"
      className="flex items-center justify-between gap-4 border-b border-amber-200 bg-amber-50 px-4 py-2 text-amber-900"
    >
      <div className="flex min-w-0 items-center gap-2">
        <svg className="h-4 w-4 shrink-0 text-amber-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
        </svg>
        <span className="text-xs font-medium">
          The recommended warehouse size is Medium, otherwise query latency may be volatile.
        </span>
        <span className="hidden shrink-0 rounded bg-amber-100 px-1.5 py-0.5 font-mono text-[10px] text-amber-800 min-[1100px]:inline">
          {warehouse.warehouse_size}{typeLabel ? ` · ${typeLabel}` : ""}
        </span>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        {manageHref && (
          <a
            href={manageHref}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 rounded px-2 py-1 text-xs font-semibold text-amber-900 underline decoration-amber-500 underline-offset-2 hover:bg-amber-100"
          >
            Manage warehouse
            <ExternalLink size={12} aria-hidden="true" />
          </a>
        )}
        <button
          type="button"
          onClick={dismiss}
          aria-label="Dismiss warehouse size recommendation"
          className="rounded p-1 text-amber-700 hover:bg-amber-100 hover:text-amber-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-600"
        >
          <X size={14} aria-hidden="true" />
        </button>
      </div>
    </div>
  );
}
