import type { ReactNode } from "react";
import { LoadingPanels } from "./Spinner";
import { TabRefreshButton } from "./TabRefreshButton";

interface TabRefreshRegionProps {
  isLoading: boolean;
  isRefreshing: boolean;
  loadingSections: string[];
  onRefresh: () => Promise<void>;
  refreshError?: string | null;
  children: ReactNode;
}

export function TabRefreshRegion({
  isLoading,
  isRefreshing,
  loadingSections,
  onRefresh,
  refreshError,
  children,
}: TabRefreshRegionProps) {
  return (
    <div className="relative">
      <div className="absolute right-0 top-0 z-20">
        <TabRefreshButton onRefresh={onRefresh} isRefreshing={isRefreshing} />
      </div>
      {refreshError && (
        <div role="alert" className="mb-4 flex items-center justify-between gap-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
          <span>{refreshError}</span>
          <button type="button" onClick={() => void onRefresh()} disabled={isRefreshing} className="shrink-0 font-semibold underline disabled:opacity-50">
            Retry
          </button>
        </div>
      )}
      <div className={isLoading ? "pt-[54px]" : ""}>
        {isLoading ? <LoadingPanels sections={loadingSections} /> : children}
      </div>
    </div>
  );
}
