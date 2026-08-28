import type { ReactNode } from "react";
import { LoadingPanels } from "./Spinner";
import { TabRefreshButton } from "./TabRefreshButton";

interface TabRefreshRegionProps {
  isLoading: boolean;
  isRefreshing: boolean;
  loadingSections: string[];
  onRefresh: () => Promise<void>;
  children: ReactNode;
}

export function TabRefreshRegion({
  isLoading,
  isRefreshing,
  loadingSections,
  onRefresh,
  children,
}: TabRefreshRegionProps) {
  return (
    <>
      <div className="mb-4 flex h-[38px] items-center justify-end">
        <TabRefreshButton onRefresh={onRefresh} isRefreshing={isRefreshing} />
      </div>
      {isLoading ? <LoadingPanels sections={loadingSections} /> : children}
    </>
  );
}
