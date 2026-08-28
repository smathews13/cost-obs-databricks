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
    <div className="relative">
      <div className="absolute right-0 top-0 z-20">
        <TabRefreshButton onRefresh={onRefresh} isRefreshing={isRefreshing} />
      </div>
      <div className={isLoading ? "pt-[54px]" : ""}>
        {isLoading ? <LoadingPanels sections={loadingSections} /> : children}
      </div>
    </div>
  );
}
