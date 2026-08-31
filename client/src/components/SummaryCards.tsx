import { useState } from "react";
import type { BillingSummary } from "@/types/billing";
import { formatKpiCurrency, formatNumber } from "@/utils/formatters";
import { KPITrendModal } from "./KPITrendModal";
import { KPICard } from "./ui/KPICard";

interface SummaryCardsProps {
  data: BillingSummary | undefined;
  isLoading: boolean;
  startDate?: string;
  endDate?: string;
  workspaceIds?: string[];
}

interface CardProps {
  title: string;
  value: string;
  subtitle?: string;
  infoTooltip?: string;
  icon: React.ReactNode;
  isLoading: boolean;
  onClick?: () => void;
}

function isMutedValue(value: string) {
  const n = value.replace(/[$,]/g, "").trim();
  return n === "0" || n === "0.00" || n === "N/A" || n === "N/A" || n === "n/a";
}

function Card({ title, value, subtitle, infoTooltip, icon, isLoading, onClick }: CardProps) {
  const muted = !isLoading && isMutedValue(value);
  return (
    <KPICard
      title={title}
      value={value}
      subtitle={subtitle}
      infoText={infoTooltip}
      icon={icon}
      isLoading={isLoading}
      onActivate={onClick}
      ariaLabel={`See ${title} trend`}
      valueClassName={`whitespace-nowrap ${muted ? "text-muted-fg" : ""}`}
    />
  );
}

type KPIType = "total_spend" | "total_dbus" | "avg_daily_spend" | "workspace_count";

export function SummaryCards({ data, isLoading, startDate, endDate, workspaceIds }: SummaryCardsProps) {
  const [selectedKPI, setSelectedKPI] = useState<{
    kpi: KPIType;
    label: string;
  } | null>(null);

  const handleCardClick = (kpi: KPIType, label: string) => {
    if (startDate && endDate) {
      setSelectedKPI({ kpi, label });
    }
  };

  return (
    <>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card
          title="Total Spend"
          value={formatKpiCurrency(data?.total_spend ?? 0)}
          subtitle={data?.days_in_range != null ? `over ${data.days_in_range} days` : undefined}
          isLoading={isLoading}
          onClick={!isLoading && data && startDate && endDate ? () => handleCardClick("total_spend", "Total Spend") : undefined}
          icon={
            <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          }
        />
        <Card
          title="Total DBUs"
          value={formatNumber(data?.total_dbus ?? 0)}
          subtitle={data?.days_in_range != null ? `over ${data.days_in_range} days` : undefined}
          isLoading={isLoading}
          onClick={!isLoading && data && startDate && endDate ? () => handleCardClick("total_dbus", "Total DBUs") : undefined}
          icon={
            <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
            </svg>
          }
        />
        <Card
          title="Average Daily Spend"
          value={formatKpiCurrency(data?.avg_daily_spend ?? 0)}
          subtitle={data?.workspace_count != null ? `across ${data.workspace_count} workspaces` : "daily average"}
          isLoading={isLoading}
          onClick={!isLoading && data && startDate && endDate ? () => handleCardClick("avg_daily_spend", "Average Daily Spend") : undefined}
          icon={
            <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
            </svg>
          }
        />
        <Card
          title="Workspaces"
          value={formatNumber(data?.workspace_count ?? 0)}
          subtitle="active workspaces"
          infoTooltip="Average number of distinct workspaces with billable usage per day in the selected period. Matches the daily trend average."
          isLoading={isLoading}
          onClick={!isLoading && data && startDate && endDate ? () => handleCardClick("workspace_count", "Daily Active Workspaces") : undefined}
          icon={
            <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
            </svg>
          }
        />
      </div>

      {selectedKPI && startDate && endDate && (
        <KPITrendModal
          kpi={selectedKPI.kpi}
          kpiLabel={selectedKPI.label}
          isOpen={!!selectedKPI}
          onClose={() => setSelectedKPI(null)}
          startDate={startDate}
          endDate={endDate}
          workspaceIds={workspaceIds}
        />
      )}
    </>
  );
}
