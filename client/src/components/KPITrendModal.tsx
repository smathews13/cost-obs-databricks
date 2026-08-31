import { format, parseISO } from "date-fns";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { useKPITrend, usePlatformKPITrend, useAppsKPITrend } from "@/hooks/useKPITrend";
import { formatCurrency, formatNumber, formatBytesNoDecimal, formatRowCount, formatComputeSecondsCompact } from "@/utils/formatters";
import { C, FONT_MONO } from "@/theme";
import { changeTone } from "@/components/brandHelpers";
import { InkTooltip } from "@/components/chartTheme";
import { axisTick, gridStroke, baselineStroke } from "@/components/chartConstants";
import { Spinner } from "@/components/Spinner";
import { Dialog } from "@/components/ui/Dialog";

interface KPITrendModalProps {
  kpi: string;
  kpiLabel: string;
  isOpen: boolean;
  onClose: () => void;
  startDate: string;
  endDate: string;
  formatValue?: (value: number, kpi: string) => string;
  variant?: "billing" | "platform" | "apps";
  workspaceIds?: string[];
  queryKeyPrefix?: string;
}

const SPEND_KPIS = new Set(["total_spend", "avg_daily_spend", "aiml_spend", "apps_spend", "tagged_spend", "untagged_spend", "infra_cost", "avg_cost_per_cluster", "sql_spend", "aiml_avg_endpoint_cost", "apps_avg_cost_per_app", "cost_per_tag", "user_spend", "power_user_spend", "avg_spend_per_user"]);

function defaultBillingFormat(value: number, kpi: string): string {
  if (SPEND_KPIS.has(kpi)) {
    return formatCurrency(value);
  }
  if (kpi === "tag_coverage_pct") {
    return `${value.toFixed(1)}%`;
  }
  return formatNumber(value);
}

function defaultPlatformFormat(value: number, kpi: string): string {
  if (kpi === "total_bytes_read") return formatBytesNoDecimal(value);
  if (kpi === "total_rows_read") return formatRowCount(value);
  if (kpi === "total_compute_seconds") return formatComputeSecondsCompact(value);
  if (kpi === "avg_query_duration") return `${value.toFixed(1)}s`;
  if (kpi === "stickiness") return `${value.toFixed(1)}%`;
  return formatNumber(value);
}

export function KPITrendModal({
  kpi,
  kpiLabel,
  isOpen,
  onClose,
  startDate,
  endDate,
  formatValue,
  variant = "billing",
  workspaceIds,
  queryKeyPrefix,
}: KPITrendModalProps) {
  // All hooks must be called on every render, but only the selected variant may
  // own the query. Enabling every hook made billing and platform requests share
  // the same caller-supplied query key, so the first (often incompatible)
  // endpoint populated the modal with an empty response.
  const billingTrend = useKPITrend(
    kpi, startDate, endDate, "daily", workspaceIds, queryKeyPrefix, isOpen && variant === "billing",
  );
  const platformTrend = usePlatformKPITrend(
    kpi, startDate, endDate, "daily", workspaceIds, queryKeyPrefix, isOpen && variant === "platform",
  );
  const appsTrend = useAppsKPITrend(
    kpi, startDate, endDate, "daily", workspaceIds, isOpen && variant === "apps",
  );
  const { data, isLoading } = variant === "platform" ? platformTrend : variant === "apps" ? appsTrend : billingTrend;

  const fmt = formatValue ?? (variant === "platform" ? defaultPlatformFormat : defaultBillingFormat);

  if (!isOpen) return null;

  const formattedStart = format(parseISO(startDate), "MMM d, yyyy");
  const formattedEnd = format(parseISO(endDate), "MMM d, yyyy");
  const changePct = data?.summary?.change_percent ?? 0;
  const tone = changeTone(changePct);

  return (
    <Dialog
      open={isOpen}
      onClose={onClose}
      title={kpiLabel}
      subtitle="Trend Analysis"
      className="max-w-4xl"
      closeLabel={`Close ${kpiLabel} trend`}
    >
          {isLoading ? (
            <div className="flex h-80 items-center justify-center">
              <Spinner size="md" />
            </div>
          ) : data?.data_points && data.data_points.length > 0 ? (
            <>
              <div className="mb-6 grid grid-cols-4 gap-4">
                {[
                  ["Start", fmt(data.summary.period_start_value, kpi)],
                  ["End", fmt(data.summary.period_end_value, kpi)],
                  ["Average", fmt(data.summary.avg_value, kpi)],
                ].map(([label, value]) => (
                  <div key={label} className="p-4" style={{ background: C.oatPage, borderRadius: 8 }}>
                    <p className="text-[11px] font-bold uppercase tracking-wide" style={{ color: C.slate }}>{label}</p>
                    <p className="mt-1 text-[22px] font-medium" style={{ color: C.ink, fontFamily: FONT_MONO }}>{value}</p>
                  </div>
                ))}
                <div className="p-4" style={{ background: tone.bg, borderRadius: 8 }}>
                  <p className="text-[11px] font-bold uppercase tracking-wide" style={{ color: C.slate }}>Change</p>
                  <p className="mt-1 text-[22px] font-medium" style={{ color: tone.fg, fontFamily: FONT_MONO }}>
                    {tone.label === "±0.0%" ? "±0.0%" : `${changePct > 0 ? "+" : ""}${changePct.toFixed(1)}%`}
                  </p>
                </div>
              </div>

              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={data.data_points} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <CartesianGrid vertical={false} stroke={gridStroke} />
                    <XAxis
                      dataKey="date"
                      tickFormatter={(value) => format(parseISO(value), "MMM d")}
                      tick={axisTick}
                      axisLine={{ stroke: baselineStroke }}
                    />
                    <YAxis
                      tickFormatter={(value) => fmt(value, kpi)}
                      tick={axisTick}
                      axisLine={{ stroke: baselineStroke }}
                      width={80}
                    />
                    <Tooltip content={<InkTooltip formatter={(v) => fmt(v, kpi)} />} />
                    <Area isAnimationActive={false}
                      type="monotone"
                      dataKey="value"
                      stroke={C.lava}
                      strokeWidth={2.2}
                      fill={C.lava}
                      fillOpacity={0.16}
                      dot={false}
                      activeDot={{ r: 3 }}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>

              <div className="mt-4 flex items-center gap-2 px-4 py-3" style={{ background: tone.bg, borderRadius: 8 }}>
                {data.summary.trend === "increasing" && (
                  <>
                    <TrendingUp className="h-5 w-5" style={{ color: C.lavaHover }} />
                    <span className="text-sm font-medium" style={{ color: C.ink }}>
                      Trending upward by {data.summary.change_percent.toFixed(1)}% from {formattedStart} to {formattedEnd}
                    </span>
                  </>
                )}
                {data.summary.trend === "decreasing" && (
                  <>
                    <TrendingDown className="h-5 w-5" style={{ color: C.lavaHover }} />
                    <span className="text-sm font-medium" style={{ color: C.ink }}>
                      Trending downward by {Math.abs(data.summary.change_percent).toFixed(1)}% from {formattedStart} to {formattedEnd}
                    </span>
                  </>
                )}
                {data.summary.trend === "flat" && (
                  <>
                    <Minus className="h-5 w-5" style={{ color: C.lavaHover }} />
                    <span className="text-sm font-medium" style={{ color: C.ink }}>
                      Relatively stable (±{Math.abs(data.summary.change_percent).toFixed(1)}%) from {formattedStart} to {formattedEnd}
                    </span>
                  </>
                )}
              </div>
            </>
          ) : (
            <div className="flex h-64 flex-col items-center justify-center" style={{ color: C.slate }}>
              <p className="text-lg font-medium">No data available</p>
              <p className="text-sm">Try selecting a different date range</p>
            </div>
          )}
    </Dialog>
  );
}
