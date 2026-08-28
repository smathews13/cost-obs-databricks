import { useMemo } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Cell,
  ResponsiveContainer,
  LabelList,
} from "recharts";
import { formatCurrency } from "@/utils/formatters";
import { formatIdentity } from "@/utils/identity";
import { C } from "@/theme";

interface UserGroupSpendData {
  groups: { group_name: string; total_spend: number; total_dbus: number; user_count: number; percentage: number }[];
  total_spend: number;
  source?: string;
  error?: string;
}

interface UserGroupBreakdownProps {
  data: UserGroupSpendData | undefined;
  isLoading: boolean;
}

const GROUP_COLORS = [
  C.s2, C.lava, C.s5, C.s3, C.s4,
  C.s5, C.s1, C.lava, C.s3, C.slate,
];

export function UserGroupBreakdown({ data, isLoading }: UserGroupBreakdownProps) {
  const isGroups = data?.source === "groups";
  const title = isGroups ? "Spend by User Group" : "Spend by User";

  const barData = useMemo(() => {
    if (!data?.groups?.length) return [];
    return [...data.groups]
      .sort((a, b) => b.total_spend - a.total_spend)
      .slice(0, 10)
      .map((g) => ({
        name: isGroups ? g.group_name : formatIdentity(g.group_name),
        total_spend: g.total_spend,
        user_count: g.user_count,
      }));
  }, [data]);

  if (isLoading) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <h3 className="mb-4 text-lg font-medium text-gray-900">Spend by User</h3>
        <div className="h-80 animate-pulse rounded" style={{ backgroundColor: C.hairline }} />
      </div>
    );
  }

  if (data?.error) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <h3 className="mb-4 text-lg font-medium text-gray-900">{title}</h3>
        <div className="flex h-80 flex-col items-center justify-center gap-2 text-gray-500">
          <p className="text-base font-medium">User spend data unavailable</p>
          <p className="text-sm">Could not retrieve spend by user from billing data</p>
        </div>
      </div>
    );
  }

  if (!data || data.groups.length === 0) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <h3 className="mb-4 text-lg font-medium text-gray-900">{title}</h3>
        <div className="flex h-80 flex-col items-center justify-center gap-2 text-gray-500">
          <p className="text-base font-medium">No data available</p>
          <p className="text-sm">Try expanding the date range</p>
        </div>
      </div>
    );
  }

  return (
    <div className="animate-fade-in rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
      <h3 className="mb-4 text-lg font-medium text-gray-900">{title}</h3>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={barData} layout="vertical" margin={{ left: 30, right: 70 }}>
          <XAxis type="number" tickFormatter={(v) => formatCurrency(v)} stroke={C.muted} fontSize={12} tickMargin={8} />
          <YAxis
            type="category"
            dataKey="name"
            width={100}
            stroke={C.muted}
            fontSize={12}
            tickMargin={8}
            tickFormatter={(v: string) => (v.length > 16 ? v.substring(0, 16) + "…" : v)}
          />
          <Tooltip
            formatter={(value: number | undefined) => formatCurrency(value ?? 0)}
            labelFormatter={(label) => isGroups ? `Group: ${label}` : `User: ${label}`}
            contentStyle={{
              backgroundColor: "white",
              border: "1px solid #e5e7eb",
              borderRadius: "8px",
            }}
          />
          <Bar isAnimationActive={false} dataKey="total_spend" name="Spend" radius={[0, 4, 4, 0]}>
            {barData.map((_entry, idx) => (
              <Cell key={idx} fill={GROUP_COLORS[idx % GROUP_COLORS.length]} />
            ))}
            <LabelList dataKey="total_spend" position="right" formatter={(v: unknown) => formatCurrency(v as number)} style={{ fontSize: 11, fill: C.slate }} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
