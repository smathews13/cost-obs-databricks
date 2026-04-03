import { formatCurrency, formatNumber } from "@/utils/formatters";

interface LakebaseViewProps {
  startDate: string;
  endDate: string;
}

// Placeholder data for wireframe
const MOCK_LAKEBASE = {
  totalInstances: 3,
  totalStorageGb: 248.5,
  totalComputeHours: 1420,
  totalSpend: 3250,
  instances: [
    {
      name: "cost-obs-primary",
      status: "RUNNING",
      engine: "PostgreSQL 16",
      storageGb: 124.2,
      computeHours: 720,
      spend: 1680,
      tables: 18,
      connections: 4,
      region: "us-east-1",
    },
    {
      name: "cost-obs-cache",
      status: "RUNNING",
      engine: "PostgreSQL 16",
      storageGb: 86.3,
      computeHours: 520,
      spend: 1120,
      tables: 12,
      connections: 2,
      region: "us-east-1",
    },
    {
      name: "cost-obs-staging",
      status: "STOPPED",
      engine: "PostgreSQL 16",
      storageGb: 38.0,
      computeHours: 180,
      spend: 450,
      tables: 18,
      connections: 0,
      region: "us-west-2",
    },
  ],
  migrationStatus: {
    totalTables: 12,
    migrated: 0,
    pending: 12,
    tables: [
      { name: "billing_usage_cache", type: "Materialized View", status: "Pending", rows: "~2.4M", sizeMb: 340 },
      { name: "sku_breakdown_cache", type: "Materialized View", status: "Pending", rows: "~850", sizeMb: 2 },
      { name: "workspace_breakdown_cache", type: "Materialized View", status: "Pending", rows: "~120", sizeMb: 8 },
      { name: "daily_spend_cache", type: "Materialized View", status: "Pending", rows: "~45K", sizeMb: 24 },
      { name: "interactive_breakdown_cache", type: "Materialized View", status: "Pending", rows: "~3.2K", sizeMb: 12 },
      { name: "pipeline_objects_cache", type: "Materialized View", status: "Pending", rows: "~680", sizeMb: 5 },
      { name: "tag_coverage_cache", type: "Materialized View", status: "Pending", rows: "~90", sizeMb: 1 },
      { name: "aiml_summary_cache", type: "Materialized View", status: "Pending", rows: "~200", sizeMb: 3 },
      { name: "apps_summary_cache", type: "Materialized View", status: "Pending", rows: "~50", sizeMb: 1 },
      { name: "sql_warehouse_cache", type: "Materialized View", status: "Pending", rows: "~15K", sizeMb: 45 },
      { name: "alert_rules", type: "Table", status: "Pending", rows: "~25", sizeMb: 0.1 },
      { name: "use_cases", type: "Table", status: "Pending", rows: "~40", sizeMb: 0.2 },
    ],
  },
};

function StatusBadge({ status }: { status: string }) {
  const colors = status === "RUNNING"
    ? "bg-green-100 text-green-800"
    : status === "STOPPED"
    ? "bg-gray-100 text-gray-600"
    : "bg-amber-100 text-amber-800";

  return (
    <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${colors}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${status === "RUNNING" ? "bg-green-500" : status === "STOPPED" ? "bg-gray-400" : "bg-amber-500"}`} />
      {status}
    </span>
  );
}

function MigrationBadge({ status }: { status: string }) {
  const colors = status === "Migrated"
    ? "bg-green-100 text-green-800"
    : status === "In Progress"
    ? "bg-blue-100 text-blue-800"
    : "bg-gray-100 text-gray-600";

  return (
    <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${colors}`}>
      {status}
    </span>
  );
}

export function LakebaseView(_props: LakebaseViewProps) {
  const data = MOCK_LAKEBASE;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-3">
        <div className="rounded-lg p-2 bg-emerald-600">
          <svg className="h-6 w-6 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
          </svg>
        </div>
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Lakebase</h1>
          <p className="text-sm text-gray-500">Managed PostgreSQL instances and migration status</p>
        </div>
      </div>

      {/* Preview Banner */}
      <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-3">
        <div className="flex items-center gap-2">
          <svg className="h-5 w-5 text-emerald-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
          </svg>
          <div>
            <span className="text-sm font-medium text-emerald-800">Experimental Preview</span>
            <span className="ml-2 text-sm text-emerald-600">
              Lakebase integration is under development. Enabling this will eventually migrate app backing stores from materialized views to Lakebase PostgreSQL.
            </span>
          </div>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-lg border bg-white p-5 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
          <p className="text-xs font-medium uppercase tracking-wider text-gray-500">Instances</p>
          <p className="mt-2 text-2xl font-bold text-gray-900">{data.totalInstances}</p>
          <p className="mt-1 text-xs text-gray-500">{data.instances.filter(i => i.status === "RUNNING").length} running</p>
        </div>
        <div className="rounded-lg border bg-white p-5 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
          <p className="text-xs font-medium uppercase tracking-wider text-gray-500">Storage</p>
          <p className="mt-2 text-2xl font-bold text-gray-900">{data.totalStorageGb.toFixed(1)} GB</p>
          <p className="mt-1 text-xs text-gray-500">across all instances</p>
        </div>
        <div className="rounded-lg border bg-white p-5 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
          <p className="text-xs font-medium uppercase tracking-wider text-gray-500">Compute Hours</p>
          <p className="mt-2 text-2xl font-bold text-gray-900">{formatNumber(data.totalComputeHours)}</p>
          <p className="mt-1 text-xs text-gray-500">this billing period</p>
        </div>
        <div className="rounded-lg border bg-white p-5 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
          <p className="text-xs font-medium uppercase tracking-wider text-gray-500">Lakebase Spend</p>
          <p className="mt-2 text-2xl font-bold text-emerald-600">{formatCurrency(data.totalSpend)}</p>
          <p className="mt-1 text-xs text-gray-500">this billing period</p>
        </div>
      </div>

      {/* Instances Table */}
      <div className="rounded-lg border bg-white p-6 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
        <h3 className="mb-4 text-lg font-semibold text-gray-900">Lakebase Instances</h3>
        <div className="overflow-x-auto">
          <table className="min-w-full">
            <thead className="bg-gray-50">
              <tr className="border-b border-gray-200">
                <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Instance</th>
                <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Status</th>
                <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Engine</th>
                <th className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Tables</th>
                <th className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Storage</th>
                <th className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Connections</th>
                <th className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Spend</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {data.instances.map((inst) => (
                <tr key={inst.name} className="hover:bg-gray-50">
                  <td className="px-3 py-3">
                    <div className="text-sm font-medium text-gray-900">{inst.name}</div>
                    <div className="text-xs text-gray-500">{inst.region}</div>
                  </td>
                  <td className="px-3 py-3"><StatusBadge status={inst.status} /></td>
                  <td className="px-3 py-3 text-sm text-gray-600">{inst.engine}</td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">{inst.tables}</td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">{inst.storageGb.toFixed(1)} GB</td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">{inst.connections}</td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm font-medium text-gray-900">{formatCurrency(inst.spend)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Migration Status */}
      <div className="rounded-lg border bg-white p-6 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Migration Status</h3>
            <p className="text-sm text-gray-500">
              Progress of migrating app backing stores from materialized views to Lakebase
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <span className="text-2xl font-bold text-gray-900">{data.migrationStatus.migrated}</span>
              <span className="text-sm text-gray-500"> / {data.migrationStatus.totalTables}</span>
            </div>
            <div className="h-10 w-10 rounded-full border-4 border-gray-200 flex items-center justify-center">
              <span className="text-xs font-bold text-gray-400">0%</span>
            </div>
          </div>
        </div>

        {/* Progress bar */}
        <div className="mb-6 h-3 w-full overflow-hidden rounded-full bg-gray-200">
          <div
            className="h-full rounded-full bg-emerald-500 transition-all"
            style={{ width: `${(data.migrationStatus.migrated / data.migrationStatus.totalTables) * 100}%` }}
          />
        </div>

        <div className="overflow-x-auto">
          <table className="min-w-full">
            <thead className="bg-gray-50">
              <tr className="border-b border-gray-200">
                <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Table / View</th>
                <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Current Type</th>
                <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Status</th>
                <th className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Rows</th>
                <th className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Size</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {data.migrationStatus.tables.map((table) => (
                <tr key={table.name} className="hover:bg-gray-50">
                  <td className="px-3 py-3 text-sm font-medium text-gray-900 font-mono">{table.name}</td>
                  <td className="px-3 py-3">
                    <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                      table.type === "Materialized View" ? "bg-purple-100 text-purple-800" : "bg-blue-100 text-blue-800"
                    }`}>
                      {table.type}
                    </span>
                  </td>
                  <td className="px-3 py-3"><MigrationBadge status={table.status} /></td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">{table.rows}</td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">
                    {table.sizeMb >= 1 ? `${table.sizeMb} MB` : `${(table.sizeMb * 1024).toFixed(0)} KB`}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Cost Comparison Placeholder */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <div className="rounded-lg border bg-white p-6 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
          <h3 className="mb-4 text-lg font-semibold text-gray-900">Lakebase Usage Over Time</h3>
          <div className="flex h-48 flex-col items-center justify-center gap-3 rounded-lg border-2 border-dashed border-gray-200 bg-gray-50">
            <svg className="h-12 w-12 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
            </svg>
            <p className="text-sm font-medium text-gray-400">Compute hours &amp; storage over time</p>
            <p className="text-xs text-gray-400">Will display once Lakebase billing data is available</p>
          </div>
        </div>
        <div className="rounded-lg border bg-white p-6 shadow-sm" style={{ borderColor: '#E5E5E5' }}>
          <h3 className="mb-4 text-lg font-semibold text-gray-900">Cost: MV Refresh vs. Lakebase</h3>
          <div className="flex h-48 flex-col items-center justify-center gap-3 rounded-lg border-2 border-dashed border-gray-200 bg-gray-50">
            <svg className="h-12 w-12 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3" />
            </svg>
            <p className="text-sm font-medium text-gray-400">Side-by-side cost comparison</p>
            <p className="text-xs text-gray-400">Materialized view refresh cost vs. Lakebase hosting cost</p>
          </div>
        </div>
      </div>
    </div>
  );
}
