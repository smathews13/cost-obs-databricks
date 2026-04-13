import { useState, useEffect, useRef } from "react";
import { useQuery, type UseMutationResult } from "@tanstack/react-query";
import type { AppSettings } from "../SettingsDialog";
import { usePricing } from "@/context/PricingContext";

interface WarehouseInfo {
  id: string;
  name: string;
  size: string | null;
  state: string;
  is_current: boolean;
}

interface AppConfigInfo {
  warehouse: { id: string; name: string | null; size: string | null; state: string } | null;
  identity: { display_name: string | null; user_name: string | null } | null;
  storage_location: { catalog: string; schema: string } | null;
}

interface TelemetryConfig {
  catalog: string;
  schema_name: string;
  table_prefix: string;
}

interface SettingsConfigProps {
  configLoading: boolean;
  appConfig: AppConfigInfo | undefined;
  warehouses: WarehouseInfo[];
  warehousesLoading: boolean;
  pendingWarehouseSwitch: { id: string; name: string; state: string } | null;
  setPendingWarehouseSwitch: (v: { id: string; name: string; state: string } | null) => void;
  switchWarehouseMutation: UseMutationResult<any, Error, string, unknown>;
  saveStatus: string | null;
  localSettings: AppSettings;
  updateSetting: <K extends keyof AppSettings>(key: K, value: AppSettings[K]) => void;
}

export function SettingsConfig({
  configLoading,
  appConfig,
  warehouses,
  warehousesLoading,
  pendingWarehouseSwitch,
  setPendingWarehouseSwitch,
  switchWarehouseMutation,
  saveStatus,
  localSettings,
  updateSetting,
}: SettingsConfigProps) {
  const { useAccountPrices, setUseAccountPrices, discountPercent, available: pricingAvailable, loading: pricingLoading } = usePricing();
  const [pricingToggling, setPricingToggling] = useState(false);
  const [telemetry, setTelemetry] = useState<TelemetryConfig>({ catalog: "", schema_name: "", table_prefix: "" });
  const [telemetryLoading, setTelemetryLoading] = useState(true);
  const [telemetrySaving, setTelemetrySaving] = useState(false);
  const [telemetryStatus, setTelemetryStatus] = useState<string | null>(null);
  const { data: lakebaseStatus = null, isLoading: lakebaseLoading } = useQuery<{
    configured: boolean;
    connected?: boolean;
    endpoint_name?: string;
    host?: string;
    database?: string;
    user?: string;
    missing_vars?: string[];
  } | null>({
    queryKey: ["settings-lakebase-status"],
    queryFn: () => fetch("/api/settings/lakebase-status").then(r => r.json()).catch(() => ({ configured: false })),
    staleTime: 5 * 60 * 1000,
  });
  const [mvRefreshing, setMvRefreshing] = useState(false);
  const { data: tablesStatus = null, isLoading: tablesLoading, refetch: refetchTables } = useQuery<{
    catalog: string | null;
    schema: string | null;
    tables: Array<{
      name: string;
      exists: boolean | null;
      row_count: number | null;
      max_date: string | null;
      days_behind: number | null;
      error?: string;
    }>;
  } | null>({
    queryKey: ["settings-tables-status"],
    queryFn: () => fetch("/api/settings/tables").then(r => r.json()).catch(() => null),
    staleTime: 2 * 60 * 1000,
  });

  async function handleMvRefresh() {
    setMvRefreshing(true);
    try {
      await fetch("/api/settings/refresh-mvs", { method: "POST" });
      await refetchTables();
    } finally {
      setMvRefreshing(false);
    }
  }
  const { data: accountPrices = null, isLoading: accountPricesLoading } = useQuery<{
    available: boolean;
    prices: Array<{ sku_name: string; cloud: string; currency_code: string; usage_unit: string; list_price: number; effective_list_price: number; start_time: string | null; end_time: string | null }>;
    source: string | null;
    count: number;
    message?: string;
  } | null>({
    queryKey: ["settings-account-prices"],
    queryFn: () => fetch("/api/settings/account-prices").then(r => r.json()).catch(() => ({ available: false, prices: [], source: null, count: 0 })),
    staleTime: 5 * 60 * 1000,
  });
  const [priceSearch, setPriceSearch] = useState("");
  const [genieCreating, setGenieCreating] = useState(false);
  const [genieCreateStatus, setGenieCreateStatus] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const genieCreateStatusTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    fetch("/api/settings/telemetry")
      .then((r) => r.json())
      .then((d) => setTelemetry(d))
      .catch(() => {})
      .finally(() => setTelemetryLoading(false));
  }, []);


  const saveTelemetry = async () => {
    setTelemetrySaving(true);
    try {
      await fetch("/api/settings/telemetry", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(telemetry),
      });
      setTelemetryStatus("Telemetry settings saved");
      setTimeout(() => setTelemetryStatus(null), 3000);
    } catch {
      setTelemetryStatus("Failed to save telemetry settings");
      setTimeout(() => setTelemetryStatus(null), 3000);
    } finally {
      setTelemetrySaving(false);
    }
  };

  const createGenieSpace = async () => {
    setGenieCreating(true);
    setGenieCreateStatus(null);
    try {
      const res = await fetch("/api/setup/create-genie-space", { method: "POST" });
      const data = await res.json();
      if (data.space_id) {
        updateSetting("genieSpaceId", data.space_id);
        updateSetting("enableGenie", true);
        setGenieCreateStatus({ type: "success", message: `Genie Space created (${data.space_id})` });
      } else if (data.status === "already_exists") {
        updateSetting("genieSpaceId", data.space_id || "");
        updateSetting("enableGenie", true);
        setGenieCreateStatus({ type: "success", message: "Using existing Genie Space" });
      } else {
        setGenieCreateStatus({ type: "error", message: data.message || "Failed to create Genie Space" });
      }
    } catch {
      setGenieCreateStatus({ type: "error", message: "Request failed — check server logs" });
    } finally {
      setGenieCreating(false);
      if (genieCreateStatusTimer.current) clearTimeout(genieCreateStatusTimer.current);
      genieCreateStatusTimer.current = setTimeout(() => setGenieCreateStatus(null), 6000);
    }
  };

  return (
    <div className="space-y-5">
      <p className="text-sm text-gray-500">
        Runtime configuration for this app instance. Change the SQL warehouse to switch compute resources.
      </p>

      {saveStatus && (
        <div className="rounded-lg border border-green-200 bg-green-50 px-4 py-2 text-sm text-green-700">
          {saveStatus}
        </div>
      )}

      {configLoading ? (
        <div className="py-8 text-center text-sm text-gray-400">Loading configuration...</div>
      ) : (
        <>
          {/* SQL Warehouse */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
              </svg>
              <h4 className="text-sm font-semibold text-gray-900">SQL Warehouse</h4>
            </div>
            {appConfig?.warehouse && (
              <div className="mb-3 space-y-2">
                <div className="flex items-center justify-between rounded-lg border border-gray-200 bg-white p-3">
                  <div className="text-sm text-gray-500">Current Warehouse</div>
                  <div className="flex items-center gap-2">
                    <span className={`inline-block h-2 w-2 rounded-full ${appConfig.warehouse.state === "RUNNING" ? "bg-green-500" : appConfig.warehouse.state === "STOPPED" ? "bg-gray-400" : "bg-yellow-500"}`} />
                    <span className="text-sm font-medium text-gray-900">{appConfig.warehouse.name || appConfig.warehouse.id}</span>
                    <span className="text-xs text-gray-400">({appConfig.warehouse.size || "—"}) · {appConfig.warehouse.state}</span>
                  </div>
                </div>
              </div>
            )}
            <div className="rounded-lg border border-gray-200 bg-white p-3">
              <div className="mb-2">
                <div className="text-sm font-medium text-gray-900">Switch Warehouse</div>
                <div className="text-xs text-gray-500">Select a different SQL warehouse to power the app</div>
              </div>
              {warehousesLoading ? (
                <div className="py-3 text-center text-sm text-gray-400">Loading warehouses...</div>
              ) : warehouses.length === 0 ? (
                <div className="py-3 text-center text-sm text-gray-400">No warehouses found</div>
              ) : (
                <>
                <div className="space-y-1.5 max-h-48 overflow-y-auto">
                  {warehouses.map((wh) => (
                    <button
                      key={wh.id}
                      onClick={() => {
                        if (!wh.is_current) setPendingWarehouseSwitch({ id: wh.id, name: wh.name, state: wh.state });
                      }}
                      disabled={wh.is_current || switchWarehouseMutation.isPending}
                      className={`flex w-full items-center justify-between rounded-lg border px-3 py-2.5 text-left text-sm transition-colors ${
                        wh.is_current
                          ? "border-orange-200 bg-orange-50"
                          : pendingWarehouseSwitch?.id === wh.id
                            ? "border-orange-300 bg-orange-50"
                            : "border-gray-200 bg-white hover:border-orange-200 hover:bg-orange-50/50"
                      } ${switchWarehouseMutation.isPending ? "opacity-50 cursor-wait" : ""}`}
                    >
                      <div className="flex items-center gap-2.5 min-w-0">
                        <span className={`inline-block h-2 w-2 shrink-0 rounded-full ${wh.state === "RUNNING" ? "bg-green-500" : wh.state === "STOPPED" ? "bg-gray-400" : "bg-yellow-500"}`} />
                        <div className="min-w-0">
                          <div className="font-medium text-gray-900 truncate">{wh.name}</div>
                          <div className="text-xs text-gray-400">{wh.size || "—"} · {wh.state}</div>
                        </div>
                      </div>
                      {wh.is_current ? (
                        <span className="shrink-0 rounded px-2 py-0.5 text-xs font-medium" style={{ backgroundColor: '#FF362120', color: '#FF3621' }}>
                          Active
                        </span>
                      ) : (
                        <span className="shrink-0 text-xs text-gray-400">
                          {wh.state === "STOPPED" ? "Will start" : "Select"}
                        </span>
                      )}
                    </button>
                  ))}
                </div>
                {pendingWarehouseSwitch && (
                  <div className="mt-3 rounded-lg border border-orange-200 bg-orange-50 p-3">
                    <div className="flex items-start gap-2">
                      <svg className="h-5 w-5 shrink-0 text-orange-500 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                      </svg>
                      <div className="flex-1">
                        <div className="text-sm font-medium text-gray-900">
                          Switch to {pendingWarehouseSwitch.name}?
                        </div>
                        <div className="text-xs text-gray-600 mt-0.5">
                          {pendingWarehouseSwitch.state === "STOPPED"
                            ? "This warehouse is stopped and will be started automatically. It may take a few minutes to become available."
                            : "All queries will be routed to this warehouse immediately."}
                        </div>
                        <div className="flex gap-2 mt-2">
                          <button
                            onClick={() => setPendingWarehouseSwitch(null)}
                            className="rounded-md border border-gray-300 bg-white px-3 py-1 text-xs font-medium text-gray-700 hover:bg-gray-50"
                          >
                            Cancel
                          </button>
                          <button
                            onClick={() => switchWarehouseMutation.mutate(pendingWarehouseSwitch.id)}
                            disabled={switchWarehouseMutation.isPending}
                            className="rounded-md px-3 py-1 text-xs font-medium text-white transition-colors disabled:opacity-50"
                            style={{ backgroundColor: '#FF3621' }}
                          >
                            {switchWarehouseMutation.isPending ? "Switching..." : "Confirm Switch"}
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
                </>
              )}
            </div>
          </div>

          {/* App Identity */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
              </svg>
              <h4 className="text-sm font-semibold text-gray-900">App Identity</h4>
            </div>
            <div className="space-y-2">
              <div className="rounded-lg border border-gray-200 bg-white p-3">
                <div className="text-sm font-medium text-gray-900 mb-1">Display Name</div>
                <input
                  type="text"
                  value={localSettings.appDisplayName}
                  onChange={(e) => updateSetting("appDisplayName", e.target.value)}
                  placeholder={appConfig?.identity?.display_name || "e.g., Cost Observability"}
                  className="w-full rounded-md border border-gray-300 px-3 py-1.5 text-sm focus:border-orange-500 focus:outline-none focus:ring-1 focus:ring-orange-500"
                />
                <p className="mt-1 text-xs text-gray-400">
                  Overrides the app name shown in the header. Leave blank to use the default ({appConfig?.identity?.display_name || "service principal name"}).
                </p>
              </div>
              {appConfig?.identity && (
                <div className="flex items-center justify-between rounded-lg border border-gray-200 bg-white p-3">
                  <div className="text-sm text-gray-500">Service Principal</div>
                  <div className="text-sm font-medium text-gray-900">{appConfig.identity.user_name || "—"}</div>
                </div>
              )}
            </div>
          </div>

          {/* Enable AI Features */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
              </svg>
              <h4 className="text-sm font-semibold text-gray-900">AI Features</h4>
            </div>
            <div className="rounded-lg border border-gray-200 bg-white p-4">
              <label className="flex items-start gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={localSettings.enableAIFeatures}
                  onChange={(e) => {
                    updateSetting("enableAIFeatures", e.target.checked);
                    if (!e.target.checked) updateSetting("enableGenie", false);
                  }}
                  className="mt-0.5 h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
                />
                <div>
                  <div className="text-sm font-medium text-gray-900">Enable AI Features</div>
                  <div className="mt-0.5 text-xs text-gray-500">
                    Enables AI-powered features across the app, including the Genie Assistant and AI-assisted analysis of cost spikes on the KPIs tab. Disable to turn off all AI capabilities for this deployment.
                  </div>
                </div>
              </label>
            </div>
          </div>

          {/* Genie Assistant */}
          <div className={localSettings.enableAIFeatures ? "" : "opacity-50 pointer-events-none"}>
            <div className="flex items-center gap-2 mb-3">
              <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
              </svg>
              <h4 className="text-sm font-semibold text-gray-900">Genie Assistant</h4>
            </div>
            <div className="rounded-lg border border-gray-200 bg-white p-4 space-y-3">
              <label className="flex items-start gap-3 cursor-pointer">
                <input
                  type="checkbox"
                  checked={localSettings.enableGenie}
                  onChange={(e) => updateSetting("enableGenie", e.target.checked)}
                  className="mt-0.5 h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
                />
                <div className="flex-1">
                  <div className="text-sm font-medium text-gray-900">Enable Genie Assistant</div>
                  <div className="mt-0.5 text-xs text-gray-500">
                    Show the Genie AI assistant on the DBU Overview tab for natural language questions about your cost data.
                  </div>
                </div>
              </label>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Genie Space ID</label>
                <div className="flex gap-2">
                  <input
                    type="text"
                    value={localSettings.genieSpaceId}
                    onChange={(e) => updateSetting("genieSpaceId", e.target.value)}
                    placeholder="e.g. 01f0abcd1234..."
                    className="flex-1 rounded-md border border-gray-300 px-3 py-1.5 text-sm text-gray-900 placeholder-gray-400 focus:border-orange-500 focus:ring-1 focus:ring-orange-500"
                  />
                  {!localSettings.genieSpaceId && (
                    <button
                      onClick={createGenieSpace}
                      disabled={genieCreating}
                      className="rounded-md border border-gray-300 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-wait whitespace-nowrap transition-colors"
                    >
                      {genieCreating ? "Creating…" : "Auto-Create"}
                    </button>
                  )}
                </div>
                <p className="mt-1 text-[11px] text-gray-400">
                  Enter an existing Genie Space ID, or click Auto-Create to deploy one automatically using your workspace's billing tables.
                </p>
                {genieCreateStatus && (
                  <div className={`mt-2 rounded-md px-3 py-2 text-xs ${genieCreateStatus.type === "success" ? "bg-green-50 text-green-700" : "bg-red-50 text-red-700"}`}>
                    {genieCreateStatus.message}
                  </div>
                )}
              </div>
            </div>
          </div>

          {/* Storage Location & Tables */}
          <div>
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 19a2 2 0 01-2-2V7a2 2 0 012-2h4l2 2h4a2 2 0 012 2v1M5 19h14a2 2 0 002-2v-5a2 2 0 00-2-2H9a2 2 0 00-2 2v5a2 2 0 01-2 2z" />
                </svg>
                <h4 className="text-sm font-semibold text-gray-900">Storage Location & Tables</h4>
              </div>
              <button
                onClick={handleMvRefresh}
                disabled={mvRefreshing}
                className="inline-flex items-center gap-1 rounded px-2 py-1 text-xs text-gray-400 hover:text-gray-600 hover:bg-gray-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                title="Rebuild materialized views"
              >
                <svg className={`h-3.5 w-3.5 ${mvRefreshing ? "animate-spin" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                {mvRefreshing ? "Refreshing…" : "Refresh"}
              </button>
            </div>

            {/* Catalog / Schema pills */}
            {appConfig?.storage_location ? (
              <div className="mb-3 flex items-center gap-2 flex-wrap">
                <span className="text-xs text-gray-500">Catalog</span>
                <span className="rounded-md bg-orange-50 border border-orange-200 px-2 py-0.5 text-xs font-mono font-medium text-orange-800">
                  {appConfig.storage_location.catalog}
                </span>
                <span className="text-gray-300">·</span>
                <span className="text-xs text-gray-500">Schema</span>
                <span className="rounded-md bg-orange-50 border border-orange-200 px-2 py-0.5 text-xs font-mono font-medium text-orange-800">
                  {appConfig.storage_location.schema}
                </span>
              </div>
            ) : (
              <div className="mb-3 rounded-lg border border-gray-200 bg-gray-50 p-3 text-sm text-gray-400">Could not retrieve storage location</div>
            )}

            {/* Table list */}
            {tablesLoading ? (
              <div className="py-3 text-center text-xs text-gray-400">Checking tables...</div>
            ) : tablesStatus?.tables?.length ? (
              <div className="rounded-lg border border-gray-200 overflow-hidden">
                <table className="min-w-full divide-y divide-gray-100 text-xs">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-3 py-2 text-left font-medium text-gray-500">Table</th>
                      <th className="px-3 py-2 text-right font-medium text-gray-500">Rows</th>
                      <th className="px-3 py-2 text-right font-medium text-gray-500">Latest date</th>
                      <th className="px-3 py-2 text-right font-medium text-gray-500">Freshness</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 bg-white">
                    {tablesStatus.tables.map((t) => {
                      const stale = t.days_behind != null && t.days_behind > 1;
                      const missing = t.exists === false;
                      const unknown = t.exists === null;
                      return (
                        <tr key={t.name} className={missing ? "bg-red-50" : stale ? "bg-amber-50" : ""}>
                          <td className="px-3 py-2 font-mono text-gray-700 flex items-center gap-1.5">
                            {missing ? (
                              <span className="text-red-400">✗</span>
                            ) : unknown ? (
                              <span className="text-gray-300">?</span>
                            ) : (
                              <span className="text-green-500">✓</span>
                            )}
                            {t.name}
                            {t.error && (
                              <span className="ml-1 text-red-400" title={t.error}>⚠</span>
                            )}
                          </td>
                          <td className="px-3 py-2 text-right text-gray-500 tabular-nums">
                            {t.row_count != null ? t.row_count.toLocaleString() : "—"}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-gray-500">
                            {t.max_date ? t.max_date.slice(0, 10) : "—"}
                          </td>
                          <td className="px-3 py-2 text-right">
                            {t.days_behind == null ? (
                              <span className="text-gray-300">—</span>
                            ) : t.days_behind === 0 ? (
                              <span className="text-green-600 font-medium">Today</span>
                            ) : t.days_behind === 1 ? (
                              <span className="text-green-600">1d behind</span>
                            ) : t.days_behind <= 3 ? (
                              <span className="text-amber-600 font-medium">{t.days_behind}d behind</span>
                            ) : (
                              <span className="text-red-600 font-medium">{t.days_behind}d behind</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 text-xs text-gray-400">Could not retrieve table status</div>
            )}
          </div>

          {/* App Telemetry */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
              <h4 className="text-sm font-semibold text-gray-900">App Telemetry</h4>
            </div>
            <p className="mb-3 text-xs text-gray-500">
              Configure where the app writes OTel traces, metrics, and logs. Databricks Apps will create{" "}
              <span className="font-mono">otel_spans</span>, <span className="font-mono">otel_metrics</span>, and{" "}
              <span className="font-mono">otel_logs</span> tables (optionally prefixed) in the catalog and schema below.
            </p>
            {telemetryStatus && (
              <div className="mb-3 rounded-lg border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                {telemetryStatus}
              </div>
            )}
            {telemetryLoading ? (
              <div className="py-4 text-center text-sm text-gray-400">Loading...</div>
            ) : (
              <div className="rounded-lg border border-gray-200 bg-white p-3 space-y-3">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Catalog</label>
                    <input
                      type="text"
                      value={telemetry.catalog}
                      onChange={(e) => setTelemetry((t) => ({ ...t, catalog: e.target.value }))}
                      placeholder={appConfig?.storage_location?.catalog || "e.g. main"}
                      className="w-full rounded-md border border-gray-300 px-3 py-1.5 text-sm font-mono focus:border-orange-500 focus:outline-none focus:ring-1 focus:ring-orange-500"
                    />
                  </div>
                  <div>
                    <label className="block text-xs font-medium text-gray-700 mb-1">Schema</label>
                    <input
                      type="text"
                      value={telemetry.schema_name}
                      onChange={(e) => setTelemetry((t) => ({ ...t, schema_name: e.target.value }))}
                      placeholder={appConfig?.storage_location?.schema || "e.g. default"}
                      className="w-full rounded-md border border-gray-300 px-3 py-1.5 text-sm font-mono focus:border-orange-500 focus:outline-none focus:ring-1 focus:ring-orange-500"
                    />
                  </div>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-700 mb-1">Table Prefix <span className="text-gray-400 font-normal">(optional)</span></label>
                  <input
                    type="text"
                    value={telemetry.table_prefix}
                    onChange={(e) => setTelemetry((t) => ({ ...t, table_prefix: e.target.value }))}
                    placeholder="e.g. cost_obs_"
                    className="w-full rounded-md border border-gray-300 px-3 py-1.5 text-sm font-mono focus:border-orange-500 focus:outline-none focus:ring-1 focus:ring-orange-500"
                  />
                  <p className="mt-1 text-[11px] text-gray-400">
                    If set, tables will be named <span className="font-mono">{telemetry.table_prefix || "<prefix>_"}otel_spans</span>, etc.
                  </p>
                </div>
                <div className="flex justify-end pt-1">
                  <button
                    onClick={saveTelemetry}
                    disabled={telemetrySaving}
                    className="rounded-md px-3 py-1.5 text-xs font-medium text-white transition-colors disabled:opacity-50"
                    style={{ backgroundColor: '#FF3621' }}
                  >
                    {telemetrySaving ? "Saving..." : "Save Telemetry Config"}
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Lakebase */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14M12 5l7 7-7 7" />
              </svg>
              <h4 className="text-sm font-semibold text-gray-900">Lakebase (PostgreSQL)</h4>
              {!lakebaseLoading && (
                lakebaseStatus?.configured ? (
                  <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium ${lakebaseStatus.connected ? "bg-green-100 text-green-700" : "bg-yellow-100 text-yellow-700"}`}>
                    <span className={`h-1.5 w-1.5 rounded-full ${lakebaseStatus.connected ? "bg-green-500" : "bg-yellow-500"}`} />
                    {lakebaseStatus.connected ? "Connected" : "Configured (unreachable)"}
                  </span>
                ) : (
                  <span className="inline-flex items-center gap-1 rounded-full bg-gray-100 px-2 py-0.5 text-[10px] font-medium text-gray-500">
                    <span className="h-1.5 w-1.5 rounded-full bg-gray-400" />
                    Not configured
                  </span>
                )
              )}
            </div>
            <div className={`rounded-lg border p-3 space-y-2 ${lakebaseStatus?.configured ? "border-gray-200 bg-white" : "border-gray-100 bg-gray-50"}`}>
              {lakebaseLoading ? (
                <div className="py-2 text-center text-sm text-gray-400">Loading...</div>
              ) : !lakebaseStatus?.configured ? (
                <div className="text-sm text-gray-400">
                  <p className="font-medium mb-0.5">Lakebase not in use</p>
                  <p className="text-xs">Add a Lakebase resource to this app in the Databricks Apps UI to enable persistent PostgreSQL storage for permissions and configuration.</p>
                  {lakebaseStatus?.missing_vars && lakebaseStatus.missing_vars.length > 0 && (
                    <p className="text-xs mt-1 font-mono text-yellow-600">Missing: {lakebaseStatus.missing_vars.join(", ")}</p>
                  )}
                </div>
              ) : (
                <>
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-500">Endpoint</span>
                    <span className="font-mono text-xs text-gray-900">{lakebaseStatus.endpoint_name}</span>
                  </div>
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-500">Host</span>
                    <span className="font-mono text-xs text-gray-900 truncate max-w-[60%] text-right">{lakebaseStatus.host}</span>
                  </div>
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-500">Database</span>
                    <span className="font-mono text-xs text-gray-900">{lakebaseStatus.database}</span>
                  </div>
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-500">User</span>
                    <span className="font-mono text-xs text-gray-900">{lakebaseStatus.user}</span>
                  </div>
                </>
              )}
            </div>
          </div>
        </>
      )}

      {/* ── Account Pricing ─────────────────────────────────────────────────── */}
      <div>
        <div className="flex items-center gap-2 mb-3">
          <svg className="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 7h.01M7 3h5c.512 0 1.024.195 1.414.586l7 7a2 2 0 010 2.828l-7 7a2 2 0 01-2.828 0l-7-7A1.994 1.994 0 013 12V7a4 4 0 014-4z" />
          </svg>
          <h4 className="text-sm font-semibold text-gray-900">Account Pricing</h4>
          {accountPrices?.source && (
            <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs text-gray-500">
              {accountPrices.source === "account_prices" ? "Negotiated rates" : "List prices"}
            </span>
          )}
        </div>
        <div className="rounded-lg border border-gray-200 bg-white p-4 space-y-3">
          {/* Use account prices toggle */}
          <div className="flex items-center justify-between rounded-lg border border-gray-100 bg-gray-50 px-3 py-2.5">
            <div>
              <p className="text-xs font-medium text-gray-800">Use account prices</p>
              <p className="text-xs text-gray-500 mt-0.5">
                Apply your negotiated rates from <code className="rounded bg-gray-100 px-0.5">system.billing.account_prices</code> to all spend figures.
                {useAccountPrices && pricingAvailable && discountPercent > 0 && (
                  <span className="ml-1 font-medium text-green-600">{discountPercent.toFixed(1)}% discount active.</span>
                )}
                {useAccountPrices && !pricingAvailable && !pricingLoading && (
                  <span className="ml-1 text-amber-600">Table not yet available (private preview) — showing list prices.</span>
                )}
              </p>
            </div>
            <button
              role="switch"
              aria-checked={useAccountPrices}
              disabled={pricingToggling}
              onClick={async () => {
                setPricingToggling(true);
                await setUseAccountPrices(!useAccountPrices);
                setPricingToggling(false);
              }}
              className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors focus:outline-none ${
                useAccountPrices ? "bg-green-500" : "bg-gray-200"
              } ${pricingToggling ? "opacity-50" : ""}`}
            >
              <span
                className={`pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow transform transition-transform ${
                  useAccountPrices ? "translate-x-4" : "translate-x-0"
                }`}
              />
            </button>
          </div>
          <p className="text-xs text-gray-500">
            Prices sourced from <code className="rounded bg-gray-100 px-1">system.billing.account_prices</code> (negotiated account rates, private preview) or{" "}
            <code className="rounded bg-gray-100 px-1">system.billing.list_prices</code> as fallback.
            Used to compute effective spend vs. list-price spend.
          </p>
          {accountPricesLoading ? (
            <p className="text-xs text-gray-400">Loading...</p>
          ) : !accountPrices?.available ? (
            <p className="text-xs text-gray-400">{accountPrices?.message || "Pricing tables not accessible."}</p>
          ) : (
            <>
              <div className="flex items-center gap-2">
                <input
                  type="text"
                  placeholder="Filter by SKU name..."
                  value={priceSearch}
                  onChange={e => setPriceSearch(e.target.value)}
                  className="rounded border border-gray-200 px-2 py-1.5 text-xs w-64 focus:outline-none focus:ring-1 focus:ring-[#FF3621]"
                />
                <span className="text-xs text-gray-400">{accountPrices.count} SKUs</span>
              </div>
              <div className="overflow-x-auto rounded-lg border border-gray-200 max-h-64">
                <table className="min-w-full divide-y divide-gray-200 text-xs">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">SKU Name</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Cloud</th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">List Price</th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Effective Price</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Unit</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 bg-white">
                    {accountPrices.prices
                      .filter(p => !priceSearch || p.sku_name.toLowerCase().includes(priceSearch.toLowerCase()))
                      .map((p, i) => (
                        <tr key={`${p.sku_name}-${p.cloud}-${i}`} className="hover:bg-gray-50">
                          <td className="px-3 py-2 font-mono text-gray-700">{p.sku_name}</td>
                          <td className="px-3 py-2 text-gray-500">{p.cloud}</td>
                          <td className="px-3 py-2 text-right text-gray-600">${p.list_price.toFixed(4)}</td>
                          <td className={`px-3 py-2 text-right font-medium ${p.effective_list_price < p.list_price ? "text-green-600" : "text-gray-900"}`}>
                            ${p.effective_list_price.toFixed(4)}
                            {p.effective_list_price < p.list_price && (
                              <span className="ml-1 text-green-500">
                                ({((1 - p.effective_list_price / p.list_price) * 100).toFixed(0)}% off)
                              </span>
                            )}
                          </td>
                          <td className="px-3 py-2 text-gray-400">{p.usage_unit}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
