export type ReadinessStatus =
  | "healthy"
  | "not_configured"
  | "timeout_starting"
  | "permission_denied"
  | "internal_error"
  | "unavailable";

export interface ReadinessCheck {
  table?: string;
  name: string;
  description: string;
  required?: boolean;
  granted: boolean;
  category: "core" | "enhanced";
  source?: string;
  fix_sql?: string;
  error?: string;
}

export interface ReadinessWarehouse {
  name: string;
  description: string;
  category: "core";
  source: "app_resource" | "http_path" | "none";
  granted: boolean;
  error?: string;
  fix_sql?: string;
}

export interface ReadinessResult {
  overall: "ready" | "core_ready" | "needs_action" | "not_ready";
  warehouse: ReadinessWarehouse;
  core: ReadinessCheck[];
  enhanced: ReadinessCheck[];
  sp_client_id: string;
  verified_at?: string | null;
}

/** Guards against missing fields so the UI never crashes on a partial payload. */
export function normalizeReadinessResult(raw: unknown): ReadinessResult | null {
  if (!raw || typeof raw !== "object") return null;
  const result = raw as Record<string, unknown>;
  const overall =
    (result.overall as ReadinessResult["overall"]) ?? "not_ready";
  const warehouse = result.warehouse as ReadinessWarehouse | undefined;
  if (!warehouse) return null;

  return {
    overall,
    warehouse: {
      name: String(warehouse.name ?? "SQL Warehouse"),
      description: String(warehouse.description ?? ""),
      category: "core",
      source: (warehouse.source as ReadinessWarehouse["source"]) ?? "none",
      granted: Boolean(warehouse.granted),
      error:
        warehouse.error != null ? String(warehouse.error) : undefined,
      fix_sql:
        warehouse.fix_sql != null ? String(warehouse.fix_sql) : undefined,
    },
    core: Array.isArray(result.core)
      ? (result.core as ReadinessCheck[])
      : [],
    enhanced: Array.isArray(result.enhanced)
      ? (result.enhanced as ReadinessCheck[])
      : [],
    sp_client_id: String(result.sp_client_id ?? ""),
    verified_at:
      result.verified_at != null ? String(result.verified_at) : null,
  };
}
