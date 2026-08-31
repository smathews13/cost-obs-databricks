import type { InfraCostsResponse } from "@/types/billing";

export function getCloudInstanceFamily(
  instanceType: string | null | undefined,
  cloud: string,
): string {
  if (!instanceType) return "unknown";
  if (cloud.toUpperCase() === "AZURE") {
    const match = instanceType.match(/^(Standard_[A-Z]+)/);
    return match ? match[1] : "unknown";
  }
  if (cloud.toUpperCase() === "GCP") {
    return instanceType.split("-")[0] || "unknown";
  }
  return instanceType.split(".")[0] || "unknown";
}

export function applyInfraPricing(
  data: InfraCostsResponse | undefined,
  multiplier: number,
): InfraCostsResponse | undefined {
  if (!data || multiplier === 1) return data;
  const scale = (value: number) => value * multiplier;
  return {
    ...data,
    clusters: (data.clusters || []).map((cluster) => ({
      ...cluster,
      databricks_spend: scale(cluster.databricks_spend ?? 0),
    })),
    total_databricks_spend: scale(data.total_databricks_spend ?? 0),
    billing_summary: data.billing_summary ? {
      ...data.billing_summary,
      databricks_compute_spend: scale(
        data.billing_summary.databricks_compute_spend ?? 0,
      ),
      avg_databricks_spend_per_cluster: scale(
        data.billing_summary.avg_databricks_spend_per_cluster ?? 0,
      ),
    } : undefined,
  };
}
