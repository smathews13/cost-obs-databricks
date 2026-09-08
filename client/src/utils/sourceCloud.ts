export type SourceCloud = "aws" | "azure" | "gcp";

export function resolveSourceCloud(
  cloud: unknown,
  label = "",
  catalog = "",
): SourceCloud | null {
  const normalized = String(cloud ?? "").trim().toLowerCase();
  if (normalized === "gcp" || normalized === "google" || normalized === "google cloud") {
    return "gcp";
  }
  if (normalized === "azure" || normalized === "microsoft azure") return "azure";
  if (normalized === "aws" || normalized === "amazon" || normalized === "amazon web services") {
    return "aws";
  }

  const hint = `${label} ${catalog}`.toLowerCase().replaceAll(/[-_\s]/g, "");
  if (["gcp", "google", "west4", "central1", "east1", "east4"].some((marker) => hint.includes(marker))) {
    return "gcp";
  }
  if (["azure", "eastus", "westus", "westeurope"].some((marker) => hint.includes(marker))) {
    return "azure";
  }
  if (["aws", "useast", "uswest", "euwest"].some((marker) => hint.includes(marker))) {
    return "aws";
  }
  return null;
}
