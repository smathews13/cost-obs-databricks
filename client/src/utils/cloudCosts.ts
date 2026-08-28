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
