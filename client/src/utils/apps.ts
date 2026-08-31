// Dark, distinct colors whose contrast with white text meets WCAG AA.
const APP_FALLBACK_COLORS = [
  "#1565C0",
  "#00695C",
  "#6A1B9A",
  "#AD1457",
  "#C62828",
  "#4527A0",
  "#2E7D32",
  "#283593",
  "#37474F",
  "#4E342E",
] as const;

function hashAppIdentity(identity: string): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < identity.length; index += 1) {
    hash ^= identity.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

export function getAppFallbackColor(identity: string): string {
  const normalized = identity.trim().toLowerCase() || "?";
  return APP_FALLBACK_COLORS[
    hashAppIdentity(normalized) % APP_FALLBACK_COLORS.length
  ];
}

export function getAppInitials(name: string, appId: string): string {
  const label = name.trim() || appId.trim() || "?";
  const words = label.split(/[\s_-]+/).filter(Boolean);
  if (words.length > 1) {
    return `${words[0][0]}${words[1][0]}`.toUpperCase();
  }
  return label.slice(0, 2).toUpperCase();
}
