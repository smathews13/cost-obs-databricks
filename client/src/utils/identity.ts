/**
 * Utilities for displaying Databricks identities (users vs service principals).
 *
 * Service principals appear as bare UUIDs in billing data (identity_metadata.run_as).
 * Pattern: 8-4-4-4-8..12 hex chars.
 */

export const SP_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{8,12}$/i;

export function isServicePrincipal(id: string): boolean {
  return SP_REGEX.test((id ?? "").trim());
}

/**
 * Short display label for any identity, with optional resolved SP name map:
 *  - Service principal UUID → resolved display name, or "SP-xxxxx" abbreviation if not resolved
 *  - Email address          → "alice"  (username before @)
 *  - Other                  → value as-is
 */
export function formatIdentity(id: string, spNames?: Record<string, string>): string {
  if (!id) return id;
  const v = id.trim();
  if (isServicePrincipal(v)) {
    return spNames?.[v] ?? `SP-${v.replace(/-/g, "").slice(0, 5)}`;
  }
  if (v.includes("@")) {
    return v.split("@")[0];
  }
  return v;
}

/**
 * Full tooltip label — shows the raw ID for copy-pasting.
 */
export function identityTitle(id: string): string {
  return id ?? "";
}

/**
 * Fetch display names for a list of SP UUIDs from the server.
 * Returns a map of {uuid: display_name}.
 */
export async function resolveSpNames(ids: string[]): Promise<Record<string, string>> {
  const spIds = ids.filter(isServicePrincipal);
  if (spIds.length === 0) return {};
  try {
    const res = await fetch(`/api/identities/resolve?ids=${encodeURIComponent(spIds.join(","))}`);
    if (!res.ok) return {};
    const data = await res.json();
    return data.identities ?? {};
  } catch {
    return {};
  }
}
