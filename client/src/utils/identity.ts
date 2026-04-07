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
 * Short display label for any identity:
 *  - Service principal UUID → "SP-xxxxx" (first 5 hex chars)
 *  - Email address          → "alice"    (username before @)
 *  - Other                  → value as-is
 */
export function formatIdentity(id: string): string {
  if (!id) return id;
  const v = id.trim();
  if (isServicePrincipal(v)) {
    return `SP-${v.replace(/-/g, "").slice(0, 5)}`;
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

