/**
 * Utilities for displaying Databricks identities (users vs service principals).
 *
 * Service principals appear as bare UUIDs in billing data (identity_metadata.run_as).
 * Pattern: 8-4-4-4-8..12 hex chars.
 */

import { createContext, useContext } from "react";

export const SP_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{8,12}$/i;

// React context for the service-principal display-name map (application_id -> display_name).
// App.tsx provides the value fetched from /api/user/service-principals so consumers
// don't need to drill the map through props.
export const SpNameMapContext = createContext<Record<string, string>>({});
export const useSpNameMap = () => useContext(SpNameMapContext);

export interface CostExportContext {
  anonymizeUsers: boolean;
  companyName?: string;
  sourceLabels?: string[];
  cloudProvider?: string | null;
}

export function isServicePrincipal(id: string): boolean {
  return SP_REGEX.test((id ?? "").trim());
}

/**
 * Short display label for any identity:
 *  - Service principal UUID → resolved SCIM display_name, else "SP-xxxxx"
 *  - Email address          → "alice"    (username before @)
 *  - Other                  → value as-is
 *
 * Pass an optional `spNameMap` (application_id -> display_name) sourced from
 * /api/user/service-principals to get real SP names instead of the hex hash.
 */
export function formatIdentity(id: string, spNameMap?: Record<string, string>): string {
  if (!id) return id;
  const v = id.trim();
  if (isServicePrincipal(v)) {
    // Lookup case-insensitively: backend normalizes keys to lowercase but
    // billing identity_metadata.run_as can arrive with mixed casing.
    const resolved = spNameMap?.[v.toLowerCase()] ?? spNameMap?.[v];
    if (resolved) return resolved;
    return `SP-${v.replace(/-/g, "").slice(0, 5)}`;
  }
  if (v.includes("@")) {
    return v.split("@")[0];
  }
  return v;
}

/**
 * Full tooltip label: shows the raw ID for copy-pasting.
 */
export function identityTitle(id: string): string {
  return id ?? "";
}

/**
 * Returns a stable anon label for a user email given its sort index.
 * Service principals are always returned as-is.
 * @param id    - raw identity string (email or SP UUID)
 * @param index - 0-based rank in the sorted user list
 * @param enabled - whether anonymization is active
 */
export function anonymizeIdentity(id: string, index: number, enabled: boolean): string {
  if (!enabled || isServicePrincipal(id)) return formatIdentity(id);
  return `User ${index + 1}`;
}

export function buildAnonymizedIdentityMap(
  users: Array<{ user_email: string; total_spend: number }>,
): Map<string, string> {
  const map = new Map<string, string>();
  let index = 0;
  [...users].sort((a, b) => b.total_spend - a.total_spend).forEach((user) => {
    if (!isServicePrincipal(user.user_email)) {
      map.set(user.user_email, `User ${index + 1}`);
      index += 1;
    }
  });
  return map;
}

function stableIdentityHash(value: string): string {
  let hash = 2166136261;
  for (const char of value.trim().toLowerCase()) {
    hash ^= char.charCodeAt(0);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36).toUpperCase().padStart(7, "0");
}

/** A stable export label shared by PDF and spreadsheet generation. */
export function anonymizeExportIdentity(id: string, enabled: boolean): string {
  const value = (id ?? "").trim();
  if (!enabled || !value || isServicePrincipal(value)) return value;
  if (["unknown", "n/a", "none", "-"].includes(value.toLowerCase())) return value;
  return `User ${stableIdentityHash(value)}`;
}

const EMAIL_IN_TEXT_RE = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const IDENTITY_FIELD_RE = /(^|_)(user|email|owner|creator|updater|executed_by|run_as|deployed_by)($|_)/i;

/**
 * Clone an export payload while replacing every human identity deterministically.
 * Email addresses embedded in paths or text are also removed, which guarantees an
 * anonymized report cannot leak a raw address through a less-obvious field.
 */
export function anonymizeExportPayload<T>(value: T, enabled: boolean): T {
  if (!enabled) return value;

  const visit = (current: unknown, key = ""): unknown => {
    if (Array.isArray(current)) return current.map((item) => visit(item, key));
    if (current && typeof current === "object") {
      return Object.fromEntries(
        Object.entries(current).map(([childKey, childValue]) => [childKey, visit(childValue, childKey)]),
      );
    }
    if (typeof current !== "string") return current;
    if (isServicePrincipal(current)) return current;
    const withoutEmails = current.replace(
      EMAIL_IN_TEXT_RE,
      (email) => anonymizeExportIdentity(email, true),
    );
    if (withoutEmails !== current) return withoutEmails;
    return IDENTITY_FIELD_RE.test(key)
      ? anonymizeExportIdentity(current, true)
      : current;
  };

  return visit(value) as T;
}

