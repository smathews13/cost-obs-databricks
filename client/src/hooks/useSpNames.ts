import { useQuery } from "@tanstack/react-query";
import { resolveSpNames, isServicePrincipal } from "@/utils/identity";

/**
 * Resolves a list of identity strings to SP display names.
 * Filters to UUIDs only before fetching. Results are cached for 10 minutes.
 *
 * Usage:
 *   const spNames = useSpNames(rows.map(r => r.executed_by));
 *   formatIdentity(id, spNames)
 */
export function useSpNames(ids: (string | null | undefined)[]): Record<string, string> {
  const spIds = [...new Set((ids ?? []).filter((id): id is string => !!id && isServicePrincipal(id)))];

  const { data } = useQuery({
    queryKey: ["sp-names", spIds.sort().join(",")],
    queryFn: () => resolveSpNames(spIds),
    enabled: spIds.length > 0,
    staleTime: 10 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
  });

  return data ?? {};
}
