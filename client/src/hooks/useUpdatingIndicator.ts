import { useCallback, useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";

// Drives the same "Updating…" spinner the data-source filter uses, for dropdowns
// whose change triggers a background refetch (workspace filter, date range) rather
// than an awaited invalidate. Call `arm()` when the user applies a change; `updating`
// stays true from then until a fetch cycle completes (seen in-flight, then settled),
// with a safety timeout so it never sticks if nothing actually refetches.
export function useUpdatingIndicator(maxMs = 12000): { updating: boolean; arm: () => void } {
  const queryClient = useQueryClient();
  const [updating, setUpdating] = useState(false);
  const active = useRef(false);
  const sawInFlight = useRef(false);
  const grace = useRef<ReturnType<typeof setTimeout> | null>(null);
  const maxT = useRef<ReturnType<typeof setTimeout> | null>(null);

  const stop = useCallback(() => {
    active.current = false;
    setUpdating(false);
    sawInFlight.current = false;
    if (grace.current) { clearTimeout(grace.current); grace.current = null; }
    if (maxT.current) { clearTimeout(maxT.current); maxT.current = null; }
  }, []);

  useEffect(() => {
    return queryClient.getQueryCache().subscribe(() => {
      if (!active.current) return;
      if (queryClient.isFetching() > 0) {
        sawInFlight.current = true;
      } else if (sawInFlight.current) {
        stop();
      }
    });
  }, [queryClient, stop]);

  useEffect(() => () => {
    active.current = false;
    if (grace.current) clearTimeout(grace.current);
    if (maxT.current) clearTimeout(maxT.current);
  }, []);

  const arm = useCallback(() => {
    active.current = true;
    sawInFlight.current = false;
    setUpdating(true);
    if (grace.current) clearTimeout(grace.current);
    if (maxT.current) clearTimeout(maxT.current);
    // If no fetch has started shortly after the change, the result was served from
    // cache (query key within staleTime): clear rather than hang on the safety timer.
    grace.current = setTimeout(() => { if (!sawInFlight.current) stop(); }, 500);
    maxT.current = setTimeout(stop, maxMs);  // hard safety so it can never stick
  }, [maxMs, stop]);

  return { updating, arm };
}

// Shared inline spinner (matches SourceLabelFilter's).
export const UPDATING_SPINNER_PATH = "M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z";
