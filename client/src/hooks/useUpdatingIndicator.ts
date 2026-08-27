import { useEffect, useRef, useState } from "react";
import { useIsFetching } from "@tanstack/react-query";

// Drives the same "Updating…" spinner the data-source filter uses, for dropdowns
// whose change triggers a background refetch (workspace filter, date range) rather
// than an awaited invalidate. Call `arm()` when the user applies a change; `updating`
// stays true from then until a fetch cycle completes (seen in-flight, then settled),
// with a safety timeout so it never sticks if nothing actually refetches.
export function useUpdatingIndicator(maxMs = 12000): { updating: boolean; arm: () => void } {
  const fetching = useIsFetching();
  const [updating, setUpdating] = useState(false);
  const sawInFlight = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (!updating) return;
    if (fetching > 0) {
      sawInFlight.current = true;
    } else if (sawInFlight.current) {
      setUpdating(false);
      sawInFlight.current = false;
    }
  }, [updating, fetching]);

  useEffect(() => () => { if (timer.current) clearTimeout(timer.current); }, []);

  const arm = () => {
    sawInFlight.current = false;
    setUpdating(true);
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => { setUpdating(false); sawInFlight.current = false; }, maxMs);
  };

  return { updating, arm };
}

// Shared inline spinner (matches SourceLabelFilter's).
export const UPDATING_SPINNER_PATH = "M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z";
