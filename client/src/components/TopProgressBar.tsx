import { useIsFetching } from "@tanstack/react-query";
import { Spinner } from "./Spinner";

export function TopProgressBar() {
  const fetching = useIsFetching();
  if (!fetching) return null;
  return (
    <div className="pointer-events-none fixed left-1/2 top-1 z-9999 -translate-x-1/2">
      <Spinner size="xs" />
    </div>
  );
}
