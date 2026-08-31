import { useId } from "react";
import { useQuery } from "@tanstack/react-query";
import { Clock } from "lucide-react";
import {
  abbreviateCommit,
  formatDeploymentBadgeDate,
  formatDeploymentTimestamp,
  type DeploymentMetadata,
} from "@/utils/deploymentMetadata";

export function DeploymentBadge({
  metadata,
  loading = false,
}: {
  metadata: DeploymentMetadata | null | undefined;
  loading?: boolean;
}) {
  const tooltipId = useId();
  const badgeDate = formatDeploymentBadgeDate(metadata?.deployed_at ?? null);
  const deployedAt = formatDeploymentTimestamp(metadata?.deployed_at ?? null);
  const commit = abbreviateCommit(metadata?.commit_sha ?? null);
  const triggerText = badgeDate ?? "Deploy info";

  return (
    <span data-testid="deployment-badge" className="deployment-badge relative shrink-0">
      <button
        type="button"
        aria-label={`Deployment information: ${triggerText}`}
        aria-describedby={tooltipId}
        className="rail-control-border inline-flex h-[32px] items-center gap-[6px] rounded-[8px] border bg-white/[.07] px-[8px] text-[12px] font-medium text-[#E9EFED] transition-colors hover:bg-[#243F49] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35 focus-visible:ring-offset-1 focus-visible:ring-offset-[#1B3139] xl:px-[9px]"
      >
        <Clock size={15} strokeWidth={1.8} aria-hidden="true" />
        <span data-testid="deployment-badge-date" className="hidden whitespace-nowrap xl:inline">
          {triggerText}
        </span>
      </button>
      <span
        id={tooltipId}
        role="tooltip"
        className="deployment-badge-tooltip pointer-events-none absolute left-0 top-full z-50 mt-2 w-max max-w-[min(520px,80vw)] rounded-[6px] border border-white/[.10] bg-[#0B2026] px-3 py-2 text-[11.5px] leading-relaxed text-white shadow-lg"
      >
        {loading ? (
          "Loading deployment details…"
        ) : (
          <>
            <span>{deployedAt ? `Deployed ${deployedAt}` : "Deployment date unavailable"}</span>
            {metadata?.deployer && <span> · by {metadata.deployer}</span>}
            {commit && <span> · commit {commit}</span>}
          </>
        )}
      </span>
    </span>
  );
}

export function DeploymentBadgeFromApi() {
  const { data, isLoading } = useQuery<DeploymentMetadata | null>({
    queryKey: ["deployment-metadata"],
    queryFn: async () => {
      const response = await fetch("/api/deployment");
      if (!response.ok) return null;
      return response.json();
    },
    staleTime: Infinity,
    retry: 1,
  });

  return <DeploymentBadge metadata={data} loading={isLoading} />;
}
