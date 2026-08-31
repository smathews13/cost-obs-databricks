import { useQuery } from "@tanstack/react-query";
import { Clock } from "lucide-react";
import { InfoPopover } from "@/components/ui/InfoPopover";
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
  const badgeDate = formatDeploymentBadgeDate(metadata?.deployed_at ?? null);
  const deployedAt = formatDeploymentTimestamp(metadata?.deployed_at ?? null);
  const commit = abbreviateCommit(metadata?.commit_sha ?? null);
  const triggerText = badgeDate ?? "Deploy info";

  return (
    <span data-testid="deployment-badge" className="shrink-0">
      <InfoPopover
        label={`Deployment information: ${triggerText}`}
        className=""
        placement="bottom"
        panelClassName="w-max max-w-[calc(100vw-1rem)] border border-white/[.10] bg-[#0B2026] text-[11.5px]"
        triggerClassName="rail-control-border inline-flex h-[32px] items-center gap-[6px] rounded-[8px] border bg-white/[.07] px-[8px] text-[12px] font-medium text-[#E9EFED] transition-colors hover:bg-[#243F49] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35 focus-visible:ring-offset-1 focus-visible:ring-offset-[#1B3139] lg:px-[9px]"
        content={loading ? (
          "Loading deployment details…"
        ) : (
          <>
            <span>{deployedAt ? `Deployed ${deployedAt}` : "Deployment date unavailable"}</span>
            {metadata?.deployer && <span> · by {metadata.deployer}</span>}
            {commit && <span> · commit {commit}</span>}
          </>
        )}
      >
        <Clock size={15} strokeWidth={1.8} aria-hidden="true" />
        <span data-testid="deployment-badge-date" className="hidden whitespace-nowrap lg:inline">
          {triggerText}
        </span>
      </InfoPopover>
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
