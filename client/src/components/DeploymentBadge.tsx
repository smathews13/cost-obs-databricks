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
  triggerClassName = "inline-flex h-[22px] w-[88px] min-w-0 shrink-0 items-center justify-center gap-[3px] overflow-hidden rounded-[4px] border border-green-300/20 bg-green-500/20 px-[6px] text-[10px] font-semibold leading-[10px] text-green-200 transition-colors hover:bg-green-400/25 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-green-300/40",
}: {
  metadata: DeploymentMetadata | null | undefined;
  loading?: boolean;
  triggerClassName?: string;
}) {
  const badgeDate = formatDeploymentBadgeDate(metadata?.deployed_at ?? null);
  const deployedAt = formatDeploymentTimestamp(metadata?.deployed_at ?? null);
  const commit = abbreviateCommit(metadata?.commit_sha ?? null);
  const triggerText = badgeDate ?? "Deploy info";
  const processStartApproximation = metadata?.source?.includes("process_start") ?? false;

  return (
    <span data-testid="deployment-badge" className="shrink-0">
      <InfoPopover
        label={`Deployment information: ${triggerText}`}
        className=""
        placement="bottom"
        panelClassName="w-max max-w-[calc(100vw-1rem)] border border-white/[.10] bg-[#0B2026] text-[11.5px]"
        triggerClassName={triggerClassName}
        content={loading ? (
          "Loading deployment details…"
        ) : (
          <>
            <span>
              {deployedAt
                ? processStartApproximation
                  ? `Server process started approximately ${deployedAt} (may reflect a restart, not deployment)`
                  : `Deployed ${deployedAt}`
                : "Deployment date unavailable"}
            </span>
            {metadata?.deployer && <span> · by {metadata.deployer}</span>}
            {commit && <span> · commit {commit}</span>}
          </>
        )}
      >
        <Clock size={11} strokeWidth={1.8} aria-hidden="true" />
        <span data-testid="deployment-badge-date" className="hidden whitespace-nowrap lg:inline">
          {triggerText}
        </span>
      </InfoPopover>
    </span>
  );
}

export function DeploymentBadgeFromApi({
  triggerClassName,
}: {
  triggerClassName?: string;
}) {
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

  return <DeploymentBadge metadata={data} loading={isLoading} triggerClassName={triggerClassName} />;
}
