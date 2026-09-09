import { useEffect, useMemo, useRef, useState } from "react";
import awsLogo from "@/assets/aws.png";
// Downsampled from the official 3000x3000 PNG: 128x128, 10,539 bytes.
import azureLogo from "@/assets/azure-128.png";
import gcpLogo from "@/assets/gcp.svg";
import { KPITrendModal } from "./KPITrendModal";
import { LoadingPanels, Spinner } from "./Spinner";
import {
  BarChart,
  Bar,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { format, parseISO } from "date-fns";
import type {
  AWSCostsResponse,
  TimeseriesResponse,
  AWSActualDashboardBundle,
  AzureActualDashboardBundle,
  GCPActualDashboardBundle,
  InfraCostsResponse,
  InfraCostsTimeseriesResponse,
  InfraBillingSummary,
} from "@/types/billing";
import { formatCurrency, formatKpiCurrency, formatNumber, workspaceUrl } from "@/utils/formatters";
import { getCloudInstanceFamily } from "@/utils/cloudCosts";
import { StatusIndicator } from "./StatusIndicator";
import { AzureActualView } from "./AzureActualView";
import { GCPActualView } from "./GCPActualView";
import { VirtualizedList } from "./VirtualizedList";
import { AWSActualView } from "./AWSActualView";
import { CloudIntegrationWizard } from "./CloudIntegrationWizard";
import type { CloudIntegration } from "./CloudIntegrationWizard";
import { C } from "@/theme";
import { PageHero, InfoPanel } from "@/components/brand";
import { InfoPopover as InfoTooltip } from "@/components/ui/InfoPopover";
import { FloatingMenu } from "@/components/ui/FloatingMenu";
import { SortableHeader } from "@/components/ui/SortableHeader";
import { KPICard } from "@/components/ui/KPICard";

type CostMode = "estimated" | "actual";
const EMPTY_CLUSTERS: AWSCostsResponse["clusters"] = [];
const EMPTY_INSTANCE_FAMILIES: AWSCostsResponse["instance_families"] = [];

interface CloudCostsViewProps {
  data: AWSCostsResponse | undefined;
  isLoading: boolean;
  timeseriesData: TimeseriesResponse | undefined;
  timeseriesLoading: boolean;
  host: string | null | undefined;
  actualData?: AWSActualDashboardBundle;
  actualLoading?: boolean;
  azureActualData?: AzureActualDashboardBundle;
  azureActualLoading?: boolean;
  gcpActualData?: GCPActualDashboardBundle;
  gcpActualLoading?: boolean;
  infraData?: InfraCostsResponse;
  infraLoading?: boolean;
  infraTimeseriesData?: InfraCostsTimeseriesResponse;
  infraTimeseriesLoading?: boolean;
  startDate?: string;
  endDate?: string;
  detectedCloud?: string;
  workspaceNameMap?: Record<string, string>;
  workspaceIds?: string[];
  accountPricingApplied?: boolean;
  loadError?: string;
  partialReasons?: Record<string, string>;
}

type SortField = "cluster_name" | "databricks_spend" | "total_dbu_hours" | "days_active";
type SortDirection = "asc" | "desc";

const FAMILY_PALETTE = [
  C.s1, C.s2, C.s3, C.s4, C.s5,
];

const INSTANCE_COLORS: Record<string, string> = {
  i3: C.s2, i3en: C.s2, i4i: C.s5,
  m4: C.s3, m5: C.s3, m5d: C.s3, m5n: C.s3,
  m6i: C.s3, m6id: C.s3, m7g: C.s3, m7gd: C.s3, m7i: C.s3,
  r5: C.s4, r5d: C.s4, r6id: C.s4, r6gd: C.s4, r8gd: C.s4,
  c5: C.s5, c5d: C.s5, c6gd: C.s5,
  g4dn: C.s1, g5: C.s1,
  p3: C.lava,
  "rd-fleet": C.s5, "rgd-fleet": C.s5,
  Standard_D: C.s2, Standard_DS: C.s2,
  Standard_E: C.s3, Standard_ES: C.s3,
  Standard_F: C.s5, Standard_FS: C.s5,
  Standard_L: C.s4, Standard_LS: C.s4,
  Standard_M: C.lava,
  Standard_NC: C.s1, Standard_ND: C.s1, Standard_NV: C.s3,
  unknown: C.slate,
};

function getInstanceColor(name: string, index: number): string {
  return INSTANCE_COLORS[name] || FAMILY_PALETTE[index % FAMILY_PALETTE.length];
}

function formatDate(dateStr: string): string {
  try {
    return format(parseISO(dateStr), "MMM d");
  } catch {
    return dateStr;
  }
}

function getClusterUrl(host: string | null | undefined, clusterId: string | null, workspaceId: string | null): string | null {
  if (!host || !clusterId) return null;
  const workspaceParam = workspaceId ? `?o=${workspaceId}` : '';
  return workspaceUrl(host, `/compute/interactive${workspaceParam}`);
}

export function CloudCostsView({
  data,
  isLoading,
  timeseriesData,
  timeseriesLoading,
  host: _host,
  actualData,
  actualLoading,
  azureActualData,
  azureActualLoading,
  gcpActualData,
  gcpActualLoading,
  infraData,
  infraLoading,
  infraTimeseriesData,
  infraTimeseriesLoading,
  startDate,
  endDate,
  detectedCloud,
  workspaceNameMap,
  workspaceIds,
  accountPricingApplied = false,
  loadError,
  partialReasons,
}: CloudCostsViewProps) {
  const [sortField, setSortField] = useState<SortField>("databricks_spend");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [currentPage, setCurrentPage] = useState(1);
  const [showHistoricalClusters, setShowHistoricalClusters] = useState(false);
  const [selectedKPI, setSelectedKPI] = useState<{kpi: string; label: string} | null>(null);
  const [selectedFamilies, setSelectedFamilies] = useState<Set<string>>(new Set());
  const [tableFamily, setTableFamily] = useState<string[]>([]);
  const tableFamilySeen = useRef<Set<string>>(new Set());
  const [tableWorkspace, setTableWorkspace] = useState<string[]>([]);
  const tableWorkspaceSeen = useRef<Set<string>>(new Set());
  const [tableWorkspaceSearch, setTableWorkspaceSearch] = useState("");
  const [clusterSearch, setClusterSearch] = useState("");
  const [familyFilterOpen, setFamilyFilterOpen] = useState(false);
  const [workspaceFilterOpen, setWorkspaceFilterOpen] = useState(false);
  const familyFilterRef = useRef<HTMLDivElement>(null);
  const workspaceFilterRef = useRef<HTMLDivElement>(null);

  const itemsPerPage = 10;

  const [activeActualCloud, setActiveActualCloud] = useState<"AWS" | "AZURE" | "GCP">(() => {
    const c = (detectedCloud || "AWS").toUpperCase();
    if (c === "AZURE") return "AZURE";
    if (c === "GCP") return "GCP";
    return "AWS";
  });

  const INTEGRATIONS_KEY = "cost-obs-cloud-integrations";
  const [cloudIntegrations, setCloudIntegrations] = useState<CloudIntegration[]>(() => {
    try { return JSON.parse(localStorage.getItem(INTEGRATIONS_KEY) || "[]"); } catch { return []; }
  });
  const [showIntegrationWizard, setShowIntegrationWizard] = useState(false);
  const [wizardCloud, setWizardCloud] = useState<"azure" | "aws" | "gcp" | null>(null);
  const [wizardExpandedStep, setWizardExpandedStep] = useState<number | null>(null);
  const [viewingIntegration, setViewingIntegration] = useState<CloudIntegration | null>(null);

  const addIntegration = (cloud: "azure" | "aws" | "gcp") => {
    if (cloudIntegrations.length >= 3) return;
    const newInt: CloudIntegration = { id: Date.now().toString(), cloud, label: cloud === "azure" ? "Azure" : cloud === "gcp" ? "GCP" : "AWS" };
    const updated = [...cloudIntegrations, newInt];
    setCloudIntegrations(updated);
    localStorage.setItem(INTEGRATIONS_KEY, JSON.stringify(updated));
  };

  const removeIntegration = (id: string) => {
    const updated = cloudIntegrations.filter(i => i.id !== id);
    setCloudIntegrations(updated);
    localStorage.setItem(INTEGRATIONS_KEY, JSON.stringify(updated));
  };

  const openWizardForExisting = (integration: CloudIntegration) => {
    setWizardCloud(integration.cloud);
    setWizardExpandedStep(null);
    setShowIntegrationWizard(true);
    setViewingIntegration(integration);
  };

  useEffect(() => {
    if (!familyFilterOpen && !workspaceFilterOpen) return;
    const handler = (e: MouseEvent) => {
      if (familyFilterRef.current && !familyFilterRef.current.contains(e.target as Node)) {
        setFamilyFilterOpen(false);
      }
      if (workspaceFilterRef.current && !workspaceFilterRef.current.contains(e.target as Node)) {
        setWorkspaceFilterOpen(false);
      }
    };
    const keyHandler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setFamilyFilterOpen(false);
        setWorkspaceFilterOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    document.addEventListener("keydown", keyHandler);
    return () => {
      document.removeEventListener("mousedown", handler);
      document.removeEventListener("keydown", keyHandler);
    };
  }, [familyFilterOpen, workspaceFilterOpen]);

  const MINIMIZE_KEY = "cost-obs-minimize-infra-info";
  const [infoMinimized, setInfoMinimized] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem(MINIMIZE_KEY) === "true";
    }
    return false;
  });

  const handleMinimizeToggle = (checked: boolean) => {
    setInfoMinimized(checked);
    if (checked) {
      localStorage.setItem(MINIMIZE_KEY, "true");
    } else {
      localStorage.removeItem(MINIMIZE_KEY);
    }
  };

  const cloud = infraData?.cloud || detectedCloud || "AWS";
  const cloudDisplayName = cloud.toUpperCase() === "AZURE" ? "Azure" : cloud.toUpperCase() === "GCP" ? "GCP" : "AWS";
  const isAzure = cloud.toUpperCase() === "AZURE";
  const isGCP = cloud.toUpperCase() === "GCP";
  const daysCount = startDate && endDate
    ? Math.round((new Date(endDate).getTime() - new Date(startDate).getTime()) / 86400000) + 1
    : null;

  const awsActualAvailable = actualData?.available === true;
  const azureActualAvailable = azureActualData?.available === true;
  const gcpActualAvailable = gcpActualData?.available === true;
  const multipleActualAvailable = [awsActualAvailable, azureActualAvailable, gcpActualAvailable].filter(Boolean).length > 1;
  const actualAvailable = awsActualAvailable || azureActualAvailable || gcpActualAvailable;
  const detectedActualCloud = (detectedCloud || "").toUpperCase() as "AWS" | "AZURE" | "GCP";
  const detectedActualAvailable =
    detectedActualCloud === "AWS" ? awsActualAvailable :
    detectedActualCloud === "AZURE" ? azureActualAvailable :
    detectedActualCloud === "GCP" ? gcpActualAvailable :
    false;
  const preferredActualCloud: "AWS" | "AZURE" | "GCP" =
    detectedActualAvailable
      ? detectedActualCloud
      : awsActualAvailable ? "AWS" : azureActualAvailable ? "AZURE" : "GCP";
  const activeActualCloudAvailable =
    activeActualCloud === "AWS" ? awsActualAvailable :
    activeActualCloud === "AZURE" ? azureActualAvailable :
    gcpActualAvailable;

  const cloudTabs: Array<{ key: "AWS" | "AZURE" | "GCP"; label: string; logo: string; activeClass: string; available: boolean }> = [
    { key: "AWS",   label: "AWS",   logo: awsLogo,   activeClass: "text-orange-600", available: awsActualAvailable },
    { key: "AZURE", label: "Azure", logo: azureLogo, activeClass: "text-blue-600",   available: azureActualAvailable },
    { key: "GCP",   label: "GCP",   logo: gcpLogo,   activeClass: "text-blue-500",   available: gcpActualAvailable },
  ];
  const CloudTabSwitcher = multipleActualAvailable ? (
    <div className="flex items-center gap-1 rounded-lg bg-gray-100 p-1">
      {cloudTabs.filter(t => t.available).map(t => (
        <button
          key={t.key}
          onClick={() => setActiveActualCloud(t.key)}
          aria-label={`${t.label} actual costs`}
          aria-pressed={activeActualCloud === t.key}
          className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
            activeActualCloud === t.key ? `bg-white shadow ${t.activeClass}` : "text-gray-600 hover:text-gray-900"
          }`}
        >
          <img src={t.logo} className="h-3.5 w-3.5 object-contain" alt={t.label} />
          {t.label}
        </button>
      ))}
    </div>
  ) : null;
  const [costMode, setCostMode] = useState<CostMode>("estimated");

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("desc");
      setCurrentPage(1);
    }
  };

  const showLoading = isLoading || infraLoading || (costMode === "actual" && (
    activeActualCloud === "AZURE" ? azureActualLoading :
    activeActualCloud === "GCP"   ? gcpActualLoading :
    actualLoading
  ));

  const ModeToggle = actualAvailable ? (
    <div className="mb-6 flex items-center justify-between pr-12">
      <div className="flex items-center gap-2">
        <span className="inline-flex items-center gap-1.5 rounded-full bg-orange-100 px-3 py-1 text-sm font-medium text-orange-800">
          <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          {multipleActualAvailable ? "Multi-Cloud Cost Data Available" : azureActualAvailable ? "Azure Cost Data Available" : gcpActualAvailable ? "GCP Cost Data Available" : "AWS CUR Data Available"}
        </span>
        {CloudTabSwitcher}
      </div>
      <div className="flex items-center gap-2">
        <div className="flex rounded-lg bg-gray-100 p-1">
          <button
            onClick={() => {
              if (!activeActualCloudAvailable) setActiveActualCloud(preferredActualCloud);
              setCostMode("actual");
            }}
            aria-pressed={costMode === "actual"}
            className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
              costMode === "actual"
                ? "bg-white text-orange-600 shadow"
                : "text-gray-600 hover:text-gray-900"
            }`}
          >
            Actual Costs
          </button>
          <button
            onClick={() => setCostMode("estimated")}
            aria-pressed={costMode === "estimated"}
            className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
              costMode === "estimated"
                ? "bg-white text-orange-600 shadow"
                : "text-gray-600 hover:text-gray-900"
            }`}
          >
            Usage & Metadata
          </button>
        </div>
        {cloudIntegrations.length < 3 && (
          <button
            onClick={() => { setWizardCloud(null); setWizardExpandedStep(null); setViewingIntegration(null); setShowIntegrationWizard(true); }}
            className="inline-flex items-center gap-1.5 rounded-[8px] bg-[#2272B4] px-4 py-1.5 text-sm font-semibold text-white transition-colors hover:bg-[#1B5F96] focus-visible:outline-none focus-visible:shadow-(--focus)"
          >
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            Integrate cloud costs
          </button>
        )}
      </div>
    </div>
  ) : null;

  const EstimationInfoBox = data && (
    <InfoPanel
      title="Cloud Costs tab methodology"
      minimized={infoMinimized}
      onToggle={handleMinimizeToggle}
    >
      {isAzure ? (
        <ul className="list-inside list-disc space-y-1">
          <li>Shows Databricks DBUs, {accountPricingApplied ? "account-price" : "list-price"} spend, active clusters, and Azure VM instance metadata</li>
          <li>Databricks spend comes from billed DBU usage {accountPricingApplied ? "with the account pricing factor applied" : "joined to the applicable system list price"}</li>
          <li>DBUs are not VM node-hours, so the app does not infer an Azure currency cost from them</li>
          <li>Connect Azure Cost Management for authoritative VM, disk, and network costs</li>
        </ul>
      ) : isGCP ? (
        <ul className="list-inside list-disc space-y-1">
          <li>Shows Databricks DBUs, {accountPricingApplied ? "account-price" : "list-price"} spend, active clusters, and GCP machine-type metadata</li>
          <li>Databricks spend comes from billed DBU usage {accountPricingApplied ? "with the account pricing factor applied" : "joined to the applicable system list price"}</li>
          <li>DBUs are not VM node-hours, so the app does not infer a GCP currency cost from them</li>
          <li>Connect GCP Billing Export for authoritative VM, disk, storage, and network costs</li>
        </ul>
      ) : (
        <ul className="list-inside list-disc space-y-1">
          <li>Shows Databricks DBUs, {accountPricingApplied ? "account-price" : "list-price"} spend, active clusters, and EC2 instance-type metadata</li>
          <li>Databricks spend comes from billed DBU usage {accountPricingApplied ? "with the account pricing factor applied" : "joined to the applicable system list price"}</li>
          <li>DBUs are not VM node-hours, so the app does not infer an AWS currency cost from them</li>
          <li>Connect AWS CUR 2.0 for authoritative EC2, EBS, network, and discount-adjusted costs</li>
        </ul>
      )}
      <p className="mt-3 text-xs italic" style={{ color: C.slate }}>
        For exact costs, integrate {isAzure ? "Azure Cost Management" : isGCP ? "GCP Billing Export" : "AWS CUR 2.0"} below.
      </p>
    </InfoPanel>
  );

  const CurSetupBanner = !actualAvailable ? (
    <div className="mb-6">
      <div className="flex items-center justify-between gap-3 pr-12">
        <div className="flex items-center gap-3">
          <div className="flex rounded-lg bg-gray-100 p-1">
            <button
              onClick={() => setCostMode("actual")}
              aria-pressed={costMode === "actual"}
              className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
                costMode === "actual" ? "bg-white text-orange-600 shadow" : "text-gray-500 hover:text-gray-900"
              }`}
              title={`Configure ${isAzure ? "Azure Cost Management Export" : isGCP ? "GCP Billing Export" : "AWS CUR"} to enable actual costs`}
            >
              Actual Costs
            </button>
            <button
              onClick={() => setCostMode("estimated")}
              aria-pressed={costMode === "estimated"}
              className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
                costMode === "estimated" ? "bg-white text-orange-600 shadow" : "text-gray-500 hover:text-gray-900"
              }`}
            >
              Usage & Metadata
            </button>
          </div>
          <span className="flex items-center gap-1.5 text-sm text-gray-500">
            <svg className="h-4 w-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Add cloud billing integrations to see actual costs from {isAzure ? "Azure Cost Management" : isGCP ? "GCP Billing Export" : "AWS CUR"} alongside your estimates.
          </span>
        </div>
        {cloudIntegrations.length < 3 && (
          <button
            onClick={() => { setWizardCloud(null); setWizardExpandedStep(null); setViewingIntegration(null); setShowIntegrationWizard(true); }}
            className="inline-flex items-center gap-1.5 rounded-[8px] bg-[#2272B4] px-4 py-1.5 text-sm font-semibold text-white transition-colors hover:bg-[#1B5F96] focus-visible:outline-none focus-visible:shadow-(--focus)"
          >
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            Integrate cloud costs
          </button>
        )}
      </div>

      {cloudIntegrations.length > 0 && (
        <div className="mt-3 space-y-2">
          <div className="text-xs font-medium uppercase tracking-wider text-gray-500">Additional Cloud Integrations</div>
          {cloudIntegrations.map((integration) => (
            <div key={integration.id} className="flex items-center justify-between bg-white px-3 py-2.5" style={{ border: `1px solid ${C.hairline}`, borderRadius: 12 }}>
              <div className="flex items-center gap-2">
                <img
                  src={integration.cloud === "azure" ? azureLogo : integration.cloud === "gcp" ? gcpLogo : awsLogo}
                  alt=""
                  className="h-5 w-5 object-contain"
                />
                <span className="text-sm text-gray-700">{integration.cloud === "azure" ? "Azure Cost Management Export" : integration.cloud === "gcp" ? "GCP Billing Export (BigQuery)" : "AWS CUR 2.0"}</span>
                <span className="rounded px-2 py-0.5 text-xs font-medium" style={{ background: C.amberTint, color: C.amberInk }}>
                  Checklist saved
                </span>
              </div>
              <div className="flex items-center gap-2">
                <button onClick={() => openWizardForExisting(integration)} className="rounded px-2 py-1 text-xs font-medium focus-visible:outline-none focus-visible:shadow-(--focus)" style={{ color: C.lava }}>
                  View setup guide
                </button>
                <button
                  onClick={() => removeIntegration(integration.id)}
                  aria-label={`Remove ${integration.label} integration`}
                  className="rounded p-1 text-gray-500 hover:bg-red-50 hover:text-red-500"
                  title="Remove integration"
                >
                  <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  ) : null;

  const billingSummary: InfraBillingSummary | undefined = infraData?.billing_summary;
  const IntegrationWizard = (
    <CloudIntegrationWizard
      show={showIntegrationWizard}
      onClose={() => { setShowIntegrationWizard(false); setViewingIntegration(null); }}
      wizardCloud={wizardCloud}
      setWizardCloud={setWizardCloud}
      wizardExpandedStep={wizardExpandedStep}
      setWizardExpandedStep={setWizardExpandedStep}
      viewingIntegration={viewingIntegration}
      cloudIntegrations={cloudIntegrations}
      addIntegration={addIntegration}
      isAzure={isAzure}
      isGCP={isGCP}
      awsActualAvailable={awsActualAvailable}
      azureActualAvailable={azureActualAvailable}
      gcpActualAvailable={gcpActualAvailable}
    />
  );
  const clusters = data?.clusters ?? EMPTY_CLUSTERS;
  const instanceFamilies = data?.instance_families ?? EMPTY_INSTANCE_FAMILIES;

  const cloudSummary = useMemo(() => {
    if (!data) return { databricksSpend: 0, totalDBUHours: 0, totalClusterCount: 0, avgActiveClustersPerDay: 0, avgDatabricksSpendPerCluster: 0 };
    const bs = billingSummary;
    const clustersWithTypes = data.clusters.filter(c => c.driver_instance_type || c.worker_instance_type);
    return {
      databricksSpend: bs?.databricks_compute_spend ?? data.total_databricks_spend ?? 0,
      totalDBUHours: data.total_dbu_hours ?? 0,
      totalClusterCount: data.total_cluster_count ?? data.clusters.length,
      avgActiveClustersPerDay: bs?.avg_clusters_per_day ?? clustersWithTypes.length,
      avgDatabricksSpendPerCluster: bs?.avg_databricks_spend_per_cluster ?? 0,
    };
  }, [data, billingSummary]);

  const historicalClusterCount = clusters.filter(c => !c.driver_instance_type && !c.worker_instance_type).length;

  const filteredClusters = useMemo(
    () => showHistoricalClusters ? clusters : clusters.filter(c => c.driver_instance_type || c.worker_instance_type),
    [clusters, showHistoricalClusters],
  );

  const familyFilteredClusters = useMemo(
    () => selectedFamilies.size === 0
      ? filteredClusters
      : filteredClusters.filter(c => {
          const df = getCloudInstanceFamily(c.driver_instance_type, cloud);
          const wf = getCloudInstanceFamily(c.worker_instance_type, cloud);
          return selectedFamilies.has(df) || selectedFamilies.has(wf);
        }),
    [filteredClusters, selectedFamilies, cloud],
  );

  const availableTableFamilies = useMemo(() => {
    const families = new Set<string>();
    instanceFamilies.forEach(f => {
      if (f.instance_family && f.instance_family !== 'unknown') families.add(f.instance_family);
    });
    return [...families].sort();
  }, [instanceFamilies]);

  useEffect(() => {
    const seen = tableFamilySeen.current;
    const fresh = availableTableFamilies.filter(x => !seen.has(x));
    if (fresh.length === 0) return;
    setTableFamily(prev => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach(x => seen.add(x));
  }, [availableTableFamilies]);

  const isTableFamilyFilterActive = tableFamily.length > 0 && tableFamily.length < availableTableFamilies.length;

  const availableTableWorkspaces = useMemo(() => {
    const ws = new Set<string>();
    familyFilteredClusters.forEach(c => { if (c.workspace_id) ws.add(c.workspace_id); });
    return [...ws].sort();
  }, [familyFilteredClusters]);

  // Fallback name lookup from cluster rows (populated via backend LEFT JOIN on
  // system.access.workspaces_latest). Used when workspaceNameMap is missing an id.
  const clusterWorkspaceNames = useMemo(() => {
    const map: Record<string, string> = {};
    for (const c of familyFilteredClusters) {
      const wsId = c.workspace_id;
      const wsName = c.workspace_name;
      if (wsId && wsName && !map[wsId]) map[wsId] = wsName;
    }
    return map;
  }, [familyFilteredClusters]);
  const resolveWsName = (id: string) => workspaceNameMap?.[id] || clusterWorkspaceNames[id] || id;

  useEffect(() => {
    const seen = tableWorkspaceSeen.current;
    const fresh = availableTableWorkspaces.filter(x => !seen.has(x));
    if (fresh.length === 0) return;
    setTableWorkspace(prev => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach(x => seen.add(x));
  }, [availableTableWorkspaces]);

  const isTableWorkspaceFilterActive = tableWorkspace.length > 0 && tableWorkspace.length < availableTableWorkspaces.length;

  const tableFilteredClusters = familyFilteredClusters.filter(c => {
    if (isTableFamilyFilterActive) {
      const df = getCloudInstanceFamily(c.driver_instance_type, cloud);
      const wf = getCloudInstanceFamily(c.worker_instance_type, cloud);
      if (!tableFamily.includes(df) && !tableFamily.includes(wf)) return false;
    }
    if (isTableWorkspaceFilterActive && !tableWorkspace.includes(c.workspace_id || "")) return false;
    return true;
  });

  const searchFilteredClusters = clusterSearch
    ? tableFilteredClusters.filter(c =>
        (c.cluster_name || "").toLowerCase().includes(clusterSearch.toLowerCase()) ||
        (c.cluster_id || "").toLowerCase().includes(clusterSearch.toLowerCase())
      )
    : tableFilteredClusters;

  const sortedClusters = [...searchFilteredClusters].sort((a, b) => {
    const modifier = sortDirection === "asc" ? 1 : -1;
    if (sortField === "cluster_name") {
      return ((a.cluster_name || "").localeCompare(b.cluster_name || "")) * modifier;
    }
    const aVal = (a[sortField] as number) ?? 0;
    const bVal = (b[sortField] as number) ?? 0;
    return (aVal - bVal) * modifier;
  });

  const totalPages = Math.ceil(sortedClusters.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedClusters = sortedClusters.slice(startIndex, endIndex);
  const filteredDatabricksSpend = sortedClusters.reduce(
    (sum, cluster) => sum + (cluster.databricks_spend || 0),
    0,
  );
  const filteredDbuHours = sortedClusters.reduce(
    (sum, cluster) => sum + (cluster.total_dbu_hours || 0),
    0,
  );
  const detailLimit = data?.detail_limit ?? 100;
  const detailTruncated = data?.detail_truncated === true;

  const familyChartData = instanceFamilies
    .filter((f) => f.instance_family && f.instance_family !== "unknown")
    .slice(0, 10)
    .map((f) => ({
      name: f.instance_family,
      value: f.total_dbu_hours,
    }));

  const timeseriesFamilies: string[] = timeseriesData?.instance_families || [];

  const filteredTimeseriesData = useMemo(() => {
    if (!timeseriesData?.timeseries) return null;
    if (selectedFamilies.size === 0) return timeseriesData.timeseries;
    return timeseriesData.timeseries.map((point) => {
      let filteredCost = 0;
      for (const family of selectedFamilies) {
        filteredCost += (point[family] as number) || 0;
      }
      return { ...point, "Cloud Cost": filteredCost };
    });
  }, [timeseriesData, selectedFamilies]);

  if (showLoading) {
    return <LoadingPanels sections={[
      "Infrastructure Costs",
      "Cluster Costs",
      "Usage by Instance Family",
      "Cluster Cost Breakdown",
    ]} />;
  }

  if (loadError) {
    return (
      <div className="space-y-6">
        <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
          <h3 className="text-lg font-semibold" style={{ color: C.ink }}>
            Cloud cost data could not be loaded
          </h3>
          <p className="mt-2 text-sm" style={{ color: C.body }}>
            Required Databricks usage totals did not complete after automatic retries.
            Wait for current SQL work to finish, then refresh Cloud Costs.
          </p>
          <p className="mt-3 rounded-lg px-3 py-2 text-xs" style={{ background: C.maroonTint, color: C.maroon }}>
            {loadError}
          </p>
        </div>
      </div>
    );
  }

  const actualPartialNotice = (
    <div
      className="rounded-lg border px-4 py-3"
      style={{ borderColor: C.amber, backgroundColor: C.amberTint }}
      role="status"
    >
      <p className="text-sm font-semibold" style={{ color: C.amberInk }}>
        Some actual-cost details are temporarily unavailable
      </p>
      <p className="mt-1 text-sm" style={{ color: C.body }}>
        The authoritative total and available sections are shown. Refresh Cloud Costs
        after current SQL work finishes to retry missing detail.
      </p>
    </div>
  );

  if (costMode === "actual" && activeActualCloud === "AZURE" && azureActualData?.available) {
    return (
      <div className="space-y-6">
        {azureActualData.availability === "partial" && actualPartialNotice}
        <AzureActualView azureActualData={azureActualData} cloudTabSwitcher={CloudTabSwitcher} onSwitchToEstimated={() => setCostMode("estimated")} />
      </div>
    );
  }
  if (costMode === "actual" && activeActualCloud === "GCP" && gcpActualData?.available) {
    return (
      <div className="space-y-6">
        {gcpActualData.availability === "partial" && actualPartialNotice}
        <GCPActualView gcpActualData={gcpActualData} cloudTabSwitcher={CloudTabSwitcher} onSwitchToEstimated={() => setCostMode("estimated")} />
      </div>
    );
  }
  if (costMode === "actual" && activeActualCloud === "AWS" && actualData?.available) {
    return (
      <div className="space-y-6">
        {actualData.availability === "partial" && actualPartialNotice}
        <AWSActualView actualData={actualData} cloudTabSwitcher={CloudTabSwitcher} onSwitchToEstimated={() => setCostMode("estimated")} />
      </div>
    );
  }
  if (costMode === "actual" && !actualAvailable) {
    const transientActualFailure = Object.keys(partialReasons ?? {}).some(
      (name) => name.endsWith("_actual"),
    );
    return (
      <div className="space-y-6">
        {CurSetupBanner}
        <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
          <h3 className="text-lg font-semibold text-gray-900">
            {transientActualFailure
              ? "Actual cloud costs are temporarily unavailable"
              : "Actual cloud costs are not connected yet"}
          </h3>
          <p className="mt-2 text-sm text-gray-500">
            {transientActualFailure
              ? "Usage & Metadata is still available. Switch back now and retry actual costs after current SQL work finishes."
              : "Add an AWS CUR, Azure Cost Management, or GCP Billing export integration to populate this view."}
          </p>
        </div>
        {IntegrationWizard}
      </div>
    );
  }
  if (infraData?.availability === "unavailable" || data?.error) {
    const errorKind = infraData?.error_kind;
    const errorTitle =
      errorKind === "permission"
        ? "Cloud cost permissions are missing"
        : errorKind === "metadata"
          ? "Classic cluster metadata is unavailable"
          : "Cloud cost data could not be loaded";
    return (
      <div className="space-y-6">
        {ModeToggle}
        {CurSetupBanner}
        <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
          <h3 className="text-lg font-semibold" style={{ color: C.ink }}>{errorTitle}</h3>
          <p className="mt-2 text-sm" style={{ color: C.body }}>
            {infraData?.reason_detail || "The cluster query failed, so the app cannot confirm zero classic infrastructure usage."}
          </p>
          {(infraData?.error || data?.error) && (
            <p className="mt-3 rounded-lg px-3 py-2 text-xs" style={{ background: C.maroonTint, color: C.maroon }}>
              {infraData?.error || data?.error}
            </p>
          )}
        </div>
        {IntegrationWizard}
      </div>
    );
  }
  if (!data || clusters.length === 0) {
    const hasBillingSummary = billingSummary != null && (billingSummary.databricks_compute_spend ?? 0) > 0;
    return (
      <div className="space-y-6">
        {ModeToggle}
        {CurSetupBanner}
        {(infraData?.availability === "partial"
          || Object.keys(partialReasons ?? {}).length > 0) && (
          <div
            className="rounded-lg border px-4 py-3"
            style={{ borderColor: C.amber, backgroundColor: C.amberTint }}
            role="status"
          >
            <p className="text-sm font-semibold" style={{ color: C.amberInk }}>
              Partial Cloud Costs data
            </p>
            <p className="mt-1 text-sm" style={{ color: C.body }}>
              {infraData?.reason_detail
                || "Available usage and DBU totals are shown. Refresh Cloud Costs after current SQL work finishes to retry missing detail."}
            </p>
          </div>
        )}
        {hasBillingSummary ? (
          <div className="co-kpi-grid grid grid-cols-1 gap-4 sm:grid-cols-3">
            <KPICard
              title="Databricks Compute Spend"
              value={formatKpiCurrency(billingSummary.databricks_compute_spend ?? 0)}
              subtitle={`${billingSummary.days_in_range ?? 0} days (all-purpose + jobs + DLT)`}
              onActivate={(billingSummary.databricks_compute_spend ?? 0) > 0 && startDate && endDate
                ? () => setSelectedKPI({ kpi: "infra_cost", label: "Daily Databricks Compute Spend" })
                : undefined}
              ariaLabel="See Databricks Compute Spend trend"
            />
            <KPICard title="Avg Active Clusters / Day" value={formatNumber(billingSummary.avg_clusters_per_day ?? 0)} subtitle="daily average" />
            <KPICard
              title="Databricks Spend / Cluster"
              value={formatKpiCurrency(billingSummary.avg_databricks_spend_per_cluster ?? 0)}
              subtitle="per cluster per day"
              onActivate={(billingSummary.avg_databricks_spend_per_cluster ?? 0) > 0 && startDate && endDate
                ? () => setSelectedKPI({ kpi: "avg_cost_per_cluster", label: "Daily Databricks Spend per Cluster" })
                : undefined}
              ariaLabel="See Databricks Spend / Cluster trend"
            />
          </div>
        ) : (
          <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
            <h3 className="text-lg font-semibold text-gray-900">
              {infraData?.reason === "no_usage_for_filter_or_date"
                ? "No cloud usage matches this selection"
                : infraData?.reason === "serverless_only"
                  ? "Only serverless usage was found"
                  : "No classic cluster usage found"}
            </h3>
            <p className="mt-2 text-sm text-gray-500">
              {infraData?.reason_detail ||
                "No matching classic cluster_id usage was found for the selected workspaces and date range."}
            </p>
          </div>
        )}
        {selectedKPI && startDate && endDate && (
          <KPITrendModal
            kpi={selectedKPI.kpi}
            kpiLabel={selectedKPI.label}
            isOpen
            onClose={() => setSelectedKPI(null)}
            startDate={startDate}
            endDate={endDate}
            workspaceIds={workspaceIds}
            queryKeyPrefix="infra-kpi-trend"
          />
        )}
        {IntegrationWizard}
      </div>
    );
  }

  return (
    <div className="animate-fade-in space-y-6">
      {EstimationInfoBox}

      <PageHero
        dateRange={{ startDate, endDate }}
        icon={
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z" />
          </svg>
        }
        title="Cloud Costs"
        subtitle="Classic cluster usage, Databricks spend, and cloud instance metadata"
        workspaceScope={workspaceIds && workspaceIds.length > 0
          ? workspaceIds.length === 1
            ? workspaceNameMap?.[workspaceIds[0]] || workspaceIds[0]
            : `${workspaceIds.length} workspaces`
          : "Account-wide"}
      />
      {ModeToggle}
      {CurSetupBanner}
      {Object.keys(partialReasons ?? {}).length > 0 && (
        <div
          className="rounded-lg border px-4 py-3"
          style={{ borderColor: C.amber, backgroundColor: C.amberTint }}
          role="status"
        >
          <p className="text-sm font-semibold" style={{ color: C.amberInk }}>
            Some cloud cost details are temporarily unavailable
          </p>
          <p className="mt-1 text-sm" style={{ color: C.body }}>
            Available usage, DBU totals, and cluster metadata are shown. Refresh Cloud Costs
            after current SQL work finishes to retry the missing sections.
          </p>
        </div>
      )}
      <div className="co-kpi-grid grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <KPICard
          title="Databricks Compute Spend"
          value={formatKpiCurrency(cloudSummary.databricksSpend)}
          subtitle={startDate && endDate ? (() => {
            const days = Math.round((new Date(endDate).getTime() - new Date(startDate).getTime()) / 86400000) + 1;
            const dailyAvg = cloudSummary.databricksSpend > 0 ? cloudSummary.databricksSpend / days : 0;
            return `${formatCurrency(dailyAvg)}/day avg · ${days} days`;
          })() : undefined}
          onActivate={cloudSummary.databricksSpend > 0 && startDate && endDate ? () => setSelectedKPI({ kpi: "infra_cost", label: "Daily Databricks Compute Spend" }) : undefined}
          ariaLabel="See Databricks Compute Spend trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
        />
        <KPICard
          title="Total Cluster DBUs"
          value={formatNumber(cloudSummary.totalDBUHours)}
          subtitle={`across ${cloudSummary.totalClusterCount} clusters`}
          onActivate={cloudSummary.totalDBUHours > 0 && startDate && endDate ? () => setSelectedKPI({ kpi: "infra_dbu_hours", label: "Daily Cluster DBUs" }) : undefined}
          ariaLabel="See Total Cluster DBUs trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
        />
        <KPICard
          title="Active Clusters"
          value={formatNumber(cloudSummary.avgActiveClustersPerDay)}
          subtitle="daily average"
          infoText="Average number of distinct clusters with billing activity per day in the selected period. Includes all cluster types (job clusters, interactive clusters)."
          onActivate={cloudSummary.avgActiveClustersPerDay > 0 && startDate && endDate ? () => setSelectedKPI({ kpi: "infra_clusters", label: `Daily Active ${cloudDisplayName} Clusters` }) : undefined}
          ariaLabel="See Active Clusters trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 00-2-2m-2-4h.01M17 16h.01" /></svg>}
        />
        <KPICard
          title="Databricks Spend / Cluster"
          value={formatKpiCurrency(cloudSummary.avgDatabricksSpendPerCluster)}
          subtitle={startDate && endDate ? `average over ${Math.round((new Date(endDate).getTime() - new Date(startDate).getTime()) / 86400000) + 1} days` : undefined}
          infoText="Databricks DBU spend divided by active clusters. This is not a cloud VM cost estimate."
          onActivate={cloudSummary.avgDatabricksSpendPerCluster > 0 && startDate && endDate ? () => setSelectedKPI({ kpi: "avg_cost_per_cluster", label: "Daily Databricks Spend per Cluster" }) : undefined}
          ariaLabel="See Databricks Spend / Cluster trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>}
        />
      </div>

      {selectedKPI && startDate && endDate && (
        <KPITrendModal
          kpi={selectedKPI.kpi}
          kpiLabel={selectedKPI.label}
          isOpen={!!selectedKPI}
          onClose={() => setSelectedKPI(null)}
          startDate={startDate}
          endDate={endDate}
          workspaceIds={workspaceIds}
          queryKeyPrefix="infra-kpi-trend"
        />
      )}

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        {(infraTimeseriesLoading || timeseriesLoading) ? (
          <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
            <h3 className="mb-4 text-lg font-semibold text-gray-900">Cluster DBUs{daysCount ? ` over ${daysCount} Days` : ""}</h3>
            <div className="flex h-80 items-center justify-center"><Spinner size="md" /></div>
          </div>
        ) : (infraTimeseriesData?.timeseries && infraTimeseriesData.timeseries.length > 0) ? (
          <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
            <h3 className="mb-4 text-lg font-semibold text-gray-900">Cluster DBUs{daysCount ? ` over ${daysCount} Days` : ""}</h3>
            <ResponsiveContainer width="100%" height={320}>
              <AreaChart data={infraTimeseriesData.timeseries} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="infraCostGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={C.s1} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={C.s1} stopOpacity={0.05} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="date" tickFormatter={formatDate} stroke={C.muted} fontSize={12} tickMargin={8} />
                <YAxis tickFormatter={(v) => formatNumber(v)} stroke={C.muted} fontSize={12} width={80} />
                <Tooltip
                  formatter={(value: number | undefined) => [`${formatNumber(value ?? 0)} DBUs`, "Usage"]}
                  labelFormatter={(label) => format(parseISO(label as string), "MMM d, yyyy")}
                  contentStyle={{ backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" }}
                />
                <Area isAnimationActive={false} type="monotone" dataKey="total_dbu_hours" stroke={C.s1} strokeWidth={2} fill="url(#infraCostGradient)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        ) : (filteredTimeseriesData && filteredTimeseriesData.length > 0) ? (
          <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-lg font-semibold text-gray-900">{cloudDisplayName} Cluster Costs{daysCount ? ` over ${daysCount} Days` : ""} <span className="ml-1 inline-flex items-center rounded px-1.5 py-0.5 align-middle text-[10px] font-semibold tracking-wide" style={{ backgroundColor: C.amberTint, color: C.amberInk, border: `1px solid ${C.hairline}` }}>est.</span></h3>
              {timeseriesFamilies.length > 0 && (
                <div className="flex flex-wrap items-center gap-2">
                  <button
                    onClick={() => {
                      setSelectedFamilies(new Set());
                      setCurrentPage(1);
                    }}
                    className={`rounded-full px-3 py-1 text-xs font-medium ${
                      selectedFamilies.size === 0 ? "text-white" : "bg-blue-50 text-blue-700 hover:bg-blue-100"
                    }`}
                    style={selectedFamilies.size === 0 ? { backgroundColor: C.s2 } : undefined}
                  >
                    All
                  </button>
                  {timeseriesFamilies.filter(f => f !== "unknown").slice(0, 8).map((family, idx) => (
                    <button
                      key={family}
                      onClick={() => {
                        setSelectedFamilies((prev) => {
                          const next = new Set(prev);
                          if (next.has(family)) { next.delete(family); } else { next.add(family); }
                          return next;
                        });
                        setCurrentPage(1);
                      }}
                      className={`rounded-full px-3 py-1 text-xs font-medium ${
                        selectedFamilies.has(family) ? "text-white" : "bg-blue-50 text-blue-700 hover:bg-blue-100"
                      }`}
                      style={selectedFamilies.has(family) ? { backgroundColor: getInstanceColor(family, idx) } : undefined}
                    >
                      {family}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <ResponsiveContainer width="100%" height={320}>
              <AreaChart data={filteredTimeseriesData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="cloudCostGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor={C.s1} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={C.s1} stopOpacity={0.05} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="date" tickFormatter={formatDate} stroke={C.muted} fontSize={12} tickMargin={8} />
                <YAxis tickFormatter={(v) => formatCurrency(v)} stroke={C.muted} fontSize={12} width={80} />
                <Tooltip
                  formatter={(value: number | undefined) => formatCurrency(value ?? 0)}
                  labelFormatter={(label) => format(parseISO(label as string), "MMM d, yyyy")}
                  contentStyle={{ backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" }}
                />
                <Area isAnimationActive={false} type="monotone" dataKey="Cloud Cost" stroke={C.s1} strokeWidth={2} fill="url(#cloudCostGradient)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        ) : null}

        <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
          <h3 className="mb-4 text-lg font-semibold text-gray-900">
            Usage by Instance Family{detailTruncated ? ` · top ${detailLimit} cluster detail` : ""}
          </h3>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={familyChartData} layout="vertical" margin={{ left: 10, right: 20 }}>
              <XAxis type="number" tickFormatter={(v) => `${(v / 1000).toFixed(0)}K`} stroke={C.muted} fontSize={12} tickMargin={8} />
              <YAxis type="category" dataKey="name" width={100} fontSize={12} stroke={C.muted} interval={0} />
              <Tooltip
                formatter={(value: number | undefined) => [formatNumber(value ?? 0) + " DBUs", "Usage"]}
                contentStyle={{ backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" }}
              />
              <Bar isAnimationActive={false} dataKey="value" radius={[0, 4, 4, 0]}>
                {familyChartData.map((entry, index) => (
                  <Cell key={entry.name} fill={getInstanceColor(entry.name, index)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between">
          <div>
            <h3 className="flex items-center gap-2 text-lg font-semibold text-gray-900">
              {cloudDisplayName} Cluster Leaderboard
              <InfoTooltip className="" text="Databricks spend uses billed DBU usage and system list prices. Cloud VM currency cost still requires a cloud billing integration or authoritative node-hour data." />
            </h3>
            <p className="text-sm text-gray-500">
              {detailTruncated
                ? `Showing top ${detailLimit} of ${cloudSummary.totalClusterCount} clusters by DBU usage`
                : `${sortedClusters.length} cluster${sortedClusters.length !== 1 ? "s" : ""}`}
              {selectedFamilies.size > 0 ? ` · ${[...selectedFamilies].join(", ")} only` : ""}{isTableFamilyFilterActive ? ` · filtered to ${tableFamily.length} families` : ""}{isTableWorkspaceFilterActive ? ` · filtered to ${tableWorkspace.length} workspaces` : ""}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
              <input
                type="checkbox"
                checked={showHistoricalClusters}
                onChange={(e) => { setShowHistoricalClusters(e.target.checked); setCurrentPage(1); }}
                className="h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
              />
              <span>Show historical clusters ({historicalClusterCount})</span>
            </label>
            <InfoTooltip className="" label="About historical clusters" text="Historical clusters have no instance type information available. These are typically old or deleted clusters that no longer have detailed configuration data." />
            {availableTableWorkspaces.length > 1 && (
              <div className="relative" ref={workspaceFilterRef}>
                <button
                  type="button"
                  onClick={() => { setWorkspaceFilterOpen(o => !o); setFamilyFilterOpen(false); setTableWorkspaceSearch(""); }}
                  aria-haspopup="menu"
                  aria-expanded={workspaceFilterOpen}
                  className="co-filter flex h-auto items-center gap-1.5 px-3 py-1 text-xs"
                  style={isTableWorkspaceFilterActive ? { borderColor: C.lava, color: C.lava } : undefined}
                >
                  {!isTableWorkspaceFilterActive ? "Workspace" : tableWorkspace.length === 1 ? resolveWsName(tableWorkspace[0]) : `${tableWorkspace.length} workspaces`}
                  <svg className={`h-3 w-3 transition-transform ${workspaceFilterOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {workspaceFilterOpen && (
                  <FloatingMenu anchorRef={workspaceFilterRef} role="menu" aria-label="Filter by workspace" className="co-filter-menu w-72">
                    <div className="p-2">
                      <input
                        type="text"
                        value={tableWorkspaceSearch}
                        onChange={(e) => setTableWorkspaceSearch(e.target.value)}
                        placeholder="Search workspaces..."
                        className="w-full rounded-md border border-gray-300 px-3 py-1.5 text-sm focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
                        autoFocus
                      />
                    </div>
                    <div>
                      <div className="sticky top-0 flex items-center justify-between bg-white px-3 py-2" style={{ borderBottom: `1px solid ${C.hairline}` }}>
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Workspace</span>
                        <div className="flex items-center gap-2 text-xs">
                          <button onClick={(e) => { e.stopPropagation(); setTableWorkspace([...availableTableWorkspaces]); setCurrentPage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                          <span className="text-gray-300">·</span>
                          <button onClick={(e) => { e.stopPropagation(); setTableWorkspace([]); setCurrentPage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                        </div>
                      </div>
                      {(() => {
                        const filtered = availableTableWorkspaces.filter(w => {
                          if (!tableWorkspaceSearch) return true;
                          const q = tableWorkspaceSearch.toLowerCase();
                          return resolveWsName(w).toLowerCase().includes(q) || w.toLowerCase().includes(q);
                        });
                        if (filtered.length === 0) {
                          return <div className="px-3 py-2 text-sm text-gray-500">No matching workspaces</div>;
                        }
                        return (
                          <VirtualizedList
                            items={filtered}
                            itemHeight={36}
                            maxHeight={256}
                            getKey={(w) => w}
                            renderItem={(w) => {
                              const label = resolveWsName(w);
                              return (
                                <button
                                  type="button"
                                  role="menuitemcheckbox"
                                  aria-checked={tableWorkspace.includes(w)}
                                  onClick={() => { setTableWorkspace((prev) => prev.includes(w) ? prev.filter(x => x !== w) : [...prev, w]); setCurrentPage(1); }}
                                  className="flex w-full items-center gap-2 px-3 py-2.5 text-xs hover:bg-(--row-hover) focus-visible:outline-none focus-visible:shadow-[inset_0_0_0_2px_rgba(255,54,33,.35)]"
                                >
                                  <div className="flex h-4 w-4 shrink-0 items-center justify-center rounded border" style={{ borderColor: tableWorkspace.includes(w) ? C.lava : C.hairline, background: tableWorkspace.includes(w) ? C.lava : C.card }}>
                                    {tableWorkspace.includes(w) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                                  </div>
                                  <span className="truncate text-gray-700">{label}</span>
                                </button>
                              );
                            }}
                          />
                        );
                      })()}
                    </div>
                  </FloatingMenu>
                )}
              </div>
            )}
            {availableTableFamilies.length > 0 && (
              <div className="relative" ref={familyFilterRef}>
                <button
                  type="button"
                  onClick={() => { setFamilyFilterOpen(o => !o); setWorkspaceFilterOpen(false); }}
                  aria-haspopup="menu"
                  aria-expanded={familyFilterOpen}
                  className="co-filter flex h-auto items-center gap-1.5 px-3 py-1 text-xs"
                  style={isTableFamilyFilterActive ? { borderColor: C.lava, color: C.lava } : undefined}
                >
                  {!isTableFamilyFilterActive ? "Families" : tableFamily.length === 1 ? tableFamily[0] : `${tableFamily.length} Families`}
                  <svg className={`h-3 w-3 transition-transform ${familyFilterOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {familyFilterOpen && (
                  <FloatingMenu anchorRef={familyFilterRef} role="menu" aria-label="Filter by instance family" className="co-filter-menu max-h-64 min-w-[180px] overflow-y-auto">
                    <div className="sticky top-0 flex items-center justify-between bg-white px-3 py-2" style={{ borderBottom: `1px solid ${C.hairline}` }}>
                      <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Families</span>
                      <div className="flex items-center gap-2 text-xs">
                        <button onClick={(e) => { e.stopPropagation(); setTableFamily([...availableTableFamilies]); setCurrentPage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                        <span className="text-gray-300">·</span>
                        <button onClick={(e) => { e.stopPropagation(); setTableFamily([]); setCurrentPage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                      </div>
                    </div>
                    {availableTableFamilies.map(f => (
                      <button
                        type="button"
                        role="menuitemcheckbox"
                        aria-checked={tableFamily.includes(f)}
                        key={f}
                        onClick={() => { setTableFamily((prev) => prev.includes(f) ? prev.filter(x => x !== f) : [...prev, f]); setCurrentPage(1); }}
                        className="flex w-full items-center gap-2 px-3 py-2.5 text-xs hover:bg-(--row-hover) focus-visible:outline-none focus-visible:shadow-[inset_0_0_0_2px_rgba(255,54,33,.35)]"
                      >
                        <div className="flex h-4 w-4 shrink-0 items-center justify-center rounded border" style={{ borderColor: tableFamily.includes(f) ? C.lava : C.hairline, background: tableFamily.includes(f) ? C.lava : C.card }}>
                          {tableFamily.includes(f) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                        </div>
                        <span className="truncate text-gray-700">{f}</span>
                      </button>
                    ))}
                  </FloatingMenu>
                )}
              </div>
            )}
            <div className="relative">
              <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                placeholder="Search clusters..."
                value={clusterSearch}
                onChange={(e) => { setClusterSearch(e.target.value); setCurrentPage(1); }}
                className="w-44 rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
              />
            </div>
          </div>
        </div>

        <div className="overflow-x-hidden">
          <table className="w-full table-fixed divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <SortableHeader field="cluster_name" activeField={sortField} direction={sortDirection} onSort={(field) => handleSort(field as SortField)} className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                  Cluster
                </SortableHeader>
                <th className="w-44 px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Instance Types</th>
                <SortableHeader
                  field="databricks_spend"
                  activeField={sortField}
                  direction={sortDirection}
                  onSort={(field) => handleSort(field as SortField)}
                  align="right"
                  className="w-28 whitespace-nowrap px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500"
                  accessory={<InfoTooltip className="" stopClick text={`Databricks ${accountPricingApplied ? "account-price" : "list-price"} spend from billed DBU usage. This is not cloud VM cost; connect cloud billing to see VM, disk, network, and infrastructure costs.`} />}
                >
                  DBU Spend
                </SortableHeader>
                <SortableHeader field="total_dbu_hours" activeField={sortField} direction={sortDirection} onSort={(field) => handleSort(field as SortField)} align="right" className="w-28 px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                  DBU Hours
                </SortableHeader>
                <SortableHeader field="days_active" activeField={sortField} direction={sortDirection} onSort={(field) => handleSort(field as SortField)} align="right" className="w-16 px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                  Days
                </SortableHeader>
                <th className="w-16 px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">% Spend</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 bg-white">
              {paginatedClusters.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-3 py-8 text-center text-sm text-gray-500">
                    No clusters match the current filters.
                  </td>
                </tr>
              )}
              {paginatedClusters.map((cluster, idx) => {
                const url = getClusterUrl(_host, cluster.cluster_id, cluster.workspace_id);
                return (
                  <tr key={`${cluster.cluster_id}-${idx}`} className="hover:bg-gray-50">
                    <td className="px-3 py-3">
                      {url ? (
                        <div className="flex flex-col gap-1">
                          <a href={url} target="_blank" rel="noopener noreferrer" className="group flex max-w-xs items-center gap-1 truncate text-sm font-medium text-blue-600 hover:text-blue-800">
                            <span className="truncate">{cluster.cluster_name || cluster.cluster_id}</span>
                            <svg className="h-3 w-3 flex-shrink-0 opacity-0 transition-opacity group-hover:opacity-100" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                            </svg>
                          </a>
                          <div className="flex items-center gap-2">
                            {cluster.state && <StatusIndicator status={cluster.state} type="cluster" />}
                            {cluster.cluster_source && (
                              <span className="inline-flex rounded-full bg-gray-100 px-2 py-0.5 text-xs text-gray-600">{cluster.cluster_source}</span>
                            )}
                            {cluster.cluster_name && cluster.cluster_name !== cluster.cluster_id && (
                              <span className="text-xs text-gray-500">{cluster.cluster_id}</span>
                            )}
                          </div>
                        </div>
                      ) : (
                        <div className="flex flex-col gap-1">
                          <div className="max-w-xs truncate text-sm font-medium text-gray-900">{cluster.cluster_name || cluster.cluster_id}</div>
                          <div className="flex items-center gap-2">
                            {cluster.state && <StatusIndicator status={cluster.state} type="cluster" />}
                            {cluster.cluster_source && (
                              <span className="inline-flex rounded-full bg-gray-100 px-2 py-0.5 text-xs text-gray-600">{cluster.cluster_source}</span>
                            )}
                            {cluster.cluster_name && cluster.cluster_name !== cluster.cluster_id && (
                              <span className="text-xs text-gray-500">{cluster.cluster_id}</span>
                            )}
                          </div>
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-3">
                      <div className="flex flex-col gap-1">
                        {cluster.driver_instance_type && (
                          <div className="inline-flex items-center gap-1">
                            <span className="inline-flex max-w-full truncate rounded bg-blue-50 px-2 py-0.5 text-xs font-mono text-blue-700" title={`D: ${cluster.driver_instance_type}`}>D: {cluster.driver_instance_type}</span>
                            <InfoTooltip
                              className=""
                              size="compact"
                              label="About driver instance type"
                              text="D means driver node. The instance type comes from current cluster metadata and may be unavailable for historical, deleted, or inaccessible clusters."
                            />
                          </div>
                        )}
                        {cluster.worker_instance_type && (
                          <div className="group relative inline-flex items-center gap-1">
                            <span className="inline-flex max-w-full truncate rounded bg-green-50 px-2 py-0.5 text-xs font-mono text-green-700" title={`W: ${cluster.worker_instance_type}`}>W: {cluster.worker_instance_type}</span>
                            <InfoTooltip
                              className=""
                              size="compact"
                              label="About worker instance type"
                              text="W means worker nodes. The instance type comes from current cluster metadata and may be unavailable for historical, deleted, or inaccessible clusters."
                            />
                          </div>
                        )}
                        {!cluster.driver_instance_type && !cluster.worker_instance_type && (
                          <div className="group relative inline-flex items-center gap-1">
                            <span className="inline-flex rounded bg-gray-100 px-2 py-0.5 text-xs text-gray-500">Historical cluster</span>
                            <InfoTooltip className="" label="Why instance type is unavailable" text="This cluster no longer exists in the workspace. Instance type information is only available for currently configured clusters." />
                          </div>
                        )}
                      </div>
                    </td>
                    <td className="whitespace-nowrap px-3 py-3 text-right text-sm font-medium text-gray-900">{formatCurrency(cluster.databricks_spend || 0)}</td>
                    <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">{formatNumber(cluster.total_dbu_hours)}</td>
                    <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">{cluster.days_active}</td>
                    <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-500">
                      {filteredDatabricksSpend > 0
                        ? `${((cluster.databricks_spend / filteredDatabricksSpend) * 100).toFixed(1)}%`
                        : "0.0%"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
            <tfoot className="bg-gray-50">
              <tr>
                <td colSpan={2} className="px-3 py-3 text-sm font-medium text-gray-700">
                  {detailTruncated ? `Top ${detailLimit} detail subtotal` : "Total"} ({sortedClusters.length} clusters)
                </td>
                <td className="whitespace-nowrap px-3 py-3 text-right text-sm font-bold text-gray-900">
                  {formatCurrency(filteredDatabricksSpend)}
                </td>
                <td className="whitespace-nowrap px-3 py-3 text-right text-sm font-medium text-gray-700">{formatNumber(filteredDbuHours)}</td>
                <td colSpan={2}></td>
              </tr>
            </tfoot>
          </table>
        </div>
        {totalPages > 1 && (
          <div className="mt-4 flex items-center justify-between border-t border-gray-200 pt-4">
            <p className="text-sm text-gray-700">
              Showing <span className="font-medium">{startIndex + 1}</span> to{" "}
              <span className="font-medium">{Math.min(endIndex, sortedClusters.length)}</span> of{" "}
              <span className="font-medium">{sortedClusters.length}</span> clusters
            </p>
            <div className="flex gap-2">
              <button onClick={() => setCurrentPage(Math.max(1, currentPage - 1))} disabled={currentPage === 1} className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50">
                Previous
              </button>
              {Array.from({ length: totalPages }, (_, i) => i + 1)
                .filter((page) => page === 1 || page === totalPages || (page >= currentPage - 1 && page <= currentPage + 1))
                .map((page, idx, arr) => {
                  const prevPage = arr[idx - 1];
                  const showEllipsis = prevPage && page - prevPage > 1;
                  return (
                    <>
                      {showEllipsis && <span key={`ellipsis-${page}`} className="px-2 py-1 text-gray-500">...</span>}
                      <button
                        key={page}
                        onClick={() => setCurrentPage(page)}
                        className={`rounded px-3 py-1 text-sm font-medium ${currentPage === page ? "bg-orange-600 text-white" : "border border-gray-300 text-gray-700 hover:bg-gray-50"}`}
                      >
                        {page}
                      </button>
                    </>
                  );
                })}
              <button onClick={() => setCurrentPage(Math.min(totalPages, currentPage + 1))} disabled={currentPage === totalPages} className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50">
                Next
              </button>
            </div>
          </div>
        )}
      </div>

      {IntegrationWizard}
    </div>
  );
}
