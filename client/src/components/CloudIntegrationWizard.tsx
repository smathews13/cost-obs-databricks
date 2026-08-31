import { useEffect, useId, useRef, useState } from "react";
import { createPortal } from "react-dom";
import awsLogo from "@/assets/aws.png";
import azureLogo from "@/assets/azure-128.png";
import gcpLogo from "@/assets/gcp.svg";
import { C } from "@/theme";

export type CloudIntegration = { id: string; cloud: "azure" | "aws" | "gcp"; label: string };

interface CloudIntegrationWizardProps {
  show: boolean;
  onClose: () => void;
  wizardCloud: "azure" | "aws" | "gcp" | null;
  setWizardCloud: (c: "azure" | "aws" | "gcp" | null) => void;
  wizardExpandedStep: number | null;
  setWizardExpandedStep: (s: number | null) => void;
  viewingIntegration: CloudIntegration | null;
  cloudIntegrations: CloudIntegration[];
  addIntegration: (cloud: "azure" | "aws" | "gcp") => void;
  isAzure: boolean;
  isGCP: boolean;
}

const WIZARD_STEPS_KEY = "cost-obs-wizard-checked-steps";

export function CloudIntegrationWizard({
  show, onClose,
  wizardCloud, setWizardCloud,
  wizardExpandedStep, setWizardExpandedStep,
  viewingIntegration,
  cloudIntegrations, addIntegration,
  isAzure, isGCP,
}: CloudIntegrationWizardProps) {
  const titleId = useId();
  const descriptionId = useId();
  const dialogRef = useRef<HTMLDivElement>(null);
  const [checkedSteps, setCheckedSteps] = useState<Record<string, boolean>>(() => {
    try { return JSON.parse(localStorage.getItem(WIZARD_STEPS_KEY) || "{}"); } catch { return {}; }
  });

  const toggleStep = (key: string) => {
    setCheckedSteps(prev => {
      const next = { ...prev, [key]: !prev[key] };
      localStorage.setItem(WIZARD_STEPS_KEY, JSON.stringify(next));
      return next;
    });
  };

  useEffect(() => {
    if (!show) return;
    const previousOverflow = document.body.style.overflow;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handleKeyDown);
    document.body.style.overflow = "hidden";
    dialogRef.current?.focus();
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousOverflow;
    };
  }, [show, onClose]);

  if (!show) return null;

  return createPortal(
    <div
      className="animate-backdrop fixed inset-0 z-9999 overflow-y-auto"
      style={{ background: "rgba(11,32,38,.45)" }}
      onMouseDown={(event) => event.target === event.currentTarget && onClose()}
    >
      <div
        className="flex min-h-full items-center justify-center p-4 sm:p-6"
        onMouseDown={(event) => event.target === event.currentTarget && onClose()}
      >
        <div
          ref={dialogRef}
          role="dialog"
          aria-modal="true"
          aria-labelledby={titleId}
          aria-describedby={descriptionId}
          tabIndex={-1}
          className="cloud-integration-wizard animate-dialog relative flex max-h-[92vh] w-full max-w-4xl flex-col overflow-hidden bg-white focus:outline-none"
          style={{ borderRadius: 12, boxShadow: "var(--sh-modal)" }}
          onMouseDown={(event) => event.stopPropagation()}
        >
          <div className="flex items-center justify-between px-6 py-4" style={{ borderBottom: `1px solid ${C.hairline}` }}>
            <div>
              <h2 id={titleId} className="text-lg font-semibold" style={{ color: C.ink }}>
                {wizardCloud === null
                  ? "Integrate Cloud Environment Costs"
                  : `${wizardCloud === "azure" ? "Azure" : wizardCloud === "gcp" ? "Google Cloud" : "AWS"} Cost Integration: Setup Guide`}
              </h2>
              <p id={descriptionId} className="mt-0.5 text-sm" style={{ color: C.slate }}>
                {wizardCloud === null
                  ? "Choose the cloud environment you'd like to integrate billing data from."
                  : "Follow the steps below to enable actual cloud cost data in this app."}
              </p>
            </div>
            <button
              type="button"
              onClick={onClose}
              aria-label="Close cloud integration guide"
              className="rounded-lg p-2 transition-colors hover:bg-(--row-hover) focus-visible:outline-none focus-visible:shadow-(--focus)"
              style={{ color: C.slate }}
            >
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <div className="overflow-y-auto px-5 py-5 sm:px-6">
            {wizardCloud === null ? (
              <div className="space-y-4">
                <p className="text-sm" style={{ color: C.body }}>
                  You can integrate billing data from any cloud environment regardless of where your Databricks workspace is hosted. Up to 3 cloud cost integrations are supported.
                </p>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                  <button
                    type="button"
                    onClick={() => { setWizardCloud("azure"); setWizardExpandedStep(null); }}
                    disabled={cloudIntegrations.some(i => i.cloud === "azure") || cloudIntegrations.length >= 3}
                    aria-label="Configure Microsoft Azure cost integration"
                    className="group flex min-h-48 flex-col items-center justify-center gap-3 bg-(--card-surface) p-5 text-center transition-all hover:-translate-y-0.5 hover:bg-(--row-hover) focus-visible:outline-none focus-visible:shadow-(--focus) disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0"
                    style={{ border: `1px solid ${C.hairline}`, borderRadius: 12, boxShadow: "var(--sh-card)" }}
                  >
                    <img src={azureLogo} alt="Azure" className="h-12 w-auto object-contain" />
                    <div>
                      <div className="flex items-center justify-center gap-2">
                        <span className="font-semibold" style={{ color: C.ink }}>Microsoft Azure</span>
                      </div>
                      <div className="mt-0.5 text-xs" style={{ color: C.slate }}>Azure Cost Management Export via SDP</div>
                      {isAzure && <div className="mt-2 flex justify-center"><span className="rounded px-2 py-0.5 text-xs font-medium" style={{ background: C.coralTint, color: C.lava }}>Current workspace cloud</span></div>}
                    </div>
                    {cloudIntegrations.some(i => i.cloud === "azure") && (
                      <span className="rounded px-2 py-0.5 text-xs font-medium" style={{ background: C.greenTint, color: C.greenInk }}>Already added</span>
                    )}
                  </button>
                  <button
                    type="button"
                    onClick={() => { setWizardCloud("aws"); setWizardExpandedStep(null); }}
                    disabled={cloudIntegrations.some(i => i.cloud === "aws") || cloudIntegrations.length >= 3}
                    aria-label="Configure Amazon Web Services cost integration"
                    className="group flex min-h-48 flex-col items-center justify-center gap-3 bg-(--card-surface) p-5 text-center transition-all hover:-translate-y-0.5 hover:bg-(--row-hover) focus-visible:outline-none focus-visible:shadow-(--focus) disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0"
                    style={{ border: `1px solid ${C.hairline}`, borderRadius: 12, boxShadow: "var(--sh-card)" }}
                  >
                    <img src={awsLogo} alt="AWS" className="h-12 w-auto object-contain" />
                    <div>
                      <div className="flex items-center justify-center gap-2">
                        <span className="font-semibold" style={{ color: C.ink }}>Amazon Web Services</span>
                      </div>
                      <div className="mt-0.5 text-xs" style={{ color: C.slate }}>AWS CUR 2.0 Standard Data Export</div>
                      {!isAzure && !isGCP && <div className="mt-2 flex justify-center"><span className="rounded px-2 py-0.5 text-xs font-medium" style={{ background: C.coralTint, color: C.lava }}>Current workspace cloud</span></div>}
                    </div>
                    {cloudIntegrations.some(i => i.cloud === "aws") && (
                      <span className="rounded px-2 py-0.5 text-xs font-medium" style={{ background: C.greenTint, color: C.greenInk }}>Already added</span>
                    )}
                  </button>
                  <button
                    type="button"
                    onClick={() => { setWizardCloud("gcp"); setWizardExpandedStep(null); }}
                    disabled={cloudIntegrations.some(i => i.cloud === "gcp") || cloudIntegrations.length >= 3}
                    aria-label="Configure Google Cloud cost integration"
                    className="group flex min-h-48 flex-col items-center justify-center gap-3 bg-(--card-surface) p-5 text-center transition-all hover:-translate-y-0.5 hover:bg-(--row-hover) focus-visible:outline-none focus-visible:shadow-(--focus) disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0"
                    style={{ border: `1px solid ${C.hairline}`, borderRadius: 12, boxShadow: "var(--sh-card)" }}
                  >
                    <img src={gcpLogo} alt="GCP" className="h-12 w-auto object-contain" />
                    <div>
                      <div className="flex items-center justify-center gap-2">
                        <span className="font-semibold" style={{ color: C.ink }}>Google Cloud</span>
                      </div>
                      <div className="mt-0.5 text-xs" style={{ color: C.slate }}>GCP Billing Export via BigQuery</div>
                      {isGCP && <div className="mt-2 flex justify-center"><span className="rounded px-2 py-0.5 text-xs font-medium" style={{ background: C.coralTint, color: C.lava }}>Current workspace cloud</span></div>}
                    </div>
                    {cloudIntegrations.some(i => i.cloud === "gcp") && (
                      <span className="rounded px-2 py-0.5 text-xs font-medium" style={{ background: C.greenTint, color: C.greenInk }}>Already added</span>
                    )}
                  </button>
                </div>
              </div>
            ) : (
              <div className="space-y-3">
                <p className="text-sm" style={{ color: C.body }}>
                  {wizardCloud === "azure"
                    ? "Export Azure billing data via Cost Management, then ingest into Databricks using an SDP pipeline. Follow the steps below:"
                    : wizardCloud === "gcp"
                    ? "Export GCP billing data to BigQuery, then bring it into Databricks via the BigQuery connector or GCS transfer. Follow the steps below:"
                    : "Export AWS cost data using CUR 2.0 Standard Data Export, then ingest into Databricks from S3. Follow the steps below:"}
                </p>

                {(wizardCloud === "azure" ? [
                  "Open Azure Portal → Cost Management + Billing → Exports",
                  "Create a Daily Cost Export (type: Cost and usage, format: CSV)",
                  "Note the storage account name, container, and directory path",
                  "Create an SDP ingestion pipeline in Databricks for the Azure Blob path",
                  "Grant Databricks SP 'Storage Blob Data Reader' and verify data (Final Step)",
                ] : wizardCloud === "gcp" ? [
                  "Open GCP Console → Billing → Billing Export",
                  "Enable Standard usage cost export to a BigQuery dataset",
                  "Note the GCP project ID and BigQuery dataset name",
                  "Create an Omni connection or use BigQuery connector in Databricks",
                  "Validate data in Databricks: confirm billing rows in Delta (Final Step)",
                ] : [
                  "Sign in to AWS Console → Billing & Cost Management → Data Exports",
                  "Create a Standard Data Export (CUR 2.0) with resource IDs and daily frequency",
                  "Note the S3 bucket name, path prefix, and AWS account ID",
                  "Add S3 location as an External Location in Databricks Unity Catalog",
                  "Run the ingestion workflow and validate cost data (Final Step)",
                ]).map((title, i) => {
                  const step = i + 1;
                  const isLast = step === 5;
                  const stepKey = `${wizardCloud}-${viewingIntegration?.id || 'new'}-step-${step}`;
                  const isChecked = !!checkedSteps[stepKey];
                  const panelId = `${titleId}-step-${step}`;
                  return (
                    <div
                      key={step}
                      className="overflow-hidden bg-white"
                      style={{
                        border: `1px solid ${isLast ? C.coralBrd : C.hairline}`,
                        borderRadius: 12,
                      }}
                    >
                      <div className="flex items-center hover:bg-(--row-hover)">
                        <button
                          type="button"
                          onClick={() => setWizardExpandedStep(wizardExpandedStep === step ? null : step)}
                          aria-expanded={wizardExpandedStep === step}
                          aria-controls={panelId}
                          className="flex min-w-0 flex-1 items-center gap-3 px-4 py-3 text-left transition-colors focus-visible:outline-none focus-visible:shadow-[inset_0_0_0_2px_rgba(255,54,33,.35)]"
                        >
                          <span
                            className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-xs font-semibold"
                            style={
                              isChecked
                                ? { background: C.greenTint, color: C.greenInk }
                                : isLast
                                  ? { background: C.lava, color: C.white }
                                  : { background: C.coralTint, color: C.lava }
                            }
                          >
                            {isChecked ? (
                              <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
                              </svg>
                            ) : step}
                          </span>
                          <span
                            className={`flex-1 font-medium ${isChecked ? "line-through" : ""}`}
                            style={{ color: isChecked ? C.slate : C.ink }}
                          >
                            {title}
                          </span>
                          <svg className={`h-5 w-5 shrink-0 transition-transform ${wizardExpandedStep === step ? "rotate-180" : ""}`} style={{ color: C.slate }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                          </svg>
                        </button>
                        <button
                          type="button"
                          role="checkbox"
                          aria-checked={isChecked}
                          aria-label={`${isChecked ? "Mark incomplete" : "Mark complete"}: ${title}`}
                          onClick={() => toggleStep(stepKey)}
                          className="mr-4 flex h-6 w-6 shrink-0 cursor-pointer items-center justify-center rounded border-2 bg-white transition-colors focus-visible:outline-none focus-visible:shadow-(--focus)"
                          style={{
                            borderColor: isChecked ? C.successFill : C.hairline,
                            background: isChecked ? C.successFill : C.card,
                          }}
                        >
                          {isChecked && (
                            <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                            </svg>
                          )}
                        </button>
                      </div>
                      {wizardExpandedStep === step && (
                        <div
                          id={panelId}
                          role="region"
                          className="animate-slide-down px-4 py-4 text-sm"
                          style={{ borderTop: `1px solid ${C.hairline}`, background: C.oatPage, color: C.body }}
                        >
                          {wizardCloud === "azure" ? (
                            step === 1 ? (
                              <>
                                <p className="mb-3">Go to the <a href="https://portal.azure.com/#view/Microsoft_Azure_CostManagement/Menu/~/exports" target="_blank" rel="noopener noreferrer">Azure Portal → Cost Management + Billing → Exports</a> to create a cost export.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>In the Azure Portal, search for <strong>Cost Management + Billing</strong> and open it</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>In the left sidebar, click <strong>Exports</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Click <strong>+ Add</strong> to create a new export</span></li>
                                </ol>
                                <div className="guide-note mt-3 rounded-lg p-3 text-sm">
                                  <strong>💡 Tip:</strong> Create the export at the subscription or billing account scope to capture all resource costs.
                                </div>
                              </>
                            ) : step === 2 ? (
                              <>
                                <p className="mb-3">Create a new export with type <strong>Cost and usage (actual)</strong> and daily CSV delivery to Azure Blob Storage.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>Set export type: <strong>Cost and usage (actual)</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Set frequency: <strong>Daily export of month-to-date costs</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Set format: <strong>CSV</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">d.</span><span>Choose or create an Azure Blob Storage container as the export destination</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">e.</span><span>Click <strong>Create</strong> and confirm the export starts running</span></li>
                                </ol>
                                <div className="guide-reference mt-3 rounded-lg p-3 text-sm">
                                  <strong>Reference:</strong> <a href="https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-export-acm-data" target="_blank" rel="noopener noreferrer" className="underline">Azure Cost Management export tutorial</a>
                                </div>
                              </>
                            ) : step === 3 ? (
                              <>
                                <p className="mb-3">Note the storage details you'll need to connect Databricks to the export.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>Note the <strong>storage account name</strong> (e.g., <code className="rounded bg-gray-200 px-1">mycompanybilling</code>)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Note the <strong>container name</strong> (e.g., <code className="rounded bg-gray-200 px-1">cost-exports</code>)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Note the <strong>directory path</strong> configured for the export</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">d.</span><span>Confirm the export has generated at least one file in the container before proceeding</span></li>
                                </ol>
                              </>
                            ) : step === 4 ? (
                              <>
                                <p className="mb-3">In Databricks, create an SDP (Streaming Declarative Pipeline) to ingest the Azure Blob Storage cost data into Delta.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>In Databricks, create an External Location pointing to your storage container (Catalog → External Data → External Locations)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Create a new Declarative Pipeline configured to read from the Azure Blob path</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Set the pipeline schedule to <strong>Continuous</strong> or <strong>Daily</strong> to match your export frequency</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">d.</span><span>Run the pipeline and verify data flows into the target Delta table</span></li>
                                </ol>
                              </>
                            ) : (
                              <>
                                <p className="mb-3">Grant the Databricks service principal access to the storage container, then validate data is flowing.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>In the Azure Portal, navigate to your storage container → <strong>Access Control (IAM)</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Add role assignment: <strong>Storage Blob Data Reader</strong> for the Databricks service principal</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Run the pipeline again and confirm billing rows appear in the Delta table</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">d.</span><span>Once data is available, return here and click <strong>Add this integration</strong></span></li>
                                </ol>
                              </>
                            )
                          ) : wizardCloud === "gcp" ? (
                            step === 1 ? (
                              <>
                                <p className="mb-3">Open the <a href="https://console.cloud.google.com/billing" target="_blank" rel="noopener noreferrer">GCP Console → Billing → Billing Export</a> to configure standard cost exports.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>In the GCP Console, open <strong>Billing</strong> from the top navigation menu</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>In the left sidebar, click <strong>Billing export</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>You'll see the BigQuery export section: this is where you'll enable cost data export in Step 2</span></li>
                                </ol>
                                <div className="guide-note mt-3 rounded-lg p-3 text-sm">
                                  <strong>⏱ Note:</strong> Initial GCP billing export can take up to 48 hours. After that, data is updated daily.
                                </div>
                              </>
                            ) : step === 2 ? (
                              <>
                                <p className="mb-3">Enable <strong>Standard usage cost</strong> export to a BigQuery dataset in your GCP project.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>Under <strong>BigQuery export</strong>, click <strong>Edit settings</strong> for <em>Standard usage cost</em></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Choose or create a BigQuery project and dataset (e.g., <code className="rounded bg-gray-200 px-1">billing_export</code> in your project)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Click <strong>Save</strong> to enable the export</span></li>
                                </ol>
                                <div className="guide-reference mt-3 rounded-lg p-3 text-sm">
                                  <strong>Reference:</strong> <a href="https://cloud.google.com/billing/docs/how-to/export-data-bigquery-setup" target="_blank" rel="noopener noreferrer" className="underline">GCP Billing Export to BigQuery setup guide</a>
                                </div>
                              </>
                            ) : step === 3 ? (
                              <>
                                <p className="mb-3">Note the GCP project ID and BigQuery dataset name: you'll need these to configure the Databricks connector.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>Note the <strong>GCP project ID</strong> (visible in the GCP Console project selector)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Note the <strong>BigQuery dataset name</strong> you chose (e.g., <code className="rounded bg-gray-200 px-1">billing_export</code>)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>In BigQuery, verify the export table exists (e.g., <code className="rounded bg-gray-200 px-1">gcp_billing_export_v1_XXXXXX_XXXXXX_XXXXXX</code>)</span></li>
                                </ol>
                              </>
                            ) : step === 4 ? (
                              <>
                                <p className="mb-3">In Databricks, use the BigQuery connector or Lakehouse Federation to query the billing data, or use GCS transfer to bring it into Delta.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span><strong>Option A (Omni/Federation):</strong> Create a Lakehouse Federation connection to BigQuery in Databricks (Catalog → External Data → Connections)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span><strong>Option B (GCS transfer):</strong> Export BigQuery data to GCS using a scheduled query, then create a GCS External Location in Databricks Unity Catalog</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Create a Databricks workflow or notebook to load the billing data into a Delta table on your chosen schedule</span></li>
                                </ol>
                              </>
                            ) : (
                              <>
                                <p className="mb-3">Run the ingestion workflow and verify billing rows appear in Databricks.</p>
                                <ul className="space-y-1 mb-4">
                                  <li>• Run your notebook or workflow and verify billing data rows appear in the target Delta table</li>
                                  <li>• Check that cost data covers the expected date range (GCP billing has a 1-day lag)</li>
                                  <li>• Once data is available, return here and click <strong>Add this integration</strong></li>
                                </ul>
                                <div className="guide-note rounded-lg p-3 text-sm">
                                  <strong>ℹ️ Note:</strong> GCP billing export includes Compute Engine, Cloud Storage, networking, and all GCP services. BigQuery export data typically reflects costs with a 1-day lag.
                                </div>
                              </>
                            )
                          ) : (
                            step === 1 ? (
                              <>
                                <p className="mb-3">Sign in to the <a href="https://console.aws.amazon.com/billing/home#/dataexports" target="_blank" rel="noopener noreferrer">AWS Management Console</a> and navigate to Billing &amp; Cost Management → Data Exports.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>In the AWS console, use the top navigation to go to <strong>Billing &amp; Cost Management</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>In the left sidebar, click <strong>Data Exports</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Click <strong>Create export</strong> to begin the Standard Data Export wizard</span></li>
                                </ol>
                                <div className="guide-note mt-3 rounded-lg p-3 text-sm">
                                  <strong>💡 Tip:</strong> Use your AWS payer (management) account so that costs from all linked accounts are included.
                                </div>
                              </>
                            ) : step === 2 ? (
                              <>
                                <p className="mb-3">Create a <a href="https://docs.aws.amazon.com/cur/latest/userguide/dataexports-create-standard.html" target="_blank" rel="noopener noreferrer">Standard Data Export (CUR 2.0)</a> with resource IDs and daily delivery to an S3 bucket.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>Select export type: <strong>Standard Data Export</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Enable <strong>Include resource IDs</strong> and set export frequency to <strong>Daily</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Choose an existing S3 bucket or create one dedicated to cost exports (e.g., <code className="rounded bg-gray-200 px-1">my-company-billing-exports</code>)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">d.</span><span>Set compression type: <strong>Parquet</strong>, file versioning: <strong>Overwrite existing data export file</strong></span></li>
                                </ol>
                                <div className="guide-note mt-3 rounded-lg p-3 text-sm">
                                  <strong>⏱ Note:</strong> CUR data typically takes 24 hours to start appearing after the export is created.
                                </div>
                              </>
                            ) : step === 3 ? (
                              <>
                                <p className="mb-3">Once the export is running, note the key details you'll need to configure the Databricks ingestion.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>Note the <strong>S3 bucket name</strong> (e.g., <code className="rounded bg-gray-200 px-1">my-company-billing-exports</code>)</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Note the <strong>path prefix</strong> configured for your export</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Note your <strong>AWS account ID</strong> (12-digit number) visible in the top-right of the console</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">d.</span><span>Confirm the export has generated at least one file in S3 before proceeding</span></li>
                                </ol>
                              </>
                            ) : step === 4 ? (
                              <>
                                <p className="mb-3">In Databricks, use the AWS Cost and Usage ingestion workflow or add the S3 location as an External Location in Unity Catalog.</p>
                                <ol className="space-y-2">
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">a.</span><span>In Databricks, go to <strong>Catalog → External Data → Storage Credentials → Create credential</strong></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">b.</span><span>Choose <strong>AWS IAM role</strong> and attach a policy with <code className="rounded bg-gray-200 px-1">s3:GetObject</code> and <code className="rounded bg-gray-200 px-1">s3:ListBucket</code> on your CUR bucket</span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">c.</span><span>Go to <strong>External Locations → Create external location</strong>, set URL to <code className="rounded bg-gray-200 px-1">s3://your-bucket/cur-prefix/</code></span></li>
                                  <li className="flex gap-2"><span className="font-medium text-gray-700">d.</span><span>Click <strong>Test connection</strong> to verify Databricks can read the S3 path</span></li>
                                </ol>
                                <div className="guide-reference mt-3 rounded-lg p-3 text-sm">
                                  <strong>Reference:</strong> <a href="https://docs.aws.amazon.com/cur/latest/userguide/dataexports-create-standard.html" target="_blank" rel="noopener noreferrer" className="underline">AWS CUR 2.0 Standard Data Export docs</a>
                                </div>
                              </>
                            ) : (
                              <>
                                <p className="mb-3">Run the ingestion workflow and verify cost data is flowing into Databricks.</p>
                                <ul className="space-y-1 mb-4">
                                  <li>• In Databricks, use the AWS Cost and Usage ingestion workflow or a custom notebook to read Parquet files from the External Location into a Delta table</li>
                                  <li>• Verify cost rows appear in your target table (e.g., <code className="rounded bg-gray-200 px-1">billing.aws.cur_gold</code>)</li>
                                  <li>• Once data is available, return here and click <strong>Add this integration</strong></li>
                                </ul>
                                <div className="guide-note rounded-lg p-3 text-sm">
                                  <strong>ℹ️ Note:</strong> S3 storage charges and data egress are not included in CUR exports. AWS CUR only includes the latest tag key-value pair per resource.
                                </div>
                              </>
                            )
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}

                <div className="flex flex-wrap items-center gap-3 pt-3" style={{ borderTop: `1px solid ${C.hairline}` }}>
                  <a
                    href={wizardCloud === "azure"
                      ? "https://github.com/databricks-solutions/cloud-infra-costs/tree/main/azure"
                      : wizardCloud === "gcp"
                      ? "https://github.com/databricks-solutions/cloud-infra-costs/tree/main/gcp"
                      : "https://github.com/databricks-solutions/cloud-infra-costs/tree/main/aws"}
                    target="_blank" rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 text-xs font-medium focus-visible:outline-none focus-visible:shadow-(--focus)"
                  >
                    <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                    </svg>
                    {wizardCloud === "azure" ? "cloud-infra-costs/azure README" : wizardCloud === "gcp" ? "cloud-infra-costs/gcp README" : "cloud-infra-costs/aws README"}
                  </a>
                  <a
                    href="https://docs.databricks.com/en/dev-tools/bundles/index.html"
                    target="_blank" rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 text-xs font-medium focus-visible:outline-none focus-visible:shadow-(--focus)"
                  >
                    <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                    </svg>
                    Declarative Automation Bundles Docs
                  </a>
                </div>
              </div>
            )}
          </div>

          <div className="flex items-center justify-between px-5 py-4 sm:px-6" style={{ borderTop: `1px solid ${C.hairline}`, background: C.oatPage }}>
            <div>
              {wizardCloud !== null && !viewingIntegration && (
                <button
                  type="button"
                  onClick={() => { setWizardCloud(null); setWizardExpandedStep(null); }}
                  className="rounded-lg px-2 py-1 text-sm font-medium focus-visible:outline-none focus-visible:shadow-(--focus)"
                  style={{ color: C.slate }}
                >
                  ← Choose a different cloud
                </button>
              )}
            </div>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={onClose}
                className="rounded-lg bg-white px-4 py-2 text-sm font-medium transition-colors hover:bg-(--row-hover) focus-visible:outline-none focus-visible:shadow-(--focus)"
                style={{ border: `1px solid ${C.hairline}`, color: C.ink }}
              >
                Close
              </button>
              {wizardCloud !== null && !viewingIntegration && !cloudIntegrations.some(i => i.cloud === wizardCloud) && cloudIntegrations.length < 3 && (
                <button
                  type="button"
                  onClick={() => {
                    if (wizardCloud) addIntegration(wizardCloud);
                    onClose();
                  }}
                  className="btn-brand px-4 py-2 text-sm focus-visible:outline-none"
                >
                  Mark as configured
                </button>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>,
    document.body
  );
}
