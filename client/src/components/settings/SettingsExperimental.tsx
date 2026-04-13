import type { AppSettings } from "../SettingsDialog";

interface SettingsExperimentalProps {
  localSettings: AppSettings;
  updateSetting: <K extends keyof AppSettings>(key: K, value: AppSettings[K]) => void;
  saveStatus: string | null;
}

export function SettingsExperimental({ localSettings, updateSetting, saveStatus }: SettingsExperimentalProps) {
  return (
    <div className="space-y-5">
      {saveStatus && (
        <div className="rounded-lg border border-green-200 bg-green-50 px-4 py-2 text-sm text-green-700">
          {saveStatus}
        </div>
      )}

      <div>
        <div className="flex items-center gap-2 mb-1">
          <svg className="h-4 w-4 text-amber-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
          </svg>
          <h4 className="text-sm font-semibold text-gray-900">Experimental Features</h4>
        </div>
        <p className="mb-4 text-xs text-gray-500">
          These features are in preview and may change or be removed. Enable them to try out new functionality.
        </p>

        <div className="space-y-3">
          {/* App Hosting Cost Comparison — hidden from customers, preserved for future use
          <label className="flex items-start gap-3 rounded-lg border border-gray-200 bg-white p-4 cursor-pointer hover:border-gray-300 transition-colors">
            <input type="checkbox" checked={localSettings.enableAppHostingComparison} onChange={(e) => updateSetting("enableAppHostingComparison", e.target.checked)} className="mt-0.5 h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500" />
            <div>
              <div className="text-sm font-medium text-gray-900">App Hosting Cost Comparison</div>
              <div className="mt-0.5 text-xs text-gray-500">Show a comparison panel in the Apps tab that estimates hosting costs across alternative platforms (e.g. AWS, Azure, GCP) versus Databricks Apps.</div>
            </div>
          </label>
          */}

          {/* Cost Accuracy Checks — hidden from customers, preserved for future use
          <label className="flex items-start gap-3 rounded-lg border border-gray-200 bg-white p-4 cursor-pointer hover:border-gray-300 transition-colors">
            <input type="checkbox" checked={localSettings.enableAccuracyChecks} onChange={(e) => updateSetting("enableAccuracyChecks", e.target.checked)} className="mt-0.5 h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500" />
            <div>
              <div className="text-sm font-medium text-gray-900">Cost Accuracy Checks</div>
              <div className="mt-0.5 text-xs text-gray-500">Add an Accuracy Checks tab to Settings for running cross-validation queries that verify cost reporting is correct — detecting double-counting, missing attribution, and price lookup gaps.</div>
            </div>
          </label>
          */}

          {/* Anonymize Users */}
          <label className="flex items-start gap-3 rounded-lg border border-gray-200 bg-white p-4 cursor-pointer hover:border-gray-300 transition-colors">
            <input
              type="checkbox"
              checked={localSettings.anonymizeUsers}
              onChange={(e) => updateSetting("anonymizeUsers", e.target.checked)}
              className="mt-0.5 h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
            />
            <div>
              <div className="text-sm font-medium text-gray-900">Anonymize Users</div>
              <div className="mt-0.5 text-xs text-gray-500">
                Replace human user emails with generic labels (User 1, User 2, …) throughout the Users tab.
                Service principals are not affected. Useful for demos and screenshots.
              </div>
            </div>
          </label>

          {/* Platform Alerts */}
          <label className="flex items-start gap-3 rounded-lg border border-gray-200 bg-white p-4 cursor-pointer hover:border-gray-300 transition-colors">
            <input
              type="checkbox"
              checked={localSettings.enableAlerts}
              onChange={(e) => updateSetting("enableAlerts", e.target.checked)}
              className="mt-0.5 h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
            />
            <div>
              <div className="text-sm font-medium text-gray-900">Platform Alerts</div>
              <div className="mt-0.5 text-xs text-gray-500">
                Add an Alerts tab for configuring cost spike detection, daily spend thresholds,
                and workspace-level budget alerts with email and Slack notifications.
              </div>
              <div className="mt-1.5 inline-flex items-center gap-1 rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-medium text-amber-700 border border-amber-200">
                <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
                </svg>
                Preview
              </div>
            </div>
          </label>

          {/* Use Case Tracking */}
          <label className="flex items-start gap-3 rounded-lg border border-gray-200 bg-white p-4 cursor-pointer hover:border-gray-300 transition-colors">
            <input
              type="checkbox"
              checked={localSettings.enableUseCaseTracking}
              onChange={(e) => updateSetting("enableUseCaseTracking", e.target.checked)}
              className="mt-0.5 h-4 w-4 rounded border-gray-300 text-orange-600 focus:ring-orange-500"
            />
            <div>
              <div className="text-sm font-medium text-gray-900">Use Case Tracking</div>
              <div className="mt-0.5 text-xs text-gray-500">
                Add a Use Cases tab for tracking and categorizing Databricks workloads by business use case,
                including cost attribution and usage patterns per use case.
              </div>
              <div className="mt-1.5 inline-flex items-center gap-1 rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-medium text-amber-700 border border-amber-200">
                <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z" />
                </svg>
                Preview
              </div>
            </div>
          </label>

          {/* Cost Forecasting — coming soon, not yet functional */}
          <div className="flex items-start gap-3 rounded-lg border border-gray-100 bg-gray-50 p-4 opacity-50 cursor-not-allowed select-none">
            <input
              type="checkbox"
              disabled
              checked={false}
              className="mt-0.5 h-4 w-4 rounded border-gray-300 text-gray-400 cursor-not-allowed"
            />
            <div>
              <div className="text-sm font-medium text-gray-400">Cost Forecasting</div>
              <div className="mt-0.5 text-xs text-gray-400">
                Add a Forecasting tab that projects future consumption based on historical usage
                patterns. Includes month-end estimates, growth driver analysis, and budget scenario
                modeling (optimistic / most likely / pessimistic).
              </div>
              <div className="mt-1.5 inline-flex items-center gap-1 rounded-full bg-gray-100 px-2 py-0.5 text-[10px] font-medium text-gray-400 border border-gray-200">
                Coming Soon
              </div>
            </div>
          </div>

          {/* Lakebase — coming soon, not yet functional */}
          <div className="flex items-start gap-3 rounded-lg border border-gray-100 bg-gray-50 p-4 opacity-50 cursor-not-allowed select-none">
            <input
              type="checkbox"
              disabled
              checked={false}
              className="mt-0.5 h-4 w-4 rounded border-gray-300 text-gray-400 cursor-not-allowed"
            />
            <div>
              <div className="text-sm font-medium text-gray-400">Lakebase</div>
              <div className="mt-0.5 text-xs text-gray-400">
                Replace the materialized views and tables backing the app with Lakebase (managed
                PostgreSQL). Adds a Lakebase tab for monitoring instance health, storage, compute
                hours, and migration progress from materialized views to Lakebase tables.
              </div>
              <div className="mt-1.5 inline-flex items-center gap-1 rounded-full bg-gray-100 px-2 py-0.5 text-[10px] font-medium text-gray-400 border border-gray-200">
                Coming Soon
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
