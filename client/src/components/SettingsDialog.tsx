import { useState, useEffect, useCallback } from "react";
import { createPortal } from "react-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { SettingsAccuracyChecks, SettingsDebugger } from "./settings";
import { READINESS_QUERY_KEY } from "@/hooks/useFeatureAvailability";
import { SetupWizard } from "./SetupWizard";
import {
  ToastProvider, useToast, Badge, T,
} from "./settings/dubois";
import {
  GeneralSection, DashboardTabsSection, AlertsSection, DataTablesSection,
  AccessSection, ResourcesSection, ExperimentalSection,
} from "./settings/sections";

export interface TabVisibility {
  dbu: boolean;
  infra: boolean;
  optimizer: boolean;
  kpis: boolean;
  aiml: boolean;
  sql: boolean;
  apps: boolean;
  tagging: boolean;
  "use-cases": boolean;
  "users-groups": boolean;
}

const DEFAULT_VISIBILITY: TabVisibility = {
  dbu: true, infra: true, optimizer: true, kpis: true, aiml: true,
  sql: true, apps: true, tagging: true, "use-cases": false, "users-groups": true,
};

const STORAGE_KEY = "coc-tab-visibility";

export function loadTabVisibility(): TabVisibility {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) return { ...DEFAULT_VISIBILITY, ...JSON.parse(stored) };
  } catch { /* ignore */ }
  return DEFAULT_VISIBILITY;
}

function saveTabVisibility(visibility: TabVisibility) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(visibility));
}

// ── App Settings ────────────────────────────────────────────────────────────
export interface AppSettings {
  defaultDateRangeDays: 7 | 14 | 30 | 60 | 90;
  refreshIntervalMinutes: 0 | 5 | 15 | 30;
  compactMode: boolean;              // deprecated — migrated to `density`
  darkMode: boolean;                 // deprecated — migrated to `theme`
  density: "comfortable" | "compact";
  theme: "light" | "dark" | "system";
  defaultLandingTab: string;
  showWorkspaceNames: boolean;
  anomalySensitivity: "low" | "medium" | "high";
  expSetupWizardLink: boolean;
  expDebuggerLink: boolean;
  companyName: string;
  appDisplayName: string;
  monthlyBudget: number;
  costAllocationTags: string;
  alertSpikePercent: number;
  alertDailyBudget: number;
  alertWorkspaceBudget: number;
  slackWebhookUrl: string;
  enableAppHostingComparison: boolean;
  enableUseCaseTracking: boolean;
  enableAccuracyChecks: boolean;
  anonymizeUsers: boolean;
}

const DEFAULT_APP_SETTINGS: AppSettings = {
  defaultDateRangeDays: 30,
  refreshIntervalMinutes: 0,
  compactMode: false,
  darkMode: false,
  density: "comfortable",
  theme: "system",
  defaultLandingTab: "dbu",
  showWorkspaceNames: true,
  anomalySensitivity: "medium",
  expSetupWizardLink: false,
  expDebuggerLink: false,
  companyName: "",
  appDisplayName: "",
  monthlyBudget: 0,
  costAllocationTags: "",
  alertSpikePercent: 20,
  alertDailyBudget: 50000,
  alertWorkspaceBudget: 10000,
  slackWebhookUrl: "",
  enableAppHostingComparison: false,
  enableUseCaseTracking: false,
  enableAccuracyChecks: false,
  anonymizeUsers: false,
};

const APP_SETTINGS_KEY = "coc-app-settings";

export function loadAppSettings(): AppSettings {
  try {
    const stored = localStorage.getItem(APP_SETTINGS_KEY);
    if (stored) {
      const parsed = JSON.parse(stored);
      const merged = { ...DEFAULT_APP_SETTINGS, ...parsed } as AppSettings;
      // Migrate deprecated boolean toggles into the new selects (only when the new
      // keys are absent, so an explicit new choice always wins).
      if (parsed.theme === undefined) merged.theme = parsed.darkMode ? "dark" : "light";
      if (parsed.density === undefined) merged.density = parsed.compactMode ? "compact" : "comfortable";
      return merged;
    }
  } catch { /* ignore */ }
  return { ...DEFAULT_APP_SETTINGS };
}

function saveAppSettings(settings: AppSettings) {
  // Keep the deprecated booleans in sync so any un-migrated reader still works.
  const out = { ...settings, darkMode: settings.theme === "dark", compactMode: settings.density === "compact" };
  localStorage.setItem(APP_SETTINGS_KEY, JSON.stringify(out));
}

type NavKey = "general" | "tabs" | "alerts" | "data" | "access" | "resources" | "experimental";
type Overlay = "setup" | "debugger" | "accuracy" | null;

interface SettingsDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onTabVisibilityChange: (visibility: TabVisibility) => void;
  onSettingsChange: (settings: AppSettings) => void;
  tabVisibility: TabVisibility;
  appSettings: AppSettings;
}

export function SettingsDialog(props: SettingsDialogProps) {
  if (!props.isOpen) return null;
  return createPortal(
    <ToastProvider>
      <SettingsShell {...props} />
    </ToastProvider>,
    document.body,
  );
}

function SettingsShell({ onClose, onTabVisibilityChange, onSettingsChange, tabVisibility, appSettings }: SettingsDialogProps) {
  const rqClient = useQueryClient();
  const toast = useToast();
  const [nav, setNav] = useState<NavKey>("general");
  const [overlay, setOverlay] = useState<Overlay>(null);
  const [localVisibility, setLocalVisibility] = useState<TabVisibility>(tabVisibility);
  const [localSettings, setLocalSettings] = useState<AppSettings>(appSettings);
  const [dirty, setDirty] = useState(false);

  const { data: permissions } = useQuery<{ admins: string[]; current_user?: string | null }>({
    queryKey: ["user-permissions"],
    queryFn: () => fetch("/api/settings/user-permissions").then(r => { if (!r.ok) throw new Error(); return r.json(); }),
    staleTime: 60 * 1000,
  });
  const isAdmin = !permissions || !permissions.admins?.length || (!!permissions.current_user && permissions.admins.includes(permissions.current_user));

  const { data: appConfig } = useQuery<import("./settings/SettingsConfig").AppConfigInfo>({
    queryKey: ["app-config"],
    queryFn: () => fetch("/api/settings/config").then(r => { if (!r.ok) throw new Error(); return r.json(); }),
  });

  const appName = localSettings.appDisplayName || appConfig?.identity?.display_name || "Cost Observability";
  const version = appConfig?.version?.commit_sha;

  useEffect(() => {
    setLocalVisibility(tabVisibility);
    setLocalSettings(appSettings);
    setDirty(false);
  }, [tabVisibility, appSettings]);

  // Kick non-admins out of admin-only sections/overlays.
  useEffect(() => {
    const adminSections: NavKey[] = ["alerts", "data", "access", "experimental"];
    if (permissions && !isAdmin && adminSections.includes(nav)) setNav("general");
    if (permissions && !isAdmin && overlay) setOverlay(null);
  }, [permissions, isAdmin, nav, overlay]);

  const requestClose = useCallback(() => {
    if (dirty && !window.confirm("You have unsaved changes. Discard them?")) return;
    onClose();
  }, [dirty, onClose]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") requestClose(); };
    document.addEventListener("keydown", onKey);
    document.body.style.overflow = "hidden";
    return () => { document.removeEventListener("keydown", onKey); document.body.style.overflow = ""; };
  }, [requestClose]);

  const updateSetting = <K extends keyof AppSettings>(key: K, value: AppSettings[K]) => {
    setLocalSettings((prev) => ({ ...prev, [key]: value }));
    setDirty(true);
  };
  const toggleTab = (key: keyof TabVisibility) => {
    setLocalVisibility((prev) => {
      const updated = { ...prev, [key]: !prev[key] };
      if (!Object.values(updated).some(Boolean)) return prev;
      return updated;
    });
    setDirty(true);
  };

  const handleSave = () => {
    // Landing-tab fallback: if the chosen tab is now hidden, use the first visible.
    let settings = localSettings;
    const landingHidden = !localVisibility[localSettings.defaultLandingTab as keyof TabVisibility];
    if (landingHidden) {
      const firstVisible = (Object.keys(localVisibility) as (keyof TabVisibility)[]).find((k) => localVisibility[k]);
      if (firstVisible) settings = { ...localSettings, defaultLandingTab: firstVisible };
    }
    saveAppSettings(settings);
    saveTabVisibility(localVisibility);
    onSettingsChange(settings);
    onTabVisibilityChange(localVisibility);
    setLocalSettings(settings);
    // Persist server-backed settings (thresholds always; webhook only when changed).
    fetch("/api/settings/alert-thresholds", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ spike_threshold_percent: settings.alertSpikePercent, daily_budget: settings.alertDailyBudget, workspace_budget: settings.alertWorkspaceBudget }),
      signal: AbortSignal.timeout(10000),
    }).catch(() => {});
    if (settings.slackWebhookUrl && settings.slackWebhookUrl !== appSettings.slackWebhookUrl) {
      fetch("/api/settings/webhook", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ slack_webhook_url: settings.slackWebhookUrl }), signal: AbortSignal.timeout(10000),
      }).catch(() => {});
    }
    setDirty(false);
    toast(landingHidden ? "Settings saved — landing tab reset to first visible" : "Settings saved");
  };

  const handleReset = () => {
    if (!window.confirm("Reset all settings to defaults?")) return;
    setLocalSettings({ ...DEFAULT_APP_SETTINGS });
    setLocalVisibility({ ...DEFAULT_VISIBILITY });
    saveAppSettings({ ...DEFAULT_APP_SETTINGS });
    saveTabVisibility({ ...DEFAULT_VISIBILITY });
    onSettingsChange({ ...DEFAULT_APP_SETTINGS });
    onTabVisibilityChange({ ...DEFAULT_VISIBILITY });
    setDirty(false);
    toast("Settings reset to defaults");
  };

  const navItems: { key: NavKey; label: string; admin?: boolean }[] = [
    { key: "general", label: "General" },
    { key: "tabs", label: "Dashboard tabs" },
    { key: "alerts", label: "Alerts & notifications", admin: true },
    { key: "data", label: "Data & tables", admin: true },
    { key: "access", label: "Access", admin: true },
    { key: "resources", label: "Resources" },
    { key: "experimental", label: "Experimental", admin: true },
  ];
  const visibleNav = navItems.filter((n) => !n.admin || isAdmin);

  const navBtn = (key: NavKey, label: string, badge?: React.ReactNode) => {
    const active = nav === key && !overlay;
    return (
      <button key={key} onClick={() => { setOverlay(null); setNav(key); }}
        style={{
          display: "flex", alignItems: "center", gap: 6, width: "100%", textAlign: "left",
          padding: "7px 10px", borderRadius: 6, fontSize: 13, fontWeight: active ? 600 : 500,
          color: active ? T.text : T.textSecondary, backgroundColor: active ? "#FFFFFF" : "transparent",
          border: active ? `1px solid ${T.borderGroup}` : "1px solid transparent", cursor: "pointer",
        }}>
        {label}{badge}
      </button>
    );
  };

  return (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/30 p-4" onClick={(e) => e.target === e.currentTarget && requestClose()}>
      <div style={{ width: "100%", maxWidth: 1120, height: "min(760px, 90vh)", backgroundColor: "#FFFFFF", borderRadius: 8, border: `1px solid ${T.borderGroup}`, boxShadow: "0 20px 60px rgba(0,0,0,0.25)", display: "flex", flexDirection: "column", overflow: "hidden" }} onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "16px 20px", borderBottom: `1px solid ${T.borderGroup}` }}>
          <div>
            <div style={{ fontSize: 16, fontWeight: 600, color: T.text }}>Settings</div>
            <div style={{ fontSize: 12, color: T.textSecondary, fontFamily: undefined }}>{appName} · Databricks App</div>
          </div>
          <button onClick={requestClose} style={{ padding: 4, borderRadius: 6, color: T.textSecondary, background: "none", border: "none", cursor: "pointer" }} aria-label="Close">
            <svg style={{ width: 20, height: 20 }} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" /></svg>
          </button>
        </div>

        <div style={{ flex: 1, minHeight: 0, display: "flex" }}>
          {/* Left nav */}
          <div style={{ width: 208, flexShrink: 0, backgroundColor: T.navBg, borderRight: `1px solid ${T.borderGroup}`, display: "flex", flexDirection: "column", padding: 12 }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 2, flex: 1 }}>
              {visibleNav.map((n) => navBtn(n.key, n.label))}
            </div>
            <div style={{ borderTop: `1px solid ${T.borderGroup}`, paddingTop: 10, marginTop: 10, display: "flex", flexDirection: "column", gap: 4 }}>
              {isAdmin && localSettings.expSetupWizardLink && (
                <button onClick={() => setOverlay("setup")} style={footerLink(overlay === "setup")}>Setup wizard <Badge>Experimental</Badge></button>
              )}
              {isAdmin && localSettings.expDebuggerLink && (
                <button onClick={() => setOverlay("debugger")} style={footerLink(overlay === "debugger")}>Debugger <Badge>Experimental</Badge></button>
              )}
              {localSettings.enableAccuracyChecks && (
                <button onClick={() => setOverlay("accuracy")} style={footerLink(overlay === "accuracy")}>Accuracy checks</button>
              )}
              <div style={{ fontSize: 11, color: T.textFaint, padding: "4px 10px", fontFamily: undefined }}>cost-obs{version ? ` · ${version.slice(0, 7)}` : ""}</div>
            </div>
          </div>

          {/* Content */}
          <div style={{ flex: 1, minHeight: 0, overflowY: "auto", backgroundColor: "#FFFFFF" }}>
            <div style={{ maxWidth: 720, padding: "20px 24px" }}>
              {overlay === "setup" && isAdmin && (
                <SetupWizard embedded onComplete={() => {
                  rqClient.invalidateQueries({ queryKey: ["app-config"] });
                  rqClient.invalidateQueries({ queryKey: ["settings-tables-status"] });
                  rqClient.invalidateQueries({ queryKey: READINESS_QUERY_KEY });
                  setOverlay(null);
                }} />
              )}
              {overlay === "debugger" && isAdmin && (
                <SettingsDebugger onGoToConfig={() => { setOverlay(null); setNav("data"); }} />
              )}
              {overlay === "accuracy" && <SettingsAccuracyChecks />}

              {!overlay && nav === "general" && <GeneralSection localSettings={localSettings} updateSetting={updateSetting} tabVisibility={localVisibility} />}
              {!overlay && nav === "tabs" && <DashboardTabsSection localVisibility={localVisibility} toggleTab={toggleTab} enableUseCaseTracking={localSettings.enableUseCaseTracking} />}
              {!overlay && nav === "alerts" && <AlertsSection localSettings={localSettings} updateSetting={updateSetting} />}
              {!overlay && nav === "data" && <DataTablesSection localSettings={localSettings} updateSetting={updateSetting} />}
              {!overlay && nav === "access" && <AccessSection />}
              {!overlay && nav === "resources" && <ResourcesSection />}
              {!overlay && nav === "experimental" && <ExperimentalSection localSettings={localSettings} updateSetting={updateSetting} />}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 20px", borderTop: `1px solid ${T.borderGroup}` }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <button onClick={handleReset} style={{ fontSize: 13, color: T.textSecondary, background: "none", border: "none", cursor: "pointer" }}>Reset to defaults</button>
            {dirty && <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: T.warningFg }}><span style={{ width: 6, height: 6, borderRadius: 999, backgroundColor: "#E0A82E" }} />Unsaved changes</span>}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button onClick={requestClose} style={{ height: 32, borderRadius: 4, padding: "0 14px", fontSize: 13, fontWeight: 500, color: T.text, backgroundColor: "#FFFFFF", border: `1px solid ${T.borderControl}`, cursor: "pointer" }}>Cancel</button>
            <button onClick={handleSave} disabled={!dirty}
              style={{ height: 32, borderRadius: 4, padding: "0 14px", fontSize: 13, fontWeight: 500, color: "#FFFFFF", backgroundColor: dirty ? T.primary : "#A9C6DC", cursor: dirty ? "pointer" : "not-allowed", border: "none" }}>
              Save changes
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function footerLink(active: boolean): React.CSSProperties {
  return {
    display: "flex", alignItems: "center", gap: 6, width: "100%", textAlign: "left",
    padding: "6px 10px", borderRadius: 6, fontSize: 12.5, fontWeight: active ? 600 : 500,
    color: active ? T.text : T.textSecondary, backgroundColor: active ? "#FFFFFF" : "transparent",
    border: active ? `1px solid ${T.borderGroup}` : "1px solid transparent", cursor: "pointer",
  };
}
