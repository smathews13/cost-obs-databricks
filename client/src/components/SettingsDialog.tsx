import { useState, useEffect, useCallback, useRef } from "react";
import { createPortal } from "react-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { SettingsDebugger } from "./settings";
import { READINESS_QUERY_KEY } from "@/hooks/useFeatureAvailability";
import { SetupWizard } from "./SetupWizard";
import {
  ToastProvider, useToast, Badge, T,
} from "./settings/dubois";
import {
  GeneralSection, DashboardTabsSection, AlertsSection, DataTablesSection,
  AccessSection, ResourcesSection, ExperimentalSection,
} from "./settings/sections";
import { APP_VERSION } from "@/theme";
import {
  DEFAULT_APP_SETTINGS,
  DEFAULT_VISIBILITY,
  hydrateSettingsFromServer,
  persistAppSettings,
  persistTabVisibility,
  type AppSettings,
  type TabVisibility,
  type UnifiedSettings,
} from "@/utils/settingsHydration";

export {
  type AppSettings,
  type TabVisibility,
};

type NavKey = "general" | "tabs" | "alerts" | "data" | "access" | "resources" | "experimental";
type Overlay = "setup" | "debugger" | null;

export type SettingsCapabilities = NonNullable<UnifiedSettings["capabilities"]>;

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
  const [isSaving, setIsSaving] = useState(false);
  const editingRef = useRef(false);
  const saveInFlightRef = useRef(false);

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

  // Friendly SP name for the header (identity.display_name from /config is just the
  // SP client-id UUID; sp_display_name is the human label).
  const { data: authStatus } = useQuery<{ sp_display_name?: string } | null>({
    queryKey: ["settings-auth-status"],
    queryFn: () => fetch("/api/settings/auth-status").then(r => r.ok ? r.json() : null).catch(() => null),
    staleTime: 60 * 1000,
  });

  // Unified settings aggregator (Phase 2). Seeds the modal from the server; falls back
  // to localStorage/defaults when unavailable so the modal still works pre-migration.
  const { data: unified } = useQuery<UnifiedSettings | null>({
    queryKey: ["unified-settings"],
    queryFn: () => fetch("/api/settings").then(r => r.ok ? r.json() : null).catch(() => null),
    staleTime: 60 * 1000,
  });
  const caps = unified?.capabilities;
  const seededRef = useRef(false);
  useEffect(() => {
    if (!unified || dirty || editingRef.current || saveInFlightRef.current || seededRef.current) return;
    seededRef.current = true;
    const hydrated = hydrateSettingsFromServer(unified, localSettings, localVisibility);
    setLocalSettings(hydrated.appSettings);
    setLocalVisibility(hydrated.tabVisibility);
  }, [unified, dirty, localSettings, localVisibility]);

  // Prefer a real display name; never show the raw SP client-id UUID. Falls back to
  // the app's product name.
  const isUuid = (s?: string | null) => !!s && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s.trim());
  const spName = authStatus?.sp_display_name;
  const idName = appConfig?.identity?.display_name;
  const appName =
    localSettings.appDisplayName ||
    (spName && !isUuid(spName) ? spName : "") ||
    (idName && !isUuid(idName) ? idName : "") ||
    "cost-obs-v1";
  const version = appConfig?.version?.commit_sha;

  useEffect(() => {
    if (editingRef.current || saveInFlightRef.current) return;
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
    if (saveInFlightRef.current) return;
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
    editingRef.current = true;
    setLocalSettings((prev) => ({ ...prev, [key]: value }));
    setDirty(true);
  };
  const toggleTab = (key: keyof TabVisibility) => {
    editingRef.current = true;
    setLocalVisibility((prev) => {
      const updated = { ...prev, [key]: !prev[key] };
      if (!Object.values(updated).some(Boolean)) return prev;
      return updated;
    });
    setDirty(true);
  };

  const handleSave = async () => {
    if (saveInFlightRef.current || !dirty) return;
    saveInFlightRef.current = true;
    setIsSaving(true);
    // Landing-tab fallback: if the chosen tab is now hidden, use the first visible.
    let settings = localSettings;
    const landingHidden = !localVisibility[localSettings.defaultLandingTab as keyof TabVisibility];
    if (landingHidden) {
      const firstVisible = (Object.keys(localVisibility) as (keyof TabVisibility)[]).find((k) => localVisibility[k]);
      if (firstVisible) settings = { ...localSettings, defaultLandingTab: firstVisible };
    }
    // One Save = one PUT to the unified aggregator, which dispatches each sub-object
    // to its domain store (app_settings / alert-thresholds / webhook). Price basis and
    // refresh schedule save immediately via their own controls, so they're omitted here.
    const body: Record<string, unknown> = {
      general: {
        company_name: settings.companyName, app_display_name: settings.appDisplayName,
        default_date_range_days: settings.defaultDateRangeDays, default_landing_tab: settings.defaultLandingTab,
        auto_refresh_minutes: settings.refreshIntervalMinutes, density: settings.density, theme: settings.theme,
        show_workspace_names: settings.showWorkspaceNames,
        anonymize_users: settings.anonymizeUsers,
      },
      tab_visibility: localVisibility,
      thresholds: {
        spike_threshold_percent: settings.alertSpikePercent, daily_budget: settings.alertDailyBudget,
        workspace_budget: settings.alertWorkspaceBudget, anomaly_sensitivity: settings.anomalySensitivity,
      },
      experimental: {
        exp_setup_wizard_link: settings.expSetupWizardLink,
        exp_debugger_link: settings.expDebuggerLink,
        enable_architecture_view: settings.enableArchitectureView,
      },
    };
    if (settings.slackWebhookUrl && settings.slackWebhookUrl !== appSettings.slackWebhookUrl) {
      body.webhook = { slack_webhook_url: settings.slackWebhookUrl };
    }
    try {
      const response = await fetch("/api/settings", {
        method: "PUT", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body), signal: AbortSignal.timeout(10000),
      });
      const savedSnapshot = await response.json().catch(() => null) as UnifiedSettings | { detail?: string } | null;
      if (!response.ok) {
        const detail = savedSnapshot && "detail" in savedSnapshot ? savedSnapshot.detail : null;
        throw new Error(detail || `Server returned ${response.status}`);
      }

      seededRef.current = true;
      rqClient.setQueryData(["unified-settings"], savedSnapshot);
      persistAppSettings(settings);
      persistTabVisibility(localVisibility);
      setLocalSettings(settings);
      editingRef.current = false;
      setDirty(false);
      onSettingsChange(settings);
      onTabVisibilityChange(localVisibility);
      toast(landingHidden ? "Settings saved: landing tab reset to first visible" : "Settings saved");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      toast(`Settings were not saved: ${message}`);
    } finally {
      saveInFlightRef.current = false;
      setIsSaving(false);
    }
  };

  const handleReset = async () => {
    if (saveInFlightRef.current) return;
    if (!window.confirm("Reset all settings to defaults?")) return;
    saveInFlightRef.current = true;
    setIsSaving(true);
    const d = { ...DEFAULT_APP_SETTINGS };
    // Persist the reset to the server too, so it survives reopen (the modal re-seeds
    // from GET /api/settings). Admin-only on the server; no-op for consumers.
    try {
      const response = await fetch("/api/settings", {
        method: "PUT", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
        general: {
          company_name: d.companyName, app_display_name: d.appDisplayName,
          default_date_range_days: d.defaultDateRangeDays, default_landing_tab: d.defaultLandingTab,
          auto_refresh_minutes: d.refreshIntervalMinutes, density: d.density, theme: d.theme,
          show_workspace_names: d.showWorkspaceNames,
          anonymize_users: d.anonymizeUsers,
        },
        tab_visibility: DEFAULT_VISIBILITY,
        thresholds: {
          spike_threshold_percent: d.alertSpikePercent, daily_budget: d.alertDailyBudget,
          workspace_budget: d.alertWorkspaceBudget, anomaly_sensitivity: d.anomalySensitivity,
        },
        experimental: {
          exp_setup_wizard_link: d.expSetupWizardLink,
          exp_debugger_link: d.expDebuggerLink,
          enable_architecture_view: d.enableArchitectureView,
        },
        }),
        signal: AbortSignal.timeout(10000),
      });
      const savedSnapshot = await response.json().catch(() => null) as UnifiedSettings | { detail?: string } | null;
      if (!response.ok) {
        const detail = savedSnapshot && "detail" in savedSnapshot ? savedSnapshot.detail : null;
        throw new Error(detail || `Server returned ${response.status}`);
      }

      seededRef.current = true;
      rqClient.setQueryData(["unified-settings"], savedSnapshot);
      persistAppSettings(d);
      persistTabVisibility(DEFAULT_VISIBILITY);
      setLocalSettings(d);
      setLocalVisibility({ ...DEFAULT_VISIBILITY });
      editingRef.current = false;
      setDirty(false);
      onSettingsChange(d);
      onTabVisibilityChange({ ...DEFAULT_VISIBILITY });
      toast("Settings reset to defaults");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      toast(`Settings were not reset: ${message}`);
    } finally {
      saveInFlightRef.current = false;
      setIsSaving(false);
    }
  };

  const navItems: { key: NavKey; label: string; admin?: boolean }[] = [
    { key: "general", label: "General" },
    { key: "tabs", label: "Dashboard tabs" },
    { key: "data", label: "Data & tables", admin: true },
    { key: "alerts", label: "Alerts & notifications", admin: true },
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
          color: active ? T.text : T.textSecondary, backgroundColor: active ? T.surface : "transparent",
          border: active ? `1px solid ${T.borderGroup}` : "1px solid transparent", cursor: "pointer",
        }}>
        {label}{badge}
      </button>
    );
  };

  return (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/30 p-4" onClick={(e) => e.target === e.currentTarget && requestClose()}>
      <div style={{ width: "100%", maxWidth: 1120, height: "min(760px, 90vh)", backgroundColor: T.surface, borderRadius: 8, border: `1px solid ${T.borderGroup}`, boxShadow: "0 20px 60px rgba(0,0,0,0.25)", display: "flex", flexDirection: "column", overflow: "hidden" }} onClick={(e) => e.stopPropagation()}>
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
              <div style={{ fontSize: 11, color: T.textFaint, padding: "4px 10px", fontFamily: "DM Mono, ui-monospace, monospace" }}>cost-obs {APP_VERSION}{version ? ` · ${version.slice(0, 7)}` : ""}</div>
            </div>
          </div>

          {/* Content */}
          <div style={{ flex: 1, minHeight: 0, overflowY: "auto", backgroundColor: T.surface }}>
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
              {!overlay && nav === "general" && <GeneralSection localSettings={localSettings} updateSetting={updateSetting} tabVisibility={localVisibility} caps={caps} />}
              {!overlay && nav === "tabs" && <DashboardTabsSection localVisibility={localVisibility} toggleTab={toggleTab} />}
              {!overlay && nav === "alerts" && <AlertsSection localSettings={localSettings} updateSetting={updateSetting} caps={caps} />}
              {!overlay && nav === "data" && <DataTablesSection localSettings={localSettings} updateSetting={updateSetting} caps={caps} />}
              {!overlay && nav === "access" && <AccessSection />}
              {!overlay && nav === "resources" && <ResourcesSection />}
              {!overlay && nav === "experimental" && <ExperimentalSection localSettings={localSettings} updateSetting={updateSetting} />}
            </div>
          </div>
        </div>

        {/* Footer */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 20px", borderTop: `1px solid ${T.borderGroup}` }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <button onClick={handleReset} disabled={isSaving} style={{ fontSize: 13, color: T.textSecondary, background: "none", border: "none", cursor: isSaving ? "not-allowed" : "pointer", opacity: isSaving ? 0.5 : 1 }}>Reset to defaults</button>
            {dirty && <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: T.warningFg }}><span style={{ width: 6, height: 6, borderRadius: 999, backgroundColor: "#E0A82E" }} />Unsaved changes</span>}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button onClick={requestClose} style={{ height: 32, borderRadius: 4, padding: "0 14px", fontSize: 13, fontWeight: 500, color: T.text, backgroundColor: T.surface, border: `1px solid ${T.borderControl}`, cursor: "pointer" }}>Cancel</button>
            <button onClick={handleSave} disabled={!dirty || isSaving}
              style={{ height: 32, borderRadius: 4, padding: "0 14px", fontSize: 13, fontWeight: 500, color: "#FFFFFF", backgroundColor: dirty && !isSaving ? T.primaryFill : "#A9C6DC", cursor: dirty && !isSaving ? "pointer" : "not-allowed", border: "none" }}>
              {isSaving ? "Saving…" : "Save changes"}
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
    color: active ? T.text : T.textSecondary, backgroundColor: active ? T.surface : "transparent",
    border: active ? `1px solid ${T.borderGroup}` : "1px solid transparent", cursor: "pointer",
  };
}
