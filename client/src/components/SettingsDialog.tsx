import { useState, useEffect, useCallback, useMemo, useRef } from "react";
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
  buildSettingsUpdate,
  hydrateSettingsFromServer,
  mergeUnifiedSettings,
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
type SaveStatus =
  | { kind: "idle" }
  | { kind: "saving"; count: number }
  | { kind: "saved"; count: number }
  | { kind: "error"; message: string };

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
  const [savedVisibility, setSavedVisibility] = useState<TabVisibility>(tabVisibility);
  const [savedSettings, setSavedSettings] = useState<AppSettings>(appSettings);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>({ kind: "idle" });
  const editingRef = useRef(false);
  const saveInFlightRef = useRef(false);
  const editRevisionRef = useRef(0);

  const pendingUpdate = useMemo(
    () => buildSettingsUpdate(localSettings, savedSettings, localVisibility, savedVisibility),
    [localSettings, savedSettings, localVisibility, savedVisibility],
  );
  const dirtyCount = pendingUpdate.updatedCount;
  const isSaving = saveStatus.kind === "saving";

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
    if (!unified || dirtyCount > 0 || editingRef.current || saveInFlightRef.current || seededRef.current) return;
    seededRef.current = true;
    const hydrated = hydrateSettingsFromServer(unified, localSettings, localVisibility);
    setLocalSettings(hydrated.appSettings);
    setLocalVisibility(hydrated.tabVisibility);
    setSavedSettings(hydrated.appSettings);
    setSavedVisibility(hydrated.tabVisibility);
  }, [unified, dirtyCount, localSettings, localVisibility]);

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

  // Kick non-admins out of admin-only sections/overlays.
  useEffect(() => {
    const adminSections: NavKey[] = ["alerts", "data", "access", "experimental"];
    if (permissions && !isAdmin && adminSections.includes(nav)) setNav("general");
    if (permissions && !isAdmin && overlay) setOverlay(null);
  }, [permissions, isAdmin, nav, overlay]);

  const requestClose = useCallback(() => {
    if (saveInFlightRef.current) return;
    if (dirtyCount > 0 && !window.confirm("You have unsaved changes. Discard them?")) return;
    onClose();
  }, [dirtyCount, onClose]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") requestClose(); };
    document.addEventListener("keydown", onKey);
    document.body.style.overflow = "hidden";
    return () => { document.removeEventListener("keydown", onKey); document.body.style.overflow = ""; };
  }, [requestClose]);

  const updateSetting = <K extends keyof AppSettings>(key: K, value: AppSettings[K]) => {
    editingRef.current = true;
    seededRef.current = true;
    editRevisionRef.current += 1;
    setSaveStatus({ kind: "idle" });
    setLocalSettings((prev) => ({ ...prev, [key]: value }));
  };
  const toggleTab = (key: keyof TabVisibility) => {
    const updated = { ...localVisibility, [key]: !localVisibility[key] };
    if (!Object.values(updated).some(Boolean)) return;
    editingRef.current = true;
    seededRef.current = true;
    editRevisionRef.current += 1;
    setSaveStatus({ kind: "idle" });
    setLocalVisibility(updated);
    if (!updated[localSettings.defaultLandingTab as keyof TabVisibility]) {
      const firstVisible = (Object.keys(updated) as (keyof TabVisibility)[]).find((tab) => updated[tab]);
      if (firstVisible) {
        setLocalSettings((current) => ({ ...current, defaultLandingTab: firstVisible }));
      }
    }
  };

  const saveDraft = async (
    settings: AppSettings,
    visibility: TabVisibility,
    successToast = "Settings saved",
  ) => {
    if (saveInFlightRef.current) return;
    const update = buildSettingsUpdate(settings, savedSettings, visibility, savedVisibility);
    if (update.updatedCount === 0) {
      setSaveStatus({ kind: "saved", count: 0 });
      return;
    }
    saveInFlightRef.current = true;
    setSaveStatus({ kind: "saving", count: update.updatedCount });
    const submittedRevision = editRevisionRef.current;
    // Landing-tab fallback: if the chosen tab is now hidden, use the first visible.
    let submittedSettings = settings;
    const landingHidden = !visibility[settings.defaultLandingTab as keyof TabVisibility];
    if (landingHidden) {
      const firstVisible = (Object.keys(visibility) as (keyof TabVisibility)[]).find((k) => visibility[k]);
      if (firstVisible) submittedSettings = { ...settings, defaultLandingTab: firstVisible };
    }
    const submittedUpdate = buildSettingsUpdate(
      submittedSettings,
      savedSettings,
      visibility,
      savedVisibility,
    );
    try {
      const response = await fetch("/api/settings", {
        method: "PUT", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(submittedUpdate.payload), signal: AbortSignal.timeout(15000),
      });
      const result = await response.json().catch(() => null) as { status?: string; updated_count?: number; detail?: string } | null;
      if (!response.ok) {
        throw new Error(result?.detail || `Server returned ${response.status}`);
      }

      seededRef.current = true;
      rqClient.setQueryData<UnifiedSettings | null>(["unified-settings"], (current) =>
        mergeUnifiedSettings(current, submittedUpdate.payload));
      rqClient.invalidateQueries({ queryKey: ["unified-settings"], refetchType: "none" });
      persistAppSettings(submittedSettings);
      persistTabVisibility(visibility);
      setSavedSettings(submittedSettings);
      setSavedVisibility(visibility);
      if (editRevisionRef.current === submittedRevision) {
        setLocalSettings(submittedSettings);
        editingRef.current = false;
        setSaveStatus({ kind: "saved", count: result?.updated_count ?? submittedUpdate.updatedCount });
      } else {
        setSaveStatus({ kind: "idle" });
      }
      onSettingsChange(submittedSettings);
      onTabVisibilityChange(visibility);
      toast(landingHidden ? "Settings saved: landing tab reset to first visible" : successToast);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      setSaveStatus({ kind: "error", message });
      toast(`Settings were not saved: ${message}`);
    } finally {
      saveInFlightRef.current = false;
    }
  };

  const handleSave = () => saveDraft(localSettings, localVisibility);

  const handleReset = async () => {
    if (saveInFlightRef.current) return;
    if (!window.confirm("Reset all settings to defaults?")) return;
    const d = { ...DEFAULT_APP_SETTINGS };
    const visibility = { ...DEFAULT_VISIBILITY };
    editingRef.current = true;
    seededRef.current = true;
    editRevisionRef.current += 1;
    setLocalSettings(d);
    setLocalVisibility(visibility);
    setSaveStatus({ kind: "idle" });
    await saveDraft(d, visibility, "Settings reset to defaults");
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
            <div style={{ width: "100%", padding: "20px 24px" }}>
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
            {saveStatus.kind === "saving" ? (
              <span role="status" style={{ display: "inline-flex", alignItems: "center", gap: 7, fontSize: 12, color: T.primary }}>
                <span style={{ width: 11, height: 11, borderRadius: 999, border: `2px solid ${T.borderControl}`, borderTopColor: T.primary }} />
                Saving {saveStatus.count} setting{saveStatus.count === 1 ? "" : "s"}…
              </span>
            ) : saveStatus.kind === "error" ? (
              <span role="alert" title={saveStatus.message} style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: T.dangerFg }}>
                <span aria-hidden style={{ fontWeight: 700 }}>×</span>
                Save failed · {dirtyCount} unsaved setting{dirtyCount === 1 ? "" : "s"}
              </span>
            ) : dirtyCount > 0 ? (
              <span role="status" style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: T.warningFg }}>
                <span style={{ width: 7, height: 7, borderRadius: 999, backgroundColor: "#E0A82E" }} />
                {dirtyCount} unsaved setting{dirtyCount === 1 ? "" : "s"}
              </span>
            ) : saveStatus.kind === "saved" ? (
              <span role="status" style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12, color: T.successFg }}>
                <svg aria-hidden width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="m3 8.3 3.1 3.1L13 4.8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" /></svg>
                {saveStatus.count} setting{saveStatus.count === 1 ? "" : "s"} updated
              </span>
            ) : null}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button onClick={requestClose} style={{ height: 32, borderRadius: 4, padding: "0 14px", fontSize: 13, fontWeight: 500, color: T.text, backgroundColor: T.surface, border: `1px solid ${T.borderControl}`, cursor: "pointer" }}>Cancel</button>
            <button onClick={handleSave} disabled={dirtyCount === 0 || isSaving}
              style={{ height: 32, borderRadius: 4, padding: "0 14px", fontSize: 13, fontWeight: 500, color: "#FFFFFF", backgroundColor: dirtyCount > 0 && !isSaving ? T.primaryFill : "#A9C6DC", cursor: dirtyCount > 0 && !isSaving ? "pointer" : "not-allowed", border: "none" }}>
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
