import { afterEach, describe, expect, it } from "vitest";
import {
  hydrateSettingsFromServer,
  loadAppSettings,
  loadTabVisibility,
  persistAppSettings,
  persistTabVisibility,
} from "../settingsHydration";

afterEach(() => {
  localStorage.clear();
});

describe("settings hydration", () => {
  it("maps the complete unified server snapshot into dashboard settings", () => {
    const currentSettings = loadAppSettings();
    const currentVisibility = loadTabVisibility();
    const hydrated = hydrateSettingsFromServer({
      general: {
        company_name: "Acme",
        app_display_name: "FinOps",
        default_date_range_days: 60,
        default_landing_tab: "infra",
        auto_refresh_minutes: 15,
        density: "compact",
        theme: "dark",
        show_workspace_names: false,
        anonymize_users: true,
      },
      thresholds: {
        spike_threshold_percent: 33,
        daily_budget: 1234,
        workspace_budget: 456,
        anomaly_sensitivity: "high",
      },
      experimental: {
        exp_setup_wizard_link: true,
        exp_debugger_link: true,
        enable_architecture_view: true,
      },
      tab_visibility: {
        dbu: false,
        infra: true,
        optimizer: false,
      },
    }, currentSettings, currentVisibility);

    expect(hydrated.appSettings).toMatchObject({
      companyName: "Acme",
      appDisplayName: "FinOps",
      defaultDateRangeDays: 60,
      defaultLandingTab: "infra",
      refreshIntervalMinutes: 15,
      density: "compact",
      compactMode: true,
      theme: "dark",
      darkMode: true,
      showWorkspaceNames: false,
      anonymizeUsers: true,
      alertSpikePercent: 33,
      alertDailyBudget: 1234,
      alertWorkspaceBudget: 456,
      anomalySensitivity: "high",
      expSetupWizardLink: true,
      expDebuggerLink: true,
      enableArchitectureView: true,
    });
    expect(hydrated.tabVisibility).toMatchObject({
      dbu: false,
      infra: true,
      optimizer: false,
    });
  });

  it("persists hydrated settings and visibility through the shared storage helpers", () => {
    const hydrated = hydrateSettingsFromServer({
      general: {
        default_date_range_days: 14,
        theme: "system",
        anonymize_users: true,
      },
      tab_visibility: { aiml: false, tagging: false },
    }, loadAppSettings(), loadTabVisibility());

    persistAppSettings(hydrated.appSettings);
    persistTabVisibility(hydrated.tabVisibility);

    expect(loadAppSettings()).toMatchObject({
      defaultDateRangeDays: 14,
      theme: "system",
      anonymizeUsers: true,
    });
    expect(loadTabVisibility()).toMatchObject({ aiml: false, tagging: false });
  });

  it("ignores malformed server values instead of clobbering valid local values", () => {
    const currentSettings = {
      ...loadAppSettings(),
      defaultDateRangeDays: 90 as const,
      theme: "system" as const,
      anomalySensitivity: "low" as const,
    };
    const hydrated = hydrateSettingsFromServer({
      general: {
        default_date_range_days: 365,
        theme: "neon",
      },
      thresholds: { anomaly_sensitivity: "extreme" },
      tab_visibility: { dbu: "no", sql: false },
    }, currentSettings, loadTabVisibility());

    expect(hydrated.appSettings.defaultDateRangeDays).toBe(90);
    expect(hydrated.appSettings.theme).toBe("system");
    expect(hydrated.appSettings.anomalySensitivity).toBe("low");
    expect(hydrated.tabVisibility.dbu).toBe(true);
    expect(hydrated.tabVisibility.sql).toBe(false);
  });
});
