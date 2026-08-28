import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import {
  loadAppSettings,
  loadTabVisibility,
  SettingsDialog,
} from "../SettingsDialog";

afterEach(() => {
  localStorage.clear();
  vi.unstubAllGlobals();
});

it("discards saved settings for the removed Use Cases feature", () => {
  localStorage.setItem("coc-tab-visibility", JSON.stringify({ "use-cases": true }));
  localStorage.setItem("coc-app-settings", JSON.stringify({
    enableUseCaseTracking: true,
    enableAppHostingComparison: true,
    enableAccuracyChecks: true,
  }));

  expect(loadTabVisibility()).not.toHaveProperty("use-cases");
  expect(loadAppSettings()).not.toHaveProperty("enableUseCaseTracking");
  expect(loadAppSettings()).not.toHaveProperty("enableAppHostingComparison");
  expect(loadAppSettings()).not.toHaveProperty("enableAccuracyChecks");
});

it("closes the settings dialog after Save changes is clicked", async () => {
  const onClose = vi.fn();
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
    ok: true,
    json: async () => ({}),
  }));

  render(
    <QueryClientProvider client={client}>
      <SettingsDialog
        isOpen
        onClose={onClose}
        onTabVisibilityChange={vi.fn()}
        onSettingsChange={vi.fn()}
        tabVisibility={loadTabVisibility()}
        appSettings={loadAppSettings()}
      />
    </QueryClientProvider>,
  );

  await userEvent.type(screen.getByPlaceholderText("e.g. Acme Corp"), "Acme");
  await userEvent.click(screen.getByRole("button", { name: "Save changes" }));

  expect(onClose).toHaveBeenCalledOnce();
});

it("renders, toggles, saves, and reloads user anonymization", async () => {
  let serverValue = false;
  const putBodies: Array<{ general: { anonymize_users: boolean } }> = [];
  vi.stubGlobal("fetch", vi.fn().mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url.endsWith("/api/settings") && init?.method === "PUT") {
      const body = JSON.parse(String(init.body));
      putBodies.push(body);
      serverValue = body.general.anonymize_users;
      return { ok: true, json: async () => ({}) };
    }
    if (url.endsWith("/api/settings")) {
      return { ok: true, json: async () => ({ general: { anonymize_users: serverValue } }) };
    }
    return { ok: true, json: async () => ({}) };
  }));

  const renderDialog = () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return render(
      <QueryClientProvider client={client}>
        <SettingsDialog
          isOpen
          onClose={vi.fn()}
          onTabVisibilityChange={vi.fn()}
          onSettingsChange={vi.fn()}
          tabVisibility={loadTabVisibility()}
          appSettings={loadAppSettings()}
        />
      </QueryClientProvider>,
    );
  };

  const first = renderDialog();
  await userEvent.click(screen.getByRole("button", { name: "Experimental" }));
  expect(screen.getByText(/Replace human email addresses with User 1/)).toBeInTheDocument();
  const toggle = screen.getByRole("switch", { name: "User anonymization" });
  expect(toggle).toHaveAttribute("aria-checked", "false");

  await userEvent.click(toggle);
  expect(toggle).toHaveAttribute("aria-checked", "true");
  await userEvent.click(screen.getByRole("button", { name: "Save changes" }));

  await waitFor(() => expect(putBodies.at(-1)?.general.anonymize_users).toBe(true));
  expect(loadAppSettings().anonymizeUsers).toBe(true);

  first.unmount();
  renderDialog();
  await userEvent.click(screen.getByRole("button", { name: "Experimental" }));
  await waitFor(() => expect(screen.getByRole("switch", { name: "User anonymization" })).toHaveAttribute("aria-checked", "true"));
});

it("persists the default when user anonymization is reset", async () => {
  const saved = loadAppSettings();
  saved.anonymizeUsers = true;
  const putBodies: Array<{ general: { anonymize_users: boolean } }> = [];
  vi.stubGlobal("confirm", vi.fn(() => true));
  vi.stubGlobal("fetch", vi.fn().mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
    if (String(input).endsWith("/api/settings") && init?.method === "PUT") {
      putBodies.push(JSON.parse(String(init.body)));
    }
    return { ok: true, json: async () => ({}) };
  }));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });

  render(
    <QueryClientProvider client={client}>
      <SettingsDialog
        isOpen
        onClose={vi.fn()}
        onTabVisibilityChange={vi.fn()}
        onSettingsChange={vi.fn()}
        tabVisibility={loadTabVisibility()}
        appSettings={saved}
      />
    </QueryClientProvider>,
  );

  await userEvent.click(screen.getByRole("button", { name: "Reset to defaults" }));
  await waitFor(() => expect(putBodies.at(-1)?.general.anonymize_users).toBe(false));
  expect(loadAppSettings().anonymizeUsers).toBe(false);
});
