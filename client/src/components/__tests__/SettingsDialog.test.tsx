import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import {
  loadAppSettings,
  loadTabVisibility,
  SettingsDialog,
} from "../SettingsDialog";

it("discards saved settings for the removed Use Cases feature", () => {
  localStorage.setItem("coc-tab-visibility", JSON.stringify({ "use-cases": true }));
  localStorage.setItem("coc-app-settings", JSON.stringify({ enableUseCaseTracking: true }));

  expect(loadTabVisibility()).not.toHaveProperty("use-cases");
  expect(loadAppSettings()).not.toHaveProperty("enableUseCaseTracking");

  localStorage.clear();
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
  vi.unstubAllGlobals();
});
