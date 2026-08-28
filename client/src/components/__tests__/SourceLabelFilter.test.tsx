import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SourceLabelFilter } from "../SourceLabelFilter";
import {
  getActiveSourceLabels,
  setActiveSourceLabels,
} from "@/hooks/useBillingData";

const sourceData = (labels: string[]) => ({
  local_label: "local",
  sources: labels
    .filter((label) => label !== "local")
    .map((label) => ({ label, catalog: "shared", schema: "cost_obs" })),
});

afterEach(() => {
  setActiveSourceLabels([]);
  vi.unstubAllGlobals();
});

describe("source label reconciliation", () => {
  it("keeps All selected when a new source appears", async () => {
    let current = sourceData(["local", "west"]);
    vi.stubGlobal("fetch", vi.fn(async () => ({
      ok: true,
      json: async () => current,
    })));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["mv-sources"], current);
    const onApplied = vi.fn();

    render(
      <QueryClientProvider client={client}>
        <SourceLabelFilter onApplied={onApplied} />
      </QueryClientProvider>,
    );
    expect(await screen.findByRole("button", { name: "All sources" })).toBeInTheDocument();

    current = sourceData(["local", "west", "east"]);
    client.setQueryData(["mv-sources"], current);

    await waitFor(() => expect(screen.getByRole("button", { name: "All sources" })).toBeInTheDocument());
    expect(getActiveSourceLabels()).toEqual([]);
    expect(onApplied).not.toHaveBeenCalled();
  });

  it("intersects a partial applied scope when selected labels disappear", async () => {
    setActiveSourceLabels(["local", "west"]);
    let current = sourceData(["local", "west", "east"]);
    vi.stubGlobal("fetch", vi.fn(async () => ({
      ok: true,
      json: async () => current,
    })));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["mv-sources"], current);
    const onApplied = vi.fn();

    render(
      <QueryClientProvider client={client}>
        <SourceLabelFilter onApplied={onApplied} />
      </QueryClientProvider>,
    );
    expect(await screen.findByRole("button", { name: "2 sources" })).toBeInTheDocument();

    current = sourceData(["local", "east"]);
    client.setQueryData(["mv-sources"], current);

    await waitFor(() => expect(screen.getByRole("button", { name: "local" })).toBeInTheDocument());
    expect(getActiveSourceLabels()).toEqual(["local"]);
    expect(onApplied).toHaveBeenCalledTimes(1);
  });
});
