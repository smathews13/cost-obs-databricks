import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
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
  it("shows detected provider logos for local and shared sources", async () => {
    const current = {
      local_label: "local-gcp",
      local_cloud: "gcp" as const,
      sources: [
        { label: "shared-aws", catalog: "aws_share", schema: "cost_obs", cloud: "aws" as const },
        { label: "shared-azure", catalog: "azure_share", schema: "cost_obs", cloud: "azure" as const },
      ],
    };
    vi.stubGlobal("fetch", vi.fn(async () => ({
      ok: true,
      json: async () => current,
    })));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["mv-sources"], current);

    render(
      <QueryClientProvider client={client}>
        <SourceLabelFilter />
      </QueryClientProvider>,
    );
    await userEvent.click(await screen.findByRole("button", { name: "3 Sources" }));

    expect(screen.getByTitle("Google Cloud")).toHaveAttribute(
      "src",
      expect.stringContaining("gcp.svg"),
    );
    expect(screen.getByTitle("AWS")).toHaveAttribute(
      "src",
      expect.stringContaining("aws"),
    );
    expect(screen.getByTitle("Azure")).toHaveAttribute(
      "src",
      expect.stringContaining("azure"),
    );
  });

  it("updates module scope before invoking the App refresh callback", async () => {
    const current = sourceData(["local", "west"]);
    vi.stubGlobal("fetch", vi.fn(async () => ({
      ok: true,
      json: async () => current,
    })));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["mv-sources"], current);
    const scopesSeen: string[][] = [];

    render(
      <QueryClientProvider client={client}>
        <SourceLabelFilter onApplied={async () => {
          scopesSeen.push(getActiveSourceLabels());
        }} />
      </QueryClientProvider>,
    );

    await userEvent.click(await screen.findByRole("button", { name: "2 Sources" }));
    await userEvent.click(screen.getByRole("checkbox", { name: /west/i }));
    await userEvent.click(screen.getByRole("button", { name: "Apply" }));

    await waitFor(() => expect(scopesSeen).toEqual([["local"]]));
  });

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
    expect(await screen.findByRole("button", { name: "2 Sources" })).toBeInTheDocument();

    current = sourceData(["local", "west", "east"]);
    client.setQueryData(["mv-sources"], current);

    await waitFor(() => expect(screen.getByRole("button", { name: "3 Sources" })).toBeInTheDocument());
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

  it("rolls back a failed scope and offers a direct retry", async () => {
    const current = sourceData(["local", "west"]);
    vi.stubGlobal("fetch", vi.fn(async () => ({
      ok: true,
      json: async () => current,
    })));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["mv-sources"], current);
    const scopesSeen: string[][] = [];
    let calls = 0;

    render(
      <QueryClientProvider client={client}>
        <SourceLabelFilter onApplied={async () => {
          calls += 1;
          scopesSeen.push(getActiveSourceLabels());
          if (calls === 1) throw new Error("warehouse unavailable");
        }} />
      </QueryClientProvider>,
    );

    await userEvent.click(await screen.findByRole("button", { name: "2 Sources" }));
    await userEvent.click(screen.getByRole("checkbox", { name: /west/i }));
    await userEvent.click(screen.getByRole("button", { name: "Apply" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Source filter was not applied");
    expect(getActiveSourceLabels()).toEqual([]);
    expect(scopesSeen).toEqual([["local"], []]);
    expect(screen.getByRole("button", { name: "2 Sources" })).toBeVisible();

    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => expect(getActiveSourceLabels()).toEqual(["local"]));
    expect(scopesSeen).toEqual([["local"], [], ["local"]]);
  });
});
