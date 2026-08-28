import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import { AIMLCostCenter } from "../AIMLCostCenter";

describe("AIMLCostCenter error state", () => {
  it("shows a settled error instead of a loading or blank state", () => {
    const onRetry = vi.fn();
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    render(
      <QueryClientProvider client={client}>
        <AIMLCostCenter
          data={undefined}
          isLoading
          isError
          error={new Error("Timed out waiting for AI/ML data")}
          onRetry={onRetry}
        />
      </QueryClientProvider>,
    );

    expect(screen.getByText("Failed to load AI/ML data")).toBeInTheDocument();
    expect(screen.getByText("Timed out waiting for AI/ML data")).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: /loading/i })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRetry).toHaveBeenCalledOnce();
  });
});
