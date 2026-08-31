import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DateRangePicker } from "../DateRangePicker";

function renderPicker(onChange = vi.fn()) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  render(
    <QueryClientProvider client={client}>
      <DateRangePicker
        value={{ startDate: "2026-08-01", endDate: "2026-08-29" }}
        onChange={onChange}
      />
    </QueryClientProvider>,
  );
  fireEvent.click(screen.getByRole("button", { name: /Aug 1, 2026 to Aug 29, 2026/i }));
  return onChange;
}

function inclusiveDays(startDate: string, endDate: string): number {
  return (
    Math.round(
      (Date.parse(`${endDate}T00:00:00Z`) - Date.parse(`${startDate}T00:00:00Z`))
        / 86_400_000,
    ) + 1
  );
}

describe("DateRangePicker", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-30T12:00:00"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([7, 14, 30, 90])(
    "makes Last %i days an exact inclusive window ending yesterday",
    (days) => {
      const onChange = renderPicker();

      fireEvent.click(screen.getByRole("button", { name: `Last ${days} days` }));

      const range = onChange.mock.calls[0][0];
      expect(range.endDate).toBe("2026-08-29");
      expect(inclusiveDays(range.startDate, range.endDate)).toBe(days);
    },
  );

  it("does not reverse This month on the first day of a month", () => {
    vi.setSystemTime(new Date("2026-09-01T12:00:00"));
    const onChange = renderPicker();

    fireEvent.click(screen.getByRole("button", { name: "This month" }));

    expect(onChange).toHaveBeenCalledWith({
      startDate: "2026-08-31",
      endDate: "2026-08-31",
    });
  });

  it("does not offer a year-to-date range that can exceed six months", () => {
    renderPicker();
    expect(screen.queryByRole("button", { name: "Year to date" })).not.toBeInTheDocument();
  });

  it.each([
    ["2026-08-30T12:00:00", "2026-02-28", "2026-08-29"],
    ["2027-01-31T12:00:00", "2026-07-30", "2027-01-30"],
  ])("keeps six-month presets valid at month boundaries", (now, startDate, endDate) => {
    vi.setSystemTime(new Date(now));
    const onChange = renderPicker();
    fireEvent.click(screen.getByRole("button", { name: "Last 6 months" }));
    expect(onChange).toHaveBeenCalledWith({ startDate, endDate });
  });

  it("announces reversed and future custom-range errors without applying", () => {
    const onChange = renderPicker();
    const start = screen.getByLabelText("Start");
    const end = screen.getByLabelText("End");

    fireEvent.change(start, { target: { value: "2026-08-20" } });
    fireEvent.change(end, { target: { value: "2026-08-19" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Start date must be on or before end date.",
    );
    expect(start).toHaveAttribute("aria-invalid", "true");
    expect(onChange).not.toHaveBeenCalled();

    fireEvent.change(end, { target: { value: "2026-08-30" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));

    expect(screen.getByRole("alert")).toHaveTextContent(
      "End date must be yesterday or earlier.",
    );
    expect(onChange).not.toHaveBeenCalled();
  });

  it("rejects a custom range beyond six calendar months", () => {
    const onChange = renderPicker();
    fireEvent.change(screen.getByLabelText("Start"), {
      target: { value: "2026-02-27" },
    });
    fireEvent.change(screen.getByLabelText("End"), {
      target: { value: "2026-08-29" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Date range cannot exceed 6 calendar months.",
    );
    expect(onChange).not.toHaveBeenCalled();
  });
});
