import { describe, expect, it } from "vitest";
import {
  formatCurrency,
  formatCurrencyCompact,
  formatKpiCurrency,
  formatDurationSeconds,
  formatNumber,
} from "./formatters";

describe("magnitude-aware metric formatting", () => {
  it("keeps full million-dollar magnitudes in detailed currency", () => {
    expect(formatCurrency(1_687_075.63)).toBe("$1,687,076");
  });

  it("compacts million-dollar values only through the KPI formatter", () => {
    expect(formatKpiCurrency(1_687_075.63)).toBe("$1.69M");
    expect(formatKpiCurrency(12.34)).toBe("$12.34");
  });

  it("keeps cents when small currency values need them", () => {
    expect(formatCurrency(12.34)).toBe("$12.34");
    expect(formatCurrency(0.63)).toBe("$0.63");
    expect(formatCurrency(12)).toBe("$12");
  });

  it("uses compact number notation without unnecessary trailing zeroes", () => {
    expect(formatNumber(22_008_909)).toBe("22M");
    expect(formatNumber(5_500)).toBe("5.5K");
    expect(formatNumber(98_100_000)).toBe("98.1M");
  });

  it("keeps a large duration value and unit in one compact string", () => {
    expect(formatDurationSeconds(21_924.7 * 24 * 60 * 60)).toBe("21.9K days");
  });

  it("formats negative compact currency with the currency sign", () => {
    expect(formatCurrencyCompact(-1_500)).toBe("-$1.5K");
  });
});
