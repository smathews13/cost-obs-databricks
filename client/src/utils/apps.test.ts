import { describe, expect, it } from "vitest";
import { getAppFallbackColor, getAppInitials } from "./apps";

function whiteContrastRatio(hex: string): number {
  const channels = [1, 3, 5].map(offset => {
    const value = Number.parseInt(hex.slice(offset, offset + 2), 16) / 255;
    return value <= 0.04045
      ? value / 12.92
      : ((value + 0.055) / 1.055) ** 2.4;
  });
  const luminance =
    0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
  return 1.05 / (luminance + 0.05);
}

describe("Apps fallback appearance", () => {
  it("is deterministic, varied, and readable with white initials", () => {
    const identities = Array.from({ length: 30 }, (_, index) => `app-${index}`);
    const colors = identities.map(getAppFallbackColor);

    expect(getAppFallbackColor("app-7")).toBe(getAppFallbackColor("app-7"));
    expect(new Set(colors).size).toBeGreaterThan(5);
    for (const color of colors) {
      expect(whiteContrastRatio(color)).toBeGreaterThanOrEqual(4.5);
    }
  });

  it("uses recognizable two-letter initials", () => {
    expect(getAppInitials("cost-observability", "id")).toBe("CO");
    expect(getAppInitials("Astrolabe", "id")).toBe("AS");
    expect(getAppInitials("", "app-id")).toBe("AI");
  });
});
