import { C } from "@/theme";

// Stable category-to-color mapping shared by charts and tests. The five primary
// visible categories deliberately use distinct colors.
export const AIML_CATEGORY_COLORS: Record<string, string> = {
  "Serverless Inference": C.s5,
  "Model Training": C.s2,
  "Feature Engineering": C.s3,
  "GPU Clusters": C.lava,
  "Model Serving": C.s4,
  "MLflow": C.s1,
  "OpenAI": C.coral,
  "Anthropic": C.maroon,
  "Gemini": C.amber,
  "AI Search": C.green,
  "Fine Tuning": "color-mix(in srgb, var(--s1) 68%, white)",
};

const FALLBACK_PALETTE = [
  C.s1,
  C.s2,
  C.s3,
  C.s4,
  C.s5,
  C.lava,
  C.coral,
  C.maroon,
  C.amber,
  C.green,
  "color-mix(in srgb, var(--s1) 72%, white)",
  "color-mix(in srgb, var(--s2) 72%, white)",
  "color-mix(in srgb, var(--s3) 72%, white)",
  "color-mix(in srgb, var(--s4) 72%, white)",
  "color-mix(in srgb, var(--s5) 72%, white)",
] as const;

export function buildAimlCategoryColorMap(
  categories: Iterable<string>,
): Record<string, string> {
  const names = Array.from(new Set(categories)).sort((a, b) => a.localeCompare(b));
  const map: Record<string, string> = {};
  const used = new Set<string>();

  for (const name of names) {
    const fixed = AIML_CATEGORY_COLORS[name];
    if (fixed) {
      map[name] = fixed;
      used.add(fixed);
    }
  }

  let paletteIndex = 0;
  let derivedIndex = 0;
  for (const name of names) {
    if (map[name]) continue;
    while (
      paletteIndex < FALLBACK_PALETTE.length
      && used.has(FALLBACK_PALETTE[paletteIndex])
    ) {
      paletteIndex += 1;
    }
    let color: string | undefined = FALLBACK_PALETTE[paletteIndex++];
    while (!color || used.has(color)) {
      const hue = ((derivedIndex + 1) * 137.508) % 360;
      const lightness = 42 + (derivedIndex % 3) * 9;
      color = `hsl(${hue.toFixed(3)} 68% ${lightness}%)`;
      derivedIndex += 1;
    }
    map[name] = color;
    used.add(color);
  }
  return map;
}
