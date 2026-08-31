import { C } from "@/theme";

export function changeTone(pct: number): { bg: string; fg: string; label: string } {
  if (Math.abs(pct) < 0.5) {
    return { bg: C.coralTint, fg: C.lavaHover, label: "±0.0%" };
  }
  return {
    bg: C.coralTint,
    fg: C.lavaHover,
    label: `${pct > 0 ? "+" : ""}${pct.toFixed(1)}%`,
  };
}
