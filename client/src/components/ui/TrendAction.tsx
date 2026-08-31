interface TrendActionProps {
  onActivate?: () => void;
  ariaLabel: string;
  label?: string;
  className?: string;
}

export function TrendAction({
  onActivate,
  ariaLabel,
  label = "See trend",
  className = "",
}: TrendActionProps) {
  if (!onActivate) return null;

  return (
    <button
      type="button"
      onClick={onActivate}
      aria-label={ariaLabel}
      className={`mt-1 rounded text-left text-xs font-semibold text-lava hover:text-lava-hover focus-visible:outline-none focus-visible:shadow-(--focus) ${className}`}
    >
      {label} <span aria-hidden="true">→</span>
    </button>
  );
}
