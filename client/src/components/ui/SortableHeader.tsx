import type { ReactNode, ThHTMLAttributes } from "react";

type SortDirection = "asc" | "desc";

interface SortableHeaderProps extends Omit<ThHTMLAttributes<HTMLTableCellElement>, "aria-sort" | "onClick"> {
  field: string;
  activeField: string;
  direction: SortDirection;
  onSort: (field: string) => void;
  children: ReactNode;
  accessory?: ReactNode;
  align?: "left" | "right";
}

export function SortableHeader({
  field,
  activeField,
  direction,
  onSort,
  children,
  accessory,
  align = "left",
  className = "",
  ...props
}: SortableHeaderProps) {
  const active = field === activeField;
  const ariaSort = active
    ? direction === "asc" ? "ascending" : "descending"
    : "none";

  return (
    <th aria-sort={ariaSort} className={className} {...props}>
      <span className={`flex items-center gap-1 ${align === "right" ? "justify-end" : "justify-start"}`}>
        <button
          type="button"
          onClick={() => onSort(field)}
          className={`flex items-center gap-1 rounded py-0.5 text-inherit hover:text-gray-700 focus-visible:outline-none focus-visible:shadow-(--focus) ${
            align === "right" ? "justify-end text-right" : "justify-start text-left"
          }`}
        >
          <span>{children}</span>
          <span aria-hidden="true" className={active ? "" : "text-gray-300"}>
            {active ? (direction === "asc" ? "↑" : "↓") : "↕"}
          </span>
        </button>
        {accessory}
      </span>
    </th>
  );
}
