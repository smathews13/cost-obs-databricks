import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

interface InfoPopoverProps {
  text?: string;
  content?: ReactNode;
  children?: ReactNode;
  label?: string;
  className?: string;
  triggerClassName?: string;
  panelClassName?: string;
  placement?: "top" | "bottom";
  stopClick?: boolean;
}

export function InfoPopover({
  text,
  content,
  children,
  label = "More information",
  className = "ml-1.5",
  triggerClassName,
  panelClassName = "w-72",
  placement = "top",
  stopClick = false,
}: InfoPopoverProps) {
  const id = useId();
  const rootRef = useRef<HTMLSpanElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const [open, setOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const [position, setPosition] = useState({ top: 0, left: 0 });

  const show = () => {
    const rect = triggerRef.current?.getBoundingClientRect();
    if (rect) {
      setPosition({
        top: placement === "bottom" ? rect.bottom + 8 : Math.max(8, rect.top - 8),
        left: rect.left + rect.width / 2,
      });
    }
    setOpen(true);
  };

  const positionPanel = (panel: HTMLSpanElement | null) => {
    if (!panel) return;
    const trigger = triggerRef.current?.getBoundingClientRect();
    if (!trigger) return;
    const halfWidth = panel.getBoundingClientRect().width / 2;
    const left = Math.min(
      Math.max(8 + halfWidth, trigger.left + trigger.width / 2),
      Math.max(8 + halfWidth, window.innerWidth - 8 - halfWidth),
    );
    panel.style.top = `${placement === "bottom" ? trigger.bottom + 8 : Math.max(8, trigger.top - 8)}px`;
    panel.style.left = `${left}px`;
  };

  useEffect(() => {
    if (!open) return;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setOpen(false);
        setPinned(false);
        triggerRef.current?.focus();
      }
    };
    const closeOutside = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
        setPinned(false);
      }
    };
    document.addEventListener("keydown", closeOnEscape);
    document.addEventListener("pointerdown", closeOutside);
    return () => {
      document.removeEventListener("keydown", closeOnEscape);
      document.removeEventListener("pointerdown", closeOutside);
    };
  }, [open]);

  return (
    <span
      ref={rootRef}
      className={`inline-flex ${className}`}
      onMouseEnter={show}
      onMouseLeave={() => {
        if (!pinned && document.activeElement !== triggerRef.current) setOpen(false);
      }}
    >
      <button
        ref={triggerRef}
        type="button"
        aria-label={label}
        aria-describedby={open ? id : undefined}
        aria-expanded={open}
        onFocus={show}
        onBlur={() => {
          setOpen(false);
          setPinned(false);
        }}
        onClick={(event) => {
          if (stopClick) event.stopPropagation();
          if (pinned) {
            setOpen(false);
            setPinned(false);
          } else {
            show();
            setPinned(true);
          }
        }}
        className={triggerClassName ?? "flex h-4 w-4 items-center justify-center rounded-full bg-gray-200 text-[10px] font-semibold normal-case text-gray-600 hover:text-gray-800 focus-visible:outline-none focus-visible:shadow-(--focus)"}
      >
        {children ?? <span aria-hidden="true">i</span>}
      </button>
      {open && createPortal(
        <span
          ref={positionPanel}
          id={id}
          role="tooltip"
          className={`pointer-events-none fixed z-[10000] -translate-x-1/2 rounded-lg bg-gray-900 px-3 py-2 text-xs font-normal normal-case leading-relaxed text-white shadow-lg ${placement === "top" ? "-translate-y-full" : ""} ${panelClassName}`}
          style={{ top: position.top, left: position.left }}
        >
          {content ?? text}
        </span>,
        document.body,
      )}
    </span>
  );
}
