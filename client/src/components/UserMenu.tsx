import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowUpRight,
  ChevronDown,
  ChevronRight,
  LayoutGrid,
  LogOut,
  Mail,
  Slack,
  UserRound,
} from "lucide-react";

interface UserMenuProps {
  name: string;
  email: string;
  isAdmin: boolean;
  workspaceHost: string | null | undefined;
}

type MenuItemElement = HTMLAnchorElement | HTMLButtonElement;

const SLACK_TEAM_ID = "T02EPKPG3";
const SLACK_MEMBER_ID = "U04H3555WMB";
const FEEDBACK_EMAIL = "mailto:samuel.a.mathews@gmail.com?subject=cost-obs%20v1.2%20feedback";

function workspaceBaseUrl(host: string | null | undefined): string | null {
  const trimmed = host?.trim();
  if (!trimmed) return null;
  return (trimmed.startsWith("http://") || trimmed.startsWith("https://")
    ? trimmed
    : `https://${trimmed}`
  ).replace(/\/+$/, "");
}

export function readableUserName(name: string, email: string): string {
  const candidate = name.trim();
  const opaqueIdentity = (
    !candidate
    || /^\d/.test(candidate)
    || /^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(candidate)
  );
  if (!opaqueIdentity) return candidate;

  const localPart = email.split("@")[0]?.trim();
  if (!localPart) return email;
  return localPart
    .split(/[._-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function userInitials(name: string, email: string): string {
  const parts = readableUserName(name, email).split(/[\s._-]+/).filter(Boolean);
  if (parts.length >= 2) return `${parts[0][0]}${parts[parts.length - 1][0]}`.toUpperCase();
  if (parts.length === 1 && parts[0]) return parts[0].slice(0, 2).toUpperCase();
  return email.slice(0, 2).toUpperCase();
}

export function UserMenu({ name, email, isAdmin, workspaceHost }: UserMenuProps) {
  const [open, setOpen] = useState(false);
  const [chooserOpen, setChooserOpen] = useState(false);
  const [chooserSide, setChooserSide] = useState<"left" | "right">("left");
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const feedbackRef = useRef<HTMLButtonElement>(null);
  const mainItemRefs = useRef<Array<MenuItemElement | null>>([]);
  const chooserItemRefs = useRef<Array<MenuItemElement | null>>([]);
  const openFocusIndexRef = useRef(0);
  const hoverCloseTimerRef = useRef<number | null>(null);
  const baseUrl = useMemo(() => workspaceBaseUrl(workspaceHost), [workspaceHost]);
  const displayName = useMemo(() => readableUserName(name, email), [name, email]);
  const initials = useMemo(() => userInitials(displayName, email), [displayName, email]);

  const closeMenu = useCallback((returnFocus = true) => {
    setOpen(false);
    setChooserOpen(false);
    if (returnFocus) {
      window.setTimeout(() => triggerRef.current?.focus(), 0);
    }
  }, []);

  const openChooser = useCallback((moveFocus: boolean) => {
    if (hoverCloseTimerRef.current !== null) {
      window.clearTimeout(hoverCloseTimerRef.current);
      hoverCloseTimerRef.current = null;
    }
    const row = feedbackRef.current?.getBoundingClientRect();
    setChooserSide(row && row.left < 194 ? "right" : "left");
    setChooserOpen(true);
    if (moveFocus) {
      window.setTimeout(() => chooserItemRefs.current[0]?.focus(), 0);
    }
  }, []);

  const scheduleChooserClose = () => {
    hoverCloseTimerRef.current = window.setTimeout(() => {
      setChooserOpen(false);
      hoverCloseTimerRef.current = null;
    }, 150);
  };

  useEffect(() => () => {
    if (hoverCloseTimerRef.current !== null) window.clearTimeout(hoverCloseTimerRef.current);
  }, []);

  useEffect(() => {
    if (!open) return;
    const timer = window.setTimeout(() => {
      mainItemRefs.current[openFocusIndexRef.current]?.focus();
    }, 0);
    return () => window.clearTimeout(timer);
  }, [open]);

  useEffect(() => {
    if (!open) return;

    const onPointerDown = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) closeMenu();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        closeMenu();
      }
    };
    const onRouteChange = () => closeMenu();

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    window.addEventListener("popstate", onRouteChange);
    window.addEventListener("hashchange", onRouteChange);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
      window.removeEventListener("popstate", onRouteChange);
      window.removeEventListener("hashchange", onRouteChange);
    };
  }, [closeMenu, open]);

  const moveMainFocus = (direction: 1 | -1) => {
    const items = mainItemRefs.current.filter((item): item is MenuItemElement => item !== null);
    if (items.length === 0) return;
    const currentIndex = items.indexOf(document.activeElement as MenuItemElement);
    const nextIndex = currentIndex < 0
      ? (direction === 1 ? 0 : items.length - 1)
      : (currentIndex + direction + items.length) % items.length;
    setChooserOpen(false);
    items[nextIndex].focus();
  };

  const onMainMenuKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      moveMainFocus(event.key === "ArrowDown" ? 1 : -1);
    } else if (event.key === "ArrowRight" && document.activeElement === feedbackRef.current) {
      event.preventDefault();
      openChooser(true);
    } else if (event.key === "ArrowLeft" && chooserOpen) {
      event.preventDefault();
      setChooserOpen(false);
      feedbackRef.current?.focus();
    }
  };

  const onChooserKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      event.stopPropagation();
      const items = chooserItemRefs.current.filter((item): item is MenuItemElement => item !== null);
      const currentIndex = items.indexOf(document.activeElement as MenuItemElement);
      const direction = event.key === "ArrowDown" ? 1 : -1;
      const nextIndex = currentIndex < 0
        ? (direction === 1 ? 0 : items.length - 1)
        : (currentIndex + direction + items.length) % items.length;
      items[nextIndex]?.focus();
    } else if (event.key === "ArrowLeft") {
      event.preventDefault();
      event.stopPropagation();
      setChooserOpen(false);
      feedbackRef.current?.focus();
    }
  };

  const openSlack = () => {
    let blurred = false;
    const onBlur = () => {
      blurred = true;
    };
    window.addEventListener("blur", onBlur, { once: true });
    window.location.href = `slack://user?team=${SLACK_TEAM_ID}&id=${SLACK_MEMBER_ID}`;
    window.setTimeout(() => {
      window.removeEventListener("blur", onBlur);
      if (!blurred && document.hasFocus()) {
        window.open(`https://databricks.slack.com/team/${SLACK_MEMBER_ID}`, "_blank", "noopener,noreferrer");
      }
    }, 800);
  };

  const itemClass = "flex h-[40px] w-full items-center gap-[11px] rounded-[6px] px-[10px] text-left text-[13.5px] font-medium text-[#1B3139] hover:bg-[#FBF9F6] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35";
  const unavailableClass = !baseUrl ? " pointer-events-none opacity-50" : "";

  return (
    <div ref={rootRef} className="relative shrink-0">
      <button
        ref={triggerRef}
        type="button"
        aria-label={`User menu for ${email}`}
        aria-haspopup="menu"
        aria-expanded={open}
        data-state={open ? "open" : "closed"}
        onClick={() => {
          if (open) {
            closeMenu();
          } else {
            openFocusIndexRef.current = 0;
            setOpen(true);
          }
        }}
        onKeyDown={(event) => {
          if (event.key === "ArrowDown" || event.key === "ArrowUp") {
            event.preventDefault();
            openFocusIndexRef.current = event.key === "ArrowDown" ? 0 : 2;
            setOpen(true);
          }
        }}
        className={`rail-user-trigger rail-control-border flex min-w-0 items-center gap-0 rounded-[8px] border py-[4px] px-[5px] text-[12.5px] font-medium text-[#E9EFED] transition-[background-color,border-color,box-shadow] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35 focus-visible:ring-offset-1 focus-visible:ring-offset-[#1B3139] min-[900px]:gap-[9px] min-[900px]:pl-[5px] min-[900px]:pr-[9px] ${open ? "bg-[#294A56] shadow-[0_1px_3px_rgba(4,18,23,.28),inset_0_1px_0_rgba(117,157,170,.12)]" : "bg-white/[.07] hover:bg-[#243F49]"}`}
      >
        <span className="flex h-[24px] w-[24px] items-center justify-center rounded-full border border-[#D44D38] bg-[#FF5F46] text-[10px] font-bold text-white shadow-sm">
          {initials}
        </span>
        <UserRound
          size={13}
          strokeWidth={1.8}
          className="ml-[5px] shrink-0 text-[#B8CCD2] min-[900px]:ml-0"
          data-testid="user-menu-silhouette"
          aria-hidden="true"
        />
        <span className="hidden max-w-[88px] truncate min-[900px]:block min-[1280px]:max-w-[160px] min-[1536px]:max-w-[220px]">{email}</span>
        <ChevronDown size={12} strokeWidth={2} className={`hidden shrink-0 text-[#B8CCD2] transition-transform min-[900px]:block ${open ? "rotate-180" : ""}`} aria-hidden="true" />
      </button>

      {open && (
        <div
          role="menu"
          aria-label="User menu"
          onKeyDown={onMainMenuKeyDown}
          className="animate-fade-in absolute right-0 top-full z-50 mt-[8px] w-[300px] rounded-[10px] border border-[#E4E2DD] bg-white p-[8px] text-[#1B3139] shadow-[0_8px_28px_rgba(11,32,38,.16)]"
        >
          <div className="flex items-center gap-[10px] px-[8px] py-[7px]">
            <span className="flex h-[36px] w-[36px] shrink-0 items-center justify-center rounded-full border border-[#FFD9CE] bg-[#FFEDE8] text-[12px] font-bold text-[#D82A18]">
              {initials}
            </span>
            <span className="min-w-0 flex-1">
              <span className="block truncate text-[13.5px] font-semibold text-[#1B3139]">{displayName}</span>
              <span className="block truncate text-[11.5px] text-[#618794]">{email}</span>
            </span>
            {isAdmin && (
              <span className="rounded-full bg-[#EEEDE9] px-[8px] py-[3px] text-[11px] font-semibold text-[#618794]">
                Admin
              </span>
            )}
          </div>

          <div className="my-[6px] h-px bg-[#EFECE6]" />

          <a
            ref={(element) => { mainItemRefs.current[0] = element; }}
            role="menuitem"
            tabIndex={-1}
            href={baseUrl ? `${baseUrl}/apps` : undefined}
            aria-disabled={!baseUrl}
            onClick={() => closeMenu(false)}
            className={`${itemClass}${unavailableClass}`}
          >
            <LayoutGrid size={15} className="text-[#618794]" aria-hidden="true" />
            <span className="flex-1">Back to Apps</span>
            <ArrowUpRight size={13} className="text-[#618794]" aria-hidden="true" />
          </a>

          <div
            className="relative"
            onMouseEnter={() => openChooser(false)}
            onMouseLeave={scheduleChooserClose}
          >
            <button
              ref={(element) => {
                feedbackRef.current = element;
                mainItemRefs.current[1] = element;
              }}
              type="button"
              role="menuitem"
              tabIndex={-1}
              aria-haspopup="menu"
              aria-expanded={chooserOpen}
              onClick={() => openChooser(false)}
              className={itemClass}
            >
              <img src="/brand/databricks-symbol-red.svg" alt="" className="h-[15px] w-[14px] object-contain" />
              <span className="flex-1">Report Feedback</span>
              <ChevronRight size={13} className="text-[#618794]" aria-hidden="true" />
            </button>

            {chooserOpen && (
              <div
                role="menu"
                aria-label="Send via"
                onKeyDown={onChooserKeyDown}
                onMouseEnter={() => openChooser(false)}
                className={`animate-fade-in absolute top-0 z-60 w-[186px] rounded-[10px] border border-[#E4E2DD] bg-white p-[8px] shadow-[0_8px_28px_rgba(11,32,38,.16)] ${chooserSide === "left" ? "right-full mr-[8px]" : "left-full ml-[8px]"}`}
              >
                <div className="px-[10px] pb-[5px] pt-[3px] text-[10.5px] font-bold tracking-[.07em] text-[#618794]">
                  SEND VIA
                </div>
                <button
                  ref={(element) => { chooserItemRefs.current[0] = element; }}
                  type="button"
                  role="menuitem"
                  tabIndex={-1}
                  onClick={openSlack}
                  className="flex h-[36px] w-full items-center gap-[10px] rounded-[6px] px-[10px] text-[13px] font-medium text-[#1B3139] hover:bg-[#FBF9F6] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35"
                >
                  <Slack size={14} className="text-[#618794]" aria-hidden="true" />
                  Slack DM
                </button>
                <a
                  ref={(element) => { chooserItemRefs.current[1] = element; }}
                  role="menuitem"
                  tabIndex={-1}
                  href={FEEDBACK_EMAIL}
                  className="flex h-[36px] w-full items-center gap-[10px] rounded-[6px] px-[10px] text-[13px] font-medium text-[#1B3139] hover:bg-[#FBF9F6] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35"
                >
                  <Mail size={14} className="text-[#618794]" aria-hidden="true" />
                  Email
                </a>
              </div>
            )}
          </div>

          <div className="my-[6px] h-px bg-[#EFECE6]" />

          <a
            ref={(element) => { mainItemRefs.current[2] = element; }}
            role="menuitem"
            tabIndex={-1}
            href={baseUrl ?? undefined}
            aria-disabled={!baseUrl}
            onClick={() => closeMenu(false)}
            className={`${itemClass} text-[#D82A18]${unavailableClass}`}
          >
            <LogOut size={15} aria-hidden="true" />
            Logout
          </a>
        </div>
      )}
    </div>
  );
}
