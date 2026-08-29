import { useState, useRef, useEffect } from "react";
import { useUpdatingIndicator } from "@/hooks/useUpdatingIndicator";
import { C } from "@/theme";
import { Spinner } from "./Spinner";

interface Workspace {
  workspace_id: string | null;
  workspace_name?: string | null;
  historical?: boolean;
}

// Guards against ids that would render a bogus option: null, or the strings
// "None"/"null"/"" that leak in when a null workspace id is stringified upstream.
function isValidWorkspaceId(id: string | null | undefined): id is string {
  if (id == null) return false;
  const s = String(id).trim().toLowerCase();
  return s !== "" && s !== "none" && s !== "null";
}

interface WorkspaceFilterProps {
  workspaces: Workspace[];
  selectedIds: string[];
  onChange: (ids: string[]) => void;
  isLoading?: boolean;
  variant?: "header" | "rail";
}

export function WorkspaceFilter({ workspaces, selectedIds, onChange, isLoading, variant = "header" }: WorkspaceFilterProps) {
  const { updating, arm } = useUpdatingIndicator();
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState("");
  // Draft state: reflects checkbox clicks but is NOT applied until "Apply" is clicked.
  // draftAll=true means "all workspaces" (no filter). draftAll=false + draftIds=[]
  // means "nothing selected" (Apply is disabled until at least one is checked).
  const [draftAll, setDraftAll] = useState(true);
  const [draftIds, setDraftIds] = useState<string[]>([]);
  const searchRef = useRef<HTMLInputElement>(null);

  const validWorkspaces = workspaces.filter((ws) => isValidWorkspaceId(ws.workspace_id));

  // Sync draft from applied state each time the dropdown opens
  useEffect(() => {
    if (isOpen) {
      setDraftAll(selectedIds.length === 0);
      setDraftIds(selectedIds);
      setSearch("");
      setTimeout(() => searchRef.current?.focus(), 0);
    }
  }, [isOpen]);

  if (isLoading) {
    return (
      <div className={variant === "rail"
        ? "rail-workspace-filter rail-control-border flex h-[32px] max-w-[116px] items-center gap-[6px] whitespace-nowrap rounded-[8px] border bg-white/[.07] px-[8px] text-[12.5px] font-medium text-[#E9EFED] min-[1280px]:max-w-none min-[1280px]:gap-[8px] min-[1280px]:px-[12px]"
        : "flex items-center gap-2 whitespace-nowrap rounded-full border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-500"
      }>
        <Spinner size="sm" />
        <span className="truncate">
          <span className="min-[1180px]:hidden">Workspaces…</span>
          <span className="hidden min-[1180px]:inline">Loading workspaces…</span>
        </span>
      </div>
    );
  }

  if (validWorkspaces.length <= 1) return null;

  const allSelected = selectedIds.length === 0;
  const applyEnabled = draftAll || draftIds.length > 0;

  function draftToggle(id: string) {
    if (draftAll) {
      setDraftAll(false);
      setDraftIds([id]);
      return;
    }
    if (draftIds.includes(id)) {
      const next = draftIds.filter((x) => x !== id);
      setDraftIds(next);
      // Don't auto-switch to "all" when last is unchecked: let the user see nothing
      // selected and be forced to pick something before Apply is enabled.
    } else {
      const next = [...draftIds, id];
      if (next.length === validWorkspaces.length) {
        setDraftAll(true);
        setDraftIds([]);
      } else {
        setDraftIds(next);
      }
    }
  }

  function handleApply() {
    if (!applyEnabled) return;
    onChange(draftAll ? [] : draftIds);
    arm();
    setIsOpen(false);
  }

  function label() {
    if (allSelected) {
      return "All Workspaces";
    }
    if (selectedIds.length === 1) {
      const ws = validWorkspaces.find((w) => w.workspace_id === selectedIds[0]);
      return ws?.workspace_name || `Workspace ${selectedIds[0]}`;
    }
    return `${selectedIds.length} Workspaces`;
  }

  return (
    <div className={variant === "rail" ? "relative min-w-0 shrink" : "relative"}>
      {isOpen && (
        <div className="fixed inset-0 z-10" onClick={() => setIsOpen(false)} />
      )}

      <button
        onClick={() => setIsOpen((o) => !o)}
        aria-label={updating ? "Updating workspaces" : label()}
        className={variant === "rail"
          ? "rail-workspace-filter rail-control-border flex h-[32px] max-w-[116px] items-center gap-[6px] whitespace-nowrap rounded-[8px] border bg-white/[.07] px-[8px] text-[12.5px] font-medium text-[#E9EFED] transition-colors hover:bg-white/[.12] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35 focus-visible:ring-offset-1 focus-visible:ring-offset-[#1B3139] min-[1280px]:max-w-[190px] min-[1280px]:gap-[8px] min-[1280px]:px-[12px]"
          : "co-filter flex items-center gap-2 whitespace-nowrap px-3"
        }
      >
        {updating ? (
          <Spinner size="sm" />
        ) : (
          <svg className={variant === "rail" ? "h-[14px] w-[14px] shrink-0 opacity-70" : "h-4 w-4 shrink-0 text-gray-500"} fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
          </svg>
        )}
        <span className="max-w-[68px] truncate min-[1280px]:max-w-[140px]">
          {updating ? "Updating…" : (
            <>
              <span className="min-[1180px]:hidden">
                {allSelected ? "Workspaces" : selectedIds.length === 1 ? "1 workspace" : `${selectedIds.length} workspaces`}
              </span>
              <span className="hidden min-[1180px]:inline">{label()}</span>
            </>
          )}
        </span>
        {!allSelected && (
          <button
            onClick={(e) => { e.stopPropagation(); onChange([]); }}
            className={variant === "rail"
              ? "ml-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-white/[.14] text-current opacity-70 hover:bg-white/[.22]"
              : "ml-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-gray-200 text-gray-500 hover:bg-gray-300"
            }
            title="Clear filter"
          >
            <svg className="h-2.5 w-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
        <svg
          className={`${variant === "rail" ? "h-[12px] w-[12px] opacity-70" : "ml-0.5 h-4 w-4 text-gray-500"} shrink-0 transition-transform ${isOpen ? "rotate-180" : ""}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {isOpen && (
        <div className="co-filter-menu absolute left-0 z-20 mt-2 min-w-[220px] p-3">
          <div className="mb-2 flex items-center justify-between">
            <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Workspaces</span>
            <div className="flex gap-2">
              <button
                onClick={() => { setDraftAll(true); setDraftIds([]); }}
                className="text-xs text-gray-500 hover:text-gray-800"
              >
                All
              </button>
              <span className="text-gray-300">·</span>
              <button
                onClick={() => { setDraftAll(false); setDraftIds([]); }}
                className="text-xs text-gray-500 hover:text-gray-800"
              >
                Clear
              </button>
            </div>
          </div>
          <div className="mb-2">
            <div className="flex items-center gap-1.5 rounded-md border border-gray-200 bg-gray-50 px-2 py-1.5">
              <svg className="h-3.5 w-3.5 shrink-0 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-4.35-4.35M17 11A6 6 0 1 1 5 11a6 6 0 0 1 12 0z" />
              </svg>
              <input
                ref={searchRef}
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search workspaces…"
                className="w-full bg-transparent text-xs text-gray-700 placeholder-gray-400 outline-none"
              />
              {search && (
                <button onClick={() => setSearch("")} className="shrink-0 text-gray-500 hover:text-gray-700">
                  <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              )}
            </div>
          </div>
          <div className="space-y-1 max-h-60 overflow-y-auto">
            {(() => {
              const q = search.toLowerCase();
              const filtered = validWorkspaces.filter((ws) =>
                !search || (ws.workspace_name || `Workspace ${ws.workspace_id}` || "").toLowerCase().includes(q)
              );
              if (filtered.length === 0) {
                return <p className="px-2 py-3 text-center text-xs text-gray-500">No workspaces match</p>;
              }

              const renderRow = (ws: Workspace) => {
                const id = ws.workspace_id!;
                const checked = draftAll || draftIds.includes(id);
                return (
                  <label
                    key={id}
                    className={`flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 ${checked && !draftAll ? "bg-red-50 hover:bg-red-100" : "hover:bg-gray-50"}`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => draftToggle(id)}
                      className="h-3.5 w-3.5 rounded border-gray-300 accent-lava"
                    />
                    <span className="flex-1 truncate text-sm text-gray-700">
                      {ws.workspace_name || `Workspace ${id}`}
                    </span>
                    {ws.historical && (
                      <span className="shrink-0 rounded border border-gray-200 bg-gray-100 px-1.5 py-0.5 text-[10px] font-medium text-gray-600" title="This workspace no longer exists in the account. Its data is historical.">
                        historical
                      </span>
                    )}
                  </label>
                );
              };

              // When there's an active search or all/none selected, render flat
              if (search || draftAll || draftIds.length === 0) {
                return filtered.map(renderRow);
              }

              // Pin selected to top
              const selectedRows = filtered.filter((ws) => draftIds.includes(ws.workspace_id!));
              const unselectedRows = filtered.filter((ws) => !draftIds.includes(ws.workspace_id!));

              return (
                <>
                  {selectedRows.length > 0 && (
                    <>
                      <p className="px-2 pb-0.5 pt-1 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                        Selected
                      </p>
                      {selectedRows.map(renderRow)}
                    </>
                  )}
                  {unselectedRows.length > 0 && (
                    <>
                      {selectedRows.length > 0 && <div className="my-1 border-t border-gray-100" />}
                      <p className="px-2 pb-0.5 pt-1 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                        Others
                      </p>
                      {unselectedRows.map(renderRow)}
                    </>
                  )}
                </>
              );
            })()}
          </div>
          <div className="mt-3 flex items-center justify-between border-t border-gray-100 pt-2">
            <span className="text-[11px] text-gray-500">
              {draftAll ? `All ${validWorkspaces.length}` : `${draftIds.length} of ${validWorkspaces.length}`} selected
            </span>
            <button
              onClick={handleApply}
              disabled={!applyEnabled}
              className="rounded-md px-3 py-1.5 text-xs font-medium text-white transition-colors disabled:cursor-not-allowed disabled:opacity-40"
              style={{ backgroundColor: applyEnabled ? C.lava : C.busy }}
            >
              Apply
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
