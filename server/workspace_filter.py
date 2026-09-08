"""Workspace-scoping filter — reads COST_OBS_WORKSPACES env var or .settings/workspace_filter.json.

Set COST_OBS_WORKSPACES to a comma-separated list of workspace IDs to scope
the dashboard to specific workspaces only.  All workspaces are shown when the
variable is not set.

Example:
    COST_OBS_WORKSPACES=<workspace-id>[,<workspace-id>]
"""

import json
import logging
import os
import re
from contextvars import ContextVar

logger = logging.getLogger(__name__)

_SETTINGS_FILE = os.path.join(
    os.path.dirname(__file__), "..", ".settings", "workspace_filter.json"
)

_SAFE_ID_RE = re.compile(r'^[a-zA-Z0-9_\-\.]+$')
EMPTY_WORKSPACE_SCOPE_ID = "source-scope-no-overlap"
_include_historical_workspaces: ContextVar[bool] = ContextVar(
    "include_historical_workspaces",
    default=True,
)


def set_include_historical_workspaces(include: bool) -> object:
    return _include_historical_workspaces.set(bool(include))


def reset_include_historical_workspaces(token: object) -> None:
    try:
        _include_historical_workspaces.reset(token)  # type: ignore[arg-type]
    except Exception:
        pass


def include_historical_workspaces() -> bool:
    return _include_historical_workspaces.get()


def _is_safe_id(s: str) -> bool:
    """Accept numeric IDs and UUID/string workspace IDs; block anything that could inject SQL."""
    return bool(s and _SAFE_ID_RE.match(s))


def clear_cache() -> None:
    """No-op — kept for call-site compatibility. Cache was removed to fix multi-worker staleness."""
    pass


def get_configured_workspace_ids() -> list[str]:
    """Return validated workspace IDs. Checks env var first, then settings file. Empty = no filter.

    Always reads from source (no in-process cache) so every uvicorn worker sees fresh data
    immediately after the admin saves a new pool without a server restart.
    """
    raw = os.environ.get("COST_OBS_WORKSPACES", "").strip()
    if raw:
        parts = [w.strip() for w in raw.split(",") if w.strip()]
        valid = [p for p in parts if _is_safe_id(p)]
        invalid = sorted(set(parts) - set(valid))
        if invalid:
            logger.warning("COST_OBS_WORKSPACES: ignoring unsafe values: %s", invalid)
        return valid
    try:
        with open(_SETTINGS_FILE) as f:
            data = json.load(f)
        return [str(i) for i in data.get("workspace_ids", []) if _is_safe_id(str(i))]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def build_ws_filter_clause(
    col: str = "u.workspace_id",
    single_id: str | None = None,
    id_list: list[str] | None = None,
) -> str:
    """Return a SQL AND clause for workspace filtering, or empty string.

    id_list   — list of IDs from multi-select; takes precedence over env/file config.
    single_id — single ID (legacy compat); takes precedence over env/file config.

    Uses STRING comparison so both numeric and UUID-format workspace IDs are handled correctly.
    """
    if id_list is not None:
        if EMPTY_WORKSPACE_SCOPE_ID in id_list:
            return "AND 1 = 0"
        valid = [i for i in id_list if _is_safe_id(i)]
        if not valid:
            return ""
        quoted = ", ".join(f"'{i}'" for i in valid)
        return f"AND CAST({col} AS STRING) IN ({quoted})"
    if single_id and _is_safe_id(single_id):
        return f"AND CAST({col} AS STRING) = '{single_id}'"
    ids = get_configured_workspace_ids()
    if ids:
        quoted = ", ".join(f"'{i}'" for i in ids)
        return f"AND CAST({col} AS STRING) IN ({quoted})"
    if not include_historical_workspaces():
        return (
            f"AND EXISTS ("
            "SELECT 1 FROM system.access.workspaces_latest AS current_ws "
            f"WHERE CAST(current_ws.workspace_id AS STRING) = CAST({col} AS STRING)"
            ")"
        )
    return ""


def is_workspace_scoped() -> bool:
    """True when workspace IDs restrict data to specific workspaces."""
    return bool(get_configured_workspace_ids())


def inject_ws_filter(sql: str, clause: str) -> str:
    """Inject workspace filter clause after usage_quantity guard in a billing SQL string."""
    if not clause:
        return sql
    for anchor in ("AND u.usage_quantity > 0", "AND usage_quantity > 0"):
        if anchor in sql:
            return sql.replace(anchor, f"{anchor}\n    {clause}", 1)
    return sql
