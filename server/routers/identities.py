"""Identity resolution — map service principal UUIDs to display names."""

import logging
import time
from typing import Any

from fastapi import APIRouter, Query
from server.db import get_workspace_client

router = APIRouter()
logger = logging.getLogger(__name__)

# Simple in-process cache: {uuid: display_name}, refreshed every 10 minutes
_sp_cache: dict[str, str] = {}
_sp_cache_ts: float = 0
_SP_CACHE_TTL = 600  # 10 minutes


def _load_sp_cache() -> None:
    global _sp_cache, _sp_cache_ts
    try:
        w = get_workspace_client()
        sps = w.service_principals.list()
        new_cache: dict[str, str] = {}
        for sp in sps:
            display_name = sp.display_name or ""
            if not display_name:
                continue
            # Index by application_id (may be int or UUID string depending on workspace)
            if sp.application_id is not None:
                new_cache[str(sp.application_id)] = display_name
            # Also index by SCIM id in case billing data uses that
            if sp.id is not None:
                new_cache[str(sp.id)] = display_name
        _sp_cache = new_cache
        _sp_cache_ts = time.monotonic()
        sample = list(new_cache.keys())[:3]
        logger.info(f"Loaded {len(new_cache)} SP name entries; sample keys: {sample}")
    except Exception as e:
        logger.warning(f"Could not load service principal names: {e}")


def resolve_sp_names(ids: list[str]) -> dict[str, str]:
    """Return {uuid: display_name} for resolved SPs only. Unresolved IDs are omitted."""
    global _sp_cache, _sp_cache_ts
    if time.monotonic() - _sp_cache_ts > _SP_CACHE_TTL:
        _load_sp_cache()
    return {id_: _sp_cache[id_] for id_ in ids if id_ in _sp_cache}


@router.get("/resolve")
async def resolve_identities(
    ids: str = Query(..., description="Comma-separated list of service principal UUIDs"),
) -> dict[str, Any]:
    """Resolve service principal UUIDs to display names.

    Returns a map of {uuid: display_name}. Unknown UUIDs are returned as-is.
    """
    id_list = [i.strip() for i in ids.split(",") if i.strip()]
    if not id_list:
        return {"identities": {}}
    resolved = resolve_sp_names(id_list)
    return {"identities": resolved}
