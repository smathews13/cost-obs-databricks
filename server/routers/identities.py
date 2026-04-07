"""Identity resolution — map service principal UUIDs to display names."""

import logging
import re
import time
from typing import Any

from fastapi import APIRouter, Query
from server.db import get_workspace_client

router = APIRouter()
logger = logging.getLogger(__name__)

# Simple in-process cache: {identifier: display_name}, refreshed every 10 minutes
_sp_cache: dict[str, str] = {}
_sp_cache_ts: float = 0
_SP_CACHE_TTL = 600  # 10 minutes

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{8,12}$", re.IGNORECASE)


def _index(cache: dict[str, str], key: Any, display_name: str) -> None:
    """Add key → display_name to cache if key is non-empty."""
    if key is not None:
        k = str(key).strip()
        if k:
            cache[k] = display_name


def _load_sp_cache() -> None:
    global _sp_cache, _sp_cache_ts
    try:
        w = get_workspace_client()

        # Use raw SCIM API so we get ALL fields the SDK may not expose
        # (externalId, OAuth clientId, etc.) — the billing UUID could be any of these.
        page_size = 200
        start = 1
        new_cache: dict[str, str] = {}

        while True:
            resp = w.api_client.do(
                "GET",
                "/api/2.0/preview/scim/v2/ServicePrincipals",
                query={"count": page_size, "startIndex": start},
            )
            resources = resp.get("Resources") or []
            if not resources:
                break

            for sp in resources:
                display_name = (sp.get("displayName") or "").strip()
                if not display_name:
                    continue

                # Index by every field that looks like it could appear in billing data
                _index(new_cache, sp.get("applicationId"), display_name)
                _index(new_cache, sp.get("id"), display_name)
                _index(new_cache, sp.get("externalId"), display_name)

                # Walk all string/int values looking for UUIDs we haven't indexed yet
                for v in sp.values():
                    if isinstance(v, (str, int)):
                        s = str(v).strip()
                        if _UUID_RE.match(s) and s not in new_cache:
                            new_cache[s] = display_name

            total = resp.get("totalResults", 0)
            start += len(resources)
            if start > total:
                break

        _sp_cache = new_cache
        _sp_cache_ts = time.monotonic()

        uuid_keys = [k for k in new_cache if _UUID_RE.match(k)]
        logger.info(
            f"Loaded {len(new_cache)} SP cache entries "
            f"({len(uuid_keys)} UUID-format); "
            f"sample UUID keys: {uuid_keys[:3]}"
        )
    except Exception as e:
        logger.warning(f"Could not load service principal names: {e}")


def resolve_sp_names(ids: list[str]) -> dict[str, str]:
    """Return {id: display_name} for resolved SPs only. Unresolved IDs are omitted."""
    global _sp_cache, _sp_cache_ts
    if time.monotonic() - _sp_cache_ts > _SP_CACHE_TTL:
        _load_sp_cache()
    return {id_: _sp_cache[id_] for id_ in ids if id_ in _sp_cache}


@router.get("/resolve")
async def resolve_identities(
    ids: str = Query(..., description="Comma-separated list of service principal identifiers"),
) -> dict[str, Any]:
    """Resolve service principal identifiers to display names.

    Returns a map of {id: display_name}. Unresolved IDs are omitted.
    """
    id_list = [i.strip() for i in ids.split(",") if i.strip()]
    if not id_list:
        return {"identities": {}}
    resolved = resolve_sp_names(id_list)
    return {"identities": resolved}
