"""Shared request and detail-response limits for API routes."""

from __future__ import annotations

import calendar
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


MAX_DATE_RANGE_MONTHS = 6
MAX_WORKSPACE_IDS = _env_int("COST_OBS_MAX_WORKSPACE_IDS", 100, 1, 500)
MAX_WORKSPACE_FILTER_BYTES = _env_int(
    "COST_OBS_MAX_WORKSPACE_FILTER_BYTES", 8192, 256, 32768
)
DETAIL_MAX_ROWS = _env_int("COST_OBS_DETAIL_MAX_ROWS", 1000, 20, 5000)
DETAIL_MAX_RESPONSE_BYTES = _env_int(
    "COST_OBS_DETAIL_MAX_RESPONSE_BYTES",
    2 * 1024 * 1024,
    64 * 1024,
    8 * 1024 * 1024,
)
_WORKSPACE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _request_error(message: str, code: str) -> HTTPException:
    return HTTPException(
        status_code=422,
        detail={"message": message, "error_code": code},
    )


def default_date_range(days: int = 30) -> tuple[str, str]:
    """Return an inclusive UTC window ending on the last complete day."""
    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=max(1, days) - 1)
    return start.isoformat(), end.isoformat()


def validate_date_range(
    start_date: str | None,
    end_date: str | None,
    *,
    default_start: str,
    default_end: str,
) -> tuple[str, str]:
    """Validate the dashboard's UTC, strict-ISO, six-calendar-month contract."""
    start_text = start_date or default_start
    end_text = end_date or default_end
    try:
        if not _ISO_DATE.fullmatch(start_text) or not _ISO_DATE.fullmatch(end_text):
            raise ValueError
        start = date.fromisoformat(start_text)
        end = date.fromisoformat(end_text)
    except (TypeError, ValueError) as exc:
        raise _request_error(
            "Dates must use ISO format YYYY-MM-DD.", "INVALID_DATE"
        ) from exc
    if start > end:
        raise _request_error(
            "Start date must be on or before end date.", "INVALID_DATE_RANGE"
        )
    _, yesterday_text = default_date_range(1)
    yesterday = date.fromisoformat(yesterday_text)
    if end > yesterday:
        raise _request_error(
            "End date must be yesterday or earlier.", "DATE_NOT_COMPLETE"
        )
    month_index = end.year * 12 + end.month - 1 - MAX_DATE_RANGE_MONTHS
    boundary_year, boundary_month_index = divmod(month_index, 12)
    earliest_start = date(
        boundary_year,
        boundary_month_index + 1,
        min(
            end.day,
            calendar.monthrange(boundary_year, boundary_month_index + 1)[1],
        ),
    )
    if start < earliest_start:
        raise _request_error(
            f"Date range cannot exceed {MAX_DATE_RANGE_MONTHS} calendar months.",
            "DATE_RANGE_TOO_LARGE",
        )
    return start.isoformat(), end.isoformat()


def parse_workspace_ids(raw: str | None) -> list[str] | None:
    """Parse, validate, de-duplicate, and cap a comma-separated workspace filter."""
    if raw is None or not raw.strip():
        return None
    if len(raw.encode("utf-8")) > MAX_WORKSPACE_FILTER_BYTES:
        raise _request_error(
            "Workspace filter is too large.", "WORKSPACE_FILTER_TOO_LARGE"
        )
    values: list[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        value = item.strip()
        if not value:
            continue
        if not _WORKSPACE_ID.fullmatch(value):
            raise _request_error(
                "Workspace IDs contain unsupported characters.",
                "INVALID_WORKSPACE_ID",
            )
        if value not in seen:
            seen.add(value)
            values.append(value)
        if len(values) > MAX_WORKSPACE_IDS:
            raise _request_error(
                f"workspace_ids may contain at most {MAX_WORKSPACE_IDS} values.",
                "TOO_MANY_WORKSPACES",
            )
    return values or None


def cap_detail_items(
    items: list[dict[str, Any]],
    *,
    max_rows: int = DETAIL_MAX_ROWS,
    max_bytes: int = DETAIL_MAX_RESPONSE_BYTES,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Bound returned detail rows/bytes while reporting exact truncation metadata.

    Callers should compute aggregate totals before invoking this helper. The
    helper only limits the detail list and never changes those totals.
    """
    limited: list[dict[str, Any]] = []
    size = 2
    reason: str | None = None
    for item in items:
        if len(limited) >= max_rows:
            reason = "rows"
            break
        item_size = len(
            json.dumps(item, default=str, separators=(",", ":")).encode("utf-8")
        ) + 1
        if size + item_size > max_bytes:
            reason = "bytes"
            break
        limited.append(item)
        size += item_size
    return limited, {
        "truncated": reason is not None,
        "truncation_reason": reason,
        "returned_rows": len(limited),
        "available_rows": len(items),
        "response_bytes": size,
    }
