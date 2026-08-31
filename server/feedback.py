"""Validation and normalization for customer-safe feedback destinations."""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlencode, urlparse

_SLACK_TEAM_PATTERN = re.compile(r"^T[A-Z0-9]{8,15}$")
_SLACK_MEMBER_PATTERN = re.compile(r"^[UW][A-Z0-9]{8,15}$")
_SLACK_PROFILE_PATH_PATTERN = re.compile(r"^/team/([UW][A-Z0-9]{8,15})/?$")


def safe_https_url(value: str, *, host: str | None = None) -> str | None:
    """Return a credential-free HTTPS URL without query or fragment data."""
    candidate = value.strip()
    if not candidate or any(character.isspace() for character in candidate):
        return None
    try:
        parsed = urlparse(candidate)
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or port is not None
        or parsed.query
        or parsed.fragment
        or (host and parsed.hostname != host)
    ):
        return None
    return candidate


def safe_slack_profile_url(value: str) -> str | None:
    """Return an HTTPS Slack member profile URL, never a webhook or token URL."""
    candidate = safe_https_url(value)
    if not candidate:
        return None
    parsed = urlparse(candidate)
    hostname = (parsed.hostname or "").lower()
    match = _SLACK_PROFILE_PATH_PATTERN.fullmatch(parsed.path)
    if (
        not hostname.endswith(".slack.com")
        or hostname == "hooks.slack.com"
        or match is None
        or not _SLACK_MEMBER_PATTERN.fullmatch(match.group(1))
    ):
        return None
    return candidate.removesuffix("/")


def safe_slack_deep_link(value: str) -> str | None:
    """Return a canonical Slack user deep link with complete, valid IDs."""
    candidate = value.strip()
    if not candidate or any(character.isspace() for character in candidate):
        return None
    parsed = urlparse(candidate)
    if (
        parsed.scheme != "slack"
        or parsed.netloc != "user"
        or parsed.path not in ("", "/")
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        return None
    try:
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return None
    if set(query) != {"team", "id"}:
        return None
    team_id = query["team"]
    member_id = query["id"]
    if (
        len(team_id) != 1
        or len(member_id) != 1
        or not _SLACK_TEAM_PATTERN.fullmatch(team_id[0])
        or not _SLACK_MEMBER_PATTERN.fullmatch(member_id[0])
    ):
        return None
    return f"slack://user?{urlencode({'team': team_id[0], 'id': member_id[0]})}"


def safe_feedback_slack_url(value: str) -> str | None:
    """Validate either supported Slack feedback target and normalize it."""
    return safe_slack_deep_link(value) or safe_slack_profile_url(value)


def safe_slack_ids_target(team_id: str, member_id: str) -> str | None:
    """Build a deep link only when both legacy split IDs are complete."""
    if (
        not _SLACK_TEAM_PATTERN.fullmatch(team_id)
        or not _SLACK_MEMBER_PATTERN.fullmatch(member_id)
    ):
        return None
    return f"slack://user?{urlencode({'team': team_id, 'id': member_id})}"
