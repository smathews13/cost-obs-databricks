"""Customer-safe runtime feedback configuration."""

from server.routers import user


def test_feedback_targets_default_to_public_issue_tracker(monkeypatch):
    for name in (
        "COST_OBS_FEEDBACK_GITHUB_URL",
        "COST_OBS_FEEDBACK_EMAIL",
        "COST_OBS_FEEDBACK_SLACK_URL",
        "COST_OBS_FEEDBACK_SLACK_TEAM_ID",
        "COST_OBS_FEEDBACK_SLACK_MEMBER_ID",
        "COST_OBS_FEEDBACK_SLACK_WEB_URL",
    ):
        monkeypatch.delenv(name, raising=False)

    targets = user._feedback_targets_from_env()

    assert targets == {
        "github_issue_url": (
            "https://github.com/smathews13/cost-obs-databricks-v1.0/issues/new"
        ),
        "email_href": None,
        "slack": None,
    }


def test_feedback_targets_accept_valid_optional_destinations(monkeypatch):
    team_id = "T" + ("1" * 8)
    member_id = "U" + ("2" * 8)
    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_GITHUB_URL",
        "https://github.com/example/project/issues/new",
    )
    monkeypatch.setenv("COST_OBS_FEEDBACK_EMAIL", "feedback@example.com")
    monkeypatch.setenv("COST_OBS_FEEDBACK_SLACK_TEAM_ID", team_id)
    monkeypatch.setenv("COST_OBS_FEEDBACK_SLACK_MEMBER_ID", member_id)
    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_SLACK_WEB_URL",
        "https://example.slack.com/team/member",
    )

    targets = user._feedback_targets_from_env()

    assert targets["email_href"] == (
        "mailto:feedback@example.com?subject=cost-obs%20v1.2%20feedback"
    )
    assert targets["slack"] == {
        "url": f"slack://user?team={team_id}&id={member_id}",
        "fallback_url": "https://example.slack.com/team/member",
    }


def test_feedback_targets_accept_direct_deep_link_or_https_profile(monkeypatch):
    team_id = "T" + ("5" * 8)
    member_id = "W" + ("6" * 8)
    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_SLACK_URL",
        f"slack://user?id={member_id}&team={team_id}",
    )
    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_SLACK_WEB_URL",
        "https://example.slack.com/team/member",
    )

    assert user._feedback_targets_from_env()["slack"] == {
        "url": f"slack://user?team={team_id}&id={member_id}",
        "fallback_url": "https://example.slack.com/team/member",
    }

    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_SLACK_URL",
        "https://example.slack.com/team/member",
    )
    assert user._feedback_targets_from_env()["slack"] == {
        "url": "https://example.slack.com/team/member",
        "fallback_url": None,
    }


def test_feedback_targets_reject_credentials_and_partial_slack(monkeypatch):
    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_GITHUB_URL",
        "https://token:secret@github.com/example/project/issues/new",
    )
    monkeypatch.setenv("COST_OBS_FEEDBACK_EMAIL", "not-an-address")
    monkeypatch.setenv("COST_OBS_FEEDBACK_SLACK_TEAM_ID", "T" + ("3" * 8))
    monkeypatch.setenv("COST_OBS_FEEDBACK_SLACK_MEMBER_ID", "U" + ("4" * 8))
    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_SLACK_URL",
        "javascript:alert(1)",
    )
    monkeypatch.setenv(
        "COST_OBS_FEEDBACK_SLACK_WEB_URL",
        "https://hooks.slack.com/services/do-not-return-this",
    )

    serialized = repr(user._feedback_targets_from_env())

    assert "secret" not in serialized
    assert "hooks.slack.com" not in serialized
    assert "not-an-address" not in serialized
