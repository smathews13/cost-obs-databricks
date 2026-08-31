"""Deployment provenance exposed to the top-rail badge."""

import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

from server import db
from server.routers import health


@pytest.fixture(autouse=True)
def reset_deployment_metadata_state():
    with health._deployment_metadata_lock:
        health._deployment_metadata_cache.clear()
        health._deployment_metadata_inflight.clear()
    yield
    with health._deployment_metadata_lock:
        health._deployment_metadata_cache.clear()
        health._deployment_metadata_inflight.clear()


def test_extracts_authoritative_active_deployment_metadata():
    app = SimpleNamespace(
        active_deployment=SimpleNamespace(
            create_time="2026-08-30T21:29:46Z",
            creator="deployer@example.com",
            git_source=SimpleNamespace(
                resolved_commit="ed86035f1234567890",
                commit="older-value",
            ),
        ),
        git_source=None,
        default_git_source=None,
    )

    assert health._deployment_metadata_from_app(app) == {
        "deployed_at": "2026-08-30T21:29:46Z",
        "deployer": "deployer@example.com",
        "commit_sha": "ed86035f1234567890",
        "available": True,
        "source": "databricks_apps_api",
    }


def test_missing_deployment_metadata_is_explicitly_unavailable(monkeypatch):
    monkeypatch.delenv("COST_OBS_DEPLOYED_AT", raising=False)
    monkeypatch.delenv("COST_OBS_DEPLOYER", raising=False)
    monkeypatch.delenv("COST_OBS_COMMIT_SHA", raising=False)

    assert health._deployment_metadata_from_app(SimpleNamespace()) == {
        "deployed_at": None,
        "deployer": None,
        "commit_sha": None,
        "available": False,
        "source": "databricks_apps_api",
    }
    assert health._deployment_metadata_from_env() == {
        "deployed_at": None,
        "deployer": None,
        "commit_sha": None,
        "available": False,
        "source": "release_environment",
    }


def test_deployment_metadata_cache_is_bounded_and_short_lived():
    assert health._deployment_metadata_cache.maxsize == 8
    assert health._deployment_metadata_cache.ttl == health.cache_ttls.DEPLOYMENT_METADATA


def test_release_environment_is_a_field_by_field_fallback(monkeypatch):
    monkeypatch.setenv("COST_OBS_DEPLOYED_AT", "2026-08-30T21:29:46Z")
    monkeypatch.setenv("COST_OBS_DEPLOYER", "")
    monkeypatch.setenv("COST_OBS_COMMIT_SHA", "ed86035f")

    assert health._deployment_metadata_from_env() == {
        "deployed_at": "2026-08-30T21:29:46Z",
        "deployer": None,
        "commit_sha": "ed86035f",
        "available": True,
        "source": "release_environment",
    }


def test_endpoint_reads_current_app_from_databricks_runtime(monkeypatch):
    app = SimpleNamespace(
        active_deployment=SimpleNamespace(
            create_time="2026-08-30T21:29:46Z",
            creator="deployer@example.com",
            git_source=SimpleNamespace(resolved_commit="ed86035f1234567890"),
        ),
    )
    monkeypatch.setenv("DATABRICKS_APP_NAME", "cost-obs")
    monkeypatch.setattr(
        db,
        "get_workspace_client",
        lambda: SimpleNamespace(apps=SimpleNamespace(get=lambda name: app)),
    )

    result = asyncio.run(health.deployment_metadata())

    assert result["source"] == "databricks_apps_api"
    assert result["deployed_at"] == "2026-08-30T21:29:46Z"
    assert result["deployer"] == "deployer@example.com"
    assert result["commit_sha"] == "ed86035f1234567890"


def test_endpoint_labels_metadata_unavailable_in_local_runtime(monkeypatch):
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.delenv("COST_OBS_DEPLOYED_AT", raising=False)
    monkeypatch.delenv("COST_OBS_DEPLOYER", raising=False)
    monkeypatch.delenv("COST_OBS_COMMIT_SHA", raising=False)

    assert asyncio.run(health.deployment_metadata()) == {
        "deployed_at": None,
        "deployer": None,
        "commit_sha": None,
        "available": False,
        "source": "unavailable",
    }


def test_concurrent_endpoint_calls_share_one_sdk_fetch_and_reuse_cache(monkeypatch):
    calls = 0

    def get_app(_name):
        nonlocal calls
        calls += 1
        time.sleep(0.03)
        return SimpleNamespace(
            active_deployment=SimpleNamespace(
                create_time="2026-08-30T21:29:46Z",
                creator="deployer@example.com",
                git_source=SimpleNamespace(resolved_commit="ed86035f"),
            )
        )

    monkeypatch.setenv("DATABRICKS_APP_NAME", "cost-obs")
    monkeypatch.setattr(
        db,
        "get_workspace_client",
        lambda: SimpleNamespace(apps=SimpleNamespace(get=get_app)),
    )

    async def load_concurrently():
        return await asyncio.gather(*(health.deployment_metadata() for _ in range(8)))

    results = asyncio.run(load_concurrently())
    cached = asyncio.run(health.deployment_metadata())

    assert calls == 1
    assert all(result["commit_sha"] == "ed86035f" for result in results)
    assert cached["source"] == "databricks_apps_api"


def test_sdk_error_uses_release_environment_fallback(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_NAME", "cost-obs")
    monkeypatch.setenv("COST_OBS_DEPLOYED_AT", "2026-08-30T21:29:46Z")
    monkeypatch.setenv("COST_OBS_COMMIT_SHA", "fallback")
    monkeypatch.setattr(
        db,
        "get_workspace_client",
        lambda: SimpleNamespace(
            apps=SimpleNamespace(get=lambda _name: (_ for _ in ()).throw(RuntimeError("down")))
        ),
    )

    result = asyncio.run(health.deployment_metadata())

    assert result["source"] == "release_environment"
    assert result["commit_sha"] == "fallback"


def test_timed_out_fetch_stays_single_flight_without_spawning_more_workers(monkeypatch):
    calls = 0
    started = threading.Event()
    release = threading.Event()

    def get_app(_name):
        nonlocal calls
        calls += 1
        started.set()
        release.wait(timeout=1)
        return SimpleNamespace(
            active_deployment=SimpleNamespace(
                create_time="2026-08-30T21:29:46Z",
                creator="deployer@example.com",
                git_source=SimpleNamespace(resolved_commit="late"),
            )
        )

    monkeypatch.setenv("DATABRICKS_APP_NAME", "cost-obs")
    monkeypatch.setattr(health, "_DEPLOYMENT_METADATA_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        db,
        "get_workspace_client",
        lambda: SimpleNamespace(apps=SimpleNamespace(get=get_app)),
    )

    try:
        async def time_out_concurrently():
            return await asyncio.gather(*(health.deployment_metadata() for _ in range(6)))

        results = asyncio.run(time_out_concurrently())
        assert started.is_set()
        assert calls == 1
        assert all(result["source"] == "unavailable" for result in results)
        with health._deployment_metadata_lock:
            assert len(health._deployment_metadata_inflight) == 1
    finally:
        release.set()

    for _ in range(20):
        with health._deployment_metadata_lock:
            if not health._deployment_metadata_inflight:
                break
        time.sleep(0.01)
    assert calls == 1
