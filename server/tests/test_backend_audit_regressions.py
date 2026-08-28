import asyncio
import multiprocessing
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from server import db
from server.queries import PLATFORM_KPIS_FAST
from server.routers import aiml, apps, billing, dbsql_base, settings


class _Request:
    headers: dict[str, str] = {}

    def __init__(self, body: dict | None = None):
        self._body = body or {}

    async def json(self) -> dict:
        return self._body


class _CapturedThread:
    created: list["_CapturedThread"] = []

    def __init__(self, *, target, args=(), **_kwargs):
        self.target = target
        self.args = args
        self.created.append(self)

    def start(self):
        return None


def _dashboard_endpoint(router):
    return next(route.endpoint for route in router.routes if route.path == "/dashboard-bundle")


def test_apps_background_thread_captures_request_context():
    _CapturedThread.created.clear()
    apps._apps_bundle_inflight.clear()
    source_token = db.set_source_labels(["shared-west"])
    user_token = db._user_token.set("user-oauth-token")
    try:
        with (
            patch.object(apps, "delta_cache_get", return_value=None),
            patch.object(
                apps,
                "capture_cache_generation",
                return_value=db.CacheGeneration("apps:dashboard-bundle:all", 0),
            ),
            patch.object(apps.threading, "Thread", _CapturedThread),
        ):
            asyncio.run(
                apps.get_apps_dashboard_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    active_only=False,
                    workspace_ids=None,
                )
            )
        captured_context = _CapturedThread.created[0].target.__self__
        assert captured_context.get(db._source_labels) == ["shared-west"]
        assert captured_context.get(db._user_token) == "user-oauth-token"
    finally:
        db._user_token.reset(user_token)
        db.reset_source_labels(source_token)
        apps._apps_bundle_inflight.clear()


def test_aiml_background_thread_captures_request_context():
    _CapturedThread.created.clear()
    aiml._aiml_bundle_inflight.clear()
    source_token = db.set_source_labels(["shared-central"])
    user_token = db._user_token.set("aiml-user-token")
    try:
        with (
            patch.object(aiml, "delta_cache_get", return_value=None),
            patch.object(aiml, "_aiml_available", True),
            patch.object(
                aiml,
                "capture_cache_generation",
                return_value=db.CacheGeneration("aiml:dashboard-bundle", 0),
            ),
            patch.object(aiml.threading, "Thread", _CapturedThread),
        ):
            asyncio.run(
                aiml.get_aiml_dashboard_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids=None,
                )
            )
        captured_context = _CapturedThread.created[0].target.__self__
        assert captured_context.get(db._source_labels) == ["shared-central"]
        assert captured_context.get(db._user_token) == "aiml-user-token"
    finally:
        db._user_token.reset(user_token)
        db.reset_source_labels(source_token)
        aiml._aiml_bundle_inflight.clear()


def test_dbsql_background_thread_captures_request_context():
    _CapturedThread.created.clear()
    dbsql_base._dbsql_bundle_inflight.clear()
    router = dbsql_base.create_dbsql_router("dbsql_cost_per_query")
    endpoint = _dashboard_endpoint(router)
    dbsql_base._mv_status_cache["dbsql_cost_per_query"] = (
        time.monotonic(),
        {"mv_available": True},
    )
    source_token = db.set_source_labels(["shared-east"])
    user_token = db._user_token.set("request-token")
    try:
        with (
            patch.object(dbsql_base, "delta_cache_get", return_value=None),
            patch.object(
                dbsql_base,
                "capture_cache_generation",
                return_value=db.CacheGeneration(
                    "dbsql:dbsql_cost_per_query:dashboard-bundle", 0
                ),
            ),
            patch.object(dbsql_base.threading, "Thread", _CapturedThread),
        ):
            asyncio.run(
                endpoint(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids=None,
                )
            )
        captured_context = _CapturedThread.created[0].target.__self__
        assert captured_context.get(db._source_labels) == ["shared-east"]
        assert captured_context.get(db._user_token) == "request-token"
    finally:
        db._user_token.reset(user_token)
        db.reset_source_labels(source_token)
        dbsql_base._dbsql_bundle_inflight.clear()
        dbsql_base._mv_status_cache.clear()


def test_dbsql_mv_query_routes_and_filters_selected_source():
    template = dbsql_base._build_queries("dbsql_cost_per_query")["summary"]
    token = db.set_source_labels(["shared"])
    try:
        with (
            patch.object(
                db, "_list_existing_unified_views", return_value=["dbsql_cost_per_query"]
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
        ):
            query = dbsql_base._route_dbsql_mv_query(
                template,
                "catalog",
                "schema",
                "AND CAST(workspace_id AS STRING) IN ('123')",
            )
        assert "`catalog`.`schema`.`dbsql_cost_per_query__unified`" in query
        assert "source_label IN ('shared')" in query
        assert "CAST(workspace_id AS STRING) IN ('123')" in query
    finally:
        db.reset_source_labels(token)


def test_dbsql_mv_query_refuses_unfiltered_source_fallback():
    template = dbsql_base._build_queries("dbsql_cost_per_query")["summary"]
    token = db.set_source_labels(["shared"])
    try:
        with (
            patch.object(db, "_list_existing_unified_views", return_value=[]),
            patch.object(db, "get_mv_table_overrides", return_value={}),
        ):
            with pytest.raises(RuntimeError, match="does not physically exist"):
                dbsql_base._route_dbsql_mv_query(template, "catalog", "schema")
    finally:
        db.reset_source_labels(token)


def _reset_delta_l1():
    db._delta_l1.clear()
    db._delta_l1_endpoints.clear()
    db._delta_l1_generations.clear()


def test_cache_invalidation_rejects_worker_that_started_before_clear(tmp_path):
    _reset_delta_l1()
    with (
        patch.object(db, "_CACHE_GENERATION_STATE_PATH", str(tmp_path / "state.json")),
        patch.object(db, "_CACHE_GENERATION_LOCK_PATH", str(tmp_path / "lock")),
        patch.object(db, "get_catalog_schema", return_value=(None, None)),
    ):
        generation = db.capture_cache_generation("billing:kpis-bundle")
        db.delta_cache_invalidate("billing:kpis-bundle")
        accepted = db.delta_cache_put(
            "old-key",
            "billing:kpis-bundle",
            {"stale": True},
            generation=generation,
        )
    assert accepted is False
    assert "old-key" not in db._delta_l1


def _cache_generation_child(state_path: str, lock_path: str, connection):
    db._CACHE_GENERATION_STATE_PATH = state_path
    db._CACHE_GENERATION_LOCK_PATH = lock_path
    generation = db.capture_cache_generation("apps:dashboard-bundle:all")
    connection.send("captured")
    connection.recv()
    accepted = db.delta_cache_put(
        "old-worker-key",
        "apps:dashboard-bundle:all",
        {"stale": True},
        generation=generation,
    )
    connection.send(accepted)
    connection.close()


def test_cache_generation_fences_old_worker_across_processes(tmp_path):
    try:
        ctx = multiprocessing.get_context("fork")
    except ValueError:
        pytest.skip("cross-process cache generation test requires fork")
    state_path = str(tmp_path / "state.json")
    lock_path = str(tmp_path / "lock")
    parent, child = ctx.Pipe()
    with patch.object(db, "get_catalog_schema", return_value=(None, None)):
        process = ctx.Process(
            target=_cache_generation_child,
            args=(state_path, lock_path, child),
        )
        process.start()
        assert parent.recv() == "captured"
        with (
            patch.object(db, "_CACHE_GENERATION_STATE_PATH", state_path),
            patch.object(db, "_CACHE_GENERATION_LOCK_PATH", lock_path),
        ):
            db.delta_cache_invalidate("apps:")
        parent.send("continue")
        assert parent.recv() is False
        process.join(timeout=5)
    assert process.exitcode == 0


@pytest.mark.parametrize(
    ("save_name", "table_save_name", "file_name", "payload"),
    [
        (
            "_save_webhook_settings",
            "_save_webhook_to_table",
            "WEBHOOK_SETTINGS_FILE",
            {"slack_webhook_url": "https://hooks.slack.com/test"},
        ),
        (
            "_save_alert_thresholds",
            "_save_alert_thresholds_to_table",
            "ALERT_THRESHOLDS_FILE",
            {"daily_budget": 100},
        ),
        (
            "_save_pricing_settings",
            "_save_pricing_to_table",
            "PRICING_SETTINGS_FILE",
            {"use_account_prices": True},
        ),
    ],
)
def test_settings_writes_keep_local_fallback_but_propagate_delta_failure(
    tmp_path, save_name, table_save_name, file_name, payload
):
    target = tmp_path / f"{file_name}.json"
    with (
        patch.object(settings, file_name, str(target)),
        patch.object(
            settings,
            table_save_name,
            side_effect=RuntimeError("Delta unavailable"),
        ),
    ):
        with pytest.raises(settings.AppSettingsDurabilityError):
            getattr(settings, save_name)(payload)
    assert target.exists()


def test_settings_table_replacements_are_atomic():
    writes: list[str] = []
    with (
        patch.object(settings, "_ensure_webhook_table"),
        patch.object(settings, "_ensure_alert_thresholds_table"),
        patch.object(settings, "_ensure_pricing_table"),
        patch.object(settings, "_config_table", return_value="config_table"),
        patch("server.db.execute_write", side_effect=lambda sql, *_args: writes.append(sql)),
    ):
        settings._save_webhook_to_table({"slack_webhook_url": ""})
        settings._save_alert_thresholds_to_table({"daily_budget": 10})
        settings._save_pricing_to_table({"use_account_prices": False})
    assert len(writes) == 3
    assert all("INSERT OVERWRITE" in sql for sql in writes)
    assert all("DELETE FROM" not in sql for sql in writes)


def test_unified_put_returns_503_when_requested_group_is_not_durable():
    request = _Request({"webhook": {"slack_webhook_url": "https://example.invalid"}})
    with (
        patch.object(settings, "_require_admin"),
        patch.object(
            settings,
            "_save_webhook_settings",
            side_effect=settings.AppSettingsDurabilityError("Delta unavailable"),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(settings.put_unified_settings(request))
    assert exc.value.status_code == 503


def test_individual_settings_endpoints_return_503_on_delta_failure():
    failure = settings.AppSettingsDurabilityError("Delta unavailable")
    with (
        patch.object(settings, "_require_admin"),
        patch.object(settings, "_save_webhook_settings", side_effect=failure),
    ):
        with pytest.raises(HTTPException) as webhook_exc:
            asyncio.run(
                settings.save_webhook_settings(
                    _Request(),
                    settings.WebhookSettings(slack_webhook_url="https://example.invalid"),
                )
            )
    with (
        patch.object(settings, "_require_admin"),
        patch.object(settings, "_save_alert_thresholds", side_effect=failure),
    ):
        with pytest.raises(HTTPException) as thresholds_exc:
            asyncio.run(
                settings.save_alert_thresholds_endpoint(
                    _Request({"daily_budget": 100})
                )
            )
    with patch.object(settings, "_save_pricing_settings", side_effect=failure):
        with pytest.raises(HTTPException) as pricing_exc:
            asyncio.run(settings.set_pricing_mode({"use_account_prices": True}))
    assert webhook_exc.value.status_code == 503
    assert thresholds_exc.value.status_code == 503
    assert pricing_exc.value.status_code == 503


def test_unified_schedule_put_merges_only_submitted_keys(tmp_path):
    request = _Request({"schedule": {"hour_utc": 9}})
    with (
        patch.object(settings, "_require_admin"),
        patch.object(settings, "SCHEDULE_SETTINGS_FILE", str(tmp_path / "schedule.json")),
        patch.object(
            settings,
            "load_schedule_settings",
            return_value={
                "enabled": False,
                "frequency": "weekly",
                "hour_utc": 5,
                "lookback_days": 730,
            },
        ),
        patch.object(settings, "_save_schedule_to_table") as save,
    ):
        result = asyncio.run(settings.put_unified_settings(request))
    save.assert_called_once_with(
        {
            "enabled": False,
            "frequency": "weekly",
            "hour_utc": 9,
            "lookback_days": 730,
        }
    )
    assert result["updated_count"] == 1


def test_individual_schedule_update_preserves_unspecified_values(tmp_path):
    with (
        patch.object(settings, "_require_admin"),
        patch.object(settings, "SCHEDULE_SETTINGS_FILE", str(tmp_path / "schedule.json")),
        patch.object(
            settings,
            "load_schedule_settings",
            return_value={
                "enabled": False,
                "frequency": "monthly",
                "hour_utc": 5,
                "lookback_days": 1095,
            },
        ),
        patch.object(settings, "_save_schedule_to_table") as save,
    ):
        result = asyncio.run(
            settings.save_schedule_endpoint(_Request(), {"hour_utc": 11})
        )
    assert result == {
        "enabled": False,
        "frequency": "monthly",
        "hour_utc": 11,
        "lookback_days": 1095,
    }
    save.assert_called_once_with(result)


def test_share_last_updated_probes_all_selected_tables_and_returns_max():
    modified = {
        "one": "2026-08-20T10:00:00+00:00",
        "two": "2026-08-28T09:00:00+00:00",
        "three": "2026-08-25T12:00:00+00:00",
    }

    def execute(sql, *_args, **_kwargs):
        name = sql.rsplit("`", 2)[1]
        return [{"lastModified": modified[name]}]

    with patch("server.db.execute_query", side_effect=execute) as query:
        result = settings._share_last_updated("catalog", "schema", list(modified))
    assert result == modified["two"]
    assert query.call_count == 3


def test_shared_source_check_requires_admin():
    denied = HTTPException(status_code=403, detail="Admin required")
    with patch.object(settings, "_require_admin", side_effect=denied):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(settings.check_mv_source_freshness(_Request(), "shared"))
    assert exc.value.status_code == 403


def test_shared_source_add_and_remove_require_admin():
    denied = HTTPException(status_code=403, detail="Admin required")
    with patch.object(settings, "_require_admin", side_effect=denied):
        with pytest.raises(HTTPException) as add_exc:
            asyncio.run(
                settings.add_mv_source(
                    _Request(),
                    {"label": "shared", "catalog": "remote", "schema": "cost"},
                )
            )
        with pytest.raises(HTTPException) as remove_exc:
            asyncio.run(settings.remove_mv_source(_Request(), "shared"))
    assert add_exc.value.status_code == 403
    assert remove_exc.value.status_code == 403


def test_mv_source_delta_replacement_is_atomic():
    writes: list[tuple[str, dict | None]] = []
    sources = [
        {
            "label": "west",
            "catalog": "remote",
            "schema": "cost",
            "tables": ["daily_usage_summary"],
        }
    ]
    with (
        patch.object(db, "_mv_sources_table", return_value="mv_sources"),
        patch.object(db, "_ensure_mv_sources_table"),
        patch.object(
            db,
            "execute_write",
            side_effect=lambda sql, params=None: writes.append((sql, params)),
        ),
    ):
        db.write_delta_mv_sources(sources)
    assert len(writes) == 1
    assert "INSERT OVERWRITE" in writes[0][0]
    assert "DELETE FROM" not in writes[0][0]
    assert writes[0][1]["label_0"] == "west"


def test_concurrent_source_adds_do_not_lose_updates():
    from server import materialized_views

    shared_sources: list[dict] = []
    operation_mutex = threading.Lock()

    @contextmanager
    def operation_lock(*_args, **_kwargs):
        with operation_mutex:
            yield SimpleNamespace()

    def get_sources():
        return [dict(source) for source in shared_sources]

    def save_sources(sources):
        shared_sources[:] = [dict(source) for source in sources]

    def add(label):
        return asyncio.run(
            settings.add_mv_source(
                _Request(),
                {"label": label, "catalog": f"remote_{label}", "schema": "cost"},
            )
        )

    with (
        patch.object(settings, "_require_admin"),
        patch("server.db.get_catalog_schema", return_value=("local", "cost")),
        patch("server.db.get_mv_sources", side_effect=get_sources),
        patch("server.db.save_mv_sources", side_effect=save_sources),
        patch.object(settings, "_detect_source_cloud", return_value=None),
        patch.object(settings, "append_refresh_history"),
        patch.object(materialized_views, "unified_views_rebuild_lock", operation_lock),
        patch.object(
            materialized_views,
            "_rebuild_unified_views_locked",
            return_value={"ok": True},
        ),
    ):
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(add, ("east", "west")))

    assert all(result["ok"] for result in results)
    assert {source["label"] for source in shared_sources} == {"east", "west"}


def test_kpis_bundle_filters_lakeflow_successful_runs_by_workspace():
    captured: list[str] = []

    def execute_parallel(queries, *_args, **_kwargs):
        result = {}
        for name, function in queries:
            if name == "lakeflow_kpis":
                result[name] = function()
            else:
                result[name] = []
        return result

    def execute(sql, *_args, **_kwargs):
        captured.append(sql)
        return [{
            "result_state_available": True,
            "successful_runs": 0,
            "total_run_hours": 0,
        }]

    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(
            billing,
            "capture_cache_generation",
            return_value=db.CacheGeneration("billing:kpis-bundle", 0),
        ),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
        patch.object(billing, "execute_queries_parallel", side_effect=execute_parallel),
        patch.object(billing, "execute_query", side_effect=execute),
    ):
        result = asyncio.run(
            billing.get_kpis_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids="123",
            )
        )
    lakeflow_sql = next(sql for sql in captured if "job_run_timeline" in sql)
    assert "CAST(workspace_id AS STRING) IN ('123')" in lakeflow_sql
    assert result["kpis"]["successful_runs"] == 0
    assert result["kpis"]["successful_runs_available"] is True


def test_successful_runs_trend_filters_lakeflow_by_workspace():
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put") as cache_put,
        patch.object(
            billing,
            "capture_cache_generation",
            return_value=db.CacheGeneration("trend:sql:platform-kpi", 0),
        ) as capture_generation,
        patch.object(
            billing,
            "execute_query",
            return_value=[{"date": "2026-08-01", "value": 1}],
        ) as execute,
    ):
        asyncio.run(
            billing.get_platform_kpi_trend(
                kpi="successful_runs",
                start_date="2026-08-01",
                end_date="2026-08-28",
                granularity="daily",
                workspace_ids="456",
                tab="sql",
            )
        )
    assert "CAST(workspace_id AS STRING) IN ('456')" in execute.call_args.args[0]
    capture_generation.assert_called_once_with("trend:sql:platform-kpi")
    assert cache_put.call_args.args[1] == "trend:sql:platform-kpi"


def test_billing_trend_combines_workspace_and_source_scope():
    source_token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(
                billing,
                "capture_cache_generation",
                return_value=db.CacheGeneration("trend:dbu:billing-kpi", 0),
            ),
            patch.object(billing, "_check_mv_available", return_value=True),
            patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(db, "_list_existing_unified_views", return_value=["daily_usage_summary"]),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(
                billing,
                "execute_query",
                return_value=[{"date": "2026-08-01", "value": 1}],
            ) as execute,
        ):
            asyncio.run(
                billing.get_kpi_trend(
                    kpi="total_spend",
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    granularity="daily",
                    workspace_ids="123",
                    tab="dbu",
                )
            )
        query = execute.call_args.args[0]
        assert "`daily_usage_summary__unified`" in query
        assert "CAST(workspace_id AS STRING) IN ('123')" in query
        assert "source_label IN ('shared-west')" in query
    finally:
        db.reset_source_labels(source_token)


@pytest.mark.parametrize(
    "kpi",
    ["apps_spend", "apps_dbus", "apps_count", "apps_avg_cost_per_app"],
)
def test_apps_trends_apply_workspace_and_source_scope(kpi):
    captured: list[str] = []
    source_token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(apps, "delta_cache_get", return_value=None),
            patch.object(apps, "delta_cache_put") as cache_put,
            patch.object(
                apps,
                "capture_cache_generation",
                return_value=db.CacheGeneration("trend:apps:kpi", 0),
            ),
            patch.object(apps, "_check_mv_available", return_value=True),
            patch.object(apps, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(
                apps,
                "source_label_filter_clause",
                return_value=" AND source_label IN ('shared-west')",
            ),
            patch.object(apps, "apply_mv_overrides", side_effect=lambda sql, *_: sql),
            patch.object(
                apps,
                "execute_query",
                side_effect=lambda sql, *_args, **_kwargs: captured.append(sql) or [{
                    "date": "2026-08-01",
                    "value": 1,
                }],
            ),
        ):
            asyncio.run(
                apps.get_apps_kpi_trend(
                    kpi=kpi,
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    granularity="daily",
                    workspace_ids="123",
                )
            )
        assert "CAST(workspace_id AS STRING) IN ('123')" in captured[0]
        assert "source_label IN ('shared-west')" in captured[0]
        assert cache_put.call_args.args[1] == "trend:apps:kpi"
    finally:
        db.reset_source_labels(source_token)


def test_standalone_platform_kpis_reports_successful_runs_availability():
    with (
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(
            billing,
            "execute_query",
            return_value=[{
                "successful_runs": 0,
                "result_state_available": True,
            }],
        ) as execute,
    ):
        result = asyncio.run(
            billing.get_platform_kpis(
                start_date="2026-08-01",
                end_date="2026-08-28",
                fast=True,
            )
        )
    assert "{ws_filter}" not in execute.call_args.args[0]
    assert result["successful_runs"] == 0
    assert result["successful_runs_available"] is True
    assert "result_state_available" in PLATFORM_KPIS_FAST
