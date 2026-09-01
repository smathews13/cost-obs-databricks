"""Focused regression coverage for shared-source unified views."""

import asyncio
import multiprocessing
import re
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from server import app as app_module
from server import db, materialized_views
from server.routers import tagging


def _try_nonblocking_unified_lock(lock_path: str, connection) -> None:
    try:
        with materialized_views.unified_views_rebuild_lock(
            blocking=False, lock_path=lock_path
        ):
            connection.send(True)
    except BlockingIOError:
        connection.send(False)
    finally:
        connection.close()


def _try_startup_claim(lock_path: str, connection) -> None:
    connection.send(app_module._claim_startup_unified_views_worker(lock_path))
    connection.close()


def _fork_context():
    try:
        return multiprocessing.get_context("fork")
    except ValueError:
        pytest.skip("cross-process flock test requires fork")


def _release_startup_claim() -> None:
    claim_file = app_module._startup_unified_views_claim_file
    if claim_file is not None:
        claim_file.close()
    app_module._startup_unified_views_claim_file = None
    app_module._startup_unified_views_claim_pid = None


def test_unified_rebuild_lock_excludes_another_process(tmp_path):
    ctx = _fork_context()
    lock_path = str(tmp_path / "unified.lock")
    with materialized_views.unified_views_rebuild_lock(lock_path=lock_path):
        parent, child = ctx.Pipe(duplex=False)
        process = ctx.Process(
            target=_try_nonblocking_unified_lock, args=(lock_path, child)
        )
        process.start()
        assert parent.recv() is False
        process.join(timeout=5)
        assert process.exitcode == 0

    with materialized_views.unified_views_rebuild_lock(
        blocking=False, lock_path=lock_path
    ):
        pass


def test_only_one_uvicorn_process_claims_startup_rebuild(tmp_path):
    ctx = _fork_context()
    lock_path = str(tmp_path / "startup.lock")
    _release_startup_claim()
    try:
        assert app_module._claim_startup_unified_views_worker(lock_path) is True
        parent, child = ctx.Pipe(duplex=False)
        process = ctx.Process(target=_try_startup_claim, args=(lock_path, child))
        process.start()
        assert parent.recv() is False
        process.join(timeout=5)
        assert process.exitcode == 0
    finally:
        _release_startup_claim()


def test_base_refresh_finishes_before_unified_rebuild():
    events: list[str] = []
    workspace = SimpleNamespace(
        tables=SimpleNamespace(list=lambda **_kwargs: iter(()))
    )

    def execute(sql: str, *_args, **_kwargs):
        match = re.search(r"CREATE OR REPLACE TABLE `main`\.`cost_obs`\.`([^`]+)`", sql)
        if match:
            events.append(f"base:{match.group(1)}")
        return []

    def rebuild(catalog: str, schema: str):
        assert (catalog, schema) == ("main", "cost_obs")
        assert {
            event.removeprefix("base:")
            for event in events
            if event.startswith("base:")
        } == set(materialized_views._MV_TABLES)
        events.append("unified")
        return {"ok": True}

    with (
        patch("server.db.validate_app_storage_target"),
        patch("server.db.get_user_workspace_client", return_value=workspace),
        patch("server.db.get_workspace_client", return_value=workspace),
        patch("server.db.get_mv_sources", return_value=[{"label": "shared"}]),
        patch.object(materialized_views, "execute_query", side_effect=execute),
        patch.object(materialized_views, "_ensure_refresh_state_table"),
        patch.object(materialized_views, "_get_refresh_state", return_value=None),
        patch.object(materialized_views, "_update_refresh_state"),
        patch.object(materialized_views, "_rebuild_unified_views_locked", side_effect=rebuild),
    ):
        materialized_views.create_materialized_views("main", "cost_obs")

    assert events[-1] == "unified"


def test_unified_view_ddl_creates_missing_and_alters_existing():
    body = "SELECT *, 'local' AS source_label FROM `c`.`s`.`daily_usage_summary`"
    with patch("server.db.execute_query") as execute:
        action = materialized_views._replace_unified_view(
            "c", "s", "daily_usage_summary", body, existed=False
        )
        assert action == "created"
        assert execute.call_args.args[0].startswith(
            "CREATE VIEW `c`.`s`.`daily_usage_summary__unified` AS"
        )
        assert "CREATE OR REPLACE" not in execute.call_args.args[0]

        execute.reset_mock()
        action = materialized_views._replace_unified_view(
            "c", "s", "daily_usage_summary", body, existed=True
        )
        assert action == "altered"
        assert execute.call_args.args[0].startswith(
            "ALTER VIEW `c`.`s`.`daily_usage_summary__unified` AS"
        )


def test_unified_view_ddl_rejects_recursive_body():
    with pytest.raises(ValueError, match="must not reference its target"):
        materialized_views._replace_unified_view(
            "c",
            "s",
            "daily_usage_summary",
            "SELECT * FROM `c`.`s`.`daily_usage_summary__unified`",
            existed=True,
        )


def test_unified_view_ddl_uses_locked_drop_create_fallback_for_unsupported_alter():
    body = "SELECT *, 'local' AS source_label FROM `c`.`s`.`daily_usage_summary`"
    statements: list[str] = []

    def execute(sql: str, *_args, **_kwargs):
        statements.append(sql)
        if sql.startswith("ALTER VIEW"):
            raise RuntimeError("[PARSE_SYNTAX_ERROR] ALTER VIEW AS not supported")
        return []

    with patch("server.db.execute_query", side_effect=execute):
        action = materialized_views._replace_unified_view(
            "c", "s", "daily_usage_summary", body, existed=True
        )

    assert action == "recreated"
    assert [sql.split()[0] for sql in statements] == ["ALTER", "DROP", "CREATE"]


def test_partial_failure_preserves_other_live_registry_entries():
    saved: list[list[str]] = []

    def replace(_catalog, _schema, table_name, _body, *, existed):
        assert existed is True
        if table_name == "one":
            raise RuntimeError("transient alter failure")
        return "altered"

    with (
        patch.object(materialized_views, "_MV_TABLES", ["one", "two"]),
        patch("server.db.get_mv_sources", return_value=[
            {"label": "shared", "catalog": "share", "schema": "cost"}
        ]),
        patch("server.db.get_local_source_label", return_value="local"),
        patch("server.db.get_unified_view_tables", return_value=["one", "two"]),
        patch("server.db.save_unified_view_tables", side_effect=lambda value: saved.append(value)),
        patch.object(materialized_views, "_unified_view_exists", return_value=True),
        patch.object(materialized_views, "_table_columns", return_value=["usage_date"]),
        patch.object(materialized_views, "_replace_unified_view", side_effect=replace),
    ):
        result = materialized_views._rebuild_unified_views_locked("c", "s")

    assert result["views"]["one"]["built"] is False
    assert result["views"]["two"]["built"] is True
    assert result["ok"] is False
    assert saved == []


def test_selected_source_routes_only_to_verified_physical_view():
    token = db.set_source_labels(["shared"])
    template = (
        "SELECT SUM(total_spend) FROM "
        "`{catalog}`.`{schema}`.`daily_usage_summary` WHERE 1=1 {ws_filter}"
    )
    try:
        with (
            patch.object(db, "_list_existing_unified_views", return_value=["daily_usage_summary"]),
            patch.object(
                db,
                "get_mv_sources",
                return_value=[{
                    "label": "shared",
                    "tables": ["daily_usage_summary"],
                }],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
        ):
            clause = db.source_label_filter_clause(template)
            sql = template.format(catalog="c", schema="s", ws_filter=clause)
            routed = db.apply_mv_overrides(sql, "c", "s")

        assert "source_label IN ('shared')" in routed
        assert "`c`.`s`.`daily_usage_summary__unified`" in routed

        with (
            patch.object(db, "_list_existing_unified_views", return_value=[]),
            patch.object(
                db,
                "get_mv_sources",
                return_value=[{
                    "label": "shared",
                    "tables": ["daily_usage_summary"],
                }],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
        ):
            assert db.source_label_filter_clause(template) == ""
            base_sql = template.format(catalog="c", schema="s", ws_filter="")
            with pytest.raises(RuntimeError, match="does not physically exist"):
                db.apply_mv_overrides(base_sql, "c", "s")
            assert "source_label" not in base_sql
    finally:
        db.reset_source_labels(token)


def test_selected_source_rejects_views_it_does_not_publish():
    token = db.set_source_labels(["west4"])
    template = (
        "SELECT SUM(total_spend) FROM "
        "`{catalog}`.`{schema}`.`daily_apps_summary` WHERE 1=1 {ws_filter}"
    )
    try:
        with patch.object(
            db,
            "get_mv_sources",
            return_value=[{
                "label": "west4",
                "tables": ["daily_usage_summary"],
            }],
        ):
            with pytest.raises(
                db.SourceScopeUnsupportedError,
                match="does not publish",
            ):
                db.source_label_filter_clause(template)
    finally:
        db.reset_source_labels(token)


def test_tagging_mv_queries_include_selected_source_filter():
    captured: list[str] = []

    def execute(sql: str, *_args, **_kwargs):
        captured.append(sql)
        return []

    def execute_selected(query_funcs, *_args, **_kwargs):
        results = {}
        for name, func in query_funcs:
            results[name] = func() if name in {"summary", "tag_stats"} else []
        return results

    token = db.set_source_labels(["shared"])
    try:
        with (
            patch.object(tagging, "delta_cache_get", return_value=None),
            patch.object(tagging, "delta_cache_put"),
            patch.object(tagging, "_check_mv_available", return_value=True),
            patch.object(tagging, "get_catalog_schema", return_value=("c", "s")),
            patch.object(tagging, "execute_query", side_effect=execute),
            patch.object(tagging, "execute_queries_parallel", side_effect=execute_selected),
            patch.object(
                db,
                "_list_existing_unified_views",
                return_value=["daily_tag_coverage_summary", "daily_tag_summary"],
            ),
            patch.object(
                db,
                "get_mv_sources",
                return_value=[{
                    "label": "shared",
                    "tables": ["daily_tag_coverage_summary", "daily_tag_summary"],
                }],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
        ):
            asyncio.run(
                tagging.get_tagging_dashboard_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids=None,
                )
            )
    finally:
        db.reset_source_labels(token)

    usage_sql = next(sql for sql in captured if "daily_tag_coverage_summary__unified" in sql)
    tag_sql = next(sql for sql in captured if "daily_tag_summary__unified" in sql)
    assert "source_label IN ('shared')" in usage_sql
    assert "source_label IN ('shared')" in tag_sql
