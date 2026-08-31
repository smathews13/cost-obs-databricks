import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from server.routers import apps


def _status(state: str, message: str, **kwargs):
    return SimpleNamespace(state=state, message=message, **kwargs)


def test_extract_app_metadata_selects_safe_operational_fields():
    deployment = SimpleNamespace(
        deployment_id="deployment-1",
        status=_status("SUCCEEDED", "Deployment succeeded"),
        creator="deployer@example.com",
        create_time="2026-08-20T10:00:00Z",
        update_time="2026-08-20T10:05:00Z",
        mode="SNAPSHOT",
        source_code_path="/Workspace/Users/owner@example.com/demo",
        git_source=SimpleNamespace(
            git_repository=SimpleNamespace(
                url="https://github.example.com/customer/demo.git?access_token=secret"
            ),
            branch="main",
            tag=None,
            commit=None,
            resolved_commit="abc123def4567890",
            source_code_path="app",
        ),
        env_vars=[SimpleNamespace(name="TOKEN", value="never-return-this")],
    )
    app = SimpleNamespace(
        description="Customer demo",
        creator="owner@example.com",
        updater="editor@example.com",
        create_time="2026-08-19T09:00:00Z",
        update_time="2026-08-20T10:05:00Z",
        compute_size="MEDIUM",
        compute_min_instances=1,
        compute_max_instances=3,
        compute_status=_status("ACTIVE", "Compute is active", active_instances=2),
        app_status=_status("RUNNING", "Application is running", running_instances=2),
        active_deployment=deployment,
        pending_deployment=None,
        git_source=None,
        default_git_source=None,
        git_repository=None,
        source_code_path=None,
        default_source_code_path=None,
        thumbnail_url="/ajax-api/2.0/thumbnails/raw/apps/id/SMALL_DEFAULT.png",
        space="customer-space",
        oauth2_app_client_id="client-secret-id",
        service_principal_client_id="another-client-id",
        forward_user_access_token=True,
    )

    metadata = apps._extract_app_metadata(app)
    public = apps._public_app_metadata(metadata)

    assert public["creator"] == "owner@example.com"
    assert public["compute_status"] == {
        "state": "ACTIVE",
        "message": "Compute is active",
        "instances": 2,
    }
    assert public["deployment"]["state"] == "SUCCEEDED"
    assert public["git"]["commit"] == "abc123def4567890"
    assert public["git"]["repository_url"] == "https://github.example.com/customer/demo.git"
    assert "has_thumbnail" not in public
    assert "_thumbnail_source_url" not in public
    assert "env_vars" not in public
    assert "oauth2_app_client_id" not in public
    assert "service_principal_client_id" not in public
    assert "forward_user_access_token" not in public
    assert "never-return-this" not in repr(public)


def test_public_thumbnail_contract_allows_conventional_icons_without_false_boolean():
    assert apps._public_thumbnail_url(
        "app/id",
        True,
        {"_thumbnail_source_url": ""},
        {"url": "https://safe-app.example.databricksapps.com"},
    ) == "/api/apps/thumbnail?app_id=app%2Fid"
    assert apps._public_thumbnail_url(
        "deleted",
        False,
        {"_thumbnail_source_url": "/icon.png"},
        {"url": "https://safe-app.example.databricksapps.com"},
    ) is None


def test_active_count_uses_one_registered_population_and_inclusive_seven_day_window():
    registry = {
        "active-id": {"name": "active-app", "url": "", "metadata": {}},
        "old-id": {"name": "old-app", "url": "", "metadata": {}},
    }
    result = apps._process_apps(
        [
            {
                "app_id": "active-id",
                "last_usage_date": "2026-08-24",
                "total_spend": 10,
            },
            {
                "app_id": "old-id",
                "last_usage_date": "2026-08-23",
                "total_spend": 5,
            },
            {
                "app_id": "historical-id",
                "last_usage_date": "2026-08-30",
                "total_spend": 3,
            },
        ],
        False,
        "2026-08-01",
        "2026-08-30",
        registry,
    )

    assert result["active_window"] == {
        "start_date": "2026-08-24",
        "end_date": "2026-08-30",
        "days": 7,
        "definition": "Currently registered apps with positive Apps compute usage",
    }
    assert result["active_count"] == 1
    assert {
        app["app_id"]: app["status"]
        for app in result["apps"]
        if app["is_registered"]
    } == {"active-id": "active", "old-id": "inactive"}


def test_active_count_contract_rejects_disagreement():
    with pytest.raises(ValueError, match="active count contract mismatch"):
        apps._validate_active_count_contract(
            {"active_app_count": 155},
            {"active_count": 179},
        )
    with pytest.raises(ValueError, match="active count contract mismatch"):
        apps._validate_active_count_contract({}, {})


def test_app_detail_cache_reuses_existing_single_get_per_app(monkeypatch):
    app_detail = SimpleNamespace(
        description="Cached app",
        creator="owner@example.com",
        updater=None,
        create_time=None,
        update_time=None,
        compute_size="LARGE",
        compute_min_instances=None,
        compute_max_instances=None,
        compute_status=_status("ACTIVE", "", active_instances=1),
        app_status=_status("RUNNING", "", running_instances=1),
        active_deployment=None,
        pending_deployment=None,
        git_source=None,
        default_git_source=None,
        git_repository=None,
        source_code_path=None,
        default_source_code_path=None,
        thumbnail_url=None,
        space=None,
        url="https://cached-app.example.databricksapps.com",
        effective_resources=[
            SimpleNamespace(
                name="warehouse",
                description="Read-only warehouse",
                sql_warehouse=SimpleNamespace(id="warehouse-id", permission="CAN_USE"),
                serving_endpoint=None,
                job=None,
                database=None,
                postgres=None,
                genie_space=None,
                experiment=None,
                app=None,
                uc_securable=None,
                secret=None,
            )
        ],
        resources=[],
        service_principal_name="app-run-as",
        service_principal_id=123456789,
    )

    class FakeApps:
        def __init__(self):
            self.calls = 0

        def get(self, name):
            assert name == "cached-app"
            self.calls += 1
            return app_detail

    fake_apps = FakeApps()
    monkeypatch.setattr(
        apps,
        "get_workspace_client",
        lambda: SimpleNamespace(apps=fake_apps),
    )
    monkeypatch.setattr(apps, "_app_details_cache", {})
    monkeypatch.setattr(apps, "_app_details_cache_time", 0)
    registry = {
        "app-id": {
            "name": "cached-app",
            "url": "",
            "metadata": {},
        }
    }

    first = apps._get_app_details(registry)
    second = apps._get_app_details(registry)

    assert fake_apps.calls == 1
    assert second is first
    assert first["app-id"]["metadata"]["compute_size"] == "LARGE"
    assert {resource["type"] for resource in first["app-id"]["resources"]} == {
        "SQL_WAREHOUSE",
        "SERVICE_PRINCIPAL",
    }
    service_principal = next(
        resource
        for resource in first["app-id"]["resources"]
        if resource["type"] == "SERVICE_PRINCIPAL"
    )
    assert service_principal["id"] == "123456789"


@pytest.mark.asyncio
async def test_connected_artifacts_exposes_only_authoritative_service_principal_id(monkeypatch):
    registry = {"app-id": {"name": "app", "url": "", "metadata": {}}}
    monkeypatch.setattr(apps, "_get_app_registry", lambda: registry)
    monkeypatch.setattr(
        apps,
        "_get_app_resources",
        lambda: {
            "app": [
                {
                    "name": "app-run-as",
                    "type": "SERVICE_PRINCIPAL",
                    "description": "Run-as identity",
                    "id": "123456789",
                },
                {
                    "name": "display-name-only",
                    "type": "SERVICE_PRINCIPAL",
                    "description": "Run-as identity",
                },
            ]
        },
    )

    result = await apps.get_connected_artifacts()

    assert result["artifacts"][0]["artifact_id"] == "123456789"
    assert result["artifacts"][1]["artifact_id"] is None


def test_app_detail_refresh_is_bounded_parallel_and_single_flight(monkeypatch):
    active = 0
    max_active = 0
    calls = 0
    guard = threading.Lock()

    class FakeApps:
        def get(self, name):
            nonlocal active, max_active, calls
            with guard:
                active += 1
                calls += 1
                max_active = max(max_active, active)
            time.sleep(0.03)
            with guard:
                active -= 1
            return SimpleNamespace(
                description=name,
                active_deployment=None,
                pending_deployment=None,
                effective_resources=[],
                resources=[],
                thumbnail_url=None,
                url=f"https://{name}.example.databricksapps.com",
            )

    registry = {
        f"id-{index}": {"name": f"app-{index}", "url": "", "metadata": {}}
        for index in range(10)
    }
    monkeypatch.setattr(
        apps, "get_workspace_client", lambda: SimpleNamespace(apps=FakeApps())
    )
    monkeypatch.setattr(apps, "_app_details_cache", {})
    monkeypatch.setattr(apps, "_app_details_cache_time", 0)
    monkeypatch.setattr(apps, "_app_details_refresh_inflight", None)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: apps._get_app_details(registry), range(2)))

    assert calls == len(registry)
    assert 1 < max_active <= apps._APP_DETAILS_MAX_WORKERS
    assert results[0] is results[1]


@pytest.mark.asyncio
async def test_connected_artifacts_refresh_does_not_block_event_loop(monkeypatch):
    registry = {"id": {"name": "app", "url": "", "metadata": {}}}
    monkeypatch.setattr(apps, "_get_app_registry", lambda: registry)

    def slow_resources():
        time.sleep(0.05)
        return {"app": []}

    monkeypatch.setattr(apps, "_get_app_resources", slow_resources)
    route = asyncio.create_task(apps.get_connected_artifacts())
    started = time.monotonic()
    await asyncio.sleep(0.005)
    assert time.monotonic() - started < 0.03
    result = await route
    assert result["stale"] is False


def test_secret_binding_never_exposes_scope_or_key():
    binding = apps._resource_binding(
        SimpleNamespace(
            name="api-credential",
            description="",
            secret=SimpleNamespace(
                scope="production-secrets",
                key="super-secret-key",
                permission="READ",
            ),
            serving_endpoint=None,
            sql_warehouse=None,
            job=None,
            database=None,
            postgres=None,
            genie_space=None,
            experiment=None,
            app=None,
            uc_securable=None,
            type=None,
        )
    )

    assert binding == {
        "name": "api-credential",
        "type": "SECRET",
        "description": "Secret resource binding",
    }
    assert "production-secrets" not in repr(binding)
    assert "super-secret-key" not in repr(binding)


@pytest.mark.asyncio
async def test_thumbnail_proxy_authenticates_only_to_workspace_host(monkeypatch):
    calls = []

    class FakeResponse:
        def __init__(self, status_code, content=b"", content_type="text/plain"):
            self.status_code = status_code
            self.content = content
            self.headers = {"content-type": content_type}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def aiter_bytes(self):
            yield self.content

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            assert kwargs["follow_redirects"] is False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def stream(self, _method, target, headers):
            calls.append((target, headers))
            if headers:
                return FakeResponse(200, b"\x89PNG" + (b"x" * 32), "image/png")
            return FakeResponse(401)

    registry = {
        "app-id": {
            "name": "safe-app",
            "url": "https://safe-app.example.databricksapps.com",
            "metadata": {
                "_thumbnail_source_url": "/ajax-api/2.0/thumbnails/raw/apps/app-id/icon.png"
            },
        }
    }
    config = SimpleNamespace(
        host="https://workspace.example.com",
        token=None,
        authenticate=lambda: {"Authorization": "Bearer workspace-token"},
    )
    monkeypatch.setattr(apps, "_get_app_registry", lambda: registry)
    monkeypatch.setattr(apps, "get_workspace_client", lambda: SimpleNamespace(config=config))
    monkeypatch.setattr(apps.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(apps, "_app_details_cache", {})
    monkeypatch.setattr(apps, "_thumbnail_cache", {})

    response = await apps.get_app_thumbnail("app-id")

    assert response.status_code == 200
    assert calls == [
        (
            "https://workspace.example.com/ajax-api/2.0/thumbnails/raw/apps/app-id/icon.png",
            {},
        ),
        (
            "https://workspace.example.com/ajax-api/2.0/thumbnails/raw/apps/app-id/icon.png",
            {"Authorization": "Bearer workspace-token"},
        ),
    ]


@pytest.mark.asyncio
async def test_thumbnail_proxy_rejects_lookalike_origin_before_credentials(monkeypatch):
    auth_calls = 0

    def authenticate():
        nonlocal auth_calls
        auth_calls += 1
        return {"Authorization": "Bearer secret"}

    class NotFound:
        status_code = 404
        headers = {"content-type": "text/plain"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def aiter_bytes(self):
            if False:
                yield b""

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def stream(self, *_args, **_kwargs):
            return NotFound()

    registry = {
        "app-id": {
            "name": "safe-app",
            "url": "https://safe-app.example.databricksapps.com",
            "metadata": {
                "_thumbnail_source_url": "https://workspace.example.com.evil.test/icon.png"
            },
        }
    }
    config = SimpleNamespace(
        host="https://workspace.example.com:443",
        token=None,
        authenticate=authenticate,
    )
    monkeypatch.setattr(apps, "_get_app_registry", lambda: registry)
    monkeypatch.setattr(apps, "get_workspace_client", lambda: SimpleNamespace(config=config))
    monkeypatch.setattr(apps.httpx, "AsyncClient", Client)
    monkeypatch.setattr(apps, "_thumbnail_cache", {})

    response = await apps.get_app_thumbnail("app-id")

    assert response.status_code == 404
    assert auth_calls == 0


@pytest.mark.asyncio
async def test_thumbnail_proxy_aborts_stream_over_hard_cap(monkeypatch):
    yielded = 0

    class Streamed:
        status_code = 200
        headers = {"content-type": "image/png"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def aiter_bytes(self):
            nonlocal yielded
            for _ in range(10):
                yielded += 1
                yield b"x" * (apps.MAX_THUMBNAIL_BYTES // 2)

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def stream(self, *_args, **_kwargs):
            return Streamed()

    registry = {
        "app-id": {
            "name": "safe-app",
            "url": "",
            "metadata": {
                "_thumbnail_source_url": "/ajax-api/2.0/thumbnails/app-id.png"
            },
        }
    }
    config = SimpleNamespace(
        host="https://workspace.example.com",
        token=None,
        authenticate=lambda: {},
    )
    monkeypatch.setattr(apps, "_get_app_registry", lambda: registry)
    monkeypatch.setattr(apps, "get_workspace_client", lambda: SimpleNamespace(config=config))
    monkeypatch.setattr(apps.httpx, "AsyncClient", Client)
    monkeypatch.setattr(apps, "_thumbnail_cache", {})

    response = await apps.get_app_thumbnail("app-id")

    assert response.status_code == 404
    assert yielded == 3


@pytest.mark.asyncio
async def test_thumbnail_proxy_rejects_malformed_id_before_registry_or_sdk(monkeypatch):
    registry_calls = 0
    sdk_calls = 0

    def registry():
        nonlocal registry_calls
        registry_calls += 1
        return {}

    def workspace_client():
        nonlocal sdk_calls
        sdk_calls += 1
        return SimpleNamespace()

    cache = {}
    monkeypatch.setattr(apps, "_get_app_registry", registry)
    monkeypatch.setattr(apps, "get_workspace_client", workspace_client)
    monkeypatch.setattr(apps, "_thumbnail_cache", cache)

    response = await apps.get_app_thumbnail("../../arbitrary-target")

    assert response.status_code == 400
    assert registry_calls == 0
    assert sdk_calls == 0
    assert cache == {}


@pytest.mark.asyncio
async def test_thumbnail_proxy_rejects_unknown_registry_id_without_caching(monkeypatch):
    sdk_calls = 0

    def workspace_client():
        nonlocal sdk_calls
        sdk_calls += 1
        return SimpleNamespace()

    cache = {}
    monkeypatch.setattr(apps, "_get_app_registry", lambda: {})
    monkeypatch.setattr(apps, "get_workspace_client", workspace_client)
    monkeypatch.setattr(apps, "_thumbnail_cache", cache)

    response = await apps.get_app_thumbnail("unknown-app-id")

    assert response.status_code == 404
    assert sdk_calls == 0
    assert cache == {}


def test_thumbnail_cache_is_bounded_ttl_lru():
    assert apps._thumbnail_cache.maxsize == 128
    assert apps._thumbnail_cache.ttl == apps.cache_ttls.THUMBNAIL
