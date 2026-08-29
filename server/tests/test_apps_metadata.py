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
    assert public["has_thumbnail"] is True
    assert "_thumbnail_source_url" not in public
    assert "env_vars" not in public
    assert "oauth2_app_client_id" not in public
    assert "service_principal_client_id" not in public
    assert "forward_user_access_token" not in public
    assert "never-return-this" not in repr(public)


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

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            assert kwargs["follow_redirects"] is False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, target, headers):
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
    monkeypatch.setattr(apps, "_thumbnail_cache_time", {})
    monkeypatch.setattr(apps, "_thumbnail_cache_content_type", {})

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
