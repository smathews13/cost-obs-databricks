"""Release manifest and public-stage regression checks."""

from __future__ import annotations

import importlib.util
import json
import re
import shutil
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load_public_validator():
    path = ROOT / "scripts" / "validate_public_tree.py"
    spec = importlib.util.spec_from_file_location("validate_public_tree", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _derive_working_public_stage(destination: Path) -> None:
    subprocess.run(
        [
            "rsync",
            "-a",
            "--delete",
            f"--exclude-from={ROOT / 'mirror' / 'publish-exclude.txt'}",
            f"{ROOT}/",
            f"{destination}/",
        ],
        check=True,
    )
    shutil.copy2(ROOT / "app.yaml.example", destination / "app.yaml")


def test_customer_manifests_never_default_to_forbidden_storage():
    forbidden_pair = re.compile(
        r"COST_OBS_CATALOG[^\n]*\n\s*value:\s*main.*?"
        r"COST_OBS_SCHEMA[^\n]*\n\s*value:\s*cost_obs\b",
        re.DOTALL,
    )
    for relative in (
        "app.yaml.example",
        "app." + "azure-" + "field-eng.yaml",
        ".env.example",
        "jobs/mv_refresh_job.json.template",
    ):
        text = (ROOT / relative).read_text()
        assert not forbidden_pair.search(text), relative

    example = (ROOT / "app.yaml.example").read_text()
    assert "<your-dedicated-catalog>" in example
    assert "<your-dedicated-schema>" in example
    assert "COST_OBS_FEEDBACK_GITHUB_URL" in example

    azure_example = (ROOT / ("app." + "azure-" + "field-eng.yaml")).read_text()
    assert "<your-dedicated-catalog>" in azure_example
    assert "<your-dedicated-schema>" in azure_example
    assert not re.search(r"\b\d{12,16}\b", azure_example)
    assert not re.search(
        r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
        r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
        azure_example,
        re.IGNORECASE,
    )

    job = json.loads((ROOT / "jobs/mv_refresh_job.json.template").read_text())
    parameters = job["tasks"][0]["notebook_task"]["base_parameters"]
    assert parameters == {
        "COST_OBS_CATALOG": "__COST_OBS_CATALOG__",
        "COST_OBS_SCHEMA": "__COST_OBS_SCHEMA__",
    }


def test_current_public_stage_has_no_identifiers_secrets_or_broken_links(tmp_path):
    stage = tmp_path / "public"
    stage.mkdir()
    _derive_working_public_stage(stage)

    errors = _load_public_validator().validate_public_tree(stage)

    assert errors == []
    assert (stage / "CHANGELOG.md").exists()
    assert not (stage / "docs" / "DEPLOYMENT_PLAN.md").exists()


def test_public_validator_rejects_removed_doc_links_and_private_targets(tmp_path):
    stage = tmp_path / "public"
    stage.mkdir()
    (stage / "app.yaml").write_text(
        "env:\n"
        "  - name: COST_OBS_CATALOG\n"
        "    value: <your-dedicated-catalog>\n"
        "  - name: COST_OBS_SCHEMA\n"
        "    value: <your-dedicated-schema>\n"
    )
    private_address = "person" + chr(64) + "personal.test"
    (stage / "README.md").write_text(
        "[removed guide](docs/removed.md)\n"
        f"[feedback](mailto:{private_address})\n"
    )

    errors = _load_public_validator().validate_public_tree(stage)

    assert any("broken public link" in error for error in errors)
    assert any("hardcoded personal feedback email" in error for error in errors)


def test_public_validator_scans_source_and_compiled_assets(tmp_path):
    stage = tmp_path / "public"
    source = stage / "client" / "src"
    compiled = stage / "static" / "assets"
    source.mkdir(parents=True)
    compiled.mkdir(parents=True)
    (stage / "app.yaml").write_text(
        "env:\n"
        "  - name: COST_OBS_CATALOG\n"
        "    value: <your-dedicated-catalog>\n"
        "  - name: COST_OBS_SCHEMA\n"
        "    value: <your-dedicated-schema>\n"
    )
    slack_id = "U" + "12345678"
    workspace_id = "123456" + "789012"
    (source / "feedback.ts").write_text(f'export const member = "{slack_id}";\n')
    (compiled / "index.js").write_text(f'const workspace = "{workspace_id}";\n')

    errors = _load_public_validator().validate_public_tree(stage)

    assert any("client/src/feedback.ts" in error for error in errors)
    assert any("static/assets/index.js" in error for error in errors)


def test_sync_script_uses_committed_origin_and_never_force_pushes():
    script = (ROOT / "sync-mirror.sh").read_text()

    assert "git status --porcelain --untracked-files=all" in script
    assert 'git branch --show-current)" == "main"' in script
    assert "'@{upstream}'" in script
    assert 'git archive --format=tar "$ORIGIN_SHA"' in script
    assert 'ORIGIN_SHA="$(git rev-parse origin/main)"' in script
    assert 'HEAD_SHA" == "$ORIGIN_SHA' in script
    assert 'SOURCE_TREE="$WORK_DIR/source"' in script
    assert 'STAGE_TREE="$WORK_DIR/stage"' in script
    assert '--exclude-from="$SOURCE_TREE/mirror/publish-exclude.txt"' in script
    assert "scripts/release-check.sh" in script
    assert "validate_public_tree.py" in script
    assert "trap restore_internal_account EXIT INT TERM HUP" in script
    assert "gh auth switch --user \"$INTERNAL_ACCOUNT\"" in script
    assert "git clone --quiet --branch main --single-branch" in script
    assert "merge-base --is-ancestor" in script
    assert "Public main changed during staging" in script
    assert "push origin HEAD:main" in script
    assert "--force" not in script
    assert '"$ROOT/"' not in script


def test_release_versions_locks_and_requirements_are_consistent():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    package = json.loads((ROOT / "client" / "package.json").read_text())
    uv_lock = tomllib.loads((ROOT / "uv.lock").read_text())
    requirements = (ROOT / "requirements.txt").read_text()
    bun_lock = (ROOT / "client" / "bun.lock").read_text()

    assert pyproject["project"]["version"] == "1.2.0"
    assert package["version"] == "1.2.0"
    assert "## v1.2" in (ROOT / "CHANGELOG.md").read_text()
    assert '<a id="release-v12"></a>' in (ROOT / "README.md").read_text()

    locked_versions = {
        package["name"].lower().replace("_", "-"): package["version"]
        for package in uv_lock["package"]
    }
    requirement_versions = {
        match.group(1).lower().replace("_", "-"): match.group(2)
        for match in re.finditer(
            r"^([A-Za-z0-9_.-]+)==([^\s;]+)",
            requirements,
            re.MULTILINE,
        )
    }
    for name, version in requirement_versions.items():
        assert locked_versions.get(name) == version, name

    direct_python_dependencies = {
        re.match(r"^[A-Za-z0-9_.-]+", dependency).group(0).lower().replace("_", "-")
        for dependency in pyproject["project"]["dependencies"]
    }
    assert direct_python_dependencies <= requirement_versions.keys()

    client_dependencies = {
        **package["dependencies"],
        **package["devDependencies"],
    }
    for name, constraint in client_dependencies.items():
        assert f'"{name}": "{constraint}"' in bun_lock, name
