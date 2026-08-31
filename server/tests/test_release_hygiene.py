"""Release manifest and public-stage regression checks."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import shutil
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARCHITECTURE_PDF_SHA256 = (
    "aade7cb46480fe0d30586557a06495c8e800c72c40f60f0331c1fa5990506812"
)


def test_repository_has_no_github_workflows():
    workflows = ROOT / ".github" / "workflows"
    assert not workflows.exists() or not any(path.is_file() for path in workflows.rglob("*"))


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
    assert "COST_OBS_FEEDBACK_SLACK_URL" in example
    assert "COST_OBS_COMMIT_SHA" in example
    assert not (ROOT / "release-metadata.json").exists()

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

    validator = _load_public_validator()
    errors = validator.validate_public_tree(stage)

    assert errors == []
    assert (stage / "CHANGELOG.md").exists()
    assert not (stage / "docs" / "DEPLOYMENT_PLAN.md").exists()
    source_pdf = stage / "client" / "public" / "reports" / "cost-obs-arch-1.2.pdf"
    static_pdf = stage / "static" / "reports" / "cost-obs-arch-1.2.pdf"
    for pdf in (source_pdf, static_pdf):
        content = pdf.read_bytes()
        assert content.startswith(b"%PDF-")
        assert hashlib.sha256(content).hexdigest() == ARCHITECTURE_PDF_SHA256
        assert pdf not in set(validator._text_files(stage))


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
    assert "scripts/write_release_metadata.py" in script
    assert '--commit-sha "$ORIGIN_SHA"' in script
    assert 'git show -s --format=%cI "$ORIGIN_SHA"' in script
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


def test_release_metadata_writer_is_deterministic_and_skips_noop_rewrites(tmp_path):
    path = ROOT / "scripts" / "write_release_metadata.py"
    spec = importlib.util.spec_from_file_location("write_release_metadata", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    output = tmp_path / "release-metadata.json"
    first = module.render_metadata(
        "abc123",
        "2026-08-31T17:00:00+00:00",
        "Release Author <release@example.com>",
    )
    assert module.write_if_changed(output, first) is True
    first_mtime = output.stat().st_mtime_ns
    assert module.write_if_changed(output, first) is False
    assert output.stat().st_mtime_ns == first_mtime
    assert json.loads(output.read_text()) == {
        "commit_sha": "abc123",
        "deployed_at": "2026-08-31T17:00:00+00:00",
        "deployer": "Release Author",
    }

    second = module.render_metadata(
        "def456",
        "2026-08-31T18:00:00+00:00",
        "Release Author <release@example.com>",
    )
    assert module.write_if_changed(output, second) is True
    assert json.loads(output.read_text())["commit_sha"] == "def456"


def test_release_versions_locks_and_requirements_are_consistent():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    package = json.loads((ROOT / "client" / "package.json").read_text())
    uv_lock = tomllib.loads((ROOT / "uv.lock").read_text())
    requirements = (ROOT / "requirements.txt").read_text()
    bun_lock = (ROOT / "client" / "bun.lock").read_text()

    assert pyproject["project"]["version"] == "1.2.0"
    assert package["version"] == "1.2.0"
    assert package["packageManager"] == "bun@1.3.5"
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
    locked_project = next(
        locked_package
        for locked_package in uv_lock["package"]
        if locked_package["name"] == pyproject["project"]["name"]
    )
    locked_direct_dependencies = {
        dependency["name"].lower().replace("_", "-")
        for dependency in locked_project["dependencies"]
    }
    assert locked_direct_dependencies == direct_python_dependencies
    assert direct_python_dependencies <= requirement_versions.keys()

    client_dependencies = {
        **package["dependencies"],
        **package["devDependencies"],
    }
    for name, constraint in client_dependencies.items():
        assert f'"{name}": "{constraint}"' in bun_lock, name


def test_release_gate_runs_complete_deterministic_suites_and_bun_only():
    script = (ROOT / "scripts" / "release-check.sh").read_text()

    required_commands = (
        "bun install --frozen-lockfile --ignore-scripts",
        "bun run lint",
        "bun run typecheck",
        "bun run test:unit",
        '-m "not (external or integration)"',
        "server/tests",
        "uv lock --check",
        "uv export",
        'compare_requirements "$ROOT/requirements.txt" "$EXPORTED_REQUIREMENTS"',
        "validate_public_tree.py",
        "git -C \"$ROOT\" ls-files -z '*.sh'",
    )
    for command in required_commands:
        assert command in script

    assert "command -v npm" not in script
    assert "npm run" not in script
    assert "RUNNER=(npm)" not in script
    assert "src/components/ExportDialog.tsx" not in script
    assert "server/tests/test_deployment_metadata.py" not in script
    assert 'python3 "$ROOT/scripts/run_with_timeout.py" "$@"' in script
    assert 'python3 "$ROOT/scripts/run_with_timeout.py" --self-test' in script
    assert "run_with_timeout 180" in script
    assert "run_with_timeout 900" not in script
    assert "run_with_timeout 300" not in script
    assert "run_with_timeout 600" not in script
    assert "COST_OBS_RELEASE_TESTING=1" in script
    assert "COST_OBS_TEST_TIMEOUT_SECONDS" in script


def test_release_gate_compares_isolated_build_to_clean_committed_static():
    script = (ROOT / "scripts" / "release-check.sh").read_text()

    assert 'status --porcelain --untracked-files=all -- static' in script
    assert 'BUILD_OUT="$WORK_DIR/static"' in script
    assert 'bun run build:release -- --outDir "$BUILD_OUT"' in script
    assert 'compare_trees "$ROOT/static" "$BUILD_OUT"' in script
    assert "expected_files[relative] != actual_files[relative]" in script
    assert "git checkout" not in script
    assert "git reset" not in script


def test_external_backend_tests_are_marked_blocked_and_opt_in():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    pytest_config = pyproject["tool"]["pytest"]["ini_options"]
    markers = "\n".join(pytest_config["markers"])
    conftest = (ROOT / "conftest.py").read_text()
    external_test = (ROOT / "scripts" / "test_parallel.py").read_text()
    release_script = (ROOT / "scripts" / "release-check.sh").read_text()

    assert pytest_config["addopts"] == "--strict-markers"
    assert "error::pytest.PytestUnhandledThreadExceptionWarning" in pytest_config["filterwarnings"]
    assert "external:" in markers
    assert "integration:" in markers
    assert "pytest.mark.external" in external_test
    assert "pytest.mark.integration" in external_test
    assert "block_unmarked_network" in conftest
    assert "socket.AF_INET" in conftest
    assert "pytest_runtest_setup" in conftest
    assert "pytest_runtest_call" in conftest
    assert "pytest_runtest_teardown" in conftest
    setup_router = (ROOT / "server" / "routers" / "setup.py").read_text()
    assert 'os.getenv("COST_OBS_RELEASE_TESTING") == "1"' in setup_router
    assert "--with-external" in release_script
    assert '-m "external or integration"' in release_script
    assert "scripts/test_parallel.py" in release_script
