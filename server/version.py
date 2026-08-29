"""Application version loaded from the Python project metadata source."""

import tomllib
from pathlib import Path

_PROJECT_FILE = Path(__file__).resolve().parents[1] / "pyproject.toml"
with _PROJECT_FILE.open("rb") as project_file:
    APP_VERSION = str(tomllib.load(project_file)["project"]["version"])
