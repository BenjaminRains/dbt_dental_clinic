"""Repository paths and stage matrix for mdc."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

PACKAGE_DIR = Path(__file__).resolve().parent
MDC_CLI_ROOT = PACKAGE_DIR.parent
REPO_ROOT = MDC_CLI_ROOT.parent.parent

API_DIR = REPO_ROOT / "api"
ETL_DIR = REPO_ROOT / "etl_pipeline"
DBT_DIR = REPO_ROOT / "dbt_dental_models"
FRONTEND_DIR = REPO_ROOT / "frontend"
DEPLOYMENT_CREDENTIALS = REPO_ROOT / "deployment_credentials.json"
FRONTEND_DEPLOY_JSON = REPO_ROOT / ".frontend-deploy.json"


@dataclass(frozen=True)
class ComponentStage:
    """One row in the stage × component matrix."""

    component: str
    stage: str
    config_path: Path
    profile: Optional[str] = None


API_STAGES = ("local", "demo", "clinic", "test")
ETL_STAGES = ("local", "clinic", "test")
DBT_STAGES = ("local", "clinic", "demo")
# Union of stages accepted by `mdc --env` (dev/test targets only; not deployment labels).
ALL_MDC_STAGES = tuple(sorted(set(API_STAGES) | set(ETL_STAGES) | set(DBT_STAGES)))


def api_env_file(stage: str) -> Path:
    return API_DIR / f".env_api_{stage}"


def etl_env_file(stage: str) -> Path:
    return ETL_DIR / f".env_{stage}"


def etl_pipeline_config() -> Path:
    """Default pipeline.yml used by etl_pipeline.cli status."""
    return ETL_DIR / "etl_pipeline" / "config" / "pipeline.yml"


def etl_pipeline_config_arg() -> str:
    """Config path relative to ETL_DIR (cwd for mdc etl run/status)."""
    return etl_pipeline_config().relative_to(ETL_DIR).as_posix()


def dbt_env_file(stage: str) -> Path:
    return DBT_DIR / f".env_{stage}"


def default_etl_profile(stage: str) -> str:
    return "load" if stage == "local" else "full"


def iter_status_targets(env_filter: Optional[str] = None) -> list[ComponentStage]:
    """All component/stage pairs shown by mdc status."""
    targets: list[ComponentStage] = []

    for stage in API_STAGES:
        if env_filter and stage != env_filter:
            continue
        targets.append(ComponentStage("api", stage, api_env_file(stage)))

    for stage in ETL_STAGES:
        if env_filter and stage != env_filter:
            continue
        targets.append(
            ComponentStage(
                "etl",
                stage,
                etl_env_file(stage),
                profile=default_etl_profile(stage),
            )
        )

    for stage in DBT_STAGES:
        if env_filter and stage != env_filter:
            continue
        targets.append(
            ComponentStage("dbt", stage, _dbt_status_config_path(stage))
        )

    return targets


def _dbt_status_config_path(stage: str) -> Path:
    from mdc_cli.dbt_env import dbt_config_path

    return dbt_config_path(stage)


def _venv_python(venv_root: Path) -> Optional[Path]:
    if os.name == "nt":
        candidate = venv_root / "Scripts" / "python.exe"
    else:
        candidate = venv_root / "bin" / "python"
    return candidate if candidate.exists() else None


def discover_api_python() -> Optional[Path]:
    """Return api/venv python if present."""
    return _venv_python(API_DIR / "venv")


def discover_etl_python() -> Optional[Path]:
    """Return Pipenv venv python for etl_pipeline if discoverable."""
    if not (ETL_DIR / "Pipfile").exists():
        return None
    try:
        result = subprocess.run(
            ["pipenv", "--venv"],
            cwd=ETL_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return _venv_python(Path(result.stdout.strip()))


def discover_dbt_python() -> Optional[Path]:
    """Return Pipenv venv python for dbt_dental_models if discoverable."""
    if not (DBT_DIR / "Pipfile").exists():
        return None
    try:
        result = subprocess.run(
            ["pipenv", "--venv"],
            cwd=DBT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return _venv_python(Path(result.stdout.strip()))


def discover_component_python(component: str) -> Optional[Path]:
    if component == "api":
        return discover_api_python()
    if component == "etl":
        return discover_etl_python()
    if component == "dbt":
        return discover_dbt_python()
    return None


def ensure_api_importable() -> None:
    api_path = str(API_DIR)
    if api_path not in sys.path:
        sys.path.insert(0, api_path)


def ensure_etl_importable() -> None:
    etl_path = str(ETL_DIR)
    if etl_path not in sys.path:
        sys.path.insert(0, etl_path)
