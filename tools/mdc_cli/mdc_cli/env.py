"""Load validated env dicts and run subprocesses with injected env (Phase 4)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Optional, Sequence

from mdc_cli.paths import (
    ETL_DIR,
    default_etl_profile,
    discover_component_python,
    ensure_api_importable,
    ensure_etl_importable,
)
from mdc_cli.dbt_env import load_dbt_env_dict, validate_dbt_stage


def load_api_env_dict(stage: str) -> dict[str, str]:
    ensure_api_importable()
    from settings import export_api_env_dict

    return export_api_env_dict(environment=stage)


def load_etl_env_dict(stage: str, profile: Optional[str] = None) -> dict[str, str]:
    ensure_etl_importable()
    from etl_pipeline.config.settings_v2 import load_etl_env_dict

    resolved_profile = profile or default_etl_profile(stage)
    return load_etl_env_dict(
        environment=stage,
        config_dir=ETL_DIR,
        profile=resolved_profile,
    )


def build_child_env(settings: dict[str, str]) -> dict[str, str]:
    from mdc_cli.run_helper import build_isolated_child_env

    return build_isolated_child_env(settings)


def load_env_dict(
    component: str,
    stage: str,
    *,
    profile: Optional[str] = None,
) -> dict[str, str]:
    if component == "api":
        return load_api_env_dict(stage)
    if component == "etl":
        return load_etl_env_dict(stage, profile=profile)
    if component == "dbt":
        return load_dbt_env_dict(stage)
    raise ValueError(f"Unsupported component for env load: {component}")


def validate_api_stage(stage: str) -> tuple[bool, Optional[str]]:
    try:
        ensure_api_importable()
        from settings import load_api_settings

        load_api_settings(environment=stage)
        return True, None
    except ValueError as exc:
        return False, str(exc)


def validate_etl_stage(stage: str, profile: Optional[str] = None) -> tuple[bool, Optional[str]]:
    try:
        ensure_etl_importable()
        from etl_pipeline.config.settings_v2 import load_etl_connection_settings

        resolved_profile = profile or default_etl_profile(stage)
        load_etl_connection_settings(
            environment=stage,
            config_dir=ETL_DIR,
            profile=resolved_profile,
        )
        return True, None
    except ModuleNotFoundError as exc:
        return False, f"ETL dependencies missing ({exc.name}); use etl_pipeline Pipenv venv"
    except ValueError as exc:
        return False, str(exc)


def validate_component_stage(
    component: str,
    stage: str,
    *,
    profile: Optional[str] = None,
) -> tuple[bool, Optional[str]]:
    if component == "api":
        return validate_api_stage(stage)
    if component == "etl":
        return validate_etl_stage(stage, profile=profile)
    if component == "dbt":
        return validate_dbt_stage(stage)
    return False, f"Unknown component: {component}"


def run_with_env(
    component: str,
    stage: str,
    cmd: Sequence[str],
    *,
    profile: Optional[str] = None,
    cwd: Optional[Path] = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """
    Run a command with pydantic-validated env injected into an isolated child process.

    The parent shell environment is not modified; stage-scoped parent vars are not inherited.
    """
    from mdc_cli.run_helper import build_isolated_child_env, load_env_dict_isolated, venv_root_from_python

    settings = load_env_dict_isolated(component, stage, profile=profile)
    python = discover_component_python(component)
    venv_root = venv_root_from_python(python) if python else None
    child_env = build_isolated_child_env(settings, venv_root=venv_root)
    completed = subprocess.run(
        list(cmd),
        env=child_env,
        cwd=str(cwd) if cwd else None,
        check=False,
        text=True,
    )
    if check and completed.returncode != 0:
        raise subprocess.CalledProcessError(
            completed.returncode, list(cmd), output=completed.stdout, stderr=completed.stderr
        )
    return completed
