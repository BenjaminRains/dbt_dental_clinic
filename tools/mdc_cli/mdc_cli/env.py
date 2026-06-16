"""Load validated env dicts and run subprocesses with injected env (Phase 4)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Optional, Sequence

from mdc_cli.paths import (
    ETL_DIR,
    default_etl_profile,
    ensure_api_importable,
    ensure_etl_importable,
)
from mdc_cli.dbt_env import load_dbt_env_dict, validate_dbt_stage


def build_child_env(settings: dict[str, str]) -> dict[str, str]:
    """Merge validated settings into a copy of os.environ for a child process."""
    env = os.environ.copy()
    env.update(settings)
    return env


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
    Run a command with pydantic-validated env injected into the child process only.

    The parent shell environment is not modified.
    """
    settings = load_env_dict(component, stage, profile=profile)
    child_env = build_child_env(settings)
    return subprocess.run(
        list(cmd),
        env=child_env,
        cwd=str(cwd) if cwd else None,
        check=check,
        text=True,
    )
