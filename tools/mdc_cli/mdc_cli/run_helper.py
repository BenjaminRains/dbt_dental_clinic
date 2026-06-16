"""Isolated subprocess execution for Phase 4.3 stateless runs."""

from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional, Sequence

import typer

from mdc_cli.paths import DBT_DIR, discover_component_python, discover_dbt_python

# Mirrors Clear-StaleStageEnvVars in environment_manager.ps1 (+ dbt prefixes).
STAGE_ENV_PREFIXES = (
    "API_",
    "CLINIC_",
    "DEMO_POSTGRES_",
    "DEMO_API_",
    "TEST_POSTGRES_ANALYTICS_",
    "POSTGRES_ANALYTICS_",
    "OPENDENTAL_SOURCE_",
    "GLIC_OPENDENTAL_SOURCE_",
    "MYSQL_REPLICATION_",
    "TEST_OPENDENTAL_SOURCE_",
    "TEST_MYSQL_REPLICATION_",
    "ETL_",
    "METRICS_",
    "DBT_",
    "PGSSL",
)

EXPLICIT_ENV_KEYS = frozenset(
    {"PGSSLMODE", "DEMO_API_KEY", "CLINIC_API_KEY", "ENABLE_METRICS"}
)

MINIMAL_ENV_KEYS = (
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "TEMP",
    "TMP",
    "HOME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "COMSPEC",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "LOCALAPPDATA",
    "APPDATA",
    "USERDOMAIN",
    "USERNAME",
    "PYTHONIOENCODING",
    "LANG",
    "LC_ALL",
    "TZ",
)


def _is_stage_env_key(key: str) -> bool:
    if key in EXPLICIT_ENV_KEYS:
        return True
    return any(key.startswith(prefix) for prefix in STAGE_ENV_PREFIXES)


@contextmanager
def scrub_parent_stage_env() -> Iterator[None]:
    """Temporarily remove stage-scoped vars from os.environ while loading settings."""
    saved: dict[str, str] = {}
    for key in list(os.environ.keys()):
        if _is_stage_env_key(key):
            saved[key] = os.environ.pop(key)
    try:
        yield
    finally:
        os.environ.update(saved)


def build_minimal_base_env() -> dict[str, str]:
    """Platform essentials only — not the full parent shell config store."""
    base: dict[str, str] = {}
    for key in MINIMAL_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            base[key] = value
    return base


def build_isolated_child_env(
    settings: dict[str, str],
    *,
    venv_root: Optional[Path] = None,
) -> dict[str, str]:
    """Validated settings + minimal OS base; parent stage vars do not leak in."""
    env = build_minimal_base_env()
    env.update(settings)
    if venv_root is not None:
        env["VIRTUAL_ENV"] = str(venv_root)
    return env


def build_child_env(settings: dict[str, str]) -> dict[str, str]:
    """Backward-compatible alias; prefer build_isolated_child_env for runtime runs."""
    return build_isolated_child_env(settings)


def load_env_dict_isolated(
    component: str,
    stage: str,
    *,
    profile: Optional[str] = None,
) -> dict[str, str]:
    with scrub_parent_stage_env():
        from mdc_cli.env import load_env_dict

        return load_env_dict(component, stage, profile=profile)


def venv_root_from_python(python: Path) -> Path:
    return python.parent.parent


def require_component_python(component: str) -> Path:
    python = discover_component_python(component)
    if python is None:
        typer.echo(
            f"No Python venv found for {component}. "
            f"Install dependencies in the project directory first.",
            err=True,
        )
        raise typer.Exit(code=2)
    return python


def discover_dbt_executable() -> Optional[Path]:
    python = discover_dbt_python()
    if python is None:
        return None
    venv_root = venv_root_from_python(python)
    if os.name == "nt":
        candidate = venv_root / "Scripts" / "dbt.exe"
    else:
        candidate = venv_root / "bin" / "dbt"
    return candidate if candidate.exists() else None


def require_dbt_executable() -> Path:
    dbt = discover_dbt_executable()
    if dbt is None:
        typer.echo(
            "No dbt executable found in dbt_dental_models Pipenv venv. "
            "Run pipenv install in dbt_dental_models/.",
            err=True,
        )
        raise typer.Exit(code=2)
    return dbt


def _ensure_dbt_target(args: Sequence[str], stage: str) -> list[str]:
    """Append --target when the user did not pass one."""
    for index, arg in enumerate(args):
        if arg in ("--target", "-t"):
            return list(args)
        if arg.startswith("--target="):
            return list(args)
    return [*args, "--target", stage]


def run_isolated(
    *,
    settings: dict[str, str],
    cmd: Sequence[str],
    cwd: Path,
    venv_root: Optional[Path] = None,
) -> int:
    child_env = build_isolated_child_env(settings, venv_root=venv_root)
    completed = subprocess.run(
        list(cmd),
        env=child_env,
        cwd=str(cwd),
        check=False,
    )
    return int(completed.returncode)


def run_component_python(
    component: str,
    stage: str,
    cmd_tail: Sequence[str],
    *,
    profile: Optional[str] = None,
    cwd: Path,
) -> int:
    settings = load_env_dict_isolated(component, stage, profile=profile)
    python = require_component_python(component)
    cmd = [str(python), *cmd_tail]
    return run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=cwd,
        venv_root=venv_root_from_python(python),
    )


def run_dbt_command(
    stage: str,
    dbt_args: Sequence[str],
) -> int:
    settings = load_env_dict_isolated("dbt", stage)
    dbt = require_dbt_executable()
    python = discover_dbt_python()
    venv_root = venv_root_from_python(python) if python else None
    cmd = [str(dbt), *_ensure_dbt_target(dbt_args, stage)]
    return run_isolated(
        settings=settings,
        cmd=cmd,
        cwd=DBT_DIR,
        venv_root=venv_root,
    )


def echo_run_banner(component: str, stage: str, config_path: Path, detail: str) -> None:
    rel = config_path
    try:
        from mdc_cli.paths import REPO_ROOT

        rel = config_path.relative_to(REPO_ROOT)
    except ValueError:
        pass
    typer.echo(f"{component.upper()}  {stage}  {rel}  {detail}")
