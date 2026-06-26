"""Load dbt shell environment (mirrors dbt-init file/JSON rules, Phase 4.2b)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from mdc_cli import paths
from mdc_cli.paths import DEPLOYMENT_CREDENTIALS
from mdc_cli.postgres_env import (
    POSTGRES_ANALYTICS_REQUIRED,
    clinic_postgres_from_credentials,
    load_clinic_postgres_env_dict,
    load_local_warehouse_postgres_env_dict,
    overlay_os_env,
    read_env_file,
)

LOCAL_CLINIC_REQUIRED = POSTGRES_ANALYTICS_REQUIRED

DEMO_REQUIRED = (
    "DEMO_POSTGRES_HOST",
    "DEMO_POSTGRES_PORT",
    "DEMO_POSTGRES_DB",
    "DEMO_POSTGRES_USER",
    "DEMO_POSTGRES_PASSWORD",
)

DBT_ENV_PREFIXES = ("POSTGRES_ANALYTICS_", "DEMO_POSTGRES_", "DBT_", "PGSSL")


def dbt_config_path(stage: str) -> Path:
    """Primary config source path for status/validate display."""
    if stage == "local":
        return paths.dbt_env_file(stage)
    if stage == "demo":
        return DEPLOYMENT_CREDENTIALS
    if DEPLOYMENT_CREDENTIALS.exists():
        return DEPLOYMENT_CREDENTIALS
    return paths.dbt_env_file("clinic")


def _read_env_file(path: Path) -> dict[str, str]:
    return read_env_file(path)


def _overlay_os_env(base: dict[str, str]) -> dict[str, str]:
    return overlay_os_env(base, extra_prefixes=DBT_ENV_PREFIXES)


def _load_credentials_json() -> dict:
    from mdc_cli.postgres_env import _load_credentials_json as load_json

    return load_json()


def _clinic_from_credentials(data: dict) -> Optional[dict[str, str]]:
    return clinic_postgres_from_credentials(data, default_schema="dbt")


def _demo_from_credentials(data: dict) -> Optional[dict[str, str]]:
    pg = (data.get("demo_database") or {}).get("postgresql")
    if not pg or not pg.get("database"):
        return None
    password = pg.get("password")
    if password is None:
        return None
    return {
        "DEMO_POSTGRES_HOST": "localhost",
        "DEMO_POSTGRES_PORT": "5434",
        "DEMO_POSTGRES_DB": str(pg["database"]),
        "DEMO_POSTGRES_USER": str(pg.get("user", "opendental_demo_user")),
        "DEMO_POSTGRES_PASSWORD": str(password),
        "DEMO_POSTGRES_SCHEMA": "raw",
    }


def _require_keys(env: dict[str, str], keys: tuple[str, ...], source: str) -> None:
    missing = [key for key in keys if not (env.get(key) or "").strip()]
    if missing:
        sample = ", ".join(missing[:4])
        suffix = "..." if len(missing) > 4 else ""
        raise ValueError(f"Missing required vars ({sample}{suffix}) from {source}.")


def load_dbt_env_dict(stage: str) -> dict[str, str]:
    """
    Build flat env dict for dbt profiles.yml env_var() references.

    Phase 6: local warehouse from dbt_dental_models/.env_local;
    clinic RDS from deployment_credentials.json (dbt_dental_models/.env_clinic deprecated).
    """
    if stage not in ("local", "clinic", "demo"):
        raise ValueError(
            f"Unsupported dbt stage '{stage}'. Expected one of: local, clinic, demo"
        )

    env: dict[str, str] = {
        "DBT_TARGET": stage,
        "DBT_PROFILES_DIR": str(paths.DBT_DIR),
    }

    if stage == "local":
        env.update(load_local_warehouse_postgres_env_dict())

    elif stage == "clinic":
        merged: dict[str, str] = {}
        source = str(DEPLOYMENT_CREDENTIALS)
        deprecated_file = paths.dbt_env_file("clinic")
        if deprecated_file.exists():
            deprecated_vals = _read_env_file(deprecated_file)
            if deprecated_vals:
                import warnings

                warnings.warn(
                    f"{deprecated_file.name} is deprecated for clinic dbt; "
                    f"use {DEPLOYMENT_CREDENTIALS.name} clinic_database.postgresql.",
                    DeprecationWarning,
                    stacklevel=2,
                )
        try:
            merged.update(load_clinic_postgres_env_dict(default_schema="dbt"))
        except ValueError:
            file_vals = _read_env_file(deprecated_file)
            if not file_vals:
                raise
            merged.update(file_vals)
            source = str(deprecated_file)
        env.update(_overlay_os_env(merged))
        _require_keys(env, LOCAL_CLINIC_REQUIRED, source)

    else:  # demo
        creds = _demo_from_credentials(_load_credentials_json())
        if not creds:
            raise ValueError(
                f"demo_database.postgresql not found in {DEPLOYMENT_CREDENTIALS.name}."
            )
        env.update(_overlay_os_env(creds))
        _require_keys(env, DEMO_REQUIRED, str(DEPLOYMENT_CREDENTIALS))

    return env


def validate_dbt_stage(stage: str) -> tuple[bool, Optional[str]]:
    try:
        load_dbt_env_dict(stage)
        return True, None
    except ValueError as exc:
        return False, str(exc)
