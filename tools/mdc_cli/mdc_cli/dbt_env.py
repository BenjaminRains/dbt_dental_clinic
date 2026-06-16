"""Load dbt shell environment (mirrors dbt-init file/JSON rules, Phase 4.2b)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from mdc_cli import paths
from mdc_cli.paths import DEPLOYMENT_CREDENTIALS

LOCAL_CLINIC_REQUIRED = (
    "POSTGRES_ANALYTICS_HOST",
    "POSTGRES_ANALYTICS_PORT",
    "POSTGRES_ANALYTICS_DB",
    "POSTGRES_ANALYTICS_USER",
    "POSTGRES_ANALYTICS_PASSWORD",
)

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
    if not path.exists():
        return {}
    try:
        from dotenv import dotenv_values

        raw = dotenv_values(path) or {}
    except Exception:
        return {}

    result: dict[str, str] = {}
    for key, value in raw.items():
        if not key or key == "DBT_TARGET" or value is None:
            continue
        text = str(value).strip().strip('"').strip("'")
        if text:
            result[key] = text
    return result


def _overlay_os_env(base: dict[str, str]) -> dict[str, str]:
    """Phase 0: non-blank OS env wins over file/JSON values."""
    result = dict(base)
    for key in list(result.keys()):
        os_val = os.getenv(key)
        if os_val is not None and str(os_val).strip():
            result[key] = str(os_val).strip()

    for key, val in os.environ.items():
        if not val or not str(val).strip():
            continue
        if any(key.startswith(prefix) for prefix in DBT_ENV_PREFIXES):
            result[key] = str(val).strip()
    return result


def _load_credentials_json() -> dict:
    if not DEPLOYMENT_CREDENTIALS.exists():
        raise ValueError(
            f"deployment_credentials.json not found at {DEPLOYMENT_CREDENTIALS}. "
            "Copy deployment_credentials.json.template and fill in values."
        )
    try:
        return json.loads(DEPLOYMENT_CREDENTIALS.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {DEPLOYMENT_CREDENTIALS}: {exc}") from exc


def _clinic_from_credentials(data: dict) -> Optional[dict[str, str]]:
    pg = (data.get("clinic_database") or {}).get("postgresql")
    if not pg or not pg.get("host"):
        return None
    password = pg.get("password")
    if not password:
        return None
    return {
        "POSTGRES_ANALYTICS_HOST": str(pg["host"]),
        "POSTGRES_ANALYTICS_PORT": str(pg.get("port", 5432)),
        "POSTGRES_ANALYTICS_DB": str(pg.get("database", "opendental_analytics")),
        "POSTGRES_ANALYTICS_USER": str(pg.get("user", "analytics_user")),
        "POSTGRES_ANALYTICS_PASSWORD": str(password),
        "POSTGRES_ANALYTICS_SCHEMA": "dbt",
        "POSTGRES_ANALYTICS_SSLMODE": "require",
    }


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

    Matches dbt-init precedence: clinic JSON → .env_clinic; local .env_local;
    demo deployment_credentials.json with localhost:5434 for local port-forwarding.
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
        file_path = paths.dbt_env_file("local")
        file_vals = _read_env_file(file_path)
        if not file_vals:
            raise ValueError(
                f"No dbt local env found. Create {file_path} "
                "(see etl_pipeline/.env_local.template or docs/ENVIRONMENT_FILES.md)."
            )
        env.update(_overlay_os_env(file_vals))
        _require_keys(env, LOCAL_CLINIC_REQUIRED, str(file_path))

    elif stage == "clinic":
        merged: dict[str, str] = {}
        source = str(paths.dbt_env_file("clinic"))
        if DEPLOYMENT_CREDENTIALS.exists():
            try:
                creds = _clinic_from_credentials(_load_credentials_json())
                if creds:
                    merged.update(creds)
                    source = str(DEPLOYMENT_CREDENTIALS)
            except ValueError:
                raise
            except Exception:
                pass
        if not merged:
            file_path = paths.dbt_env_file("clinic")
            file_vals = _read_env_file(file_path)
            if not file_vals:
                raise ValueError(
                    f"No clinic dbt env found. Add clinic_database to "
                    f"{DEPLOYMENT_CREDENTIALS.name} or create {file_path}."
                )
            merged.update(file_vals)
            source = str(file_path)
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
