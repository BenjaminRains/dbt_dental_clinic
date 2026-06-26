"""Shared PostgreSQL warehouse env resolution (Phase 6 credential dedup)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from mdc_cli.credentials import _dig
from mdc_cli.paths import DEPLOYMENT_CREDENTIALS, dbt_env_file
from mdc_cli.secrets_manager import normalize_clinic_password_value

POSTGRES_ANALYTICS_PREFIX = "POSTGRES_ANALYTICS_"

POSTGRES_ANALYTICS_REQUIRED = (
    "POSTGRES_ANALYTICS_HOST",
    "POSTGRES_ANALYTICS_PORT",
    "POSTGRES_ANALYTICS_DB",
    "POSTGRES_ANALYTICS_USER",
    "POSTGRES_ANALYTICS_PASSWORD",
)


def read_env_file(path: Path) -> dict[str, str]:
    """Parse a dotenv file into a flat string dict (no OS overlay)."""
    if not path.exists():
        return {}
    try:
        from dotenv import dotenv_values

        raw = dotenv_values(path) or {}
    except Exception:
        return {}

    result: dict[str, str] = {}
    for key, value in raw.items():
        if not key or value is None:
            continue
        text = str(value).strip().strip('"').strip("'")
        if text:
            result[key] = text
    return result


def strip_prefix_keys(env: dict[str, str], prefix: str) -> dict[str, str]:
    """Remove keys starting with prefix (used to drop stale duplicated analytics creds)."""
    return {key: value for key, value in env.items() if not key.startswith(prefix)}


def overlay_os_env(base: dict[str, str], extra_prefixes: tuple[str, ...] = ()) -> dict[str, str]:
    """Non-blank OS env wins over file/JSON values (Phase 0 precedence)."""
    result = dict(base)
    for key in list(result.keys()):
        os_val = os.getenv(key)
        if os_val is not None and str(os_val).strip():
            result[key] = str(os_val).strip()

    scan_prefixes = (POSTGRES_ANALYTICS_PREFIX, "PGSSL", *extra_prefixes)
    for key, val in os.environ.items():
        if not val or not str(val).strip():
            continue
        if any(key.startswith(prefix) for prefix in scan_prefixes):
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


def clinic_postgres_node_to_env(
    pg: dict,
    *,
    default_schema: str = "raw",
) -> Optional[dict[str, str]]:
    host = pg.get("host")
    password_raw = pg.get("password")
    if not host or not password_raw:
        return None
    password = normalize_clinic_password_value(str(password_raw))
    if not password:
        return None
    user = pg.get("user") or pg.get("username") or "analytics_user"
    database = pg.get("database") or pg.get("dbname") or "opendental_analytics"
    return {
        "POSTGRES_ANALYTICS_HOST": str(host),
        "POSTGRES_ANALYTICS_PORT": str(pg.get("port", 5432)),
        "POSTGRES_ANALYTICS_DB": str(database),
        "POSTGRES_ANALYTICS_USER": str(user),
        "POSTGRES_ANALYTICS_PASSWORD": password,
        "POSTGRES_ANALYTICS_SCHEMA": str(pg.get("schema", default_schema)),
        "POSTGRES_ANALYTICS_SSLMODE": str(pg.get("sslmode", "require")),
    }


def clinic_postgres_from_credentials(
    data: dict,
    *,
    default_schema: str = "raw",
) -> Optional[dict[str, str]]:
    """
    Resolve clinic RDS connection from deployment_credentials.json.

    Precedence:
      1. clinic_database.postgresql (canonical, Phase 6)
      2. backend_api.clinic_database_reference.rds.database_connections.opendental_analytics
      3. backend_api.clinic_database_reference.rds.credentials.secrets.opendental_analytics.current_value
    """
    candidates = [
        (data.get("clinic_database") or {}).get("postgresql"),
        _dig(
            data,
            "backend_api",
            "clinic_database_reference",
            "rds",
            "database_connections",
            "opendental_analytics",
        ),
        _dig(
            data,
            "backend_api",
            "clinic_database_reference",
            "rds",
            "credentials",
            "secrets",
            "opendental_analytics",
            "current_value",
        ),
    ]
    for pg in candidates:
        if isinstance(pg, dict):
            env = clinic_postgres_node_to_env(pg, default_schema=default_schema)
            if env:
                return env
    return None


def load_clinic_postgres_env_dict(*, default_schema: str = "raw") -> dict[str, str]:
    """Clinic RDS metadata from deployment_credentials.json (password may still be stale)."""
    creds = clinic_postgres_from_credentials(
        _load_credentials_json(),
        default_schema=default_schema,
    )
    if not creds:
        raise ValueError(
            f"No clinic PostgreSQL config in {DEPLOYMENT_CREDENTIALS.name}. "
            "Add clinic_database.postgresql (or backend_api.clinic_database_reference RDS sections)."
        )
    return overlay_os_env(creds)


def load_local_warehouse_postgres_env_dict() -> dict[str, str]:
    """Local analytics warehouse from dbt_dental_models/.env_local (Phase 6 authority)."""
    file_path = dbt_env_file("local")
    file_vals = read_env_file(file_path)
    if not file_vals:
        raise ValueError(
            f"No local warehouse env found. Create {file_path} "
            "(see dbt_dental_models/.env_local.template)."
        )
    merged = overlay_os_env(file_vals)
    missing = [key for key in POSTGRES_ANALYTICS_REQUIRED if not (merged.get(key) or "").strip()]
    if missing:
        sample = ", ".join(missing[:4])
        suffix = "..." if len(missing) > 4 else ""
        raise ValueError(
            f"Missing required vars ({sample}{suffix}) from {file_path}."
        )
    merged.setdefault("POSTGRES_ANALYTICS_SCHEMA", "raw")
    merged.setdefault("POSTGRES_ANALYTICS_SSLMODE", "prefer")
    merged.setdefault("PGSSLMODE", merged["POSTGRES_ANALYTICS_SSLMODE"])
    return merged


def deprecated_etl_analytics_keys(stage: str) -> list[str]:
    """POSTGRES_ANALYTICS_* keys that should be removed from etl_pipeline/.env_<stage> (Phase 6)."""
    from mdc_cli.paths import etl_env_file

    if stage not in ("local", "clinic"):
        return []
    file_vals = read_env_file(etl_env_file(stage))
    return sorted(key for key in file_vals if key.startswith(POSTGRES_ANALYTICS_PREFIX))


def apply_postgres_ssl_env(env: dict[str, str]) -> dict[str, str]:
    """Mirror POSTGRES_ANALYTICS_SSLMODE into PGSSLMODE for libpq/psycopg2."""
    merged = dict(env)
    sslmode = (merged.get("POSTGRES_ANALYTICS_SSLMODE") or merged.get("PGSSLMODE") or "").strip()
    if sslmode:
        merged.setdefault("PGSSLMODE", sslmode)
    return merged
