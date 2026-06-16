"""
Typed ETL configuration via pydantic-settings (Phase 2).

Loads stage .env files and validates source, replication, and analytics connections.
OS environment variables win over on-disk .env_<stage> files (Phase 0 precedence).
"""

from __future__ import annotations

import logging
import os
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

STAGE_PREFIXES: dict[str, dict[str, str]] = {
    "local": {
        "source": "OPENDENTAL_SOURCE_",
        "replication": "MYSQL_REPLICATION_",
        "analytics": "POSTGRES_ANALYTICS_",
    },
    "clinic": {
        "source": "OPENDENTAL_SOURCE_",
        "replication": "MYSQL_REPLICATION_",
        "analytics": "POSTGRES_ANALYTICS_",
    },
    "test": {
        "source": "TEST_OPENDENTAL_SOURCE_",
        "replication": "TEST_MYSQL_REPLICATION_",
        "analytics": "TEST_POSTGRES_ANALYTICS_",
    },
}

HOST_ENV_KEYS: dict[str, str] = {
    "local": "POSTGRES_ANALYTICS_HOST",
    "clinic": "POSTGRES_ANALYTICS_HOST",
    "test": "TEST_POSTGRES_ANALYTICS_HOST",
}

# Prefixes scanned for supplemental vars (GLIC on local, etc.) — matches providers.py
ENV_SCAN_PREFIXES: dict[str, list[str]] = {
    "local": [
        "OPENDENTAL_SOURCE_",
        "GLIC_OPENDENTAL_SOURCE_",
        "MYSQL_REPLICATION_",
        "POSTGRES_ANALYTICS_",
    ],
    "clinic": [
        "OPENDENTAL_SOURCE_",
        "MYSQL_REPLICATION_",
        "POSTGRES_ANALYTICS_",
    ],
    "test": [
        "TEST_OPENDENTAL_SOURCE_",
        "TEST_MYSQL_REPLICATION_",
        "TEST_POSTGRES_ANALYTICS_",
    ],
}


class ETLStage(str, Enum):
    LOCAL = "local"
    CLINIC = "clinic"
    TEST = "test"


class MySQLConnSettings(BaseSettings):
    """OpenDental source or MySQL replication connection."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    host: str
    port: int = 3306
    db: str
    user: str
    password: SecretStr


class PostgresAnalyticsSettings(BaseSettings):
    """PostgreSQL analytics warehouse connection."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    host: str
    port: int = 5432
    db: str
    schema: str = "raw"
    user: str
    password: SecretStr


class ETLConnectionSettings:
    """Validated ETL database connections for one stage."""

    __slots__ = ("stage", "source", "replication", "analytics")

    def __init__(
        self,
        stage: ETLStage,
        source: MySQLConnSettings,
        replication: MySQLConnSettings,
        analytics: PostgresAnalyticsSettings,
    ):
        self.stage = stage
        self.source = source
        self.replication = replication
        self.analytics = analytics


def _detect_stage(explicit: Optional[str] = None) -> ETLStage:
    raw = explicit if explicit is not None else os.getenv("ETL_ENVIRONMENT")
    if not raw:
        raise ValueError(
            "ETL_ENVIRONMENT environment variable is not set. "
            "Must be one of: local, clinic, test"
        )
    if raw == "production":
        raise ValueError(
            f"Invalid environment '{raw}'. "
            "'production' has been removed. Use 'local' for localhost or 'clinic' for clinic. "
            "Valid environments: local, clinic, test"
        )
    if raw == "demo":
        raise ValueError(
            f"Invalid environment '{raw}'. "
            "The ETL pipeline does not run against the demo stage. "
            "Valid ETL environments: local, clinic, test"
        )
    try:
        return ETLStage(raw)
    except ValueError as exc:
        raise ValueError(
            f"Invalid environment '{raw}'. Must be one of: "
            f"{[s.value for s in ETLStage]}"
        ) from exc


def _env_path(config_dir: Path, stage: ETLStage) -> Path:
    return config_dir / f".env_{stage.value}"


def _env_file_for_pydantic(config_dir: Path, stage: ETLStage) -> Optional[Path]:
    """
    Return the stage .env file for pydantic-settings, or None when OS env is authoritative.

    When the analytics host var is already in the process environment, do not pass the
    on-disk file as a second authority (Phase 0 — same rule as api/settings.py).
    """
    host_key = HOST_ENV_KEYS[stage.value]
    if os.getenv(host_key):
        logger.info(
            "ETL DB config already present in OS environment (%s set); "
            "skipping on-disk .env_%s as pydantic env_file so it cannot shadow it.",
            host_key,
            stage.value,
        )
        return None

    env_path = _env_path(config_dir, stage)
    if env_path.exists():
        logger.info(
            "Loading ETL environment variables from: %s (OS env wins over file per field)",
            env_path,
        )
        return env_path

    logger.warning("ETL environment file not found: %s", env_path)
    logger.info("Using environment variables from system/os.environ")
    return None


def _conn_to_env(prefix: str, conn: MySQLConnSettings) -> dict[str, str]:
    return {
        f"{prefix}HOST": conn.host,
        f"{prefix}PORT": str(conn.port),
        f"{prefix}DB": conn.db,
        f"{prefix}USER": conn.user,
        f"{prefix}PASSWORD": conn.password.get_secret_value(),
    }


def _analytics_to_env(prefix: str, conn: PostgresAnalyticsSettings) -> dict[str, str]:
    result = _conn_to_env(prefix, conn)
    result[f"{prefix}SCHEMA"] = conn.schema
    return result


def etl_settings_to_env_dict(settings: ETLConnectionSettings) -> dict[str, str]:
    """Flatten typed settings to the env-var dict FileConfigProvider / Settings expect."""
    prefixes = STAGE_PREFIXES[settings.stage.value]
    env_dict: dict[str, str] = {}
    env_dict.update(_conn_to_env(prefixes["source"], settings.source))
    env_dict.update(_conn_to_env(prefixes["replication"], settings.replication))
    env_dict.update(_analytics_to_env(prefixes["analytics"], settings.analytics))
    return env_dict


def _supplement_env_dict(
    stage: str,
    env_dict: dict[str, str],
    env_path: Optional[Path],
) -> dict[str, str]:
    """Merge supplemental prefixed vars (e.g. GLIC_*) from OS env and file gaps."""
    prefixes = ENV_SCAN_PREFIXES[stage]
    file_vals: dict[str, Optional[str]] = {}
    if env_path and env_path.exists():
        try:
            from dotenv import dotenv_values

            file_vals = dotenv_values(env_path) or {}
        except Exception:
            file_vals = {}

    candidate_keys = set(os.environ.keys()) | set(file_vals.keys())
    for key in candidate_keys:
        if not any(key.startswith(p) for p in prefixes):
            continue
        os_val = os.environ.get(key)
        if os_val:
            env_dict[key] = os_val
        elif key not in env_dict and file_vals.get(key):
            env_dict[key] = str(file_vals[key])

    env_dict["ETL_ENVIRONMENT"] = os.getenv("ETL_ENVIRONMENT", stage)
    return env_dict


def load_etl_connection_settings(
    *,
    environment: Optional[str] = None,
    config_dir: Path,
) -> ETLConnectionSettings:
    """Load and validate typed ETL connection settings."""
    stage = _detect_stage(environment)
    env_path = _env_path(config_dir, stage)
    host_key = HOST_ENV_KEYS[stage.value]

    if not env_path.exists() and not os.getenv(host_key):
        raise ValueError(
            f"Environment file .env_{stage.value} not found. Please create {env_path}"
        )

    env_file = _env_file_for_pydantic(config_dir, stage)
    prefixes = STAGE_PREFIXES[stage.value]

    logger.info("Validating ETL environment: %s", stage.value)
    try:
        source = MySQLConnSettings(
            _env_file=env_file,
            _env_prefix=prefixes["source"],
        )
        replication = MySQLConnSettings(
            _env_file=env_file,
            _env_prefix=prefixes["replication"],
        )
        analytics = PostgresAnalyticsSettings(
            _env_file=env_file,
            _env_prefix=prefixes["analytics"],
        )
    except ValidationError as exc:
        raise ValueError(
            f"Invalid ETL configuration for {stage.value} environment: {exc}"
        ) from exc

    logger.info("ETL environment validation passed")
    return ETLConnectionSettings(stage, source, replication, analytics)


def load_etl_env_dict(
    *,
    environment: Optional[str] = None,
    config_dir: Path,
) -> dict[str, str]:
    """
    Load ETL env configuration as a flat dict for FileConfigProvider.get_config('env').

    This is the delegation entry point used by providers.py.
    """
    stage = _detect_stage(environment)
    settings = load_etl_connection_settings(
        environment=stage.value,
        config_dir=config_dir,
    )
    env_dict = etl_settings_to_env_dict(settings)
    env_path = _env_path(config_dir, stage)
    path_for_supplement = env_path if env_path.exists() else None
    return _supplement_env_dict(stage.value, env_dict, path_for_supplement)
