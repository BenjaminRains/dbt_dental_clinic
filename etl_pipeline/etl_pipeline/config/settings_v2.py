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

from pydantic import Field, SecretStr, ValidationError
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
from typing import Any, Tuple, Type


class IgnoreBlankEnvSettingsSource(PydanticBaseSettingsSource):
    """Drop blank env / dotenv values so on-disk defaults are not overridden by stale shell blanks."""

    def __init__(self, settings_cls: Type[BaseSettings], source: PydanticBaseSettingsSource):
        super().__init__(settings_cls)
        self.source = source

    def get_field_value(self, field, field_name):
        return self.source.get_field_value(field, field_name)

    def prepare_field_value(self, field_name, field, value, value_is_complex):
        return self.source.prepare_field_value(field_name, field, value, value_is_complex)

    def __call__(self) -> dict[str, Any]:
        data = self.source()
        return {
            key: value
            for key, value in data.items()
            if not (isinstance(value, str) and not value.strip())
        }

    @property
    def field_mapping(self):
        return self.source.field_mapping


class BlankEnvAwareSettings(BaseSettings):
    """Base settings that ignore empty-string env values (common in .env files and shells)."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            IgnoreBlankEnvSettingsSource(settings_cls, env_settings),
            IgnoreBlankEnvSettingsSource(settings_cls, dotenv_settings),
            file_secret_settings,
        )

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


class ETLProfile(str, Enum):
    """Subset of connections validated for a given workflow (Phase 3.5)."""

    LOAD = "load"  # replication + analytics only
    FULL = "full"  # source + replication + analytics


class MySQLConnSettings(BlankEnvAwareSettings):
    """OpenDental source or MySQL replication connection."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    host: str
    port: int = 3306
    db: str
    user: str
    password: SecretStr


class PostgresAnalyticsSettings(BlankEnvAwareSettings):
    """PostgreSQL analytics warehouse connection."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    host: str
    port: int = 5432
    db: str
    db_schema: str = Field(default="raw", validation_alias="SCHEMA")
    user: str
    password: SecretStr


class ETLConnectionSettings:
    """Validated ETL database connections for one stage."""

    __slots__ = ("stage", "profile", "source", "replication", "analytics")

    def __init__(
        self,
        stage: ETLStage,
        source: Optional[MySQLConnSettings],
        replication: MySQLConnSettings,
        analytics: PostgresAnalyticsSettings,
        profile: ETLProfile = ETLProfile.FULL,
    ):
        self.stage = stage
        self.profile = profile
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


def resolve_etl_profile(
    stage: ETLStage,
    explicit: Optional[str] = None,
) -> ETLProfile:
    """
    Resolve which connection subset to validate.

    Defaults: local → load; clinic/test → full. Override via explicit arg or ETL_PROFILE.
    """
    raw = explicit if explicit is not None else os.getenv("ETL_PROFILE")
    if raw:
        try:
            return ETLProfile(raw.strip().lower())
        except ValueError as exc:
            raise ValueError(
                f"Invalid ETL profile '{raw}'. Must be one of: "
                f"{[p.value for p in ETLProfile]}"
            ) from exc
    if stage == ETLStage.LOCAL:
        return ETLProfile.LOAD
    return ETLProfile.FULL


def _missing_env_hint(
    stage: ETLStage,
    env_path: Path,
    prefixes: dict[str, str],
    profile: ETLProfile,
) -> str:
    """Actionable hint when required keys are absent from the stage file."""
    if not env_path.exists():
        return f" Create {env_path} from {env_path}.template."
    try:
        from dotenv import dotenv_values

        file_vals = dotenv_values(env_path) or {}
    except Exception:
        return ""

    roles = (
        ("replication", "analytics")
        if profile == ETLProfile.LOAD
        else ("source", "replication", "analytics")
    )
    missing: list[str] = []
    for role in roles:
        prefix = prefixes[role]
        for suffix in ("HOST", "DB", "USER", "PASSWORD"):
            key = f"{prefix}{suffix}"
            if not (file_vals.get(key) or "").strip() and not (os.getenv(key) or "").strip():
                missing.append(key)
    if not missing:
        return ""
    sample = ", ".join(missing[:4])
    suffix = "..." if len(missing) > 4 else ""
    return (
        f" Missing required vars ({sample}{suffix}). "
        f"Update {env_path.name} - see {env_path.name}.template."
    )


def _analytics_env_complete_in_os(host_key: str, prefix: str) -> bool:
    """Phase 0: OS env is authoritative only when analytics creds are fully present (non-blank)."""
    host = (os.getenv(host_key) or "").strip()
    if not host:
        return False
    for suffix in ("DB", "USER", "PASSWORD"):
        if not (os.getenv(f"{prefix}{suffix}") or "").strip():
            return False
    return True


def _env_file_for_pydantic(config_dir: Path, stage: ETLStage) -> Optional[Path]:
    """
    Return the stage .env file for pydantic-settings, or None when OS env is authoritative.

    When the analytics host var is already in the process environment, do not pass the
    on-disk file as a second authority (Phase 0 — same rule as api/settings.py).
    """
    host_key = HOST_ENV_KEYS[stage.value]
    prefix = STAGE_PREFIXES[stage.value]["analytics"]
    if _analytics_env_complete_in_os(host_key, prefix):
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
    result[f"{prefix}SCHEMA"] = conn.db_schema
    return result


def etl_settings_to_env_dict(settings: ETLConnectionSettings) -> dict[str, str]:
    """Flatten typed settings to the env-var dict FileConfigProvider / Settings expect."""
    prefixes = STAGE_PREFIXES[settings.stage.value]
    env_dict: dict[str, str] = {"ETL_PROFILE": settings.profile.value}
    if settings.source is not None:
        env_dict.update(_conn_to_env(prefixes["source"], settings.source))
    env_dict.update(_conn_to_env(prefixes["replication"], settings.replication))
    env_dict.update(_analytics_to_env(prefixes["analytics"], settings.analytics))
    return env_dict


def connection_config_dict(settings: ETLConnectionSettings, role: str) -> dict[str, object]:
    """
    Map typed connection settings to the dict shape Settings.get_database_config() expects.

    role: 'source' | 'replication' | 'analytics'
    """
    if role == "source":
        if settings.source is None:
            raise ValueError(
                f"OpenDental source connection is not configured for "
                f"{settings.stage.value} environment "
                f"(ETL profile '{settings.profile.value}' validates replication + "
                f"analytics only). Add OPENDENTAL_SOURCE_* to "
                f".env_{settings.stage.value} or use profile 'full'."
            )
        conn = settings.source
    elif role == "replication":
        conn = settings.replication
    elif role == "analytics":
        conn = settings.analytics
        return {
            "host": conn.host,
            "port": conn.port,
            "database": conn.db,
            "schema": conn.db_schema,
            "user": conn.user,
            "password": conn.password.get_secret_value(),
        }
    else:
        raise ValueError(f"Unknown connection role: {role}")

    return {
        "host": conn.host,
        "port": conn.port,
        "database": conn.db,
        "user": conn.user,
        "password": conn.password.get_secret_value(),
    }


def supplement_env_dict(
    stage: str,
    env_dict: dict[str, str],
    env_path: Optional[Path],
) -> dict[str, str]:
    """Public wrapper for supplemental prefixed env vars (GLIC_*, etc.)."""
    return _supplement_env_dict(stage, env_dict, env_path)


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


def _try_load_source(
    env_file: Optional[Path],
    prefix: str,
    *,
    required: bool,
) -> Optional[MySQLConnSettings]:
    """Load source connection; optional when profile is load."""
    try:
        return MySQLConnSettings(
            _env_file=env_file,
            _env_prefix=prefix,
        )
    except ValidationError:
        if required:
            raise
        return None


def load_etl_connection_settings(
    *,
    environment: Optional[str] = None,
    config_dir: Path,
    profile: Optional[str] = None,
) -> ETLConnectionSettings:
    """Load and validate typed ETL connection settings."""
    stage = _detect_stage(environment)
    resolved_profile = resolve_etl_profile(stage, profile)
    env_path = _env_path(config_dir, stage)
    host_key = HOST_ENV_KEYS[stage.value]
    prefix = STAGE_PREFIXES[stage.value]["analytics"]
    if not env_path.exists() and not _analytics_env_complete_in_os(host_key, prefix):
        raise ValueError(
            f"Environment file .env_{stage.value} not found. Please create {env_path}"
        )

    env_file = _env_file_for_pydantic(config_dir, stage)
    prefixes = STAGE_PREFIXES[stage.value]

    logger.info(
        "Validating ETL environment: %s (profile=%s)",
        stage.value,
        resolved_profile.value,
    )
    source_required = resolved_profile == ETLProfile.FULL
    try:
        source = _try_load_source(
            env_file,
            prefixes["source"],
            required=source_required,
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
        hint = _missing_env_hint(stage, env_path, prefixes, resolved_profile)
        raise ValueError(
            f"Invalid ETL configuration for {stage.value} environment "
            f"(profile={resolved_profile.value}): {exc}{hint}"
        ) from exc

    if source is None and resolved_profile == ETLProfile.LOAD:
        logger.info(
            "ETL profile 'load': OPENDENTAL_SOURCE_* not configured (optional for this profile)"
        )

    logger.info("ETL environment validation passed")
    return ETLConnectionSettings(
        stage,
        source,
        replication,
        analytics,
        profile=resolved_profile,
    )


def load_etl_env_dict(
    *,
    environment: Optional[str] = None,
    config_dir: Path,
    connection_settings: Optional[ETLConnectionSettings] = None,
    profile: Optional[str] = None,
) -> dict[str, str]:
    """
    Load ETL env configuration as a flat dict for FileConfigProvider.get_config('env').

    This is the delegation entry point used by providers.py and export_env_for_shell.py.
    """
    settings = connection_settings or load_etl_connection_settings(
        environment=environment,
        config_dir=config_dir,
        profile=profile,
    )
    stage = settings.stage
    env_dict = etl_settings_to_env_dict(settings)
    env_path = _env_path(config_dir, stage)
    path_for_supplement = env_path if env_path.exists() else None
    return _supplement_env_dict(stage.value, env_dict, path_for_supplement)
