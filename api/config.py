"""
API Environment Configuration

Backward-compatible facade over api/settings.py (pydantic-settings, Phase 2).
Callers keep using APIConfig, get_config(), Environment, and DatabaseType unchanged.
"""

import logging
import os
from enum import Enum
from typing import Dict, Optional
from urllib.parse import quote

from settings import get_settings, load_api_settings, reset_settings

logger = logging.getLogger(__name__)


class Environment(Enum):
    """Supported API environments."""
    TEST = "test"
    DEMO = "demo"
    CLINIC = "clinic"
    LOCAL = "local"


class DatabaseType(Enum):
    """Database types for API connections."""
    ANALYTICS = "analytics"


class APIConfig:
    """API configuration manager — delegates to typed pydantic-settings."""

    # Preserved for tests/docs that reference prefix maps (settings.py owns loading).
    ENV_MAPPINGS = {
        Environment.TEST.value: {
            DatabaseType.ANALYTICS.value: {
                "host": "TEST_POSTGRES_ANALYTICS_HOST",
                "port": "TEST_POSTGRES_ANALYTICS_PORT",
                "database": "TEST_POSTGRES_ANALYTICS_DB",
                "user": "TEST_POSTGRES_ANALYTICS_USER",
                "password": "TEST_POSTGRES_ANALYTICS_PASSWORD",
            }
        },
        Environment.DEMO.value: {
            DatabaseType.ANALYTICS.value: {
                "host": "DEMO_POSTGRES_HOST",
                "port": "DEMO_POSTGRES_PORT",
                "database": "DEMO_POSTGRES_DB",
                "user": "DEMO_POSTGRES_USER",
                "password": "DEMO_POSTGRES_PASSWORD",
            }
        },
        Environment.CLINIC.value: {
            DatabaseType.ANALYTICS.value: {
                "host": "POSTGRES_ANALYTICS_HOST",
                "port": "POSTGRES_ANALYTICS_PORT",
                "database": "POSTGRES_ANALYTICS_DB",
                "user": "POSTGRES_ANALYTICS_USER",
                "password": "POSTGRES_ANALYTICS_PASSWORD",
            }
        },
        Environment.LOCAL.value: {
            DatabaseType.ANALYTICS.value: {
                "host": "POSTGRES_ANALYTICS_HOST",
                "port": "POSTGRES_ANALYTICS_PORT",
                "database": "POSTGRES_ANALYTICS_DB",
                "user": "POSTGRES_ANALYTICS_USER",
                "password": "POSTGRES_ANALYTICS_PASSWORD",
            }
        },
    }

    REQUIRED_VARS = {
        DatabaseType.ANALYTICS.value: ["host", "port", "database", "user", "password"]
    }

    def __init__(self, environment: Optional[str] = None):
        self._settings = load_api_settings(environment=environment)
        self.environment = self._settings.stage.value

    def get_database_config(
        self, db_type: DatabaseType = DatabaseType.ANALYTICS
    ) -> Dict[str, object]:
        if db_type != DatabaseType.ANALYTICS:
            raise ValueError(f"Unsupported database type: {db_type}")
        db = self._settings.analytics
        return {
            "host": db.host,
            "port": db.port,
            "database": db.db,
            "user": db.user,
            "password": db.password.get_secret_value(),
        }

    def get_database_url(self, db_type: DatabaseType = DatabaseType.ANALYTICS) -> str:
        config = self.get_database_config(db_type)
        host = (config["host"] or "").lower()
        user_enc = quote(str(config["user"]), safe="")
        password_enc = quote(str(config["password"]), safe="")
        url = (
            f"postgresql://{user_enc}:{password_enc}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
        explicit = self._settings.sslmode_env
        if explicit:
            sslmode = explicit
        elif "rds.amazonaws.com" in host:
            sslmode = "require"
        else:
            sslmode = "prefer"
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}sslmode={sslmode}"

    def get_api_setting(self, key: str, default=None):
        if not key:
            return default
        if key.startswith("api."):
            env_key = key.upper().replace(".", "_")
            return os.getenv(env_key, default)
        return os.getenv(key.upper(), default)

    def get_cors_origins(self) -> list:
        origins_str = self._settings.runtime.cors_origins
        if not origins_str and self.get_api_setting("api.cors_origins"):
            origins_str = self.get_api_setting("api.cors_origins")
        return [origin.strip() for origin in (origins_str or "").split(",") if origin.strip()]

    def is_debug_mode(self) -> bool:
        return self._settings.runtime.debug

    def get_log_level(self) -> str:
        return self._settings.runtime.log_level.upper()

    def get_port(self) -> int:
        return self._settings.runtime.port

    def get_host(self) -> str:
        return self._settings.runtime.host


_global_config: Optional[APIConfig] = None


def get_config() -> APIConfig:
    global _global_config
    if _global_config is None:
        _global_config = APIConfig()
    return _global_config


def reset_config() -> None:
    global _global_config
    reset_settings()
    _global_config = None


def set_config(config: APIConfig) -> None:
    global _global_config
    _global_config = config
