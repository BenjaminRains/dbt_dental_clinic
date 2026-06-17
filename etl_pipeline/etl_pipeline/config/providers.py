"""
Configuration Provider Pattern for ETL Pipeline

This file provides the configuration provider pattern for dependency injection,
supporting separate environment files (.env_local, .env_clinic, .env_test) and fail-fast
validation for missing environment variables.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path
import yaml
import logging

logger = logging.getLogger(__name__)


class ConfigProvider(ABC):
    """Abstract configuration provider interface."""
    
    @abstractmethod
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """Get configuration by type (pipeline, tables, env)."""
        pass


class FileConfigProvider(ConfigProvider):
    """File-based configuration provider with environment file support."""

    def __init__(self, config_dir: Path, environment: Optional[str] = None):
        self.config_dir = Path(config_dir)
        self.environment = environment or self._detect_environment()
        self._connection_settings = None
        self._env_vars: Dict[str, Any] = {}
        self._load_environment_file()
    
    def _detect_environment(self) -> str:
        """Detect environment from environment variables via settings_v2 authority."""
        from .settings_v2 import resolve_etl_stage

        return resolve_etl_stage().value
    
    def _load_environment_file(self):
        """Load environment variables via pydantic-settings (settings_v2.py)."""
        env_file = f".env_{self.environment}"
        try:
            from .settings_v2 import load_etl_connection_settings, load_etl_env_dict

            self._connection_settings = load_etl_connection_settings(
                environment=self.environment,
                config_dir=self.config_dir,
                profile="full",
            )
            self._env_vars = load_etl_env_dict(
                environment=self.environment,
                config_dir=self.config_dir,
                connection_settings=self._connection_settings,
            )
        except ValueError:
            raise
        except Exception as e:
            logger.error("Failed to load %s: %s", env_file, e)
            raise ValueError(f"Failed to load environment file {env_file}: {e}") from e
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """Load configuration from files or environment."""
        if config_type == 'pipeline':
            return self._load_yaml_config('pipeline')
        elif config_type == 'tables':
            return self._load_yaml_config('tables')
        elif config_type == 'env':
            return self._env_vars
        else:
            raise ValueError(f"Unknown config type: {config_type}")
    
    def _load_yaml_config(self, name: str) -> Dict[str, Any]:
        """Load YAML configuration file."""
        # Config files (pipeline.yml, tables.yml) are in etl_pipeline/etl_pipeline/config/
        # while .env files are in etl_pipeline/
        config_dir = self.config_dir / 'etl_pipeline' / 'config'
        
        for ext in ['.yml', '.yaml']:
            config_path = config_dir / f"{name}{ext}"
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        config = yaml.safe_load(f)
                        if config is None:
                            config = {}
                        logger.info(f"Loaded {name} config from {config_path}")
                        return config
                except Exception as e:
                    logger.error(f"Failed to load {name} config: {e}")
                    raise ValueError(f"Failed to load {name} config: {e}")
        
        logger.warning(f"No {name} config file found in {config_dir}")
        return {}


class DictConfigProvider(ConfigProvider):
    """Dictionary-based configuration provider for testing."""
    
    def __init__(
        self,
        environment: Optional[str] = None,
        profile: Optional[str] = None,
        **configs,
    ):
        """Initialize with configuration dictionaries."""
        self.environment = environment
        self._connection_settings = None
        raw_env = configs.get("env", {})
        self._env_vars = dict(raw_env)

        if raw_env:
            from .settings_v2 import (
                STAGE_PREFIXES,
                etl_settings_to_env_dict,
                load_etl_connection_settings_from_env,
            )

            stage_env = raw_env.get("ETL_ENVIRONMENT") or environment or "test"
            prefixes = STAGE_PREFIXES.get(stage_env, STAGE_PREFIXES["test"])
            analytics_host = raw_env.get(f"{prefixes['analytics']}HOST")
            if analytics_host and str(analytics_host).strip():
                try:
                    self._connection_settings = load_etl_connection_settings_from_env(
                        raw_env,
                        environment=stage_env,
                        profile=profile,
                    )
                except ValueError:
                    logger.debug(
                        "DictConfigProvider: skipping typed env validation "
                        "(incomplete injected env for %s)",
                        stage_env,
                    )

            if self._connection_settings is not None:
                self.environment = self._connection_settings.stage.value
                self._env_vars = {
                    **raw_env,
                    **etl_settings_to_env_dict(self._connection_settings),
                }
                self._env_vars.setdefault("ETL_ENVIRONMENT", self.environment)

        self.configs = {
            'pipeline': configs.get('pipeline', {}),
            'tables': configs.get('tables', {'tables': {}}),
            'env': self._env_vars
        }
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """Get configuration by type."""
        return self.configs.get(config_type, {})