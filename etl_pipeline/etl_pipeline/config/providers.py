"""
Configuration Provider Pattern for ETL Pipeline

This file provides the configuration provider pattern for dependency injection,
supporting separate environment files (.env_local, .env_clinic, .env_test) and fail-fast
validation for missing environment variables.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path
import os
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
        self._env_vars: Dict[str, Any] = {}
        self._load_environment_file()
    
    def _detect_environment(self) -> str:
        """Detect environment from environment variables."""
        environment = os.getenv('ETL_ENVIRONMENT')
        if not environment:
            raise ValueError(
                "ETL_ENVIRONMENT environment variable is not set. "
                "This is a critical security requirement. "
                "Please set ETL_ENVIRONMENT to 'local', 'clinic', or 'test'. "
                "No defaulting is allowed for security reasons."
            )
        valid_environments = ['local', 'clinic', 'test']
        if environment not in valid_environments:
            # Special error message for deprecated "production" environment
            if environment == "production":
                raise ValueError(
                    f"Invalid environment '{environment}'. "
                    "'production' has been removed. Use 'local' for localhost or 'clinic' for clinic. "
                    f"Valid environments: {valid_environments}"
                )
            raise ValueError(f"Invalid environment '{environment}'. Must be one of: {valid_environments}")
        return environment
    
    def _load_environment_file(self):
        """Load environment variables via pydantic-settings (settings_v2.py)."""
        env_file = f".env_{self.environment}"
        try:
            from .settings_v2 import load_etl_env_dict

            self._env_vars = load_etl_env_dict(
                environment=self.environment,
                config_dir=self.config_dir,
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
    
    def __init__(self, environment: Optional[str] = None, **configs):
        """Initialize with configuration dictionaries."""
        self.environment = environment
        self._env_vars = configs.get('env', {})
        self.configs = {
            'pipeline': configs.get('pipeline', {}),
            'tables': configs.get('tables', {'tables': {}}),
            'env': self._env_vars
        }
    
    def get_config(self, config_type: str) -> Dict[str, Any]:
        """Get configuration by type."""
        return self.configs.get(config_type, {})