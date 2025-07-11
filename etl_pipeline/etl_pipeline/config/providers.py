"""
Configuration Provider Pattern for ETL Pipeline

This file provides the configuration provider pattern for dependency injection,
supporting separate environment files (.env_production, .env_test) and fail-fast
validation for missing environment variables.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pathlib import Path
import os
import yaml
import logging
from dotenv import load_dotenv

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
                "Please set ETL_ENVIRONMENT to either 'production' or 'test'. "
                "No defaulting to production is allowed for security reasons."
            )
        valid_environments = ['production', 'test']
        if environment not in valid_environments:
            raise ValueError(f"Invalid environment '{environment}'. Must be one of: {valid_environments}")
        return environment
    
    def _load_environment_file(self):
        """Load environment variables from appropriate .env file."""
        # Load the specific environment file (.env_production or .env_test)
        env_file = f".env_{self.environment}"  # .env_production or .env_test
        
        # Environment files are in the etl_pipeline root directory (same as config_dir)
        env_path = self.config_dir / env_file
        
        if env_path.exists():
            logger.info(f"Loading environment from {env_path}")
            try:
                # Load environment variables from the specific environment file
                load_dotenv(env_path, override=True)
                
                # Capture ONLY the environment variables that are relevant to our ETL pipeline
                # This avoids capturing system variables like CHOCOLATEYINSTALL, APPDATA, etc.
                self._env_vars = {}
                
                # Define the environment variable prefixes we care about
                env_prefixes = {
                    'production': ['OPENDENTAL_SOURCE_', 'MYSQL_REPLICATION_', 'POSTGRES_ANALYTICS_'],
                    'test': ['TEST_OPENDENTAL_SOURCE_', 'TEST_MYSQL_REPLICATION_', 'TEST_POSTGRES_ANALYTICS_']
                }
                
                # Capture only variables with our prefixes
                prefixes = env_prefixes.get(self.environment, [])
                for key, value in os.environ.items():
                    for prefix in prefixes:
                        if key.startswith(prefix):
                            self._env_vars[key] = value
                            break
                
                # Also capture ETL_ENVIRONMENT
                if 'ETL_ENVIRONMENT' in os.environ:
                    self._env_vars['ETL_ENVIRONMENT'] = os.environ['ETL_ENVIRONMENT']
                    
            except Exception as e:
                logger.error(f"Failed to load {env_file}: {e}")
                raise ValueError(f"Failed to load environment file {env_file}: {e}")
        else:
            raise ValueError(f"Environment file {env_file} not found. Please create {env_path}")
    
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