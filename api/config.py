"""
API Environment Configuration

This file provides clean configuration management for the API following the
ETL pipeline's environment handling pattern with fail-fast validation.
"""

import os
import logging
from typing import Dict, Optional
from pathlib import Path
from enum import Enum
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class Environment(Enum):
    """Supported API environments."""
    TEST = "test"
    PRODUCTION = "production"
    LOCAL = "local"


class DatabaseType(Enum):
    """Database types for API connections."""
    ANALYTICS = "analytics"


class APIConfig:
    """API configuration manager with fail-fast validation."""
    
    # Environment variable mappings for each environment
    ENV_MAPPINGS = {
        Environment.TEST.value: {
            DatabaseType.ANALYTICS.value: {
                'host': 'TEST_POSTGRES_ANALYTICS_HOST',
                'port': 'TEST_POSTGRES_ANALYTICS_PORT',
                'database': 'TEST_POSTGRES_ANALYTICS_DB',
                'user': 'TEST_POSTGRES_ANALYTICS_USER',
                'password': 'TEST_POSTGRES_ANALYTICS_PASSWORD'
            }
        },
        Environment.PRODUCTION.value: {
            DatabaseType.ANALYTICS.value: {
                'host': 'POSTGRES_ANALYTICS_HOST',
                'port': 'POSTGRES_ANALYTICS_PORT',
                'database': 'POSTGRES_ANALYTICS_DB',
                'user': 'POSTGRES_ANALYTICS_USER',
                'password': 'POSTGRES_ANALYTICS_PASSWORD'
            }
        },
        Environment.LOCAL.value: {
            DatabaseType.ANALYTICS.value: {
                'host': 'POSTGRES_ANALYTICS_HOST',
                'port': 'POSTGRES_ANALYTICS_PORT',
                'database': 'POSTGRES_ANALYTICS_DB',
                'user': 'POSTGRES_ANALYTICS_USER',
                'password': 'POSTGRES_ANALYTICS_PASSWORD'
            }
        }
    }
    
    # Required variables for each database type
    REQUIRED_VARS = {
        DatabaseType.ANALYTICS.value: ['host', 'port', 'database', 'user', 'password']
    }
    
    def __init__(self, environment: Optional[str] = None):
        """Initialize API configuration with fail-fast validation."""
        self.environment = environment or self._detect_environment()
        self._load_environment_file()
        self._validate_environment()
    
    @staticmethod
    def _detect_environment() -> str:
        """Detect environment from environment variables."""
        environment = os.getenv('API_ENVIRONMENT')
        if not environment:
            raise ValueError(
                "API_ENVIRONMENT environment variable is not set. "
                "Must be one of: test, production, local"
            )
        
        valid_environments = [e.value for e in Environment]
        if environment not in valid_environments:
            raise ValueError(
                f"Invalid environment '{environment}'. "
                f"Must be one of: {valid_environments}"
            )
        
        return environment
    
    def _load_environment_file(self):
        """Load API-specific environment file."""
        # Find the project root directory (where .env files are located)
        current_dir = Path(__file__).parent  # api directory
        project_root = current_dir.parent    # project root
        
        # Use API-specific .env files
        env_file = project_root / f".env_api_{self.environment}"
        
        if env_file.exists():
            load_dotenv(env_file, override=True)
            logger.info(f"Loaded API environment variables from: {env_file}")
        else:
            logger.warning(f"API environment file not found: {env_file}")
            logger.info("Using environment variables from system/os.environ")
    
    def _validate_environment(self):
        """Fail-fast validation of environment configuration."""
        logger.info(f"Validating API environment: {self.environment}")
        
        for db_type, env_mapping in self.ENV_MAPPINGS[self.environment].items():
            missing_vars = []
            invalid_values = {}
            
            for config_key, env_var in env_mapping.items():
                value = os.getenv(env_var)
                if not value:
                    missing_vars.append(env_var)
                    continue
                
                # Validate port is numeric
                if config_key == 'port' and value:
                    try:
                        int(value)
                    except ValueError:
                        invalid_values[env_var] = value
            
            if missing_vars or invalid_values:
                error_msg = f"Missing or invalid required environment variables for {db_type} database in {self.environment} environment."
                if missing_vars:
                    error_msg += f" Missing: {missing_vars}"
                if invalid_values:
                    error_msg += f" Invalid: {invalid_values}"
                raise ValueError(error_msg)
        
        logger.info("Environment validation passed")
    
    def get_database_config(self, db_type: DatabaseType = DatabaseType.ANALYTICS) -> Dict[str, str]:
        """Get database configuration for specified type."""
        env_mapping = self.ENV_MAPPINGS[self.environment][db_type.value]
        config = {}
        
        for config_key, env_var in env_mapping.items():
            value = os.getenv(env_var)
            if not value:
                raise ValueError(f"Required environment variable {env_var} not set")
            
            # Convert port to integer
            if config_key == 'port':
                try:
                    value = int(value)
                except ValueError:
                    raise ValueError(f"Invalid port value in {env_var}: {value}")
            
            config[config_key] = value
        
        return config
    
    def get_database_url(self, db_type: DatabaseType = DatabaseType.ANALYTICS) -> str:
        """Get database URL for specified type."""
        config = self.get_database_config(db_type)
        
        return (
            f"postgresql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
    
    def get_api_setting(self, key: str, default=None):
        """Get API-specific setting with dot notation."""
        if not key:
            return default
        
        # Handle API-specific environment variables
        if key.startswith('api.'):
            env_key = key.upper().replace('.', '_')
            return os.getenv(env_key, default)
        
        # Handle direct environment variable lookup
        return os.getenv(key.upper(), default)
    
    def get_cors_origins(self) -> list:
        """Get CORS origins from environment."""
        origins_str = self.get_api_setting('api.cors_origins', 'http://localhost:3000')
        return [origin.strip() for origin in origins_str.split(',')]
    
    def is_debug_mode(self) -> bool:
        """Check if debug mode is enabled."""
        return self.get_api_setting('api.debug', 'false').lower() == 'true'
    
    def get_log_level(self) -> str:
        """Get log level for API."""
        return self.get_api_setting('api.log_level', 'info').upper()
    
    def get_port(self) -> int:
        """Get API port."""
        try:
            return int(self.get_api_setting('api.port', '8000'))
        except ValueError:
            return 8000
    
    def get_host(self) -> str:
        """Get API host."""
        return self.get_api_setting('api.host', '0.0.0.0')


# Global instance management
_global_config = None

def get_config() -> APIConfig:
    """Get global API configuration instance (lazy initialization)."""
    global _global_config
    if _global_config is None:
        _global_config = APIConfig()
    return _global_config

def reset_config():
    """Reset global configuration instance (for testing)."""
    global _global_config
    _global_config = None

def set_config(config: APIConfig):
    """Set global configuration instance (for testing)."""
    global _global_config
    _global_config = config
