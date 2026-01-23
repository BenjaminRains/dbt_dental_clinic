"""
ETL Pipeline Settings Configuration

This file provides clean configuration management with fail-fast validation
and support for separate environment files (.env_production, .env_test).
"""

import os
import logging
from typing import Dict, Optional, List, Any
from pathlib import Path
from enum import Enum

# Import custom exceptions
from ..exceptions.configuration import EnvironmentError, ConfigurationError

logger = logging.getLogger(__name__)


class DatabaseType(Enum):
    """Supported database types."""
    SOURCE = "source"           # OpenDental MySQL (readonly)
    REPLICATION = "replication" # Local MySQL copy  
    ANALYTICS = "analytics"     # PostgreSQL warehouse


class PostgresSchema(Enum):
    """PostgreSQL schema types."""
    RAW = "raw"
    STAGING = "staging" 
    INTERMEDIATE = "intermediate"
    MARTS = "marts"


class Settings:
    """Clean configuration settings manager with fail-fast validation."""
    
    # Environment variable mappings for each database type and environment
    # These map to the actual variable names in the .env files
    ENV_MAPPINGS = {
        'clinic': {
            DatabaseType.SOURCE: {
                'host': 'OPENDENTAL_SOURCE_HOST',
                'port': 'OPENDENTAL_SOURCE_PORT',
                'database': 'OPENDENTAL_SOURCE_DB',
                'user': 'OPENDENTAL_SOURCE_USER',
                'password': 'OPENDENTAL_SOURCE_PASSWORD'
            },
            DatabaseType.REPLICATION: {
                'host': 'MYSQL_REPLICATION_HOST',
                'port': 'MYSQL_REPLICATION_PORT',
                'database': 'MYSQL_REPLICATION_DB',
                'user': 'MYSQL_REPLICATION_USER',
                'password': 'MYSQL_REPLICATION_PASSWORD'
            },
            DatabaseType.ANALYTICS: {
                'host': 'POSTGRES_ANALYTICS_HOST',
                'port': 'POSTGRES_ANALYTICS_PORT',
                'database': 'POSTGRES_ANALYTICS_DB',
                'schema': 'POSTGRES_ANALYTICS_SCHEMA',
                'user': 'POSTGRES_ANALYTICS_USER',
                'password': 'POSTGRES_ANALYTICS_PASSWORD'
            }
        },
        'test': {
            DatabaseType.SOURCE: {
                'host': 'TEST_OPENDENTAL_SOURCE_HOST',
                'port': 'TEST_OPENDENTAL_SOURCE_PORT',
                'database': 'TEST_OPENDENTAL_SOURCE_DB',
                'user': 'TEST_OPENDENTAL_SOURCE_USER',
                'password': 'TEST_OPENDENTAL_SOURCE_PASSWORD'
            },
            DatabaseType.REPLICATION: {
                'host': 'TEST_MYSQL_REPLICATION_HOST',
                'port': 'TEST_MYSQL_REPLICATION_PORT',
                'database': 'TEST_MYSQL_REPLICATION_DB',
                'user': 'TEST_MYSQL_REPLICATION_USER',
                'password': 'TEST_MYSQL_REPLICATION_PASSWORD'
            },
            DatabaseType.ANALYTICS: {
                'host': 'TEST_POSTGRES_ANALYTICS_HOST',
                'port': 'TEST_POSTGRES_ANALYTICS_PORT',
                'database': 'TEST_POSTGRES_ANALYTICS_DB',
                'schema': 'TEST_POSTGRES_ANALYTICS_SCHEMA',
                'user': 'TEST_POSTGRES_ANALYTICS_USER',
                'password': 'TEST_POSTGRES_ANALYTICS_PASSWORD'
            }
        }
    }
    
    # Required variable structure for each database type
    REQUIRED_VARS = {
        DatabaseType.SOURCE: ['host', 'port', 'database', 'user', 'password'],
        DatabaseType.REPLICATION: ['host', 'port', 'database', 'user', 'password'],
        DatabaseType.ANALYTICS: ['host', 'port', 'database', 'schema', 'user', 'password']
    }
    
    def __init__(self, environment: Optional[str] = None, provider = None):
        """Initialize settings with fail-fast validation."""
        self.environment = environment or self._detect_environment()
        
        # Provider setup with environment support
        if provider is None:
            from .providers import FileConfigProvider
            # Use the etl_pipeline/config directory for .env files and config files
            # settings.py is in etl_pipeline/etl_pipeline/config/settings.py
            # .env_test is in etl_pipeline/.env_test
            # pipeline.yml and tables.yml are in etl_pipeline/etl_pipeline/config/
            # So we need to go up 3 levels: config -> etl_pipeline -> etl_pipeline -> etl_pipeline
            config_dir = Path(__file__).parent.parent.parent  # etl_pipeline directory
            provider = FileConfigProvider(config_dir, self.environment)
        else:
            # Ensure provider knows the environment for mapping
            if hasattr(provider, 'environment'):
                provider.environment = self.environment
        
        self.provider = provider
        
        # Load configurations from environment-specific files
        self.pipeline_config = self.provider.get_config('pipeline')
        self.tables_config = self.provider.get_config('tables') 
        self._env_vars = self.provider.get_config('env')
        
        # Cache
        self._connection_cache = {}
        
        # Only validate environment if using FileConfigProvider (production/integration)
        # Skip validation for DictConfigProvider (testing) to allow injected configuration
        if provider is None or not hasattr(provider, 'configs'):
            self._validate_environment()
    
    @staticmethod
    def _detect_environment() -> str:
        """Detect environment from environment variables."""
        environment = os.getenv('ETL_ENVIRONMENT')
        if not environment:
            raise EnvironmentError(
                message="ETL_ENVIRONMENT environment variable is not set",
                environment="unknown",  # Indicate that environment detection failed
                missing_variables=["ETL_ENVIRONMENT"],
                details={"critical": True}
            )
        valid_environments = ['clinic', 'test']
        if environment not in valid_environments:
            # Special error message for deprecated "production" environment
            if environment == "production":
                raise ConfigurationError(
                    message=f"Invalid environment '{environment}'. "
                    f"'production' has been removed. Use 'clinic' for clinic deployment. "
                    f"Valid environments: {valid_environments}",
                    invalid_values={"ETL_ENVIRONMENT": environment},
                    details={"valid_environments": valid_environments}
                )
            raise ConfigurationError(
                message=f"Invalid environment '{environment}'. Must be one of: {valid_environments}",
                invalid_values={"ETL_ENVIRONMENT": environment},
                details={"valid_environments": valid_environments}
            )
        return environment
    
    def _validate_environment(self):
        """Fail-fast validation of environment configuration."""
        logger.info(f"Validating environment: {self.environment}")
        validation_result = self.validate_configs()
        logger.info(f"Validation result: {validation_result}")
        if not validation_result:
            logger.error("Validation failed, raising ConfigurationError")
            raise ConfigurationError(
                message=f"Configuration validation failed for {self.environment} environment.",
                config_file=f".env_{self.environment}",
                details={"environment": self.environment}
            )
    
    def get_database_config(self, 
                          db_type: DatabaseType, 
                          schema: Optional[PostgresSchema] = None) -> Dict:
        """Get database configuration for specified type and optional schema."""
        cache_key = f"{db_type.value}_{schema.value if schema else 'default'}"
        
        if cache_key in self._connection_cache:
            return self._connection_cache[cache_key]
        
        config = self._get_base_config(db_type)
        
        # Apply pipeline config overrides
        pipeline_connections = self.pipeline_config.get('connections', {})
        if db_type.value in pipeline_connections:
            config.update(pipeline_connections[db_type.value])
        
        # Add database-specific defaults
        self._add_connection_defaults(config, db_type)
        
        # Handle PostgreSQL schema
        if db_type == DatabaseType.ANALYTICS and schema:
            config['schema'] = schema.value
        
        self._connection_cache[cache_key] = config
        return config
    
    # ============================================================================
    # CONNECTION-SPECIFIC METHODS (Single Source of Truth)
    # ============================================================================
    
    def get_source_connection_config(self) -> Dict:
        """Get OpenDental source connection configuration."""
        return self.get_database_config(DatabaseType.SOURCE)
    
    def get_replication_connection_config(self) -> Dict:
        """Get MySQL replication connection configuration."""
        return self.get_database_config(DatabaseType.REPLICATION)
    
    def get_analytics_connection_config(self, schema: PostgresSchema = PostgresSchema.RAW) -> Dict:
        """Get PostgreSQL analytics connection configuration."""
        return self.get_database_config(DatabaseType.ANALYTICS, schema)
    
    # Schema-specific analytics connections
    def get_analytics_raw_connection_config(self) -> Dict:
        """Get PostgreSQL analytics raw schema connection configuration."""
        return self.get_analytics_connection_config(PostgresSchema.RAW)
    
    def get_analytics_staging_connection_config(self) -> Dict:
        """Get PostgreSQL analytics staging schema connection configuration."""
        return self.get_analytics_connection_config(PostgresSchema.STAGING)
    
    def get_analytics_intermediate_connection_config(self) -> Dict:
        """Get PostgreSQL analytics intermediate schema connection configuration."""
        return self.get_analytics_connection_config(PostgresSchema.INTERMEDIATE)
    
    def get_analytics_marts_connection_config(self) -> Dict:
        """Get PostgreSQL analytics marts schema connection configuration."""
        return self.get_analytics_connection_config(PostgresSchema.MARTS)
    
    def _get_base_config(self, db_type: DatabaseType) -> Dict:
        """Get base configuration from environment variables using actual variable names."""
        env_mapping = self.ENV_MAPPINGS[self.environment][db_type]
        config = {}
        missing_keys = []
        invalid_values = {}
        
        for config_key, env_var in env_mapping.items():
            value = self._env_vars.get(env_var)
            if not value:
                missing_keys.append(env_var)
                continue
            if config_key == 'port' and value:
                try:
                    value = int(value)
                except ValueError:
                    invalid_values[env_var] = value
                    continue
            config[config_key] = value
        
        if missing_keys or invalid_values:
            raise ConfigurationError(
                message=f"Missing or invalid required environment variables for {db_type.value} database in {self.environment} environment.",
                config_file=f".env_{self.environment}",
                missing_keys=missing_keys if missing_keys else None,
                invalid_values=invalid_values if invalid_values else None,
                details={"db_type": db_type.value, "environment": self.environment}
            )
        return config
    
    def _add_connection_defaults(self, config: Dict, db_type: DatabaseType):
        """Add default connection parameters based on database type."""
        if db_type in [DatabaseType.SOURCE, DatabaseType.REPLICATION]:
            # MySQL defaults
            config.setdefault('connect_timeout', 10)
            config.setdefault('read_timeout', 30)
            config.setdefault('write_timeout', 30)
            config.setdefault('charset', 'utf8mb4')
        else:  # DatabaseType.ANALYTICS (PostgreSQL)
            config.setdefault('connect_timeout', 10)
            config.setdefault('application_name', 'etl_pipeline')
    
    def validate_configs(self) -> bool:
        """Validate that all required configurations are present."""
        missing_vars = []
        
        for db_type, env_mapping in self.ENV_MAPPINGS[self.environment].items():
            for env_var in env_mapping.values():
                # Use the variable name as defined in the loaded .env file
                value = self._env_vars.get(env_var)
                
                if not value:
                    missing_vars.append(f"{db_type.value}: {env_var}")
        
        if missing_vars:
            logger.error(f"Missing required variables for {self.environment} environment:")
            for var in missing_vars:
                logger.error(f"  - {var}")
            logger.error(f"Please check your .env_{self.environment} file")
            return False
        
        return True
    
    # Table configuration methods (keep existing functionality)
    def get_pipeline_setting(self, key: str, default=None):
        """Get pipeline setting with dot notation."""
        if not key:
            return default
        
        parts = key.split('.')
        value = self.pipeline_config
        
        for part in parts:
            if not isinstance(value, dict):
                return default
            value = value.get(part)
            if value is None:
                return default
        
        return value
    
    def get_table_config(self, table_name: str) -> Dict:
        """Get table configuration."""
        tables = self.tables_config.get('tables', {})
        config = tables.get(table_name, {})
        
        if not config:
            logger.warning(f"No configuration found for table {table_name}")
            return self._get_default_table_config()
        
        return config
    
    def _get_default_table_config(self) -> Dict:
        """Get default table configuration."""
        return {
            'incremental_column': None,
            'batch_size': 5000,
            'extraction_strategy': 'full_table',
            'estimated_size_mb': 0,
            'estimated_rows': 0
        }
    
    def list_tables(self) -> List[str]:
        """List all configured tables."""
        return list(self.tables_config.get('tables', {}).keys())
    
    def should_use_incremental(self, table_name: str) -> bool:
        """Check if table should use incremental loading."""
        config = self.get_table_config(table_name)
        return (config.get('extraction_strategy') == 'incremental' and 
                config.get('incremental_column') is not None)


# Global instance management
_global_settings = None

def get_settings() -> Settings:
    """Get global settings instance (lazy initialization)."""
    global _global_settings
    if _global_settings is None:
        _global_settings = Settings()
    return _global_settings

def reset_settings():
    """Reset global settings instance (for testing)."""
    global _global_settings
    _global_settings = None

def set_settings(settings: Settings):
    """Set global settings instance (for testing)."""
    global _global_settings
    _global_settings = settings


# Factory functions
def create_settings(environment: Optional[str] = None, 
                   config_dir: Optional[Path] = None,
                   **test_configs) -> Settings:
    """Create settings instance with flexible configuration."""
    if test_configs:
        from .providers import DictConfigProvider
        provider = DictConfigProvider(**test_configs)
    else:
        from .providers import FileConfigProvider
        if config_dir is None:
            # Point to etl_pipeline root directory where .env files are located
            config_dir = Path(__file__).parent.parent
        # Explicitly pass environment to ensure correct .env file loading
        provider = FileConfigProvider(config_dir, environment)
    
    return Settings(environment=environment, provider=provider)

def create_test_settings(pipeline_config: Optional[Dict] = None,
                        tables_config: Optional[Dict] = None,
                        env_vars: Optional[Dict] = None) -> Settings:
    """Create test settings with injected configuration."""
    return create_settings(
        environment='test',
        pipeline=pipeline_config or {},
        tables=tables_config or {'tables': {}},
        env=env_vars or {}
    )