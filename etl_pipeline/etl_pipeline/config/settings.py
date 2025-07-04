"""
Step 1.2: Replace etl_pipeline/config/settings.py

BACKUP YOUR EXISTING FILE FIRST!
cp etl_pipeline/config/settings.py etl_pipeline/config/settings.py.backup

Then replace with this clean implementation.
"""

import os
import logging
from typing import Dict, Optional, List, Any
from pathlib import Path
from enum import Enum

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
    """Clean configuration settings manager."""
    
    # Environment variable mappings for each database type
    ENV_MAPPINGS = {
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
    }
    
    def __init__(self, environment: str = None, provider = None):
        """Initialize settings."""
        self.environment = environment or self._detect_environment()
        self.env_prefix = "TEST_" if self.environment == 'test' else ""
        
        # Provider setup
        if provider is None:
            from .providers import FileConfigProvider
            provider = FileConfigProvider(Path(__file__).parent)
        self.provider = provider
        
        # Load configurations
        self.pipeline_config = self.provider.get_config('pipeline')
        self.tables_config = self.provider.get_config('tables') 
        self._env_vars = self.provider.get_config('env')
        
        # Cache
        self._connection_cache = {}
    
    @staticmethod
    def _detect_environment() -> str:
        """Detect environment from environment variables."""
        environment = (
            os.getenv('ETL_ENVIRONMENT') or
            os.getenv('ENVIRONMENT') or
            os.getenv('APP_ENV') or
            'production'
        )
        
        valid_environments = ['production', 'test', 'development']
        if environment not in valid_environments:
            logger.warning(f"Invalid environment '{environment}', using 'production'")
            environment = 'production'
        
        return environment
    
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
    
    def _get_base_config(self, db_type: DatabaseType) -> Dict:
        """Get base configuration from environment variables."""
        env_mapping = self.ENV_MAPPINGS[db_type]
        config = {}
        
        for key, env_var in env_mapping.items():
            # Try prefixed variable first, then base
            prefixed_var = f"{self.env_prefix}{env_var}"
            value = self._env_vars.get(prefixed_var) or self._env_vars.get(env_var)
            
            if key == 'port' and value:
                try:
                    value = int(value)
                except ValueError:
                    logger.warning(f"Invalid port value for {env_var}: {value}")
                    # Set default ports based on database type
                    if db_type in [DatabaseType.SOURCE, DatabaseType.REPLICATION]:
                        value = 3306  # MySQL default
                    else:
                        value = 5432  # PostgreSQL default
            
            config[key] = value
        
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
    
    def get_connection_string(self, 
                            db_type: DatabaseType,
                            schema: Optional[PostgresSchema] = None) -> str:
        """Get SQLAlchemy connection string."""
        config = self.get_database_config(db_type, schema)
        
        # Validate required fields
        required_fields = ['host', 'port', 'database', 'user', 'password']
        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required fields for {db_type.value}: {missing_fields}")
        
        if db_type in [DatabaseType.SOURCE, DatabaseType.REPLICATION]:
            # MySQL connection string
            return (
                f"mysql+pymysql://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}"
                f"?connect_timeout={config.get('connect_timeout', 10)}"
                f"&read_timeout={config.get('read_timeout', 30)}"
                f"&write_timeout={config.get('write_timeout', 30)}"
                f"&charset={config.get('charset', 'utf8mb4')}"
            )
        else:  # DatabaseType.ANALYTICS (PostgreSQL)
            conn_str = (
                f"postgresql+psycopg2://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}"
                f"?connect_timeout={config.get('connect_timeout', 10)}"
                f"&application_name={config.get('application_name', 'etl_pipeline')}"
            )
            
            # Add schema to search path if specified
            if config.get('schema'):
                conn_str += f"&options=-csearch_path%3D{config['schema']}"
            
            return conn_str
    
    def validate_configs(self) -> bool:
        """Validate that all required configurations are present."""
        missing_vars = []
        
        for db_type, env_mapping in self.ENV_MAPPINGS.items():
            for key, env_var in env_mapping.items():
                if key == 'schema':  # Schema is optional for PostgreSQL
                    continue
                
                # Check environment-specific variables
                prefixed_var = f"{self.env_prefix}{env_var}"
                value = self._env_vars.get(prefixed_var) or self._env_vars.get(env_var)
                
                if not value:
                    if self.environment == 'test':
                        missing_vars.append(f"{db_type.value}: {prefixed_var} or {env_var}")
                    else:
                        missing_vars.append(f"{db_type.value}: {env_var}")
        
        if missing_vars:
            logger.error(f"Missing required variables for {self.environment} environment:")
            for var in missing_vars:
                logger.error(f"  - {var}")
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
            'table_importance': 'standard',
            'estimated_size_mb': 0,
            'estimated_rows': 0
        }
    
    def list_tables(self) -> List[str]:
        """List all configured tables."""
        return list(self.tables_config.get('tables', {}).keys())
    
    def get_tables_by_importance(self, importance_level: str) -> List[str]:
        """Get tables by importance level."""
        tables = self.tables_config.get('tables', {})
        return [
            table_name for table_name, config in tables.items()
            if config.get('table_importance') == importance_level
        ]
    
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
def create_settings(environment: str = None, 
                   config_dir: Path = None,
                   **test_configs) -> Settings:
    """Create settings instance with flexible configuration."""
    if test_configs:
        from .providers import DictConfigProvider
        provider = DictConfigProvider(**test_configs)
    else:
        from .providers import FileConfigProvider
        if config_dir is None:
            config_dir = Path(__file__).parent
        provider = FileConfigProvider(config_dir)
    
    return Settings(environment=environment, provider=provider)

def create_test_settings(pipeline_config: Dict = None,
                        tables_config: Dict = None,
                        env_vars: Dict = None) -> Settings:
    """Create test settings with injected configuration."""
    return create_settings(
        environment='test',
        pipeline=pipeline_config or {},
        tables=tables_config or {'tables': {}},
        env=env_vars or {}
    )