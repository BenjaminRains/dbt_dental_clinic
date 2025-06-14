"""
Configuration settings for the ETL pipeline.
Consolidates database and pipeline configuration logic.
"""
import os
import logging
from typing import Dict, Optional
from pathlib import Path
from dotenv import load_dotenv
import yaml

# Use standard logging instead of importing from core
logger = logging.getLogger(__name__)

class Settings:
    """Configuration settings manager."""
    
    # Environment variable mappings
    ENV_MAPPINGS = {
        'source': {
            'host': 'SOURCE_MYSQL_HOST',
            'port': 'SOURCE_MYSQL_PORT',
            'database': 'SOURCE_MYSQL_DB',
            'user': 'SOURCE_MYSQL_USER',
            'password': 'SOURCE_MYSQL_PASSWORD'
        },
        'replication': {
            'host': 'REPLICATION_MYSQL_HOST',
            'port': 'REPLICATION_MYSQL_PORT',
            'database': 'REPLICATION_MYSQL_DB',
            'user': 'REPLICATION_MYSQL_USER',
            'password': 'REPLICATION_MYSQL_PASSWORD'
        },
        'analytics': {
            'host': 'ANALYTICS_POSTGRES_HOST',
            'port': 'ANALYTICS_POSTGRES_PORT',
            'database': 'ANALYTICS_POSTGRES_DB',
            'schema': 'ANALYTICS_POSTGRES_SCHEMA',
            'user': 'ANALYTICS_POSTGRES_USER',
            'password': 'ANALYTICS_POSTGRES_PASSWORD'
        }
    }
    
    def __init__(self):
        """Initialize settings and load configuration."""
        self.load_environment()
        self.pipeline_config = self.load_pipeline_config()
        self.tables_config = self.load_tables_config()
        self._connection_cache = {}
    
    @classmethod
    def load_environment(cls) -> None:
        """Load environment variables from .env files."""
        current_dir = Path(__file__).parent.parent
        
        # Try to load from etl_pipeline/.env first
        etl_env_path = current_dir / '.env'
        if etl_env_path.exists():
            logger.info(f"Loading environment from {etl_env_path}")
            load_dotenv(etl_env_path)
        else:
            # Fall back to parent directory .env
            parent_env_path = current_dir.parent / '.env'
            if parent_env_path.exists():
                logger.info(f"Loading environment from {parent_env_path}")
                load_dotenv(parent_env_path)
            else:
                logger.warning("No .env file found in etl_pipeline or parent directory")
    
    def load_pipeline_config(self) -> Dict:
        """Load pipeline configuration from YAML file."""
        config_path = Path(__file__).parent / 'pipeline.yaml'
        try:
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f) or {}
            else:
                logger.warning(f"Pipeline config file not found: {config_path}")
                return {}
        except Exception as e:
            logger.error(f"Failed to load pipeline config: {e}")
            return {}
    
    def load_tables_config(self) -> Dict:
        """Load tables configuration from YAML file."""
        config_path = Path(__file__).parent / 'tables.yaml'
        try:
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f) or {}
            else:
                logger.warning(f"Tables config file not found: {config_path}")
                return {}
        except Exception as e:
            logger.error(f"Failed to load tables config: {e}")
            return {}
    
    def get_database_config(self, db_type: str) -> Dict:
        """Get database configuration for specified type."""
        if db_type in self._connection_cache:
            return self._connection_cache[db_type]
        
        config = self._get_base_config(db_type)
        
        # Merge with any pipeline config overrides
        pipeline_connections = self.pipeline_config.get('connections', {})
        if db_type in pipeline_connections:
            config.update(pipeline_connections[db_type])
        
        # Add default connection parameters
        if db_type in ['source', 'replication']:  # Updated to match new naming
            # MySQL defaults
            config.setdefault('connect_timeout', 10)
            config.setdefault('read_timeout', 30)
            config.setdefault('write_timeout', 30)
            config.setdefault('charset', 'utf8mb4')
        else:  # analytics (PostgreSQL)
            # PostgreSQL defaults
            config.setdefault('connect_timeout', 10)
            config.setdefault('application_name', 'etl_pipeline')
        
        self._connection_cache[db_type] = config
        return config
    
    def _get_base_config(self, db_type: str) -> Dict:
        """Get base configuration for a database type."""
        if db_type not in self.ENV_MAPPINGS:
            raise ValueError(f"Unknown database type: {db_type}")
        
        env_mapping = self.ENV_MAPPINGS[db_type]
        config = {}
        
        for key, env_var in env_mapping.items():
            value = os.getenv(env_var)
            if key == 'port' and value:
                try:
                    value = int(value)
                except ValueError:
                    logger.warning(f"Invalid port value for {env_var}: {value}")
                    value = 3306 if db_type in ['source', 'replication'] else 5432  # Updated to match new naming
            config[key] = value
        
        return config
    
    def get_connection_string(self, db_type: str) -> str:
        """Get SQLAlchemy connection string for a database type."""
        config = self.get_database_config(db_type)
        
        # Validate required fields
        required_fields = ['host', 'port', 'database', 'user', 'password']
        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required database config fields for {db_type}: {missing_fields}")
        
        if db_type in ['source', 'replication']:  # Updated to match new naming
            # MySQL connection string
            return (
                f"mysql+pymysql://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}"
                f"?connect_timeout={config.get('connect_timeout', 10)}"
                f"&read_timeout={config.get('read_timeout', 30)}"
                f"&write_timeout={config.get('write_timeout', 30)}"
                f"&charset={config.get('charset', 'utf8mb4')}"
            )
        else:  # analytics (PostgreSQL)
            # PostgreSQL connection string
            conn_str = (
                f"postgresql+psycopg2://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}"
                f"?connect_timeout={config.get('connect_timeout', 10)}"
                f"&application_name={config.get('application_name', 'etl_pipeline')}"
            )
            if config.get('schema'):
                conn_str += f"&options=-csearch_path%3D{config['schema']}"
            return conn_str
    
    def validate_configs(self) -> bool:
        """Validate that all required configurations are present."""
        missing_vars = []
        empty_vars = []
        
        for db_type, env_mapping in self.ENV_MAPPINGS.items():
            for key, env_var in env_mapping.items():
                if key == 'schema':  # Schema is optional for PostgreSQL
                    continue
                    
                value = os.getenv(env_var)
                if not value:
                    missing_vars.append(f"{db_type}: {env_var}")
                elif value.lower() in ['none', 'null', '']:
                    empty_vars.append(f"{db_type}: {env_var} = '{value}'")
        
        if missing_vars or empty_vars:
            logger.error("Configuration issues found:")
            if missing_vars:
                logger.error("Missing variables:")
                for var in missing_vars:
                    logger.error(f"  - {var}")
            if empty_vars:
                logger.error("Empty/invalid variables:")
                for var in empty_vars:
                    logger.error(f"  - {var}")
            return False
        
        return True
    
    def get_pipeline_setting(self, key: str, default=None):
        """Get a pipeline configuration setting."""
        return self.pipeline_config.get(key, default)
    
    def get_table_config(self, table_name: str, table_type: str = 'source_tables') -> Dict:
        """Get configuration for a specific table."""
        tables = self.tables_config.get(table_type, {})
        return tables.get(table_name, {})
    
    def list_tables(self, table_type: str = 'source_tables') -> list:
        """List tables of the specified type."""
        tables = self.tables_config.get(table_type, {})
        return list(tables.keys())

# Create global settings instance
settings = Settings()