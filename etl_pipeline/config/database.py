"""
Database configuration module for the ELT pipeline.
Handles connection settings and environment-specific configurations.
"""
import os
from typing import Dict, Optional
from pathlib import Path
from dotenv import load_dotenv
import logging
from etl_pipeline.loaders.config_loader import PipelineConfig

logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration settings."""
    
    # Environment variable mappings
    ENV_MAPPINGS = {
        'source': {
            'host': 'OPENDENTAL_SOURCE_HOST',
            'port': 'OPENDENTAL_SOURCE_PORT',
            'database': 'OPENDENTAL_SOURCE_DB',
            'user': 'OPENDENTAL_SOURCE_USER',
            'password': 'OPENDENTAL_SOURCE_PW'
        },
        'staging': {
            'host': 'STAGING_MYSQL_HOST',
            'port': 'STAGING_MYSQL_PORT',
            'database': 'STAGING_MYSQL_DB',
            'user': 'STAGING_MYSQL_USER',
            'password': 'STAGING_MYSQL_PASSWORD'
        },
        'target': {
            'host': 'TARGET_POSTGRES_HOST',
            'port': 'TARGET_POSTGRES_PORT',
            'database': 'TARGET_POSTGRES_DB',
            'user': 'TARGET_POSTGRES_USER',
            'password': 'TARGET_POSTGRES_PASSWORD',
            'schema': 'TARGET_POSTGRES_SCHEMA'
        }
    }
    
    @classmethod
    def load_environment(cls):
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
    
    @classmethod
    def get_source_config(cls) -> Dict:
        """Get source MySQL database configuration."""
        config = cls._get_base_config('source')
        config.update(PipelineConfig().get_connection_config('source'))
        return config
    
    @classmethod
    def get_staging_config(cls) -> Dict:
        """Get staging MySQL database configuration."""
        config = cls._get_base_config('staging')
        config.update(PipelineConfig().get_connection_config('staging'))
        return config
    
    @classmethod
    def get_target_config(cls) -> Dict:
        """Get target PostgreSQL database configuration."""
        config = cls._get_base_config('target')
        config.update(PipelineConfig().get_connection_config('target'))
        return config
    
    @classmethod
    def _get_base_config(cls, db_type: str) -> Dict:
        """Get base configuration for a database type."""
        env_mapping = cls.ENV_MAPPINGS[db_type]
        config = {}
        
        for key, env_var in env_mapping.items():
            value = os.getenv(env_var)
            if key == 'port' and value:
                value = int(value)
            config[key] = value
        
        return config
    
    @classmethod
    def validate_configs(cls) -> bool:
        """Validate that all required database configurations are present."""
        missing_vars = []
        empty_vars = []
        
        for db_type, env_mapping in cls.ENV_MAPPINGS.items():
            for key, env_var in env_mapping.items():
                value = os.getenv(env_var)
                if not value:
                    missing_vars.append(f"{db_type}: {env_var}")
                elif value.lower() in ['none', 'null', '']:
                    empty_vars.append(f"{db_type}: {env_var} = '{value}'")
        
        if missing_vars or empty_vars:
            logger.error("Database configuration issues found:")
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
    
    @classmethod
    def get_connection_string(cls, db_type: str) -> str:
        """Get SQLAlchemy connection string for a database type."""
        config = getattr(cls, f"get_{db_type}_config")()
        
        if db_type in ['source', 'staging']:
            # MySQL connection string with query parameters
            return (
                f"mysql+pymysql://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}"
                f"?connect_timeout={config.get('connect_timeout', 10)}"
                f"&read_timeout={config.get('read_timeout', 30)}"
                f"&write_timeout={config.get('write_timeout', 30)}"
                f"&charset={config.get('charset', 'utf8mb4')}"
            )
        else:  # target
            # PostgreSQL connection string with query parameters
            return (
                f"postgresql+psycopg2://{config['user']}:{config['password']}@"
                f"{config['host']}:{config['port']}/{config['database']}"
                f"?connect_timeout={config.get('connect_timeout', 10)}"
                f"&application_name=etl_pipeline"
            )

# Load environment variables when module is imported
DatabaseConfig.load_environment() 