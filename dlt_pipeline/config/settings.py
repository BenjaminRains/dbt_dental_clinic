"""
Simplified configuration management for dlt pipeline.
Replaces your complex configuration system with minimal setup.
"""

import os
import yaml
from typing import Dict, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_database_config(db_type: str) -> Dict[str, str]:
    """
    Get database configuration from environment variables.
    Supports: 'source', 'analytics'
    
    Args:
        db_type: Type of database ('source' or 'analytics')
        
    Returns:
        Dict with connection parameters
    """
    
    if db_type == 'source':
        return {
            'host': os.getenv('SOURCE_MYSQL_HOST', 'localhost'),
            'port': os.getenv('SOURCE_MYSQL_PORT', '3306'),
            'user': os.getenv('SOURCE_MYSQL_USER'),
            'password': os.getenv('SOURCE_MYSQL_PASSWORD'),
            'database': os.getenv('OPENDENTAL_SOURCE_DB', 'opendental_source')
        }
    
    elif db_type == 'analytics':
        return {
            'host': os.getenv('ANALYTICS_POSTGRES_HOST', 'localhost'),
            'port': os.getenv('ANALYTICS_POSTGRES_PORT', '5432'),
            'user': os.getenv('ANALYTICS_POSTGRES_USER'),
            'password': os.getenv('ANALYTICS_POSTGRES_PASSWORD'),
            'database': os.getenv('POSTGRES_ANALYTICS_DB', 'opendental_analytics'),
            'schema': os.getenv('POSTGRES_ANALYTICS_SCHEMA', 'raw')
        }
    
    else:
        raise ValueError(f"Unknown database type: {db_type}")

def get_dlt_destination_config() -> str:
    """
    Get dlt destination configuration string.
    
    Returns:
        PostgreSQL connection string for dlt
    """
    
    config = get_database_config('analytics')
    
    return (
        f"postgresql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )

def get_table_configs() -> Dict[str, Dict]:
    """
    Load simplified table configuration from YAML.
    Only keeps the essential metadata needed for dlt.
    
    Returns:
        Dict of table configurations
    """
    
    config_path = Path(__file__).parent / 'tables.yml'
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config.get('source_tables', {})
        
    except FileNotFoundError:
        # If no config file, return sensible defaults
        return _get_default_table_configs()

def get_tables_by_importance(importance: str) -> list[str]:
    """
    Get table names filtered by importance level.
    
    Args:
        importance: Importance level ('critical', 'important', 'audit', 'reference')
        
    Returns:
        List of table names
    """
    
    table_configs = get_table_configs()
    
    return [
        name for name, config in table_configs.items()
        if config.get('table_importance') == importance
    ]

def get_incremental_tables() -> list[str]:
    """
    Get table names that use incremental extraction.
    
    Returns:
        List of table names with incremental strategy
    """
    
    table_configs = get_table_configs()
    
    return [
        name for name, config in table_configs.items()
        if config.get('extraction_strategy') == 'incremental'
    ]

# dlt configuration helpers
def get_dlt_pipeline_config() -> Dict:
    """
    Get dlt pipeline configuration.
    
    Returns:
        Dict with dlt pipeline settings
    """
    
    return {
        'pipeline_name': 'opendental_etl',
        'destination': 'postgres',
        'dataset_name': 'raw',  # PostgreSQL schema name
        'progress': 'log',
        'full_refresh': False
    }

def validate_environment() -> bool:
    """
    Validate that all required environment variables are set.
    
    Returns:
        True if environment is valid
    """
    
    required_vars = [
        'SOURCE_MYSQL_HOST',
        'SOURCE_MYSQL_USER', 
        'SOURCE_MYSQL_PASSWORD',
        'ANALYTICS_POSTGRES_HOST',
        'ANALYTICS_POSTGRES_USER',
        'ANALYTICS_POSTGRES_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    return True

# Configuration constants
IMPORTANCE_LEVELS = ['critical', 'important', 'audit', 'reference']
EXTRACTION_STRATEGIES = ['incremental', 'full_table']

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': 'dlt_pipeline.log',
            'mode': 'a',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'INFO',
            'propagate': False
        }
    }
}