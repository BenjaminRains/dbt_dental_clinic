"""
Configuration management for the ETL pipeline.
Handles loading and validation of pipeline configuration.

LEGACY CODE - REDUNDANT WITH Settings CLASS
==========================================
This PipelineConfig class is LEGACY code and is redundant with the modern
Settings class in config/settings.py.

Current Status:
- PipelineConfig provides basic configuration loading and validation
- Settings class offers more comprehensive functionality
- This creates confusion about which configuration system to use
- Not integrated with new orchestration framework

Redundancy Issues:
- Both classes load YAML configuration files
- Both validate environment variables
- Both provide table configuration access
- Settings class has better features (caching, connection strings, etc.)

Migration Path:
- New code should use Settings class from config/settings.py
- Old code can continue using PipelineConfig temporarily
- This file should be removed once all dependencies are migrated

TODO: Remove PipelineConfig class to eliminate configuration system confusion
TODO: Migrate all dependencies to use Settings class
"""

import os
from pathlib import Path
import yaml
from typing import Dict, List, Optional
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class PipelineConfig:
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize pipeline configuration.
        
        Args:
            config_path: Path to configuration file. If None, uses default path.
        """
        # Load environment variables from .env file
        env_path = Path(__file__).parent.parent.parent / '.env'
        if not env_path.exists():
            raise FileNotFoundError(f"Environment file not found: {env_path}")
        load_dotenv(env_path)
        
        # Resolve config path relative to project root
        if config_path is None:
            config_path = 'etl_pipeline/config/tables.yml'
        
        # Convert to absolute path if relative
        if not os.path.isabs(config_path):
            current_file = Path(__file__).resolve()
            project_root = current_file.parent.parent.parent  # Go up three levels to project root
            config_path = os.path.join(project_root, config_path)
        
        self.config_path = config_path
        self.config = self.load_config()
        
        # Validate environment variables before accessing them
        self.validate_config()
        
        # Load required environment variables after validation
        self.source_db = os.environ['OPENDENTAL_SOURCE_DB']
        self.replication_db = os.environ['MYSQL_REPLICATION_DB']
        self.analytics_db = os.environ['POSTGRES_ANALYTICS_DB']
        self.analytics_schema = os.environ.get('POSTGRES_ANALYTICS_SCHEMA', 'raw')  # Only schema has a default
    
    def load_config(self) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            logger.info(f"Loaded configuration for {len(config.get('source_tables', {}))} tables")
            return config
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {str(e)}")
            raise
    
    def validate_config(self):
        """Validate configuration and environment variables."""
        # Validate required environment variables
        missing_dbs = []
        if 'OPENDENTAL_SOURCE_DB' not in os.environ:
            missing_dbs.append('OPENDENTAL_SOURCE_DB')
        if 'MYSQL_REPLICATION_DB' not in os.environ:
            missing_dbs.append('MYSQL_REPLICATION_DB')
        if 'POSTGRES_ANALYTICS_DB' not in os.environ:
            missing_dbs.append('POSTGRES_ANALYTICS_DB')
        
        if missing_dbs:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_dbs)}")
        
        # Validate configuration structure
        if 'source_tables' not in self.config:
            raise ValueError("Configuration missing 'source_tables' section")
    
    def get_table_config(self, table_name: str) -> Dict:
        """Get configuration for a specific table."""
        source_tables = self.config.get('source_tables', {})
        config = source_tables.get(table_name, {})
        
        if not config:
            logger.warning(f"No configuration found for table: {table_name}")
            # Return default configuration
            return {
                'incremental_column': None,
                'batch_size': 5000,
                'extraction_strategy': 'full_table',
                'table_importance': 'reference',
                'monitoring': {
                    'alert_on_failure': False,
                    'max_extraction_time_minutes': 10,
                    'data_quality_threshold': 0.95
                }
            }
        
        return config
    
    def get_tables_by_priority(self, importance_level: str) -> List[str]:
        """Get list of tables by importance level."""
        tables = []
        source_tables = self.config.get('source_tables', {})
        
        for table_name, config in source_tables.items():
            if config.get('table_importance') == importance_level:
                tables.append(table_name)
        
        logger.info(f"Found {len(tables)} tables with importance level: {importance_level}")
        return tables
    
    def get_critical_tables(self) -> List[str]:
        """Get the top 5 critical tables."""
        critical_tables = self.get_tables_by_priority('critical')
        
        # Sort by estimated size (descending)
        source_tables = self.config.get('source_tables', {})
        critical_with_size = [
            (table, source_tables[table].get('estimated_size_mb', 0))
            for table in critical_tables
        ]
        critical_with_size.sort(key=lambda x: x[1], reverse=True)
        
        # Return top 5
        top_5_critical = [table for table, _ in critical_with_size[:5]]
        
        logger.info(f"Critical tables: {top_5_critical}")
        return top_5_critical 