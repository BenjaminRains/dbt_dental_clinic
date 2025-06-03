"""
Configuration Loader Module
Loads and validates ETL pipeline configuration from YAML files.
"""

import yaml
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass

# Use standard logging instead of importing from other modules
logger = logging.getLogger(__name__)

@dataclass
class ETLConfig:
    """ETL Configuration container"""
    pipeline: Dict[str, Any]
    databases: Dict[str, Any]
    tables: Dict[str, Any]
    monitoring: Dict[str, Any]
    
    @property
    def source_tables(self) -> Dict[str, Any]:
        """Get source tables configuration"""
        return self.tables.get('source_tables', {})
    
    @property
    def staging_tables(self) -> Dict[str, Any]:
        """Get staging tables configuration"""
        return self.tables.get('staging_tables', {})
    
    @property
    def target_tables(self) -> Dict[str, Any]:
        """Get target tables configuration"""
        return self.tables.get('target_tables', {})

class ConfigLoader:
    """Configuration loader and validator"""
    
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.logger = logger
        self._config = None
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        try:
            if not self.config_path.exists():
                self.logger.warning(f"Configuration file not found: {self.config_path}")
                # Create empty config instead of failing
                config_data = {}
            else:
                with open(self.config_path, 'r') as f:
                    config_data = yaml.safe_load(f) or {}
            
            # Load additional configuration files
            tables_config = self._load_tables_config()
            database_config = self._load_database_config()
            
            # Merge configurations
            self._config = ETLConfig(
                pipeline=config_data.get('pipeline', {}),
                databases=database_config,
                tables=tables_config,
                monitoring=config_data.get('monitoring', {})
            )
            
            self._validate_config()
            self.logger.info(f"Configuration loaded successfully from {self.config_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            # Create minimal config to prevent total failure
            self._config = ETLConfig(
                pipeline={},
                databases=self._load_database_config(),
                tables={},
                monitoring={}
            )
    
    def _load_tables_config(self) -> Dict[str, Any]:
        """Load tables configuration"""
        tables_path = self.config_path.parent / "tables.yaml"
        
        if tables_path.exists():
            try:
                with open(tables_path, 'r') as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                self.logger.error(f"Failed to load tables config: {e}")
                return {}
        else:
            self.logger.warning(f"Tables configuration not found: {tables_path}")
            return {}
    
    def _load_database_config(self) -> Dict[str, Any]:
        """Load database configuration from environment variables"""
        database_config = {}
        
        # Load environment variables for database connections
        database_config['source'] = {
            'type': 'mysql',
            'host': os.getenv('OPENDENTAL_SOURCE_HOST', 'localhost'),
            'port': int(os.getenv('OPENDENTAL_SOURCE_PORT', '3306')),
            'database': os.getenv('OPENDENTAL_SOURCE_DB', 'opendental'),
            'username': os.getenv('OPENDENTAL_SOURCE_USER', 'root'),
            'password': os.getenv('OPENDENTAL_SOURCE_PW', ''),
        }
        
        database_config['staging'] = {
            'type': 'mysql',
            'host': os.getenv('STAGING_MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('STAGING_MYSQL_PORT', '3305')),
            'database': os.getenv('STAGING_MYSQL_DB', 'opendental_staging'),
            'username': os.getenv('STAGING_MYSQL_USER', 'staging_user'),
            'password': os.getenv('STAGING_MYSQL_PASSWORD', ''),
        }
        
        database_config['target'] = {
            'type': 'postgresql',
            'host': os.getenv('TARGET_POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('TARGET_POSTGRES_PORT', '5432')),
            'database': os.getenv('TARGET_POSTGRES_DB', 'opendental_analytics'),
            'username': os.getenv('TARGET_POSTGRES_USER', 'analytics_user'),
            'password': os.getenv('TARGET_POSTGRES_PASSWORD', ''),
            'schema': os.getenv('TARGET_POSTGRES_SCHEMA', 'analytics'),
        }
        
        return database_config
    
    def _validate_config(self):
        """Validate the loaded configuration"""
        if not self._config:
            raise ValueError("Configuration not loaded")
        
        # Validate database configurations
        required_databases = ['source', 'staging', 'target']
        for db in required_databases:
            if db not in self._config.databases:
                self.logger.warning(f"Missing database configuration: {db}")
                continue
            
            db_config = self._config.databases[db]
            required_fields = ['host', 'port', 'database', 'username']
            
            missing_fields = []
            for field in required_fields:
                if field not in db_config or not db_config[field]:
                    missing_fields.append(field)
            
            if missing_fields:
                self.logger.warning(f"Missing required fields in {db} database config: {missing_fields}")
        
        self.logger.info("Configuration validation completed")
    
    def get_config(self) -> ETLConfig:
        """Get the loaded configuration"""
        if self._config is None:
            raise RuntimeError("Configuration not loaded")
        return self._config
    
    def get_database_config(self, database: str) -> Dict[str, Any]:
        """Get configuration for a specific database"""
        if database not in self._config.databases:
            raise ValueError(f"Database configuration not found: {database}")
        return self._config.databases[database]
    
    def get_table_config(self, table: str, config_type: str = 'source') -> Dict[str, Any]:
        """Get configuration for a specific table"""
        table_configs = {
            'source': self._config.source_tables,
            'staging': self._config.staging_tables,
            'target': self._config.target_tables
        }
        
        if config_type not in table_configs:
            raise ValueError(f"Invalid config type: {config_type}")
        
        tables = table_configs[config_type]
        if table not in tables:
            self.logger.warning(f"Table configuration not found: {table} in {config_type}")
            return {}
        
        return tables[table]
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """Get pipeline configuration"""
        return self._config.pipeline
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        return self._config.monitoring
    
    def reload_config(self):
        """Reload configuration from file"""
        self.logger.info("Reloading configuration...")
        self._config = None
        self._load_config()
    
    def update_config(self, section: str, updates: Dict[str, Any]):
        """Update configuration section and save to file"""
        # This would implement configuration updates
        # For now, just log the request
        self.logger.info(f"Configuration update requested for {section}: {updates}")
        # TODO: Implement configuration file updates
    
    def export_config(self, output_path: str, format: str = 'yaml'):
        """Export current configuration to file"""
        if not self._config:
            raise RuntimeError("Configuration not loaded")
            
        config_dict = {
            'pipeline': self._config.pipeline,
            'databases': self._config.databases,
            'tables': self._config.tables,
            'monitoring': self._config.monitoring
        }
        
        if format.lower() == 'yaml':
            with open(output_path, 'w') as f:
                yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
        elif format.lower() == 'json':
            import json
            with open(output_path, 'w') as f:
                json.dump(config_dict, f, indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        self.logger.info(f"Configuration exported to {output_path}")

def load_config(config_path: str = "etl_pipeline/config/pipeline.yaml") -> ConfigLoader:
    """Convenience function to load configuration"""
    return ConfigLoader(config_path)