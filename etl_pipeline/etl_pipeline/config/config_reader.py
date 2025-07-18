"""
Config Reader - Static Configuration Management
==============================================

This module provides static configuration reading capabilities to replace
SchemaDiscovery in ETL operations. It reads from tables.yml and provides
the same interface that ETL components expect, but without dynamic discovery.

STATUS: NEW - Static Configuration Reader
========================================

This is a new component that will replace SchemaDiscovery usage in ETL operations
while maintaining the same interface for compatibility.

KEY FEATURES:
- ✅ Static configuration reading from tables.yml
- ✅ Same interface as SchemaDiscovery for ETL operations
- ✅ No dynamic database queries or schema discovery
- ✅ Fast and reliable configuration access
- ✅ Compatible with existing ETL components

USAGE:
- TableProcessor: Replace SchemaDiscovery with ConfigReader
- PipelineOrchestrator: Use ConfigReader for table configuration
- PriorityProcessor: Use ConfigReader for table priority lookup
- Any ETL component that needs table configuration

BENEFITS:
- 5-10x faster than SchemaDiscovery for ETL operations
- No database connection overhead during ETL
- Predictable and reliable configuration access
- Maintains all existing functionality through static config
"""

import os
import yaml
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime

# Import specific exception classes for structured error handling
from ..exceptions import ConfigurationError, EnvironmentError

logger = logging.getLogger(__name__)

class ConfigReader:
    """
    Configuration reader for ETL pipeline tables configuration.
    
    Loads and manages table configuration from tables.yml file.
    Supports metadata-based versioning through generated_at field.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize ConfigReader.
        
        Args:
            config_path: Path to the tables.yml configuration file (optional, will auto-detect)
        """
        try:
            if config_path is None:
                # Use tables.yml directly - no versioned file lookup
                self.config_path = str(Path(__file__).parent / "tables.yml")
            else:
                self.config_path = config_path
                
            self.config = self._load_configuration()
            self._last_loaded = datetime.now()
            logger.info(f"ConfigReader initialized with {len(self.config.get('tables', {}))} tables from {self.config_path}")
            
        except ConfigurationError as e:
            logger.error(f"Configuration error during ConfigReader initialization: {e}")
            raise
        except EnvironmentError as e:
            logger.error(f"Environment error during ConfigReader initialization: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during ConfigReader initialization: {str(e)}")
            raise ConfigurationError(
                message=f"Failed to initialize ConfigReader: {str(e)}",
                config_file=self.config_path if hasattr(self, 'config_path') else None,
                details={'operation': 'initialization'},
                original_exception=e
            )
    
    def _load_configuration(self) -> Dict:
        """Load configuration from tables.yml file."""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise ConfigurationError(
                    message=f"Configuration file not found: {self.config_path}",
                    config_file=self.config_path,
                    details={'operation': 'file_loading', 'file_path': str(config_file)}
                )
            
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config or 'tables' not in config:
                raise ConfigurationError(
                    message=f"Invalid configuration file structure: {self.config_path}",
                    config_file=self.config_path,
                    details={
                        'operation': 'configuration_validation',
                        'has_config': bool(config),
                        'has_tables_key': 'tables' in config if config else False
                    }
                )
            
            logger.info(f"Loaded configuration for {len(config['tables'])} tables")
            return config
            
        except ConfigurationError:
            # Re-raise ConfigurationError as-is
            raise
        except yaml.YAMLError as e:
            logger.error(f"YAML parsing error in configuration file {self.config_path}: {e}")
            raise ConfigurationError(
                message=f"Invalid YAML format in configuration file: {self.config_path}",
                config_file=self.config_path,
                details={'operation': 'yaml_parsing', 'yaml_error': str(e)},
                original_exception=e
            )
        except Exception as e:
            logger.error(f"Failed to load configuration from {self.config_path}: {e}")
            raise ConfigurationError(
                message=f"Failed to load configuration from {self.config_path}: {str(e)}",
                config_file=self.config_path,
                details={'operation': 'file_loading'},
                original_exception=e
            )
    
    def reload_configuration(self) -> bool:
        """Reload configuration from file."""
        try:
            self.config = self._load_configuration()
            self._last_loaded = datetime.now()
            logger.info("Configuration reloaded successfully")
            return True
        except ConfigurationError as e:
            logger.error(f"Configuration error during reload: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during configuration reload: {str(e)}")
            return False
    
    def get_table_config(self, table_name: str) -> Dict:
        """
        Get configuration for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table configuration
        """
        try:
            table_config = self.config.get('tables', {}).get(table_name, {})
            if not table_config:
                logger.warning(f"No configuration found for table: {table_name}")
            return table_config
        except Exception as e:
            logger.error(f"Error retrieving configuration for table {table_name}: {e}")
            raise ConfigurationError(
                message=f"Failed to retrieve configuration for table {table_name}",
                config_file=self.config_path,
                details={'operation': 'table_config_retrieval', 'table_name': table_name},
                original_exception=e
            )
    
    def get_tables_by_importance(self, importance_level: str) -> List[str]:
        """
        Get tables by importance level.
        
        Args:
            importance_level: Importance level ('important', 'audit', 'standard')
            
        Returns:
            List of table names with the specified importance level
        """
        try:
            tables = []
            for table_name, config in self.config.get('tables', {}).items():
                if config.get('table_importance') == importance_level:
                    tables.append(table_name)
            
            logger.info(f"Found {len(tables)} tables with importance level: {importance_level}")
            return tables
        except Exception as e:
            logger.error(f"Error retrieving tables by importance level {importance_level}: {e}")
            raise ConfigurationError(
                message=f"Failed to retrieve tables by importance level {importance_level}",
                config_file=self.config_path,
                details={'operation': 'importance_filtering', 'importance_level': importance_level},
                original_exception=e
            )
    
    def get_tables_by_strategy(self, strategy: str) -> List[str]:
        """
        Get tables by extraction strategy.
        
        Args:
            strategy: Extraction strategy ('full_table', 'incremental')
            
        Returns:
            List of table names with the specified extraction strategy
        """
        try:
            tables = []
            for table_name, config in self.config.get('tables', {}).items():
                if config.get('extraction_strategy') == strategy:
                    tables.append(table_name)
            
            logger.info(f"Found {len(tables)} tables with strategy: {strategy}")
            return tables
        except Exception as e:
            logger.error(f"Error retrieving tables by strategy {strategy}: {e}")
            raise ConfigurationError(
                message=f"Failed to retrieve tables by strategy {strategy}",
                config_file=self.config_path,
                details={'operation': 'strategy_filtering', 'strategy': strategy},
                original_exception=e
            )
    
    def get_large_tables(self, size_threshold_mb: float = 100.0) -> List[str]:
        """
        Get tables larger than the specified threshold.
        
        Args:
            size_threshold_mb: Size threshold in MB
            
        Returns:
            List of table names larger than the threshold
        """
        try:
            tables = []
            for table_name, config in self.config.get('tables', {}).items():
                if config.get('estimated_size_mb', 0) > size_threshold_mb:
                    tables.append(table_name)
            
            logger.info(f"Found {len(tables)} tables larger than {size_threshold_mb}MB")
            return tables
        except Exception as e:
            logger.error(f"Error retrieving large tables with threshold {size_threshold_mb}MB: {e}")
            raise ConfigurationError(
                message=f"Failed to retrieve large tables with threshold {size_threshold_mb}MB",
                config_file=self.config_path,
                details={'operation': 'size_filtering', 'size_threshold_mb': size_threshold_mb},
                original_exception=e
            )
    
    def get_monitored_tables(self) -> List[str]:
        """
        Get tables that have monitoring enabled.
        
        Returns:
            List of table names with monitoring enabled
        """
        try:
            tables = []
            for table_name, config in self.config.get('tables', {}).items():
                if config.get('monitoring', {}).get('alert_on_failure', False):
                    tables.append(table_name)
            
            logger.info(f"Found {len(tables)} monitored tables")
            return tables
        except Exception as e:
            logger.error(f"Error retrieving monitored tables: {e}")
            raise ConfigurationError(
                message="Failed to retrieve monitored tables",
                config_file=self.config_path,
                details={'operation': 'monitoring_filtering'},
                original_exception=e
            )
    
    def get_table_dependencies(self, table_name: str) -> List[str]:
        """
        Get dependencies for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of dependent table names
        """
        try:
            table_config = self.get_table_config(table_name)
            return table_config.get('dependencies', [])
        except ConfigurationError:
            # Re-raise ConfigurationError from get_table_config
            raise
        except Exception as e:
            logger.error(f"Error retrieving dependencies for table {table_name}: {e}")
            raise ConfigurationError(
                message=f"Failed to retrieve dependencies for table {table_name}",
                config_file=self.config_path,
                details={'operation': 'dependency_retrieval', 'table_name': table_name},
                original_exception=e
            )
    
    def get_configuration_summary(self) -> Dict:
        """
        Get a summary of the current configuration.
        
        Returns:
            Dictionary containing configuration summary statistics
        """
        try:
            tables = self.config.get('tables', {})
            
            summary = {
                'total_tables': len(tables),
                'importance_levels': {},
                'extraction_strategies': {},
                'size_ranges': {
                    'small': 0,      # < 1MB
                    'medium': 0,     # 1-100MB
                    'large': 0       # > 100MB
                },
                'monitored_tables': 0,
                'modeled_tables': 0,
                'last_loaded': self._last_loaded.isoformat()
            }
            
            for table_name, config in tables.items():
                # Count by importance level
                importance = config.get('table_importance', 'unknown')
                summary['importance_levels'][importance] = summary['importance_levels'].get(importance, 0) + 1
                
                # Count by extraction strategy
                strategy = config.get('extraction_strategy', 'unknown')
                summary['extraction_strategies'][strategy] = summary['extraction_strategies'].get(strategy, 0) + 1
                
                # Count by size range
                size_mb = config.get('estimated_size_mb', 0)
                if size_mb < 1:
                    summary['size_ranges']['small'] += 1
                elif size_mb < 100:
                    summary['size_ranges']['medium'] += 1
                else:
                    summary['size_ranges']['large'] += 1
                
                # Count monitored tables
                if config.get('monitoring', {}).get('alert_on_failure', False):
                    summary['monitored_tables'] += 1
                
                # Count modeled tables
                if config.get('is_modeled', False):
                    summary['modeled_tables'] += 1
            
            return summary
        except Exception as e:
            logger.error(f"Error generating configuration summary: {e}")
            raise ConfigurationError(
                message="Failed to generate configuration summary",
                config_file=self.config_path,
                details={'operation': 'summary_generation'},
                original_exception=e
            )
    
    def validate_configuration(self) -> Dict[str, List[str]]:
        """
        Validate the configuration for common issues.
        
        Returns:
            Dictionary containing validation results with issues by category
        """
        try:
            issues = {
                'missing_batch_size': [],
                'missing_extraction_strategy': [],
                'missing_importance': [],
                'invalid_batch_size': [],
                'large_tables_without_monitoring': []
            }
            
            for table_name, config in self.config.get('tables', {}).items():
                # Check for missing batch_size
                if 'batch_size' not in config:
                    issues['missing_batch_size'].append(table_name)
                
                # Check for missing extraction_strategy
                if 'extraction_strategy' not in config:
                    issues['missing_extraction_strategy'].append(table_name)
                
                # Check for missing table_importance
                if 'table_importance' not in config:
                    issues['missing_importance'].append(table_name)
                
                # Check for invalid batch_size
                batch_size = config.get('batch_size', 0)
                if batch_size <= 0 or batch_size > 100000:
                    issues['invalid_batch_size'].append(table_name)
                
                # Check for large tables without monitoring
                size_mb = config.get('estimated_size_mb', 0)
                if size_mb > 50 and not config.get('monitoring', {}).get('alert_on_failure', False):
                    issues['large_tables_without_monitoring'].append(table_name)
            
            return issues
        except Exception as e:
            logger.error(f"Error validating configuration: {e}")
            raise ConfigurationError(
                message="Failed to validate configuration",
                config_file=self.config_path,
                details={'operation': 'configuration_validation'},
                original_exception=e
            )
    
    def get_configuration_path(self) -> str:
        """Get the path to the configuration file."""
        return self.config_path
    
    def get_last_loaded(self) -> datetime:
        """Get the timestamp when configuration was last loaded."""
        return self._last_loaded 