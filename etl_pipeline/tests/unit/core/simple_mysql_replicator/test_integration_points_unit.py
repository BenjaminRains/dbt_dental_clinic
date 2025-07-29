"""
Unit tests for SimpleMySQLReplicator integration points using provider pattern.

This module tests the SimpleMySQLReplicator integration points with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests integration with ConnectionFactory and Settings
    - Tests YAML configuration integration
    - Validates logging integration with provider pattern

Coverage Areas:
    - ConnectionFactory integration with provider pattern
    - Settings integration with provider pattern
    - YAML configuration integration with provider pattern
    - Logging integration with provider pattern
    - Provider pattern integration points

ETL Context:
    - Critical for nightly ETL pipeline execution with dental clinic data
    - Supports MariaDB v11.6 source and MySQL replication database
    - Uses provider pattern for clean dependency injection and test isolation
    - Implements Settings injection for environment-agnostic connections
"""
import pytest
from unittest.mock import patch, MagicMock, mock_open
import os
import yaml
from typing import Dict, Any

# Import ETL pipeline components
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.config import (
    DatabaseType, 
    PostgresSchema,
    reset_settings
)
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory

# Import custom exceptions for structured error handling
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseQueryError
from etl_pipeline.exceptions.data import DataExtractionError, DataLoadingError
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Import standardized fixtures
from tests.fixtures.connection_fixtures import (
    mock_connection_factory_with_settings,
    test_connection_settings,
    production_connection_settings
)
from tests.fixtures.env_fixtures import (
    test_settings,
    production_settings,
    test_env_provider,
    production_env_provider
)
from tests.fixtures.config_fixtures import (
    test_pipeline_config,
    test_tables_config
)


class TestSimpleMySQLReplicatorIntegrationPoints:
    """Unit tests for SimpleMySQLReplicator integration points using provider pattern."""
    
    def test_connection_factory_integration(self, test_settings):
        """
        Test ConnectionFactory integration with provider pattern.
        
        Validates:
            - ConnectionFactory integration with provider pattern
            - Settings injection for database connections
            - Provider pattern database connection setup
            - ConnectionFactory method calls with Settings injection
            
        ETL Pipeline Context:
            - ConnectionFactory integration for dental clinic ETL
            - Optimized for dental clinic data with database connections
            - Uses provider pattern for configuration access
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine) as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine) as mock_target:
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'patient': {
                        'incremental_columns': ['DateTStamp'],
                        'batch_size': 1000,
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify ConnectionFactory calls with Settings injection
                mock_source.assert_called_once_with(test_settings)
                mock_target.assert_called_once_with(test_settings)

    def test_settings_integration(self, test_settings):
        """
        Test Settings integration with provider pattern.
        
        Validates:
            - Settings integration with provider pattern
            - Settings injection for configuration access
            - Provider pattern configuration loading
            - Settings-based configuration access
            
        ETL Pipeline Context:
            - Settings integration for dental clinic ETL
            - Optimized for dental clinic data with configuration management
            - Uses provider pattern for configuration access
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine):
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'patient': {
                        'incremental_columns': ['DateTStamp'],
                        'batch_size': 1000,
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify Settings integration
                assert replicator.settings == test_settings
                assert replicator.settings.provider == test_settings.provider

    def test_yaml_configuration_integration(self, test_settings):
        """
        Test YAML configuration integration with provider pattern.
        
        Validates:
            - YAML configuration integration with provider pattern
            - Settings injection for configuration loading
            - Provider pattern configuration access
            - YAML file loading with provider pattern
            
        ETL Pipeline Context:
            - YAML configuration integration for dental clinic ETL
            - Optimized for dental clinic data with configuration files
            - Uses provider pattern for configuration access
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine):
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'patient': {
                        'incremental_columns': ['DateTStamp'],
                        'batch_size': 1000,
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))) as mock_file:
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify YAML file loading
                mock_file.assert_called_once()
                
                # Verify configuration loading
                assert 'patient' in replicator.table_configs
                assert replicator.table_configs['patient']['incremental_columns'] == ['DateTStamp']

    def test_logging_integration(self, test_settings):
        """
        Test logging integration with provider pattern.
        
        Validates:
            - Logging integration with provider pattern
            - Settings injection for logging configuration
            - Provider pattern logging setup
            - Logging configuration with provider pattern
            
        ETL Pipeline Context:
            - Logging integration for dental clinic ETL
            - Optimized for dental clinic data with logging
            - Uses provider pattern for configuration access
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine):
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'patient': {
                        'incremental_columns': ['DateTStamp'],
                        'batch_size': 1000,
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))), \
                 patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify logging integration
                # The logger should be called during initialization
                # We can verify that the logger is properly configured
                assert mock_logger.info.called or mock_logger.debug.called

    def test_provider_pattern_integration(self, test_settings):
        """
        Test provider pattern integration with all components.
        
        Validates:
            - Provider pattern integration with all components
            - Settings injection for all integrations
            - Provider pattern configuration access
            - Integration point consistency
            
        ETL Pipeline Context:
            - Provider pattern integration for dental clinic ETL
            - Optimized for dental clinic data with consistent patterns
            - Uses provider pattern for configuration access
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine):
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'patient': {
                        'incremental_columns': ['DateTStamp'],
                        'batch_size': 1000,
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify provider pattern integration
                assert replicator.settings.provider == test_settings.provider
                assert replicator.settings == test_settings
                
                # Verify all integration points use the same provider
                assert replicator.settings.provider is test_settings.provider 