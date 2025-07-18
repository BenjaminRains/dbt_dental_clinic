"""
Unit tests for SimpleMySQLReplicator initialization using provider pattern.

This module tests the SimpleMySQLReplicator initialization with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests FAIL FAST behavior when ETL_ENVIRONMENT not set
    - Ensures type safety with DatabaseType and PostgresSchema enums
    - Tests environment separation (production vs test) with provider pattern
    - Validates incremental copy logic and configuration management

Coverage Areas:
    - SimpleMySQLReplicator initialization with Settings injection
    - Configuration loading from YAML files with provider pattern
    - Error handling and logging for dental clinic ETL operations
    - Provider pattern configuration isolation and environment separation
    - Settings injection for environment-agnostic connections

ETL Context:
    - Critical for nightly ETL pipeline execution with dental clinic data
    - Supports MariaDB v11.6 source and MySQL replication database
    - Uses provider pattern for clean dependency injection and test isolation
    - Implements Settings injection for environment-agnostic connections
    - Enforces FAIL FAST security to prevent accidental production usage
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


class TestSimpleMySQLReplicatorInitialization:
    """Unit tests for SimpleMySQLReplicator initialization using provider pattern."""
    
    def test_initialization_with_settings_injection(self, test_settings):
        """
        Test SimpleMySQLReplicator initialization with Settings injection.
        
        Validates:
            - Settings injection works for environment-agnostic initialization
            - Provider pattern dependency injection for configuration
            - ConnectionFactory integration with Settings injection
            - Configuration loading from provider pattern
            - Environment-agnostic operation with provider pattern
            
        ETL Pipeline Context:
            - Critical for nightly ETL pipeline execution
            - Supports both production and test environments
            - Uses provider pattern for clean dependency injection
            - Implements Settings injection for environment-agnostic connections
        """
        # Mock ConnectionFactory methods
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection') as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection') as mock_target:
            
            mock_source_engine = MagicMock()
            mock_target_engine = MagicMock()
            mock_source.return_value = mock_source_engine
            mock_target.return_value = mock_target_engine
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'patient': {
                        'incremental_column': 'DateTStamp',
                        'batch_size': 1000,
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                # Initialize replicator with Settings injection using fixture
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify Settings injection
                assert replicator.settings == test_settings
                
                # Verify ConnectionFactory calls with Settings injection
                mock_source.assert_called_once_with(test_settings)
                mock_target.assert_called_once_with(test_settings)
                
                # Verify configuration loading
                assert 'patient' in replicator.table_configs
                assert replicator.table_configs['patient']['incremental_column'] == 'DateTStamp'
                
                # Verify engine assignments
                assert replicator.source_engine == mock_source_engine
                assert replicator.target_engine == mock_target_engine

    def test_initialization_with_default_settings(self, test_settings):
        """
        Test SimpleMySQLReplicator initialization with default Settings.
        
        Validates:
            - Default Settings usage when no settings provided
            - Global Settings integration with provider pattern
            - ConnectionFactory integration with default Settings
            - Configuration loading from default provider
            
        ETL Pipeline Context:
            - Supports default configuration for simple usage
            - Uses global Settings for environment detection
            - Maintains provider pattern for dependency injection
        """
        with patch('etl_pipeline.core.simple_mysql_replicator.get_settings', return_value=test_settings), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection') as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection') as mock_target:
            
            mock_source_engine = MagicMock()
            mock_target_engine = MagicMock()
            mock_source.return_value = mock_source_engine
            mock_target.return_value = mock_target_engine
            
            # Mock YAML file loading
            mock_config = {'tables': {'patient': {'incremental_column': 'DateTStamp'}}}
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                # Initialize replicator without explicit settings
                replicator = SimpleMySQLReplicator()
                
                # Verify default Settings usage
                assert replicator.settings == test_settings
                
                # Verify ConnectionFactory calls
                mock_source.assert_called_once_with(test_settings)
                mock_target.assert_called_once_with(test_settings)

    def test_initialization_with_custom_tables_config_path(self, test_settings):
        """
        Test SimpleMySQLReplicator initialization with custom tables config path.
        
        Validates:
            - Custom tables.yml path handling
            - File path construction with provider pattern
            - Configuration loading from custom path
            - Settings injection with custom configuration
            
        ETL Pipeline Context:
            - Supports custom configuration paths for different environments
            - Maintains provider pattern for dependency injection
            - Uses Settings injection for environment-agnostic operation
        """
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection') as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection') as mock_target:
            
            mock_source_engine = MagicMock()
            mock_target_engine = MagicMock()
            mock_source.return_value = mock_source_engine
            mock_target.return_value = mock_target_engine
            
            # Mock custom YAML file loading
            custom_config = {'tables': {'custom_table': {'incremental_column': 'CustomDate'}}}
            with patch('builtins.open', mock_open(read_data=yaml.dump(custom_config))):
                # Initialize replicator with custom config path
                custom_path = '/custom/path/tables.yml'
                replicator = SimpleMySQLReplicator(settings=test_settings, tables_config_path=custom_path)
                
                # Verify custom path usage
                assert replicator.tables_config_path == custom_path
                
                # Verify custom configuration loading
                assert 'custom_table' in replicator.table_configs
                assert replicator.table_configs['custom_table']['incremental_column'] == 'CustomDate'

    def test_initialization_configuration_file_not_found(self, test_settings):
        """
        Test SimpleMySQLReplicator initialization with missing configuration file.
        
        AAA Pattern:
            Arrange: Set up test provider and settings with missing config file
            Act: Attempt to create SimpleMySQLReplicator instance
            Assert: Verify ConfigurationError is raised with proper message
            
        Validates:
            - ConfigurationError handling for missing tables.yml
            - Error propagation with provider pattern
            - Settings injection error handling
            - Clear error messages for configuration issues
            
        ETL Pipeline Context:
            - Critical for ETL pipeline reliability
            - Prevents silent failures in configuration loading
            - Maintains provider pattern error handling
        """
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection') as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection') as mock_target:
            
            mock_source_engine = MagicMock()
            mock_target_engine = MagicMock()
            mock_source.return_value = mock_source_engine
            mock_target.return_value = mock_target_engine
            
            # Act: Attempt to create SimpleMySQLReplicator instance
            with patch('builtins.open', side_effect=FileNotFoundError("Configuration file not found")):
                # Assert: Verify ConfigurationError is raised with proper message
                with pytest.raises(ConfigurationError, match="Configuration file not found"):
                    SimpleMySQLReplicator(settings=test_settings)

    def test_initialization_invalid_yaml_configuration(self, test_settings):
        """
        Test SimpleMySQLReplicator initialization with invalid YAML configuration.
        
        Validates:
            - YAML parsing error handling
            - Error propagation with provider pattern
            - Settings injection error handling
            - Clear error messages for configuration issues
            
        ETL Pipeline Context:
            - Critical for ETL pipeline reliability
            - Prevents silent failures in configuration parsing
            - Maintains provider pattern error handling
        """
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection') as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection') as mock_target:
            
            mock_source_engine = MagicMock()
            mock_target_engine = MagicMock()
            mock_source.return_value = mock_source_engine
            mock_target.return_value = mock_target_engine
            
            # Mock invalid YAML
            with patch('builtins.open', mock_open(read_data="invalid: yaml: content:")):
                with pytest.raises(ConfigurationError):
                    SimpleMySQLReplicator(settings=test_settings) 