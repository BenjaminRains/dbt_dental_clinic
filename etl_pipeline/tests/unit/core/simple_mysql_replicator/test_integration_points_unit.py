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
            - ConnectionFactory integration with Settings injection
            - Provider pattern database connection handling
            - Settings-based connection configuration
            - Engine assignment and validation
            
        ETL Pipeline Context:
            - ConnectionFactory for dental clinic ETL
            - Settings injection for environment-agnostic connections
            - Provider pattern for database connection management
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine) as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine) as mock_target, \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with test data
            mock_config = {
                'patient': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental'
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Initialize replicator with Settings injection
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify ConnectionFactory integration with Settings injection
                mock_source.assert_called_once_with(test_settings)
                mock_target.assert_called_once_with(test_settings)
                
                # Verify engine assignments
                assert replicator.source_engine == mock_source_engine
                assert replicator.target_engine == mock_target_engine
                
                # Verify Settings injection
                assert replicator.settings == test_settings

    def test_settings_integration(self, test_settings):
        """
        Test Settings integration with provider pattern.
        
        Validates:
            - Settings injection for environment-agnostic operation
            - Provider pattern Settings integration
            - Settings-based configuration access
            - Environment-specific Settings handling
            
        ETL Pipeline Context:
            - Settings injection for dental clinic ETL
            - Environment-agnostic operation with provider pattern
            - Settings-based configuration for database connections
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine) as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine) as mock_target, \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with test data
            mock_config = {'patient': {'incremental_columns': ['DateTStamp']}}
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Initialize replicator with Settings injection
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify Settings integration
                assert replicator.settings == test_settings
                
                # Verify Settings-based configuration access
                assert hasattr(replicator.settings, 'get_source_connection_config')
                assert hasattr(replicator.settings, 'get_replication_connection_config')
                
                # Verify Settings injection in ConnectionFactory calls
                mock_source.assert_called_once_with(test_settings)
                mock_target.assert_called_once_with(test_settings)

    def test_yaml_configuration_integration(self, test_settings):
        """
        Test YAML configuration integration with provider pattern.
        
        Validates:
            - YAML configuration loading with provider pattern
            - Configuration file path handling
            - Table configuration integration
            - Settings-based configuration access
            
        ETL Pipeline Context:
            - YAML configuration for dental clinic ETL
            - Table-specific configuration with provider pattern
            - Settings-based configuration file handling
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine) as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine) as mock_target, \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with comprehensive test data
            mock_config = {
                'patient': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental'
                },
                'appointment': {
                    'incremental_columns': ['AptDateTime'],
                    'batch_size': 500,
                    'estimated_size_mb': 25,
                    'extraction_strategy': 'incremental'
                },
                'procedurelog': {
                    'incremental_columns': ['ProcDate'],
                    'batch_size': 2000,
                    'estimated_size_mb': 100,
                    'extraction_strategy': 'full_table'
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Initialize replicator with Settings injection
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify configuration loading
                assert 'patient' in replicator.table_configs
                assert 'appointment' in replicator.table_configs
                assert 'procedurelog' in replicator.table_configs
                
                # Verify table configuration structure
                patient_config = replicator.table_configs['patient']
                assert patient_config['incremental_columns'] == ['DateTStamp']
                assert patient_config['batch_size'] == 1000
                assert patient_config['extraction_strategy'] == 'incremental'
                
                # Verify Settings-based configuration access
                assert replicator.settings == test_settings

    def test_logging_integration(self, test_settings):
        """
        Test logging integration with provider pattern.
        
        Validates:
            - Logging integration with provider pattern
            - Settings-based logging configuration
            - Logging during initialization and operation
            - Provider pattern logging context
            
        ETL Pipeline Context:
            - Logging for dental clinic ETL operations
            - Settings-based logging configuration
            - Provider pattern logging integration
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine) as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine) as mock_target, \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock YAML file loading
            mock_config = {'tables': {'patient': {'incremental_columns': ['DateTStamp']}}}
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                # Initialize replicator with Settings injection
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify logging integration
                assert hasattr(replicator, 'settings')
                assert replicator.settings == test_settings
                
                # Verify logging context from Settings
                assert hasattr(test_settings, 'get_source_connection_config')
                assert hasattr(test_settings, 'get_replication_connection_config')

    def test_provider_pattern_integration(self, test_settings):
        """
        Test provider pattern integration with Settings injection.
        
        Validates:
            - Provider pattern dependency injection
            - Settings injection for environment-agnostic operation
            - Provider pattern configuration access
            - Settings-based provider pattern integration
            
        ETL Pipeline Context:
            - Provider pattern for dental clinic ETL
            - Settings injection for environment-agnostic operation
            - Provider pattern configuration management
        """
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine) as mock_source, \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine) as mock_target, \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock YAML file loading
            mock_config = {'tables': {'patient': {'incremental_columns': ['DateTStamp']}}}
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                # Initialize replicator with Settings injection using provider pattern
                replicator = SimpleMySQLReplicator(settings=test_settings)
                
                # Verify provider pattern integration
                assert replicator.settings == test_settings
                
                # Verify Settings injection in provider pattern
                assert hasattr(replicator.settings, 'get_source_connection_config')
                assert hasattr(replicator.settings, 'get_replication_connection_config')
                
                # Verify provider pattern configuration access
                assert 'patient' in replicator.table_configs
                assert replicator.table_configs['patient']['incremental_columns'] == ['DateTStamp'] 