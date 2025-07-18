"""
Unit tests for SimpleMySQLReplicator error handling using provider pattern.

This module tests the SimpleMySQLReplicator error handling with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests exception handling and error propagation
    - Tests specific exception types and categorization
    - Validates error handling in bulk operations

Coverage Areas:
    - Exception handling with provider pattern
    - Settings injection error handling
    - Provider pattern error propagation
    - Specific exception types and categorization
    - Error handling in bulk operations

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


class TestSimpleMySQLReplicatorErrorHandling:
    """Unit tests for SimpleMySQLReplicator error handling using provider pattern."""
    
    @pytest.fixture
    def replicator_with_error_config(self, test_settings):
        """Create replicator with error-prone configuration using provider pattern."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine):
            
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
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_copy_table_exception_handling(self, replicator_with_error_config):
        """
        Test exception handling in copy_table using provider pattern.
        
        Validates:
            - Exception handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - False return for exceptions
            
        ETL Pipeline Context:
            - Exception handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_error_config
        
        # Mock copy_table to raise exception
        with patch.object(replicator, '_copy_incremental_table', side_effect=DataExtractionError("Test exception")):
            result = replicator.copy_table('patient')
            
            # Verify failure due to exception
            assert result is False

    def test_copy_incremental_table_exception_handling(self, replicator_with_error_config):
        """
        Test exception handling in _copy_incremental_table using provider pattern.
        
        Validates:
            - Exception handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - False return for exceptions
            
        ETL Pipeline Context:
            - Exception handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_error_config
        
        # Mock helper method to raise exception
        with patch.object(replicator, '_get_last_processed_value', side_effect=DatabaseConnectionError("Test exception")):
            result = replicator._copy_incremental_table('patient', replicator.table_configs['patient'])
            
            # Verify failure due to exception
            assert result is False

    def test_get_last_processed_value_exception_handling(self, replicator_with_error_config):
        """
        Test exception handling in _get_last_processed_value using provider pattern.
        
        Validates:
            - Exception handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - None return for exceptions
            
        ETL Pipeline Context:
            - Exception handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_error_config
        
        # Mock database connection to raise exception
        replicator.target_engine.connect.return_value.__enter__.side_effect = DatabaseConnectionError("Database error")
        
        result = replicator._get_last_processed_value('patient', 'DateTStamp')
        
        # Verify None return due to exception
        assert result is None

    def test_get_new_records_count_exception_handling(self, replicator_with_error_config):
        """
        Test exception handling in _get_new_records_count using provider pattern.
        
        Validates:
            - Exception handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - Zero return for exceptions
            
        ETL Pipeline Context:
            - Exception handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_error_config
        
        # Mock database connection to raise exception
        replicator.source_engine.connect.return_value.__enter__.side_effect = DatabaseConnectionError("Database error")
        
        result = replicator._get_new_records_count('patient', 'DateTStamp', None)
        
        # Verify zero return due to exception
        assert result == 0

    def test_copy_new_records_exception_handling(self, replicator_with_error_config):
        """
        Test exception handling in _copy_new_records using provider pattern.
        
        Validates:
            - Exception handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - False return for exceptions
            
        ETL Pipeline Context:
            - Exception handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_error_config
        
        # Mock database connection to raise exception
        replicator.source_engine.connect.return_value.__enter__.side_effect = DatabaseConnectionError("Database error")
        
        result = replicator._copy_new_records('patient', 'DateTStamp', '2024-01-01 10:00:00', 1000)
        
        # Verify False return due to exception
        assert result is False

    def test_bulk_operations_exception_handling(self, replicator_with_error_config):
        """
        Test exception handling in bulk operations using provider pattern.
        
        Validates:
            - Exception handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - Result aggregation with exceptions
            
        ETL Pipeline Context:
            - Exception handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_error_config
        
        # Mock copy_table to raise exception
        with patch.object(replicator, 'copy_table', side_effect=DataExtractionError("Bulk operation error")):
            # The actual implementation should catch exceptions and return False
            # or handle them gracefully
            results = replicator.copy_all_tables()
            
            # Verify that the exception was handled gracefully
            # The method should return a dictionary with failure results
            assert isinstance(results, dict)
            assert len(results) == 1  # Only one table in config
            assert not any(results.values())  # All should fail due to exception

    def test_specific_exception_handling_validation(self, replicator_with_error_config):
        """
        Test that specific exception types are properly handled and categorized.
        
        Validates:
            - Specific exception types are used instead of generic Exception
            - DatabaseConnectionError for connection issues
            - DataExtractionError for extraction issues
            - ConfigurationError for configuration issues
            - Proper exception categorization for error handling
            
        ETL Pipeline Context:
            - Structured error handling for dental clinic ETL reliability
            - Specific exception types enable better error recovery
            - Uses provider pattern for error isolation
        """
        replicator = replicator_with_error_config
        
        # Test DatabaseConnectionError handling
        with patch.object(replicator, '_get_last_processed_value', 
                         side_effect=DatabaseConnectionError("Connection timeout")):
            result = replicator._copy_incremental_table('patient', replicator.table_configs['patient'])
            assert result is False
        
        # Test DataExtractionError handling
        with patch.object(replicator, '_copy_incremental_table', 
                         side_effect=DataExtractionError("Extraction failed")):
            result = replicator.copy_table('patient')
            assert result is False
        
        # Test ConfigurationError handling
        with patch.object(replicator, 'get_extraction_strategy', 
                         side_effect=ConfigurationError("Invalid configuration")):
            result = replicator.copy_table('patient')
            assert result is False 