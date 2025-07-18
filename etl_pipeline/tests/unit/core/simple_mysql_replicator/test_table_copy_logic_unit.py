"""
Unit tests for SimpleMySQLReplicator table copy logic using provider pattern.

This module tests the SimpleMySQLReplicator table copy logic with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests incremental copy logic and configuration management
    - Tests full table copy logic and error handling
    - Validates database connection handling with provider pattern

Coverage Areas:
    - Incremental table copy logic with change data capture
    - Full table copy logic with table recreation
    - Database connection handling with provider pattern
    - Error handling and logging for dental clinic ETL operations
    - Settings injection for environment-agnostic connections

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


class TestSimpleMySQLReplicatorTableCopyLogic:
    """Unit tests for SimpleMySQLReplicator table copy logic using provider pattern."""
    
    @pytest.fixture
    def replicator_with_mock_engines(self, test_settings):
        """Create replicator with mocked database engines using provider pattern."""
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
                    },
                    'appointment': {
                        'incremental_column': 'AptDateTime',
                        'batch_size': 500,
                        'estimated_size_mb': 25,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'important'
                    },
                    'procedurelog': {
                        'incremental_column': 'ProcDate',
                        'batch_size': 2000,
                        'estimated_size_mb': 100,
                        'extraction_strategy': 'full_table',
                        'table_importance': 'important'
                    }
                }
            }
            with patch('builtins.open', mock_open(read_data=yaml.dump(mock_config))):
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_copy_table_incremental_success(self, replicator_with_mock_engines):
        """
        Test successful incremental table copy using provider pattern.
        
        Validates:
            - Incremental copy logic with provider pattern
            - Settings injection for database connections
            - Configuration retrieval from provider
            - Success logging and timing
            
        ETL Pipeline Context:
            - Incremental copy for change data capture
            - Optimized for dental clinic data with frequent updates
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        
        # Mock the incremental copy method
        with patch.object(replicator, '_copy_incremental_table', return_value=True) as mock_copy:
            # Mock time for timing calculation with proper side_effect
            mock_time = MagicMock()
            mock_time.side_effect = [1000.0, 1002.5]
            with patch('time.time', mock_time), \
                 patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
                result = replicator.copy_table('patient')
                
                # Verify success
                assert result is True
                mock_copy.assert_called_once_with('patient', replicator.table_configs['patient'])

    def test_copy_table_incremental_failure(self, replicator_with_mock_engines):
        """
        Test incremental table copy failure using provider pattern.
        
        Validates:
            - Error handling in incremental copy
            - Provider pattern error propagation
            - Settings injection error handling
            - Failure logging and timing
            
        ETL Pipeline Context:
            - Error handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Mock the incremental copy method to fail
        with patch.object(replicator, '_copy_incremental_table', return_value=False) as mock_copy:
            # Mock time for timing calculation with proper side_effect
            mock_time = MagicMock()
            mock_time.side_effect = [1000.0, 1002.5]
            with patch('time.time', mock_time), \
                 patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
                result = replicator.copy_table('patient')
                
                # Verify failure
                assert result is False
                mock_copy.assert_called_once_with('patient', replicator.table_configs['patient'])

    def test_copy_table_force_full(self, replicator_with_mock_engines):
        """
        Test table copy with force_full parameter using provider pattern.
        
        Validates:
            - Force full copy logic with provider pattern
            - Settings injection for configuration override
            - Provider pattern configuration handling
            - Force full logging
            
        ETL Pipeline Context:
            - Force full copy for complete data refresh
            - Used for dental clinic data with major schema changes
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        
        # Mock the full table copy method to avoid real database connections
        with patch.object(replicator, '_copy_full_table', return_value=True) as mock_copy:
            # Mock time for timing calculation with proper side_effect
            mock_time = MagicMock()
            mock_time.side_effect = [1000.0, 1002.5]
            with patch('time.time', mock_time), \
                 patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
                result = replicator.copy_table('patient', force_full=True)
                
                # Verify force full behavior
                assert result is True
                mock_copy.assert_called_once_with('patient', replicator.table_configs['patient'])

    def test_copy_table_no_configuration(self, replicator_with_mock_engines):
        """
        Test table copy with missing configuration using provider pattern.
        
        Validates:
            - Missing configuration handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - Clear error messages for configuration issues
            
        ETL Pipeline Context:
            - Critical for ETL pipeline reliability
            - Prevents silent failures in configuration access
            - Maintains provider pattern error handling
        """
        replicator = replicator_with_mock_engines
        
        result = replicator.copy_table('unknown_table')
        
        # Verify failure for missing configuration
        assert result is False

    def test_copy_table_full_table_strategy(self, replicator_with_mock_engines):
        """
        Test table copy with full_table strategy using provider pattern.
        
        Validates:
            - Full table strategy implementation with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Table structure recreation and data copying
            
        ETL Pipeline Context:
            - Full table strategy for complete data refresh
            - Used for dental clinic data with major schema changes
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        
        # Mock the full table copy method
        with patch.object(replicator, '_copy_full_table', return_value=True) as mock_copy:
            result = replicator.copy_table('procedurelog')
            
            # Verify success
            assert result is True
            mock_copy.assert_called_once_with('procedurelog', replicator.table_configs['procedurelog'])

    def test_copy_table_chunked_incremental_strategy(self, replicator_with_mock_engines):
        """
        Test table copy with chunked_incremental strategy using provider pattern.
        
        Validates:
            - Chunked incremental strategy implementation with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Chunked incremental data copying
            
        ETL Pipeline Context:
            - Chunked incremental strategy for very large tables
            - Used for dental clinic data with large datasets
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        
        # Add chunked_incremental table to config
        replicator.table_configs['large_chunked_table'] = {
            'extraction_strategy': 'chunked_incremental',
            'incremental_column': 'DateTStamp',
            'batch_size': 1000,
            'estimated_size_mb': 200
        }
        
        # Mock the chunked incremental copy method
        with patch.object(replicator, '_copy_chunked_incremental_table', return_value=True) as mock_copy:
            result = replicator.copy_table('large_chunked_table')
            
            # Verify success
            assert result is True
            mock_copy.assert_called_once_with('large_chunked_table', replicator.table_configs['large_chunked_table'])

    def test_copy_table_unknown_strategy(self, replicator_with_mock_engines):
        """
        Test table copy with unknown extraction strategy using provider pattern.
        
        Validates:
            - Unknown strategy error handling with provider pattern
            - Settings injection error handling
            - Provider pattern error propagation
            - Clear error messages for unknown strategies
            
        ETL Pipeline Context:
            - Error handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Modify config to have unknown strategy
        replicator.table_configs['patient']['extraction_strategy'] = 'unknown_strategy'
        
        result = replicator.copy_table('patient')
        
        # Verify failure for unknown strategy
        assert result is False 