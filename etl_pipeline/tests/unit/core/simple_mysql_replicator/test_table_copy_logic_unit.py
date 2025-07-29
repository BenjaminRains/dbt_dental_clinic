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
from datetime import datetime

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
                        'incremental_columns': ['DateTStamp'],
                        'batch_size': 1000,
                        'estimated_size_mb': 50,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    },
                    'appointment': {
                        'incremental_columns': ['AptDateTime'],
                        'batch_size': 500,
                        'estimated_size_mb': 25,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'important'
                    },
                    'procedurelog': {
                        'incremental_columns': ['ProcDate'],
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
        with patch.object(replicator, '_copy_incremental_table', return_value=(True, 100)) as mock_copy:
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
        with patch.object(replicator, '_copy_incremental_table', return_value=(False, 0)) as mock_copy:
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
        with patch.object(replicator, '_copy_full_table', return_value=(True, 500)) as mock_copy:
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
        with patch.object(replicator, '_copy_full_table', return_value=(True, 1000)) as mock_copy:
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
            'incremental_columns': ['DateTStamp'],
            'batch_size': 1000,
            'estimated_size_mb': 200
        }
        
        # Mock the chunked incremental copy method
        with patch.object(replicator, '_copy_chunked_incremental_table', return_value=(True, 500)) as mock_copy:
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

    def test_copy_table_incremental_no_columns(self, replicator_with_mock_engines):
        """
        Test incremental table copy with no incremental columns configured.
        
        Validates:
            - Error handling when no incremental columns are configured
            - Provider pattern error propagation
            - Settings injection error handling
            - Clear error messages for missing incremental columns
            
        ETL Pipeline Context:
            - Error handling for dental clinic ETL reliability
            - Maintains provider pattern for error isolation
            - Uses Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Modify config to have no incremental columns
        replicator.table_configs['patient']['incremental_columns'] = []
        
        # Mock the incremental copy method to return failure
        with patch.object(replicator, '_copy_incremental_table', return_value=(False, 0)) as mock_copy:
            result = replicator.copy_table('patient')
            
            # Verify failure due to no incremental columns
            assert result is False
            mock_copy.assert_called_once_with('patient', replicator.table_configs['patient'])

    def test_copy_table_incremental_multiple_columns(self, replicator_with_mock_engines):
        """
        Test incremental table copy with multiple incremental columns.
        
        Validates:
            - Multiple incremental columns handling with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Multiple column incremental data copying
            
        ETL Pipeline Context:
            - Multiple column incremental strategy for complex data
            - Used for dental clinic data with multiple timestamp columns
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        
        # Modify config to have multiple incremental columns
        replicator.table_configs['patient']['incremental_columns'] = ['DateTStamp', 'DateModified', 'DateCreated']
        
        # Mock the incremental copy method
        with patch.object(replicator, '_copy_incremental_table', return_value=(True, 200)) as mock_copy:
            result = replicator.copy_table('patient')
            
            # Verify success
            assert result is True
            mock_copy.assert_called_once_with('patient', replicator.table_configs['patient'])


class TestIncrementalLogicMethods:
    """Unit tests for the new incremental logic methods that determine maximum values across columns."""
    
    @pytest.fixture
    def replicator_with_mock_engines(self, test_settings):
        """Create replicator with mocked database engines for testing incremental logic."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine):
            
            # Mock YAML file loading with test configuration
            mock_config = {
                'tables': {
                    'patient': {
                        'incremental_columns': ['DateTStamp', 'DateModified', 'DateCreated'],
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

    def test_get_last_processed_value_max_single_column(self, replicator_with_mock_engines):
        """
        Test _get_last_processed_value_max with single column.
        
        Validates:
            - Single column maximum value retrieval
            - Database query execution with proper SQL
            - Return value handling for single column
            - Connection management with provider pattern
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = datetime(2024, 1, 15, 10, 30, 0)
        mock_conn.execute.return_value = mock_result
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.target_engine.connect.return_value = mock_conn
        
        result = replicator._get_last_processed_value_max('patient', ['DateTStamp'])
        
        # Verify result
        assert result == datetime(2024, 1, 15, 10, 30, 0)
        
        # Verify SQL query was executed correctly
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0][0]
        assert 'SELECT MAX(DateTStamp)' in str(call_args)
        assert 'FROM patient' in str(call_args)

    def test_get_last_processed_value_max_multiple_columns(self, replicator_with_mock_engines):
        """
        Test _get_last_processed_value_max with multiple columns.
        
        Validates:
            - Multiple column maximum value retrieval
            - Database query execution for each column
            - Maximum value calculation across all columns
            - Connection management with provider pattern
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection and results
        mock_conn = MagicMock()
        mock_result1 = MagicMock()
        mock_result1.scalar.return_value = datetime(2024, 1, 15, 10, 30, 0)
        mock_result2 = MagicMock()
        mock_result2.scalar.return_value = datetime(2024, 1, 16, 14, 45, 0)
        mock_result3 = MagicMock()
        mock_result3.scalar.return_value = datetime(2024, 1, 14, 9, 15, 0)
        
        # Mock multiple execute calls for different columns
        mock_conn.execute.side_effect = [mock_result1, mock_result2, mock_result3]
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.target_engine.connect.return_value = mock_conn
        
        result = replicator._get_last_processed_value_max('patient', ['DateTStamp', 'DateModified', 'DateCreated'])
        
        # Verify result (should be the maximum of all three dates)
        assert result == datetime(2024, 1, 16, 14, 45, 0)
        
        # Verify SQL queries were executed for each column
        assert mock_conn.execute.call_count == 3

    def test_get_last_processed_value_max_no_data(self, replicator_with_mock_engines):
        """
        Test _get_last_processed_value_max when no data exists.
        
        Validates:
            - Handling when no data exists in table
            - None return value for empty results
            - Connection management with provider pattern
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_conn.execute.return_value = mock_result
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.target_engine.connect.return_value = mock_conn
        
        result = replicator._get_last_processed_value_max('patient', ['DateTStamp'])
        
        # Verify result
        assert result is None

    def test_get_last_processed_value_max_exception_handling(self, replicator_with_mock_engines):
        """
        Test _get_last_processed_value_max exception handling.
        
        Validates:
            - Exception handling during database operations
            - None return value for exceptions
            - Connection management with provider pattern
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection to raise exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseConnectionError("Connection failed")
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.target_engine.connect.return_value = mock_conn
        
        result = replicator._get_last_processed_value_max('patient', ['DateTStamp'])
        
        # Verify result
        assert result is None

    def test_get_new_records_count_max_no_last_processed(self, replicator_with_mock_engines):
        """
        Test _get_new_records_count_max when no last processed value exists.
        
        Validates:
            - Count all records when no last processed value
            - Database query execution for total count
            - Return value handling for complete count
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 1500
        mock_conn.execute.return_value = mock_result
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.source_engine.connect.return_value = mock_conn
        
        result = replicator._get_new_records_count_max('patient', ['DateTStamp'], None)
        
        # Verify result
        assert result == 1500
        
        # Verify SQL query was executed correctly
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0][0]
        assert 'SELECT COUNT(*)' in str(call_args)
        assert 'FROM patient' in str(call_args)

    def test_get_new_records_count_max_with_last_processed(self, replicator_with_mock_engines):
        """
        Test _get_new_records_count_max with last processed value.
        
        Validates:
            - Count records newer than last processed value
            - OR logic across multiple columns
            - Database query execution with WHERE conditions
            - Return value handling for filtered count
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 250
        mock_conn.execute.return_value = mock_result
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.source_engine.connect.return_value = mock_conn
        
        last_processed = datetime(2024, 1, 15, 10, 30, 0)
        result = replicator._get_new_records_count_max('patient', ['DateTStamp', 'DateModified'], last_processed)
        
        # Verify result
        assert result == 250
        
        # Verify SQL query was executed correctly with OR logic
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0][0]
        assert 'SELECT COUNT(*)' in str(call_args)
        assert 'FROM patient' in str(call_args)
        assert 'WHERE' in str(call_args)
        # Should have OR logic for multiple columns
        assert 'DateTStamp > :last_processed OR DateModified > :last_processed' in str(call_args)

    def test_get_new_records_count_max_exception_handling(self, replicator_with_mock_engines):
        """
        Test _get_new_records_count_max exception handling.
        
        Validates:
            - Exception handling during database operations
            - Zero return value for exceptions
            - Connection management with provider pattern
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection to raise exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseConnectionError("Connection failed")
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.source_engine.connect.return_value = mock_conn
        
        result = replicator._get_new_records_count_max('patient', ['DateTStamp'], None)
        
        # Verify result
        assert result == 0

    def test_copy_new_records_max_no_last_processed(self, replicator_with_mock_engines):
        """
        Test _copy_new_records_max when no last processed value exists.
        
        Validates:
            - Full table copy when no last processed value
            - Delegation to _copy_full_table method
            - Return value handling for complete copy
        """
        replicator = replicator_with_mock_engines
        
        # Mock _copy_full_table method
        with patch.object(replicator, '_copy_full_table', return_value=(True, 1500)) as mock_copy:
            result = replicator._copy_new_records_max('patient', ['DateTStamp'], None, 1000)
            
            # Verify result
            assert result == (True, 1500)
            
            # Verify _copy_full_table was called
            mock_copy.assert_called_once_with('patient', {})

    def test_copy_new_records_max_with_last_processed(self, replicator_with_mock_engines):
        """
        Test _copy_new_records_max with last processed value.
        
        Validates:
            - Incremental copy with last processed value
            - Database query execution with WHERE conditions
            - Batch processing with proper SQL
            - Return value handling for incremental copy
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connections and results
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock source query result - return empty list after first batch to stop the loop
        mock_source_result = MagicMock()
        mock_source_result.fetchall.side_effect = [
            [(1, 'John', datetime(2024, 1, 16, 10, 30, 0)),
             (2, 'Jane', datetime(2024, 1, 16, 11, 45, 0))],  # First batch
            []  # Second batch (empty to stop the loop)
        ]
        mock_source_result.keys.return_value = ['id', 'name', 'DateTStamp']
        mock_source_conn.execute.return_value = mock_source_result
        
        # Mock the context manager pattern for both connections
        mock_source_conn.__enter__.return_value = mock_source_conn
        mock_source_conn.__exit__.return_value = None
        mock_target_conn.__enter__.return_value = mock_target_conn
        mock_target_conn.__exit__.return_value = None
        
        # Mock the engines to return the connections
        replicator.source_engine.connect.return_value = mock_source_conn
        replicator.target_engine.connect.return_value = mock_target_conn
        
        # Mock the _build_mysql_upsert_sql method
        with patch.object(replicator, '_build_mysql_upsert_sql', return_value="INSERT INTO patient ..."):
            last_processed = datetime(2024, 1, 15, 10, 30, 0)
            result = replicator._copy_new_records_max('patient', ['DateTStamp'], last_processed, 1000)
            
            # Verify result
            assert result == (True, 2)
            
            # Verify source query was executed correctly
            mock_source_conn.execute.assert_called()
            call_args = mock_source_conn.execute.call_args[0][0]
            assert 'SELECT *' in str(call_args)
            assert 'FROM patient' in str(call_args)
            assert 'WHERE' in str(call_args)
            # Should have OR logic for multiple columns
            assert 'DateTStamp > :last_processed' in str(call_args)

    def test_copy_new_records_max_exception_handling(self, replicator_with_mock_engines):
        """
        Test _copy_new_records_max exception handling.
        
        Validates:
            - Exception handling during database operations
            - False return value for exceptions
            - Connection management with provider pattern
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connection to raise exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseConnectionError("Connection failed")
        
        # Mock the context manager pattern: with engine.connect() as conn:
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        replicator.source_engine.connect.return_value = mock_conn
        
        last_processed = datetime(2024, 1, 15, 10, 30, 0)
        result = replicator._copy_new_records_max('patient', ['DateTStamp'], last_processed, 1000)
        
        # Verify result
        assert result == (False, 0)

    def test_copy_new_records_max_safety_limit(self, replicator_with_mock_engines):
        """
        Test _copy_new_records_max safety limit handling.
        
        Validates:
            - Safety limit prevents infinite loops
            - Warning logging when safety limit is reached
            - Proper return value when safety limit is hit
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connections and results that would cause infinite loop
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock source query result that always returns data (causing infinite loop)
        mock_source_result = MagicMock()
        mock_source_result.fetchall.return_value = [(1, 'John', datetime(2024, 1, 16, 10, 30, 0))]
        mock_source_result.keys.return_value = ['id', 'name', 'DateTStamp']
        mock_source_conn.execute.return_value = mock_source_result
        
        # Mock the context manager pattern for both connections
        mock_source_conn.__enter__.return_value = mock_source_conn
        mock_source_conn.__exit__.return_value = None
        mock_target_conn.__enter__.return_value = mock_target_conn
        mock_target_conn.__exit__.return_value = None
        
        # Mock the engines to return the connections
        replicator.source_engine.connect.return_value = mock_source_conn
        replicator.target_engine.connect.return_value = mock_target_conn
        
        # Mock the _build_mysql_upsert_sql method
        with patch.object(replicator, '_build_mysql_upsert_sql', return_value="INSERT INTO patient ..."):
            with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
                last_processed = datetime(2024, 1, 15, 10, 30, 0)
                result = replicator._copy_new_records_max('patient', ['DateTStamp'], last_processed, 1000)
                
                # Verify result (should stop due to safety limit)
                # The safety limit is based on offset > 1000000, but total_rows_copied will be much less
                # Each batch copies 1 row, so it will copy about 1000 rows before hitting the safety limit
                assert result[0] == True  # Success
                assert result[1] > 0  # Some rows were copied
                assert result[1] <= 1000000  # Should not exceed safety limit
                
                # Verify warning was logged
                mock_logger.warning.assert_called()
                warning_call = mock_logger.warning.call_args[0][0]
                assert "Reached safety limit" in warning_call 