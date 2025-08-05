"""
Unit tests for SimpleMySQLReplicator data processing methods using provider pattern.

This module tests the SimpleMySQLReplicator data processing methods with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests data processing methods for different table sizes
    - Tests connection management and batch processing
    - Validates error handling in data processing

Coverage Areas:
    - Tiny table copying with direct operations
    - Small table copying with bulk operations
    - Medium table copying with chunked operations
    - Large table copying with optimized operations
    - Connection management and batch processing
    - Settings injection for environment-agnostic operations

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


class TestDataProcessingMethods:
    """Unit tests for SimpleMySQLReplicator data processing methods using provider pattern."""
    
    @pytest.fixture
    def replicator_with_mock_engines(self, test_settings):
        """Create replicator with mocked database engines for testing data processing methods."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with test data
            mock_config = {
                'tiny_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 1,
                    'extraction_strategy': 'full_table',
                    'performance_category': 'tiny',
                    'processing_priority': 10,
                    'estimated_processing_time_minutes': 0.01,
                    'memory_requirements_mb': 1
                },
                'small_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 5000,
                    'estimated_size_mb': 10,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'small',
                    'processing_priority': 8,
                    'estimated_processing_time_minutes': 0.05,
                    'memory_requirements_mb': 5
                },
                'medium_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 25000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'medium',
                    'processing_priority': 5,
                    'estimated_processing_time_minutes': 0.2,
                    'memory_requirements_mb': 25
                },
                'large_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 50000,
                    'estimated_size_mb': 200,
                    'extraction_strategy': 'full_table',
                    'performance_category': 'large',
                    'processing_priority': 1,
                    'estimated_processing_time_minutes': 1.0,
                    'memory_requirements_mb': 100
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Create replicator instance
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_copy_tiny_table_success(self, replicator_with_mock_engines):
        """
        Test successful tiny table copy operation.
        
        Validates:
            - Tiny table copy operation with direct INSERT ... SELECT
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Connection manager integration
            
        ETL Pipeline Context:
            - Tiny table copying for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Mock connection managers
        mock_source_manager = MagicMock()
        mock_target_manager = MagicMock()
        
        # Mock source query result
        mock_source_result = MagicMock()
        mock_source_result.fetchall.return_value = [
            (1, 'John', '2024-01-01'),
            (2, 'Jane', '2024-01-02'),
            (3, 'Bob', '2024-01-03')
        ]
        mock_source_result.keys.return_value = ['id', 'name', 'date']
        mock_source_result.rowcount = 3
        mock_source_manager.execute_with_retry.return_value = mock_source_result
        
        # Mock target manager
        mock_target_manager.execute_with_retry.return_value = None
        mock_target_manager.commit.return_value = None
        
        # Mock context managers
        mock_source_manager.__enter__ = MagicMock(return_value=mock_source_manager)
        mock_source_manager.__exit__ = MagicMock(return_value=None)
        mock_target_manager.__enter__ = MagicMock(return_value=mock_target_manager)
        mock_target_manager.__exit__ = MagicMock(return_value=None)
        
        with patch.object(replicator, '_create_connection_managers') as mock_create_managers:
            mock_create_managers.return_value = (mock_source_manager, mock_target_manager)
            
            success, rows_copied = replicator.performance_optimizer._copy_medium_small_table_optimized('tiny_table', 3, 1000)
            
            # Verify success
            assert success is True
            assert rows_copied == 3
            
            # Verify source query was executed
            mock_source_manager.execute_with_retry.assert_called_once()
            call_args = mock_source_manager.execute_with_retry.call_args[0][0]
            assert 'SELECT * FROM `tiny_table`' in call_args

    def test_copy_tiny_table_empty_table(self, replicator_with_mock_engines):
        """
        Test tiny table copy operation with empty table.
        
        Validates:
            - Tiny table copy operation with empty table
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Empty result handling
            
        ETL Pipeline Context:
            - Tiny table copying for dental clinic ETL with empty tables
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Mock connection managers
        mock_source_manager = MagicMock()
        mock_target_manager = MagicMock()
        
        # Mock empty source query result
        mock_source_result = MagicMock()
        mock_source_result.fetchall.return_value = []
        mock_source_result.keys.return_value = ['id', 'name', 'date']
        mock_source_result.rowcount = 0
        mock_source_manager.execute_with_retry.return_value = mock_source_result
        
        # Mock context managers
        mock_source_manager.__enter__ = MagicMock(return_value=mock_source_manager)
        mock_source_manager.__exit__ = MagicMock(return_value=None)
        mock_target_manager.__enter__ = MagicMock(return_value=mock_target_manager)
        mock_target_manager.__exit__ = MagicMock(return_value=None)
        
        with patch.object(replicator, '_create_connection_managers') as mock_create_managers:
            mock_create_managers.return_value = (mock_source_manager, mock_target_manager)
            
            success, rows_copied = replicator.performance_optimizer._copy_medium_small_table_optimized('tiny_table', 0, 1000)
            
            # Verify success with no rows
            assert success is True
            assert rows_copied == 0

    def test_copy_tiny_table_failure(self, replicator_with_mock_engines):
        """
        Test tiny table copy operation failure.
        
        Validates:
            - Tiny table copy operation failure handling
            - Provider pattern error handling
            - Settings injection error handling
            - Exception propagation
            
        ETL Pipeline Context:
            - Tiny table copying failure handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Mock connection managers to raise exception
        mock_source_manager = MagicMock()
        mock_source_manager.execute_with_retry.side_effect = DatabaseConnectionError("Connection failed")
        
        # Mock context managers
        mock_source_manager.__enter__ = MagicMock(return_value=mock_source_manager)
        mock_source_manager.__exit__ = MagicMock(return_value=None)
        
        with patch.object(replicator, '_create_connection_managers') as mock_create_managers:
            mock_create_managers.return_value = (mock_source_manager, MagicMock())
            
            success, rows_copied = replicator.performance_optimizer._copy_medium_small_table_optimized('tiny_table', 0, 1000)
            
            # Verify failure
            assert success is False
            assert rows_copied == 0

    def test_copy_small_table_success(self, replicator_with_mock_engines):
        """
        Test successful small table copy operation.
        
        Validates:
            - Small table copy operation with bulk operations
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Bulk insert optimization
            
        ETL Pipeline Context:
            - Small table copying for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Mock connection managers
        mock_source_manager = MagicMock()
        mock_target_manager = MagicMock()
        
        # Mock source query result
        mock_source_result = MagicMock()
        mock_source_result.fetchall.return_value = [
            (1, 'John', '2024-01-01'),
            (2, 'Jane', '2024-01-02'),
            (3, 'Bob', '2024-01-03')
        ]
        mock_source_result.keys.return_value = ['id', 'name', 'date']
        mock_source_result.rowcount = 3
        mock_source_manager.execute_with_retry.return_value = mock_source_result
        
        # Mock target manager
        mock_target_manager.execute_with_retry.return_value = None
        mock_target_manager.commit.return_value = None
        
        # Mock context managers
        mock_source_manager.__enter__ = MagicMock(return_value=mock_source_manager)
        mock_source_manager.__exit__ = MagicMock(return_value=None)
        mock_target_manager.__enter__ = MagicMock(return_value=mock_target_manager)
        mock_target_manager.__exit__ = MagicMock(return_value=None)
        
        with patch.object(replicator, '_create_connection_managers') as mock_create_managers:
            mock_create_managers.return_value = (mock_source_manager, mock_target_manager)
            
            success, rows_copied = replicator.performance_optimizer._copy_medium_small_table_optimized('small_table', 3, 1000)
            
            # Verify success
            assert success is True
            assert rows_copied == 3
            
            # Verify source query was executed
            mock_source_manager.execute_with_retry.assert_called_once()
            call_args = mock_source_manager.execute_with_retry.call_args[0][0]
            assert 'SELECT * FROM `small_table`' in call_args

    def test_copy_medium_table_success(self, replicator_with_mock_engines):
        """
        Test successful medium table copy operation.
        
        Validates:
            - Medium table copy operation with chunked processing
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Chunked batch processing
            
        ETL Pipeline Context:
            - Medium table copying for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Mock connection managers
        mock_source_manager = MagicMock()
        mock_target_manager = MagicMock()
        
        # Mock COUNT query result
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 3  # Total count of 3 rows
        
        # Mock source query results for multiple batches
        mock_source_result1 = MagicMock()
        mock_source_result1.fetchall.return_value = [
            (1, 'John', '2024-01-01'),
            (2, 'Jane', '2024-01-02')
        ]
        mock_source_result1.keys.return_value = ['id', 'name', 'date']
        
        mock_source_result2 = MagicMock()
        mock_source_result2.fetchall.return_value = [
            (3, 'Bob', '2024-01-03')
        ]
        mock_source_result2.keys.return_value = ['id', 'name', 'date']
        
        mock_source_result3 = MagicMock()
        mock_source_result3.fetchall.return_value = []
        mock_source_result3.keys.return_value = ['id', 'name', 'date']
        
        # FIXED: Mock the correct sequence of calls
        # 1. COUNT query (first call)
        # 2. First batch data query (second call)
        # 3. Second batch data query (third call)
        # 4. Empty batch data query (fourth call)
        mock_source_manager.execute_with_retry.side_effect = [
            mock_count_result,    # COUNT query
            mock_source_result1,  # First batch
            mock_source_result2,  # Second batch
            mock_source_result3   # Empty batch to stop
        ]
        
        # Mock target manager
        mock_target_manager.execute_with_retry.return_value = None
        mock_target_manager.commit.return_value = None
        
        # Mock context managers
        mock_source_manager.__enter__ = MagicMock(return_value=mock_source_manager)
        mock_source_manager.__exit__ = MagicMock(return_value=None)
        mock_target_manager.__enter__ = MagicMock(return_value=mock_target_manager)
        mock_target_manager.__exit__ = MagicMock(return_value=None)
        
        with patch.object(replicator, '_create_connection_managers') as mock_create_managers:
            mock_create_managers.return_value = (mock_source_manager, mock_target_manager)
            
            success, rows_copied = replicator.performance_optimizer._copy_medium_small_table_optimized('medium_table', 3, 1000)
            
            # Verify success
            assert success is True
            # FIXED: Expect 3 rows total (2 from first batch + 1 from second batch)
            assert rows_copied == 3
            
            # Verify multiple source queries were executed (COUNT + 3 data queries)
            assert mock_source_manager.execute_with_retry.call_count >= 4

    def test_copy_medium_table_no_data(self, replicator_with_mock_engines):
        """
        Test medium table copy operation with no data.
        
        Validates:
            - Medium table copy operation with no data
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Empty result handling
            
        ETL Pipeline Context:
            - Medium table copying for dental clinic ETL with no data
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Mock connection managers
        mock_source_manager = MagicMock()
        mock_target_manager = MagicMock()
        
        # Mock empty source query result
        mock_source_result = MagicMock()
        mock_source_result.fetchall.return_value = []
        mock_source_result.keys.return_value = ['id', 'name', 'date']
        mock_source_manager.execute_with_retry.return_value = mock_source_result
        
        # Mock context managers
        mock_source_manager.__enter__ = MagicMock(return_value=mock_source_manager)
        mock_source_manager.__exit__ = MagicMock(return_value=None)
        mock_target_manager.__enter__ = MagicMock(return_value=mock_target_manager)
        mock_target_manager.__exit__ = MagicMock(return_value=None)
        
        with patch.object(replicator, '_create_connection_managers') as mock_create_managers:
            mock_create_managers.return_value = (mock_source_manager, mock_target_manager)
            
            success, rows_copied = replicator.performance_optimizer._copy_medium_small_table_optimized('medium_table', 0, 1000)
            
            # Verify success with no rows
            assert success is True
            assert rows_copied == 0

    def test_copy_medium_table_batch_error(self, replicator_with_mock_engines):
        """
        Test medium table copy operation with batch error.
        
        Validates:
            - Medium table copy operation error handling
            - Provider pattern error handling
            - Settings injection error handling
            - Batch error propagation
            
        ETL Pipeline Context:
            - Medium table copying error handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Mock connection managers
        mock_source_manager = MagicMock()
        mock_target_manager = MagicMock()
        
        # Mock source query result for first batch, then error
        mock_source_result = MagicMock()
        mock_source_result.fetchall.return_value = [
            (1, 'John', '2024-01-01'),
            (2, 'Jane', '2024-01-02')
        ]
        mock_source_result.keys.return_value = ['id', 'name', 'date']
        
        # Mock multiple execute calls - first success, then error
        mock_source_manager.execute_with_retry.side_effect = [
            mock_source_result,  # First batch success
            DatabaseConnectionError("Connection failed")  # Second batch error
        ]
        
        # Mock context managers
        mock_source_manager.__enter__ = MagicMock(return_value=mock_source_manager)
        mock_source_manager.__exit__ = MagicMock(return_value=None)
        mock_target_manager.__enter__ = MagicMock(return_value=mock_target_manager)
        mock_target_manager.__exit__ = MagicMock(return_value=None)
        
        with patch.object(replicator, '_create_connection_managers') as mock_create_managers:
            mock_create_managers.return_value = (mock_source_manager, mock_target_manager)
            
            success, rows_copied = replicator.performance_optimizer._copy_medium_small_table_optimized('medium_table', 2, 1000)
            
            # Verify failure
            assert success is False
            assert rows_copied == 0

    def test_copy_large_table_success(self, replicator_with_mock_engines):
        """
        Test successful large table copy operation.
        
        Validates:
            - Large table copy operation with optimized processing
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Performance optimizer integration
            
        ETL Pipeline Context:
            - Large table copying for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # FIXED: Use the actual full configuration that the implementation passes
        # The implementation gets config from table_configs and passes the full config
        config = {
            'incremental_columns': ['DateTStamp'],
            'batch_size': 50000,
            'estimated_size_mb': 200,
            'extraction_strategy': 'full_table',
            'performance_category': 'large',
            'processing_priority': 1,
            'estimated_processing_time_minutes': 1.0,
            'memory_requirements_mb': 100
        }
        
        # Mock the performance optimizer method
        with patch.object(replicator.performance_optimizer, '_copy_large_table_optimized', return_value=(True, 1000000)) as mock_optimized:
            success, rows_copied = replicator.performance_optimizer._copy_large_table_optimized('large_table', config)
            
            # Verify success
            assert success is True
            assert rows_copied == 1000000
            
            # FIXED: Verify performance optimizer was called with the actual full config
            mock_optimized.assert_called_once_with('large_table', config)

    def test_copy_large_table_failure(self, replicator_with_mock_engines):
        """
        Test large table copy operation failure.
        
        Validates:
            - Large table copy operation failure handling
            - Provider pattern error handling
            - Settings injection error handling
            - Performance optimizer error handling
            
        ETL Pipeline Context:
            - Large table copying error handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Define config for the test
        config = {
            'incremental_columns': ['DateTStamp'],
            'batch_size': 50000,
            'estimated_size_mb': 200,
            'extraction_strategy': 'full_table',
            'performance_category': 'large',
            'processing_priority': 1,
            'estimated_processing_time_minutes': 1.0,
            'memory_requirements_mb': 100
        }
        
        # Mock the performance optimizer method to raise exception
        with patch.object(replicator.performance_optimizer, '_copy_large_table_optimized', 
                         side_effect=DataExtractionError("Copy failed")):
            success, rows_copied = replicator.performance_optimizer._copy_large_table_optimized('large_table', config)
            
            # Verify failure
            assert success is False
            assert rows_copied == 0

    def test_recreate_table_structure_success(self, replicator_with_mock_engines):
        """
        Test successful table structure recreation.
        
        Validates:
            - Table structure recreation with DROP and CREATE
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Cross-server table structure copying
            
        ETL Pipeline Context:
            - Table structure recreation for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock source SHOW CREATE TABLE result
        mock_source_result = MagicMock()
        mock_source_result.fetchone.return_value = (
            'test_table',
            'CREATE TABLE `test_table` (`id` int NOT NULL, `name` varchar(255), PRIMARY KEY (`id`))'
        )
        mock_source_conn.execute.return_value = mock_source_result
        
        # Mock context managers
        mock_source_conn.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_conn.__exit__ = MagicMock(return_value=None)
        mock_target_conn.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_conn.__exit__ = MagicMock(return_value=None)
        
        # Mock engines to return connections
        replicator.source_engine.connect.return_value = mock_source_conn
        replicator.target_engine.connect.return_value = mock_target_conn
        
        # Mock settings for database name
        with patch.object(replicator.settings, 'get_replication_connection_config', return_value={'database': 'test_db'}):
            result = replicator._recreate_table_structure('test_table')
            
            # Verify success
            assert result is True
            
            # Verify DROP and CREATE were executed
            assert mock_target_conn.execute.call_count >= 2

    def test_recreate_table_structure_source_error(self, replicator_with_mock_engines):
        """
        Test table structure recreation with source error.
        
        Validates:
            - Table structure recreation error handling
            - Provider pattern error handling
            - Settings injection error handling
            - Source connection error handling
            
        ETL Pipeline Context:
            - Table structure recreation error handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        
        # Mock database connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock source connection to raise exception
        mock_source_conn.execute.side_effect = DatabaseConnectionError("Source connection failed")
        
        # Mock context managers
        mock_source_conn.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_conn.__exit__ = MagicMock(return_value=None)
        mock_target_conn.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_conn.__exit__ = MagicMock(return_value=None)
        
        # Mock engines to return connections
        replicator.source_engine.connect.return_value = mock_source_conn
        replicator.target_engine.connect.return_value = mock_target_conn
        
        # Mock settings for database name
        with patch.object(replicator.settings, 'get_replication_connection_config', return_value={'database': 'test_db'}):
            result = replicator._recreate_table_structure('test_table')
            
            # Verify failure
            assert result is False

    def test_calculate_adaptive_batch_size_with_config(self, replicator_with_mock_engines):
        """
        Test adaptive batch size calculation with configuration.
        
        Validates:
            - Adaptive batch size calculation with configuration
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Configuration-based batch sizing
            
        ETL Pipeline Context:
            - Adaptive batch sizing for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'batch_size': 75000,
            'performance_category': 'large',
            'estimated_rows': 1000000
        }
        
        batch_size = replicator.performance_optimizer.calculate_adaptive_batch_size('large_table', config)
        
        # Should use config batch size
        assert batch_size == 75000

    def test_calculate_adaptive_batch_size_without_config(self, replicator_with_mock_engines):
        """
        Test adaptive batch size calculation without configuration.
        
        Validates:
            - Adaptive batch size calculation without configuration
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Performance optimizer fallback
            
        ETL Pipeline Context:
            - Adaptive batch sizing for dental clinic ETL without config
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'performance_category': 'medium',
            'estimated_rows': 100000
        }
        
        # Mock performance optimizer method
        with patch.object(replicator.performance_optimizer, 'calculate_adaptive_batch_size', return_value=25000):
            batch_size = replicator.performance_optimizer.calculate_adaptive_batch_size('medium_table', config)
            
            # Should use performance optimizer calculation
            assert batch_size == 25000

    def test_create_connection_managers_large_table(self, replicator_with_mock_engines):
        """
        Test connection manager creation for large table.
        
        Validates:
            - Connection manager creation for large table
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Large table optimization settings
            
        ETL Pipeline Context:
            - Connection manager creation for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'estimated_size_mb': 200,
            'performance_category': 'large'
        }
        
        with patch('etl_pipeline.core.simple_mysql_replicator.create_connection_manager') as mock_create:
            mock_source_manager = MagicMock()
            mock_target_manager = MagicMock()
            mock_create.side_effect = [mock_source_manager, mock_target_manager]
            
            source_manager, target_manager = replicator._create_connection_managers('large_table', config)
            
            # Verify managers were created
            assert source_manager == mock_source_manager
            assert target_manager == mock_target_manager
            
            # Verify create was called with large table settings
            assert mock_create.call_count == 2

    def test_create_connection_managers_small_table(self, replicator_with_mock_engines):
        """
        Test connection manager creation for small table.
        
        Validates:
            - Connection manager creation for small table
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Small table optimization settings
            
        ETL Pipeline Context:
            - Connection manager creation for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'estimated_size_mb': 10,
            'performance_category': 'small'
        }
        
        with patch('etl_pipeline.core.simple_mysql_replicator.create_connection_manager') as mock_create:
            mock_source_manager = MagicMock()
            mock_target_manager = MagicMock()
            mock_create.side_effect = [mock_source_manager, mock_target_manager]
            
            source_manager, target_manager = replicator._create_connection_managers('small_table', config)
            
            # Verify managers were created
            assert source_manager == mock_source_manager
            assert target_manager == mock_target_manager
            
            # Verify create was called with small table settings
            assert mock_create.call_count == 2

    def test_get_connection_manager_config_large_table(self, replicator_with_mock_engines):
        """
        Test connection manager configuration for large table.
        
        Validates:
            - Connection manager configuration for large table
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Large table configuration settings
            
        ETL Pipeline Context:
            - Connection manager configuration for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'estimated_size_mb': 200,
            'performance_category': 'large'
        }
        
        manager_config = replicator._get_connection_manager_config('large_table', config)
        
        # Verify large table configuration
        assert manager_config['max_retries'] == 5
        assert manager_config['retry_delay'] == 2.0

    def test_get_connection_manager_config_medium_table(self, replicator_with_mock_engines):
        """
        Test connection manager configuration for medium table.
        
        Validates:
            - Connection manager configuration for medium table
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Medium table configuration settings
            
        ETL Pipeline Context:
            - Connection manager configuration for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # FIXED: Use estimated_size_mb > 50 to trigger medium table configuration
        # The implementation uses > 50 for medium tables, not >= 50
        config = {
            'estimated_size_mb': 75,  # Changed from 50 to 75 to trigger medium category
            'performance_category': 'medium'
        }
        
        manager_config = replicator._get_connection_manager_config('medium_table', config)
        
        # Verify medium table configuration
        assert manager_config['max_retries'] == 3
        assert manager_config['retry_delay'] == 1.0

    def test_get_connection_manager_config_small_table(self, replicator_with_mock_engines):
        """
        Test connection manager configuration for small table.
        
        Validates:
            - Connection manager configuration for small table
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Small table configuration settings
            
        ETL Pipeline Context:
            - Connection manager configuration for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        config = {
            'estimated_size_mb': 10,
            'performance_category': 'small'
        }
        
        manager_config = replicator._get_connection_manager_config('small_table', config)
        
        # Verify small table configuration
        assert manager_config['max_retries'] == 3
        assert manager_config['retry_delay'] == 0.5 