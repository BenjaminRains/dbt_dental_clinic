"""
Unit tests for SimpleMySQLReplicator PerformanceOptimizations class using provider pattern.

This module tests the PerformanceOptimizations class with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests performance optimization methods and adaptive batch sizing
    - Tests bulk operations and ultra-fast operations
    - Validates performance tracking and optimization logic

Coverage Areas:
    - Performance optimization methods with provider pattern
    - Adaptive batch sizing based on table characteristics
    - Bulk operations and ultra-fast operations
    - Performance tracking and optimization logic
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
from datetime import datetime, timedelta

# Import ETL pipeline components
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator, PerformanceOptimizations
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
    clinic_connection_settings
)
from tests.fixtures.env_fixtures import (
    test_settings,
    clinic_settings,
    test_env_provider,
    clinic_env_provider
)
from tests.fixtures.config_fixtures import (
    test_pipeline_config,
    test_tables_config
)


class TestPerformanceOptimizations:
    """Unit tests for PerformanceOptimizations class using provider pattern."""
    
    @pytest.fixture
    def replicator_with_mock_engines(self, test_settings):
        """Create replicator with mocked database engines for testing performance optimizations."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with test data
            mock_config = {
                'large_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 50000,
                    'estimated_size_mb': 200,
                    'extraction_strategy': 'full_table',
                    'performance_category': 'large',
                    'processing_priority': 1,
                    'estimated_processing_time_minutes': 10,
                    'memory_requirements_mb': 500,
                    'estimated_rows': 1000000,
                    'time_gap_threshold_days': 30
                },
                'medium_table': {
                    'incremental_columns': ['AptDateTime'],
                    'batch_size': 25000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'medium',
                    'processing_priority': 5,
                    'estimated_processing_time_minutes': 2,
                    'memory_requirements_mb': 100,
                    'estimated_rows': 100000,
                    'time_gap_threshold_days': 7
                },
                'small_table': {
                    'incremental_columns': ['ProcDate'],
                    'batch_size': 10000,
                    'estimated_size_mb': 10,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'small',
                    'processing_priority': 8,
                    'estimated_processing_time_minutes': 0.5,
                    'memory_requirements_mb': 25,
                    'estimated_rows': 10000,
                    'time_gap_threshold_days': 3
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Create replicator instance
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_performance_optimizations_initialization(self, replicator_with_mock_engines):
        """
        Test PerformanceOptimizations initialization using provider pattern.
        
        Validates:
            - PerformanceOptimizations initialization with provider pattern
            - Settings injection for environment-agnostic operation
            - Performance history initialization
            - Bulk operation settings configuration
            
        ETL Pipeline Context:
            - PerformanceOptimizations for dental clinic ETL
            - Settings injection for environment-agnostic operation
            - Provider pattern for performance optimization management
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Verify initialization
        assert optimizer.replicator == replicator
        assert optimizer.performance_history == {}
        assert optimizer.batch_performance_threshold == 100
        assert optimizer.bulk_insert_buffer_size == 268435456  # 256MB
        assert optimizer.max_bulk_batch_size == 100000
        assert optimizer.min_bulk_batch_size == 1000

    def test_calculate_adaptive_batch_size_with_config_batch_size(self, replicator_with_mock_engines):
        """
        Test adaptive batch size calculation using configured batch size.
        
        Validates:
            - Batch size calculation using schema analyzer configuration
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Direct batch size usage when available
            
        ETL Pipeline Context:
            - Adaptive batch sizing for dental clinic ETL
            - Schema analyzer integration for performance optimization
            - Provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Test with config batch size
        config = {'batch_size': 75000}
        batch_size = optimizer.calculate_adaptive_batch_size('large_table', config)
        
        # Should use config batch size directly
        assert batch_size == 75000

    def test_calculate_adaptive_batch_size_large_category(self, replicator_with_mock_engines):
        """
        Test adaptive batch size calculation for large performance category.
        
        Validates:
            - Large category batch size calculation
            - Performance category-based sizing
            - Provider pattern configuration access
            - Estimated rows consideration
            
        ETL Pipeline Context:
            - Large table optimization for dental clinic ETL
            - Performance category-based sizing for efficiency
            - Provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Test large category
        config = {
            'performance_category': 'large',
            'estimated_rows': 1500000
        }
        batch_size = optimizer.calculate_adaptive_batch_size('large_table', config)
        
        # Should use large category base batch size
        assert batch_size == 100000  # max_bulk_batch_size

    def test_calculate_adaptive_batch_size_medium_category(self, replicator_with_mock_engines):
        """
        Test adaptive batch size calculation for medium performance category.
        
        Validates:
            - Medium category batch size calculation
            - Performance category-based sizing
            - Provider pattern configuration access
            - Estimated rows consideration
            
        ETL Pipeline Context:
            - Medium table optimization for dental clinic ETL
            - Performance category-based sizing for efficiency
            - Provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Test medium category
        config = {
            'performance_category': 'medium',
            'estimated_rows': 500000
        }
        batch_size = optimizer.calculate_adaptive_batch_size('medium_table', config)
        
        # Should use medium category base batch size
        assert batch_size == 50000

    def test_calculate_adaptive_batch_size_small_category(self, replicator_with_mock_engines):
        """
        Test adaptive batch size calculation for small performance category.
        
        Validates:
            - Small category batch size calculation
            - Performance category-based sizing
            - Provider pattern configuration access
            - Estimated rows consideration
            
        ETL Pipeline Context:
            - Small table optimization for dental clinic ETL
            - Performance category-based sizing for efficiency
            - Provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Test small category
        config = {
            'performance_category': 'small',
            'estimated_rows': 50000
        }
        batch_size = optimizer.calculate_adaptive_batch_size('small_table', config)
        
        # Should use small category base batch size
        assert batch_size == 25000

    def test_calculate_adaptive_batch_size_with_performance_history(self, replicator_with_mock_engines):
        """
        Test adaptive batch size calculation using performance history.
        
        Validates:
            - Performance history-based batch size adjustment
            - Provider pattern configuration access
            - Historical performance consideration
            - Optimal batch size calculation
            
        ETL Pipeline Context:
            - Performance history for dental clinic ETL optimization
            - Historical performance consideration for efficiency
            - Provider pattern for configuration access
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Add performance history
        optimizer.performance_history['test_table'] = {
            'records_per_second': 2000,
            'duration': 30.0,
            'memory_mb': 50.0,
            'rows_processed': 60000,
            'timestamp': datetime.now(),
            'strategy': 'optimized'
        }
        
        config = {
            'performance_category': 'medium',
            'estimated_rows': 100000
        }
        batch_size = optimizer.calculate_adaptive_batch_size('test_table', config)
        
        # Should calculate based on performance history (2000 records/sec * 30 seconds = 60000)
        # But capped at max_bulk_batch_size
        assert batch_size == 60000

    def test_should_use_full_refresh_no_incremental_columns(self, replicator_with_mock_engines):
        """
        Test full refresh determination when no incremental columns configured.
        
        Validates:
            - Full refresh when no incremental columns
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Default behavior for missing incremental columns
            
        ETL Pipeline Context:
            - Full refresh for dental clinic ETL when no incremental columns
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        config = {'incremental_columns': []}
        result = optimizer.should_use_full_refresh('test_table', config)
        
        # Should use full refresh when no incremental columns
        assert result is True

    def test_should_use_full_refresh_no_last_copy_time(self, replicator_with_mock_engines):
        """
        Test full refresh determination when no last copy time exists.
        
        Validates:
            - Full refresh when no last copy time
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Default behavior for first-time copying
            
        ETL Pipeline Context:
            - Full refresh for dental clinic ETL on first run
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Mock _get_last_copy_time to return None
        with patch.object(replicator, '_get_last_copy_time', return_value=None):
            config = {'incremental_columns': ['DateTStamp']}
            result = optimizer.should_use_full_refresh('test_table', config)
            
            # Should use full refresh when no last copy time
            assert result is True

    def test_should_use_full_refresh_large_time_gap(self, replicator_with_mock_engines):
        """
        Test full refresh determination when time gap exceeds threshold.
        
        Validates:
            - Full refresh when time gap exceeds threshold
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Time gap threshold logic
            
        ETL Pipeline Context:
            - Full refresh for dental clinic ETL with large time gaps
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Mock _get_last_copy_time to return old timestamp
        old_timestamp = datetime.now() - timedelta(days=45)  # 45 days ago
        with patch.object(replicator, '_get_last_copy_time', return_value=old_timestamp):
            config = {
                'incremental_columns': ['DateTStamp'],
                'time_gap_threshold_days': 30
            }
            result = optimizer.should_use_full_refresh('test_table', config)
            
            # Should use full refresh when time gap exceeds threshold
            assert result is True

    def test_should_use_full_refresh_small_time_gap(self, replicator_with_mock_engines):
        """
        Test incremental refresh determination when time gap is small.
        
        Validates:
            - Incremental refresh when time gap is small
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Time gap threshold logic
            
        ETL Pipeline Context:
            - Incremental refresh for dental clinic ETL with small time gaps
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Mock _get_last_copy_time to return recent timestamp
        recent_timestamp = datetime.now() - timedelta(days=5)  # 5 days ago
        with patch.object(replicator, '_get_last_copy_time', return_value=recent_timestamp):
            config = {
                'incremental_columns': ['DateTStamp'],
                'time_gap_threshold_days': 30
            }
            result = optimizer.should_use_full_refresh('test_table', config)
            
            # Should use incremental refresh when time gap is small
            assert result is False

    def test_should_use_full_refresh_slow_previous_incremental(self, replicator_with_mock_engines):
        """
        Test full refresh determination when previous incremental was slow.
        
        Validates:
            - Full refresh when previous incremental was slow
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Performance history consideration
            
        ETL Pipeline Context:
            - Full refresh for dental clinic ETL when incremental was slow
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Add slow performance history
        optimizer.performance_history['test_table'] = {
            'records_per_second': 50,  # Very slow
            'strategy': 'incremental'
        }
        
        # Mock _get_last_copy_time to return recent timestamp
        recent_timestamp = datetime.now() - timedelta(days=5)
        with patch.object(replicator, '_get_last_copy_time', return_value=recent_timestamp):
            config = {
                'incremental_columns': ['DateTStamp'],
                'time_gap_threshold_days': 30
            }
            result = optimizer.should_use_full_refresh('test_table', config)
            
            # Should use full refresh when previous incremental was slow
            assert result is True

    def test_should_use_full_refresh_small_table_with_gap(self, replicator_with_mock_engines):
        """
        Test full refresh determination for small tables with time gap.
        
        Validates:
            - Full refresh for small tables with time gap
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Small table optimization logic
            
        ETL Pipeline Context:
            - Full refresh for small dental clinic tables with time gaps
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Mock _get_last_copy_time to return timestamp with gap
        old_timestamp = datetime.now() - timedelta(days=10)  # 10 days ago
        with patch.object(replicator, '_get_last_copy_time', return_value=old_timestamp):
            config = {
                'incremental_columns': ['DateTStamp'],
                'estimated_size_mb': 50,  # Small table
                'time_gap_threshold_days': 30
            }
            result = optimizer.should_use_full_refresh('test_table', config)
            
            # Should use full refresh for small tables with gap > 7 days
            assert result is True

    def test_get_expected_rate_for_category(self, replicator_with_mock_engines):
        """
        Test expected rate calculation for different performance categories.
        
        Validates:
            - Expected rate calculation for all categories
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Category-based rate determination
            
        ETL Pipeline Context:
            - Expected rates for dental clinic ETL performance categories
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Test all categories
        assert optimizer._get_expected_rate_for_category('large') == 4000
        assert optimizer._get_expected_rate_for_category('medium') == 2500
        assert optimizer._get_expected_rate_for_category('small') == 1500
        assert optimizer._get_expected_rate_for_category('tiny') == 750
        assert optimizer._get_expected_rate_for_category('unknown') == 2000  # Default

    def test_apply_bulk_optimizations_success(self, replicator_with_mock_engines):
        """
        Test successful application of bulk optimizations.
        
        Validates:
            - Bulk optimization application
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - MySQL session optimization settings
            
        ETL Pipeline Context:
            - Bulk optimizations for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Mock database connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value = None
        mock_conn.commit.return_value = None
        
        # Mock context manager
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn
        
        # Apply bulk optimizations
        optimizer._apply_bulk_optimizations()
        
        # Verify optimizations were applied
        assert mock_conn.execute.call_count == 5
        mock_conn.commit.assert_called_once()

    def test_apply_bulk_optimizations_failure(self, replicator_with_mock_engines):
        """
        Test bulk optimization application failure handling.
        
        Validates:
            - Bulk optimization failure handling
            - Provider pattern error handling
            - Settings injection error handling
            - Graceful failure with warning
            
        ETL Pipeline Context:
            - Bulk optimization failure handling for dental clinic ETL
            - Provider pattern for error isolation
            - Settings injection for error context
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Mock database connection to raise exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = DatabaseConnectionError("Connection failed")
        
        # Mock context manager
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn
        
        # Apply bulk optimizations (should not raise exception)
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            optimizer._apply_bulk_optimizations()
            
            # Verify warning was logged
            mock_logger.warning.assert_called()

    def test_execute_bulk_operation_success(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['test_table'] = {'primary_key': 'id'}
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_cursor.fetchone.return_value = (3,)
        mock_conn.connection.connection.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn

        columns = ['id', 'name']
        rows = [(1, 'John'), (2, 'Jane'), (3, 'Bob')]
        with patch.object(replicator, '_clean_row_data', side_effect=lambda row, cols, table: row):
            result = replicator.performance_optimizer._execute_bulk_operation(
                'test_table', columns, rows, operation_type='insert'
            )
        assert result == 3

    def test_execute_bulk_operation_failure(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        replicator.table_configs['test_table'] = {'primary_key': 'id'}
        mock_conn = MagicMock()
        mock_conn.connection.connection.cursor.side_effect = DatabaseConnectionError("Connection failed")
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn

        with patch.object(replicator, '_clean_row_data', side_effect=lambda row, cols, table: row):
            with pytest.raises(DatabaseConnectionError):
                replicator.performance_optimizer._execute_bulk_operation(
                    'test_table', ['id', 'name'], [(1, 'John')], operation_type='insert'
                )

    def test_execute_table_copy_full_refresh(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['large_table']
        with patch.object(replicator, '_copy_full_table_unified', return_value=(True, 1_000_000)) as mock_full:
            result = replicator._execute_table_copy('large_table', config, 'full_table')
        assert result == (True, 1_000_000)
        mock_full.assert_called_once()

    def test_execute_table_copy_incremental(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['medium_table']
        with patch.object(replicator.performance_optimizer, 'should_use_full_refresh', return_value=False), \
             patch.object(replicator, '_copy_incremental_unified', return_value=(True, 500_000)) as mock_incremental:
            result = replicator._execute_table_copy('medium_table', config, 'incremental')
        assert result == (True, 500_000)
        mock_incremental.assert_called_once()

    def test_execute_table_copy_failure(self, replicator_with_mock_engines):
        replicator = replicator_with_mock_engines
        config = replicator.table_configs['large_table']
        with patch.object(replicator, '_copy_full_table_unified', side_effect=DataExtractionError("Copy failed")):
            success, rows = replicator._execute_table_copy('large_table', config, 'full_table')
        assert success is False
        assert rows == 0

    def test_track_performance_optimized(self, replicator_with_mock_engines):
        """
        Test optimized performance tracking.
        
        Validates:
            - Performance tracking with detailed metrics
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Performance history storage
            
        ETL Pipeline Context:
            - Performance tracking for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Test performance tracking
        table_name = 'test_table'
        duration = 30.0
        memory_mb = 100.0
        rows_processed = 60000
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            optimizer._track_performance_optimized(table_name, duration, memory_mb, rows_processed)
            
            # Verify performance history was stored
            assert table_name in optimizer.performance_history
            history = optimizer.performance_history[table_name]
            assert history['records_per_second'] == 2000  # 60000 / 30
            assert history['duration'] == duration
            assert history['memory_mb'] == memory_mb
            assert history['rows_processed'] == rows_processed
            assert history['strategy'] == 'optimized'
            
            # Verify logging
            assert mock_logger.info.call_count >= 2  # Performance metrics logged

    def test_track_performance_optimized_below_threshold(self, replicator_with_mock_engines):
        """
        Test performance tracking when below threshold.
        
        Validates:
            - Performance tracking with threshold checking
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Warning logging for poor performance
            
        ETL Pipeline Context:
            - Performance tracking for dental clinic ETL with threshold checking
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        # Test performance tracking below threshold for a low-volume table.
        # For small tables, rows/sec is not a meaningful signal and should
        # not generate a "Performance below threshold" warning.
        table_name = 'test_table'
        duration = 60.0  # Slow
        memory_mb = 50.0
        rows_processed = 1000  # Very few rows
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            optimizer._track_performance_optimized(table_name, duration, memory_mb, rows_processed)
            
            # Verify NO warning is logged for low-volume tables even if below threshold
            mock_logger.warning.assert_not_called()
            # Performance metrics should still be logged
            assert mock_logger.info.call_count >= 1

    def test_track_performance_optimized_incremental_below_threshold_no_warning(self, replicator_with_mock_engines):
        """
        Test that incremental runs below threshold do NOT log a warning.
        
        For incremental runs, records/sec is dominated by overhead (connections,
        queries) when processing small batches, so the threshold is not meaningful.
        """
        replicator = replicator_with_mock_engines
        optimizer = replicator.performance_optimizer
        
        table_name = 'test_table'
        duration = 0.5
        memory_mb = 10.0
        rows_processed = 15  # Small batch typical of incremental
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            optimizer._track_performance_optimized(
                table_name, duration, memory_mb, rows_processed, extraction_strategy='incremental'
            )
            # No warning should be logged for incremental
            mock_logger.warning.assert_not_called()
            assert mock_logger.info.call_count >= 1  # Performance metrics still logged
