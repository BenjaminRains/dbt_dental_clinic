"""
Unit tests for SimpleMySQLReplicator bulk operations using provider pattern.

This module tests the SimpleMySQLReplicator bulk operations with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests bulk table copying with different scenarios
    - Tests importance-based filtering and result aggregation
    - Validates error handling in bulk operations

Coverage Areas:
    - Bulk table copying with provider pattern
    - Importance-based filtering and result aggregation
    - Error handling and logging for dental clinic ETL operations
    - Settings injection for environment-agnostic connections
    - Provider pattern configuration isolation

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


class TestSimpleMySQLReplicatorBulkOperations:
    """Unit tests for SimpleMySQLReplicator bulk operations using provider pattern."""
    
    @pytest.fixture
    def replicator_with_bulk_config(self, test_settings):
        """Create replicator with bulk operation configuration using provider pattern."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with test data (updated to remove table_importance)
            mock_config = {
                'patient': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'medium',
                    'processing_priority': 1,
                    'estimated_processing_time_minutes': 5,
                    'memory_requirements_mb': 100
                },
                'appointment': {
                    'incremental_columns': ['AptDateTime'],
                    'batch_size': 500,
                    'estimated_size_mb': 25,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'small',
                    'processing_priority': 2,
                    'estimated_processing_time_minutes': 3,
                    'memory_requirements_mb': 50
                },
                'procedurelog': {
                    'incremental_columns': ['ProcDate'],
                    'batch_size': 2000,
                    'estimated_size_mb': 100,
                    'extraction_strategy': 'full_table',
                    'performance_category': 'large',
                    'processing_priority': 3,
                    'estimated_processing_time_minutes': 10,
                    'memory_requirements_mb': 200
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Create replicator instance
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_copy_all_tables_success(self, replicator_with_bulk_config):
        """
        Test successful copying of all tables using provider pattern.
        
        Validates:
            - Bulk table copying with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Result aggregation and logging
            
        ETL Pipeline Context:
            - Bulk operations for complete data refresh
            - Optimized for dental clinic data with multiple tables
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success for all tables
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_all_tables()
            
            # Verify all tables were processed
            assert len(results) == 3
            assert all(results.values())  # All successful
            
            # Verify copy_table was called for each table
            assert mock_copy.call_count == 3
            expected_tables = ['patient', 'appointment', 'procedurelog']
            for table in expected_tables:
                mock_copy.assert_any_call(table)

    def test_copy_all_tables_with_filter(self, replicator_with_bulk_config):
        """
        Test copying all tables with filter using provider pattern.
        
        Validates:
            - Filtered table copying with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Filter logic and result aggregation
            
        ETL Pipeline Context:
            - Selective table copying for targeted updates
            - Optimized for dental clinic data with specific table needs
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            table_filter = ['patient', 'appointment']
            results = replicator.copy_all_tables(table_filter)
            
            # Verify only filtered tables were processed
            assert len(results) == 2
            assert all(results.values())  # All successful
            
            # Verify copy_table was called only for filtered tables
            assert mock_copy.call_count == 2
            mock_copy.assert_any_call('patient')
            mock_copy.assert_any_call('appointment')

    def test_copy_all_tables_with_invalid_filter(self, replicator_with_bulk_config):
        """
        Test copying all tables with invalid filter using provider pattern.
        
        Validates:
            - Invalid filter handling with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Filter validation and result aggregation
            
        ETL Pipeline Context:
            - Error handling for invalid table filters
            - Optimized for dental clinic data with configuration validation
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            table_filter = ['unknown_table', 'another_unknown']
            results = replicator.copy_all_tables(table_filter)
            
            # Verify unknown tables return False (new behavior)
            assert len(results) == 2
            assert results['unknown_table'] is False
            assert results['another_unknown'] is False
            assert mock_copy.call_count == 0

    def test_copy_all_tables_mixed_results(self, replicator_with_bulk_config):
        """
        Test copying all tables with mixed success/failure results using provider pattern.
        
        Validates:
            - Mixed result handling with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Result aggregation and logging
            
        ETL Pipeline Context:
            - Partial success handling for dental clinic ETL reliability
            - Optimized for dental clinic data with error tolerance
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return mixed results
        def mock_copy_table(table_name):
            return table_name in ['patient', 'appointment']  # Only first two succeed
        
        with patch.object(replicator, 'copy_table', side_effect=mock_copy_table) as mock_copy:
            results = replicator.copy_all_tables()
            
            # Verify mixed results
            assert len(results) == 3
            assert results['patient'] is True
            assert results['appointment'] is True
            assert results['procedurelog'] is False
            
            # Verify copy_table was called for each table
            assert mock_copy.call_count == 3

    def test_copy_all_tables_all_failures(self, replicator_with_bulk_config):
        """
        Test copying all tables with all failures using provider pattern.
        
        Validates:
            - All failure handling with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Result aggregation and logging
            
        ETL Pipeline Context:
            - Complete failure handling for dental clinic ETL reliability
            - Optimized for dental clinic data with error reporting
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return failure for all tables
        with patch.object(replicator, 'copy_table', return_value=False) as mock_copy:
            results = replicator.copy_all_tables()
            
            # Verify all failures
            assert len(results) == 3
            assert not any(results.values())  # All failed
            
            # Verify copy_table was called for each table
            assert mock_copy.call_count == 3

    def test_copy_tables_by_processing_priority_high(self, replicator_with_bulk_config):
        """
        Test copying tables by high processing priority using provider pattern.
        
        Validates:
            - Priority-based filtering with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Priority level filtering logic
            
        ETL Pipeline Context:
            - High priority table copying for dental clinic data priority
            - Optimized for dental clinic data with priority levels
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_tables_by_processing_priority(max_priority=1)
            
            # Verify only high priority tables were processed (priority 1)
            assert len(results) == 1
            assert results['patient'] is True
            
            # Verify copy_table was called only for high priority tables
            assert mock_copy.call_count == 1
            mock_copy.assert_called_once_with('patient')

    def test_copy_tables_by_processing_priority_medium(self, replicator_with_bulk_config):
        """
        Test copying tables by medium processing priority using provider pattern.
        
        Validates:
            - Priority-based filtering with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Priority level filtering logic
            
        ETL Pipeline Context:
            - Medium priority table copying for dental clinic data priority
            - Optimized for dental clinic data with priority levels
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_tables_by_processing_priority(max_priority=2)
            
            # Verify only medium priority tables were processed (priority 1-2)
            assert len(results) == 2
            assert results['patient'] is True
            assert results['appointment'] is True
            
            # Verify copy_table was called only for medium priority tables
            assert mock_copy.call_count == 2
            mock_copy.assert_any_call('patient')
            mock_copy.assert_any_call('appointment')

    def test_copy_tables_by_processing_priority_all(self, replicator_with_bulk_config):
        """
        Test copying tables by all processing priorities using provider pattern.
        
        Validates:
            - Priority-based filtering with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Priority level filtering logic
            
        ETL Pipeline Context:
            - All priority table copying for dental clinic data priority
            - Optimized for dental clinic data with priority levels
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_tables_by_processing_priority(max_priority=10)
            
            # Verify all tables were processed (priority 1-3)
            assert len(results) == 3
            assert results['patient'] is True
            assert results['appointment'] is True
            assert results['procedurelog'] is True
            
            # Verify copy_table was called for all tables
            assert mock_copy.call_count == 3
            expected_tables = ['patient', 'appointment', 'procedurelog']
            for table in expected_tables:
                mock_copy.assert_any_call(table)

    def test_copy_tables_by_performance_category_large(self, replicator_with_bulk_config):
        """
        Test copying tables by large performance category using provider pattern.
        
        Validates:
            - Performance category filtering with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Category filtering logic
            
        ETL Pipeline Context:
            - Large table copying for dental clinic data priority
            - Optimized for dental clinic data with performance categories
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_tables_by_performance_category('large')
            
            # Verify only large tables were processed
            assert len(results) == 1
            assert results['procedurelog'] is True
            
            # Verify copy_table was called only for large tables
            assert mock_copy.call_count == 1
            mock_copy.assert_called_once_with('procedurelog')

    def test_copy_tables_by_performance_category_small(self, replicator_with_bulk_config):
        """
        Test copying tables by small performance category using provider pattern.
        
        Validates:
            - Performance category filtering with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Category filtering logic
            
        ETL Pipeline Context:
            - Small table copying for dental clinic data priority
            - Optimized for dental clinic data with performance categories
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_tables_by_performance_category('small')
            
            # Verify only small tables were processed
            assert len(results) == 1
            assert results['appointment'] is True
            
            # Verify copy_table was called only for small tables
            assert mock_copy.call_count == 1
            mock_copy.assert_called_once_with('appointment')

    def test_copy_tables_by_performance_category_medium(self, replicator_with_bulk_config):
        """
        Test copying tables by medium performance category using provider pattern.
        
        Validates:
            - Performance category filtering with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Category filtering logic
            
        ETL Pipeline Context:
            - Medium table copying for dental clinic data priority
            - Optimized for dental clinic data with performance categories
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_tables_by_performance_category('medium')
            
            # Verify only medium tables were processed
            assert len(results) == 1
            assert results['patient'] is True
            
            # Verify copy_table was called only for medium tables
            assert mock_copy.call_count == 1
            mock_copy.assert_called_once_with('patient')

    def test_copy_tables_by_performance_category_invalid(self, replicator_with_bulk_config):
        """
        Test copying tables by invalid performance category using provider pattern.
        
        Validates:
            - Invalid category handling with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Error handling for invalid categories
            
        ETL Pipeline Context:
            - Invalid category handling for dental clinic ETL reliability
            - Optimized for dental clinic data with configuration validation
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return success
        with patch.object(replicator, 'copy_table', return_value=True) as mock_copy:
            results = replicator.copy_tables_by_performance_category('invalid_category')
            
            # Verify no tables were processed for invalid category
            assert len(results) == 0
            assert mock_copy.call_count == 0

    def test_copy_tables_by_performance_category_mixed_results(self, replicator_with_bulk_config):
        """
        Test copying tables by performance category with mixed results using provider pattern.
        
        Validates:
            - Mixed result handling with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Result aggregation and logging
            
        ETL Pipeline Context:
            - Partial success handling for dental clinic ETL reliability
            - Optimized for dental clinic data with error tolerance
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return mixed results
        def mock_copy_table(table_name):
            return table_name == 'patient'  # Only patient succeeds
        
        with patch.object(replicator, 'copy_table', side_effect=mock_copy_table) as mock_copy:
            results = replicator.copy_tables_by_performance_category('medium')
            
            # Verify mixed results
            assert len(results) == 1
            assert results['patient'] is True
            
            # Verify copy_table was called for the medium table
            assert mock_copy.call_count == 1

    def test_copy_tables_by_performance_category_all_failures(self, replicator_with_bulk_config):
        """
        Test copying tables by performance category with all failures using provider pattern.
        
        Validates:
            - All failure handling with provider pattern
            - Settings injection for database operations
            - Provider pattern configuration access
            - Result aggregation and logging
            
        ETL Pipeline Context:
            - Complete failure handling for dental clinic ETL reliability
            - Optimized for dental clinic data with error reporting
            - Uses provider pattern for configuration access
        """
        replicator = replicator_with_bulk_config
        
        # Mock copy_table method to return failure for all tables
        with patch.object(replicator, 'copy_table', return_value=False) as mock_copy:
            results = replicator.copy_tables_by_performance_category('large')
            
            # Verify all failures
            assert len(results) == 1
            assert not any(results.values())  # All failed
            
            # Verify copy_table was called for the large table
            assert mock_copy.call_count == 1 