"""
Unit tests for SimpleMySQLReplicator performance tracking and reporting methods using provider pattern.

This module tests the SimpleMySQLReplicator performance tracking and reporting methods with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests performance tracking and metrics collection
    - Tests performance reporting and schema analyzer summary
    - Validates logging and reporting functionality

Coverage Areas:
    - Performance metrics tracking
    - Performance reporting generation
    - Schema analyzer summary generation
    - Performance history management
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


class TestPerformanceTrackingAndReporting:
    """Unit tests for SimpleMySQLReplicator performance tracking and reporting methods using provider pattern."""
    
    @pytest.fixture
    def replicator_with_mock_engines(self, test_settings):
        """Create replicator with mocked database engines for testing performance tracking and reporting."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine), \
             patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
            
            # Mock configuration with comprehensive test data
            mock_config = {
                'patient': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'medium',
                    'processing_priority': 5,
                    'estimated_processing_time_minutes': 0.1,
                    'memory_requirements_mb': 10,
                    'estimated_rows': 100000,
                    'time_gap_threshold_days': 7
                },
                'appointment': {
                    'incremental_columns': ['AptDateTime'],
                    'batch_size': 500,
                    'estimated_size_mb': 25,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'small',
                    'processing_priority': 3,
                    'estimated_processing_time_minutes': 0.05,
                    'memory_requirements_mb': 5,
                    'estimated_rows': 50000,
                    'time_gap_threshold_days': 3
                },
                'procedurelog': {
                    'incremental_columns': ['ProcDate'],
                    'batch_size': 2000,
                    'estimated_size_mb': 100,
                    'extraction_strategy': 'full_table',
                    'performance_category': 'large',
                    'processing_priority': 1,
                    'estimated_processing_time_minutes': 0.5,
                    'memory_requirements_mb': 50,
                    'estimated_rows': 500000,
                    'time_gap_threshold_days': 30
                },
                'tiny_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 100,
                    'estimated_size_mb': 1,
                    'extraction_strategy': 'full_table',
                    'performance_category': 'tiny',
                    'processing_priority': 10,
                    'estimated_processing_time_minutes': 0.01,
                    'memory_requirements_mb': 1,
                    'estimated_rows': 1000,
                    'time_gap_threshold_days': 1
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config):
                # Create replicator instance
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                return replicator

    def test_track_performance_metrics_success(self, replicator_with_mock_engines):
        """
        Test successful performance metrics tracking.
        
        Validates:
            - Performance metrics tracking with detailed information
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Performance history storage
            
        ETL Pipeline Context:
            - Performance metrics tracking for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test performance tracking
        table_name = 'patient'
        strategy = 'incremental'
        duration = 30.5
        memory_mb = 100.0
        rows_processed = 50000
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            replicator.track_performance_metrics(table_name, strategy, duration, memory_mb, rows_processed)
            
            # Verify performance metrics were stored
            assert hasattr(replicator, 'performance_metrics')
            assert table_name in replicator.performance_metrics
            
            metrics = replicator.performance_metrics[table_name]
            assert metrics['strategy'] == strategy
            assert metrics['duration'] == duration
            assert metrics['memory_mb'] == memory_mb
            assert metrics['rows_processed'] == rows_processed
            assert metrics['rows_per_second'] == rows_processed / duration
            assert 'timestamp' in metrics
            
            # Verify logging
            mock_logger.info.assert_called_once()
            log_call = mock_logger.info.call_args[0][0]
            assert "Copy performance metrics" in log_call
            assert table_name in log_call
            assert strategy in log_call

    def test_track_performance_metrics_multiple_tables(self, replicator_with_mock_engines):
        """
        Test performance metrics tracking for multiple tables.
        
        Validates:
            - Performance metrics tracking for multiple tables
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Multiple table performance history storage
            
        ETL Pipeline Context:
            - Performance metrics tracking for dental clinic ETL with multiple tables
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Track metrics for multiple tables
        tables_data = [
            ('patient', 'incremental', 30.0, 100.0, 50000),
            ('appointment', 'incremental', 15.0, 50.0, 25000),
            ('procedurelog', 'full_table', 60.0, 200.0, 100000)
        ]
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            for table_name, strategy, duration, memory_mb, rows_processed in tables_data:
                replicator.track_performance_metrics(table_name, strategy, duration, memory_mb, rows_processed)
            
            # Verify all tables were tracked
            assert len(replicator.performance_metrics) == 3
            
            for table_name, _, _, _, _ in tables_data:
                assert table_name in replicator.performance_metrics
                
            # Verify logging was called for each table
            assert mock_logger.info.call_count == 3

    def test_track_performance_metrics_zero_duration(self, replicator_with_mock_engines):
        """
        Test performance metrics tracking with zero duration.
        
        Validates:
            - Performance metrics tracking with zero duration
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Zero duration handling
            
        ETL Pipeline Context:
            - Performance metrics tracking for dental clinic ETL with zero duration
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test with zero duration
        table_name = 'patient'
        strategy = 'incremental'
        duration = 0.0
        memory_mb = 100.0
        rows_processed = 50000
        
        with patch('etl_pipeline.core.simple_mysql_replicator.logger') as mock_logger:
            replicator.track_performance_metrics(table_name, strategy, duration, memory_mb, rows_processed)
            
            # Verify performance metrics were stored
            metrics = replicator.performance_metrics[table_name]
            assert metrics['rows_per_second'] == 0  # Should handle zero duration

    def test_get_performance_report_no_metrics(self, replicator_with_mock_engines):
        """
        Test performance report generation with no metrics.
        
        Validates:
            - Performance report generation with no metrics
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Empty metrics handling
            
        ETL Pipeline Context:
            - Performance report generation for dental clinic ETL with no metrics
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Ensure no performance metrics exist
        if hasattr(replicator, 'performance_metrics'):
            del replicator.performance_metrics
        
        report = replicator.get_performance_report()
        
        # Verify report message
        assert "No copy performance metrics available" in report

    def test_get_performance_report_with_metrics(self, replicator_with_mock_engines):
        """
        Test performance report generation with metrics.
        
        Validates:
            - Performance report generation with metrics
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Metrics-based report generation
            
        ETL Pipeline Context:
            - Performance report generation for dental clinic ETL with metrics
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Add performance metrics
        replicator.performance_metrics = {
            'patient': {
                'strategy': 'incremental',
                'duration': 30.0,
                'rows_processed': 50000,
                'rows_per_second': 1667,
                'memory_mb': 100.0,
                'timestamp': datetime.now()
            },
            'appointment': {
                'strategy': 'incremental',
                'duration': 15.0,
                'rows_processed': 25000,
                'rows_per_second': 1667,
                'memory_mb': 50.0,
                'timestamp': datetime.now()
            }
        }
        
        report = replicator.get_performance_report()
        
        # Verify report structure
        assert "# MySQL Copy Performance Report" in report
        assert "## patient" in report
        assert "## appointment" in report
        assert "- Strategy: incremental" in report
        assert "Duration: 30.00s" in report
        assert "Rows Processed: 50,000" in report
        assert "Rows/Second: 1667" in report
        assert "Memory Usage: 100.0MB" in report

    def test_get_schema_analyzer_summary_success(self, replicator_with_mock_engines):
        """
        Test successful schema analyzer summary generation.
        
        Validates:
            - Schema analyzer summary generation
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Configuration analysis and reporting
            
        ETL Pipeline Context:
            - Schema analyzer summary for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        summary = replicator.get_schema_analyzer_summary()
        
        # Verify summary structure
        assert "# Schema Analyzer Configuration Summary" in summary
        assert "## Performance Categories" in summary
        assert "## Processing Priorities" in summary
        assert "## Extraction Strategies" in summary
        assert "## Overall Statistics" in summary
        assert "## Top Tables by Estimated Processing Time" in summary
        
        # Verify performance categories
        assert "large: 1 tables" in summary
        assert "medium: 1 tables" in summary
        assert "small: 1 tables" in summary
        assert "tiny: 1 tables" in summary
        
        # Verify processing priorities
        assert "Priority 1: 1 tables" in summary
        assert "Priority 3: 1 tables" in summary
        assert "Priority 5: 1 tables" in summary
        assert "Priority 10: 1 tables" in summary
        
        # Verify extraction strategies
        assert "full_table: 2 tables" in summary
        assert "incremental: 2 tables" in summary
        
        # Verify overall statistics
        assert "Total Tables: 4" in summary
        assert "Total Estimated Rows: 651,000" in summary
        assert "Total Estimated Size: 176.0MB" in summary

    def test_get_schema_analyzer_summary_empty_config(self, replicator_with_mock_engines):
        """
        Test schema analyzer summary generation with empty configuration.
        
        Validates:
            - Schema analyzer summary generation with empty configuration
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Empty configuration handling
            
        ETL Pipeline Context:
            - Schema analyzer summary for dental clinic ETL with empty config
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Clear table configs
        replicator.table_configs = {}
        
        summary = replicator.get_schema_analyzer_summary()
        
        # Verify summary structure
        assert "# Schema Analyzer Configuration Summary" in summary
        assert "## Performance Categories" in summary
        assert "## Processing Priorities" in summary
        assert "## Extraction Strategies" in summary
        assert "## Overall Statistics" in summary
        
        # Verify empty statistics
        assert "Total Tables: 0" in summary
        assert "Total Estimated Rows: 0" in summary
        assert "Total Estimated Size: 0.0MB" in summary

    def test_get_schema_analyzer_summary_missing_fields(self, replicator_with_mock_engines):
        """
        Test schema analyzer summary generation with missing fields.
        
        Validates:
            - Schema analyzer summary generation with missing fields
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Missing field handling
            
        ETL Pipeline Context:
            - Schema analyzer summary for dental clinic ETL with missing fields
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Add table with missing fields
        replicator.table_configs['incomplete_table'] = {
            'incremental_columns': ['DateTStamp']
            # Missing performance_category, processing_priority, etc.
        }
        
        summary = replicator.get_schema_analyzer_summary()
        
        # Verify summary still generates without errors
        assert "# Schema Analyzer Configuration Summary" in summary
        assert "Total Tables: 5" in summary  # 4 original + 1 incomplete

    def test_performance_metrics_initialization(self, replicator_with_mock_engines):
        """
        Test performance metrics initialization.
        
        Validates:
            - Performance metrics initialization
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Metrics storage initialization
            
        ETL Pipeline Context:
            - Performance metrics initialization for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Verify performance metrics is initialized as empty dict
        assert not hasattr(replicator, 'performance_metrics')
        
        # Track first metric to initialize
        replicator.track_performance_metrics('test_table', 'full_table', 10.0, 50.0, 1000)
        
        # Verify performance metrics was initialized
        assert hasattr(replicator, 'performance_metrics')
        assert isinstance(replicator.performance_metrics, dict)
        assert 'test_table' in replicator.performance_metrics

    def test_performance_metrics_overwrite(self, replicator_with_mock_engines):
        """
        Test performance metrics overwriting for same table.
        
        Validates:
            - Performance metrics overwriting for same table
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Metrics overwriting behavior
            
        ETL Pipeline Context:
            - Performance metrics overwriting for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Track metrics for same table twice
        replicator.track_performance_metrics('test_table', 'full_table', 10.0, 50.0, 1000)
        replicator.track_performance_metrics('test_table', 'incremental', 20.0, 100.0, 2000)
        
        # Verify only the latest metrics are stored
        assert len(replicator.performance_metrics) == 1
        assert 'test_table' in replicator.performance_metrics
        
        metrics = replicator.performance_metrics['test_table']
        assert metrics['strategy'] == 'incremental'
        assert metrics['duration'] == 20.0
        assert metrics['rows_processed'] == 2000

    def test_performance_report_formatting(self, replicator_with_mock_engines):
        """
        Test performance report formatting and structure.
        
        Validates:
            - Performance report formatting and structure
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Report formatting consistency
            
        ETL Pipeline Context:
            - Performance report formatting for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Add performance metrics with specific values
        replicator.performance_metrics = {
            'patient': {
                'strategy': 'incremental',
                'duration': 30.5,
                'rows_processed': 50000,
                'rows_per_second': 1639.34,
                'memory_mb': 100.5,
                'timestamp': datetime.now()
            }
        }
        
        report = replicator.get_performance_report()
        
        # Verify report formatting
        lines = report.split('\n')
        
        # Check header
        assert lines[0] == "# MySQL Copy Performance Report"
        assert lines[1] == ""
        
        # Check table section
        assert "## patient" in lines
        assert "- Strategy: incremental" in lines
        assert "- Duration: 30.50s" in lines
        assert "- Rows Processed: 50,000" in lines
        assert "- Rows/Second: 1639" in lines
        assert "- Memory Usage: 100.5MB" in lines

    def test_schema_analyzer_summary_formatting(self, replicator_with_mock_engines):
        """
        Test schema analyzer summary formatting and structure.
        
        Validates:
            - Schema analyzer summary formatting and structure
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Summary formatting consistency
            
        ETL Pipeline Context:
            - Schema analyzer summary formatting for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        summary = replicator.get_schema_analyzer_summary()
        
        # Verify summary formatting
        lines = summary.split('\n')
        
        # Check header
        assert lines[0] == "# Schema Analyzer Configuration Summary"
        assert lines[1] == ""
        
        # Check sections exist
        section_headers = [
            "## Performance Categories",
            "## Processing Priorities", 
            "## Extraction Strategies",
            "## Overall Statistics",
            "## Top Tables by Estimated Processing Time"
        ]
        
        for header in section_headers:
            assert header in lines

    def test_performance_metrics_calculation_accuracy(self, replicator_with_mock_engines):
        """
        Test performance metrics calculation accuracy.
        
        Validates:
            - Performance metrics calculation accuracy
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Calculation precision
            
        ETL Pipeline Context:
            - Performance metrics calculation for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test with specific values
        table_name = 'test_table'
        strategy = 'full_table'
        duration = 60.0  # 60 seconds
        memory_mb = 200.0
        rows_processed = 120000  # 120K rows
        
        replicator.track_performance_metrics(table_name, strategy, duration, memory_mb, rows_processed)
        
        # Verify calculations
        metrics = replicator.performance_metrics[table_name]
        expected_rows_per_second = 120000 / 60.0  # 2000 rows/sec
        
        assert metrics['rows_per_second'] == expected_rows_per_second
        assert metrics['duration'] == duration
        assert metrics['memory_mb'] == memory_mb
        assert metrics['rows_processed'] == rows_processed

    def test_performance_metrics_edge_cases(self, replicator_with_mock_engines):
        """
        Test performance metrics edge cases.
        
        Validates:
            - Performance metrics edge case handling
            - Provider pattern configuration access
            - Settings injection for configuration retrieval
            - Edge case robustness
            
        ETL Pipeline Context:
            - Performance metrics edge cases for dental clinic ETL
            - Provider pattern for configuration access
            - Settings injection for environment-agnostic operation
        """
        replicator = replicator_with_mock_engines
        
        # Test edge cases
        edge_cases = [
            ('zero_duration', 'full_table', 0.0, 0.0, 0),
            ('zero_memory', 'incremental', 10.0, 0.0, 1000),
            ('zero_rows', 'full_table', 10.0, 50.0, 0),
            ('very_large', 'full_table', 3600.0, 1000.0, 1000000),  # 1 hour, 1M rows
        ]
        
        for table_name, strategy, duration, memory_mb, rows_processed in edge_cases:
            replicator.track_performance_metrics(table_name, strategy, duration, memory_mb, rows_processed)
            
            # Verify metrics were stored
            assert table_name in replicator.performance_metrics
            
            metrics = replicator.performance_metrics[table_name]
            assert metrics['strategy'] == strategy
            assert metrics['duration'] == duration
            assert metrics['memory_mb'] == memory_mb
            assert metrics['rows_processed'] == rows_processed
            
            # Verify rows_per_second calculation handles edge cases
            if duration > 0:
                expected_rate = rows_processed / duration
            else:
                expected_rate = 0
            assert metrics['rows_per_second'] == expected_rate 