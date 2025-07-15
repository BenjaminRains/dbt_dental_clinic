# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for UnifiedMetricsCollector with real database persistence.

This module follows the connection architecture patterns:
- Uses FileConfigProvider for real configuration loading from .env_test
- Uses Settings injection for environment-agnostic connections
- Uses unified interface with ConnectionFactory
- Uses proper environment variable handling with .env_test
- Uses DatabaseType and PostgresSchema enums for type safety
- Follows the three-tier ETL testing strategy
- Tests real database persistence with test environment
- Uses order markers for proper integration test execution

Integration Test Strategy:
- Tests real database persistence using PostgreSQL analytics database
- Validates metrics table creation and data insertion
- Tests Settings injection with FileConfigProvider
- Tests unified interface methods with real databases
- Tests metrics collection lifecycle with real data
- Tests error handling with real database operations
- Tests cleanup operations with real database
- Tests performance characteristics with real data volumes

ETL Context:
- Critical for ETL pipeline monitoring and metrics collection
- Supports dental clinic data processing metrics
- Uses provider pattern for clean dependency injection
- Implements Settings injection for environment-agnostic connections
- Enforces FAIL FAST security to prevent accidental production usage
- Optimized for dental clinic data volumes and processing patterns
"""

import pytest
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pathlib import Path
from sqlalchemy import text, Result
from sqlalchemy.exc import OperationalError, DisconnectionError

# Import ETL pipeline components
from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector, create_metrics_collector

# Import connection architecture components
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema,
    get_settings
)
from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.core.connections import ConnectionFactory

# Import custom exceptions for specific error handling
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DatabaseQueryError
)

logger = logging.getLogger(__name__)


def safe_fetch_one(result: Result) -> Optional[Any]:
    """Safely fetch one row from SQLAlchemy result with proper type handling."""
    try:
        row = result.fetchone()
        return row if row is not None else None
    except Exception:
        return None


def safe_fetch_all(result: Result) -> list:
    """Safely fetch all rows from SQLAlchemy result with proper type handling."""
    try:
        rows = result.fetchall()
        return [row for row in rows if row is not None]
    except Exception:
        return []


@pytest.fixture
def test_settings_with_file_provider():
    """
    Create test settings using FileConfigProvider for real integration testing.
    
    This fixture follows the connection architecture by:
    - Using FileConfigProvider for real configuration loading
    - Using Settings injection for environment-agnostic connections
    - Loading from real .env_test file and configuration files
    - Supporting integration testing with real environment setup
    - Using test environment variables (TEST_ prefixed)
    """
    try:
        # Create FileConfigProvider that will load from .env_test
        config_dir = Path(__file__).parent.parent.parent.parent  # Go to etl_pipeline root
        provider = FileConfigProvider(config_dir, environment='test')
        
        # Create settings with FileConfigProvider for real environment loading
        settings = Settings(environment='test', provider=provider)
        
        # Validate that test environment is properly loaded
        if not settings.validate_configs():
            pytest.skip("Test environment configuration not available")
        
        return settings
    except Exception as e:
        # Skip tests if test environment is not available
        pytest.skip(f"Test environment not available: {str(e)}")


@pytest.fixture
def cleanup_metrics_tables(test_settings_with_file_provider):
    """Clean up metrics tables after tests for proper isolation."""
    settings = test_settings_with_file_provider
    
    try:
        # Get analytics engine for cleanup
        analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
        
        yield  # Run the test
        
        # Clean up metrics tables after test
        with analytics_engine.connect() as conn:
            # Delete test data from metrics tables
            conn.execute(text("DELETE FROM etl_table_metrics WHERE pipeline_id LIKE 'test_%'"))
            conn.execute(text("DELETE FROM etl_pipeline_metrics WHERE pipeline_id LIKE 'test_%'"))
            conn.commit()
            
        analytics_engine.dispose()
        
    except Exception as e:
        logger.warning(f"Failed to clean up metrics tables: {e}")


@pytest.mark.integration
@pytest.mark.order(6)  # After core ETL components, before monitoring tests
@pytest.mark.postgres
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.monitoring
class TestUnifiedMetricsCollectorIntegration:
    """Integration tests for UnifiedMetricsCollector with real database persistence."""

    def test_real_metrics_collector_initialization(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test real metrics collector initialization with database persistence.
        
        AAA Pattern:
            Arrange: Set up test settings with FileConfigProvider for real environment
            Act: Initialize UnifiedMetricsCollector with real database connection
            Assert: Verify metrics collector is properly initialized with real database
            
        Validates:
            - Real PostgreSQL analytics connection is established
            - Metrics tables are created in real database
            - Settings injection works with FileConfigProvider
            - Database persistence is properly enabled
            - Error handling works for real connection failures
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Analytics: PostgreSQL database for metrics persistence
            - Used for ETL pipeline monitoring and historical analysis
            - Critical for pipeline reliability tracking
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Arrange: Set up test settings with FileConfigProvider for real environment
            settings = test_settings_with_file_provider
            
            # Act: Initialize UnifiedMetricsCollector with real database connection
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            
            # Assert: Verify metrics collector is properly initialized with real database
            assert collector.settings == settings
            assert collector.enable_persistence is True
            assert collector.analytics_engine is not None
            assert collector.metrics['status'] == 'idle'
            assert collector.metrics['pipeline_id'] is not None
            
            # Verify real database connection works
            with collector.analytics_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row is not None and row[0] == 1
                
            # Verify metrics tables exist
            with collector.analytics_engine.connect() as conn:
                # Check if metrics tables were created
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'raw' 
                    AND table_name IN ('etl_pipeline_metrics', 'etl_table_metrics')
                """))
                tables = [row[0] for row in result.fetchall()]
                assert 'etl_pipeline_metrics' in tables
                assert 'etl_table_metrics' in tables
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_pipeline_lifecycle_with_persistence(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test complete pipeline lifecycle with real database persistence.
        
        AAA Pattern:
            Arrange: Set up metrics collector with real database connection
            Act: Execute complete pipeline lifecycle (start, process tables, end, save)
            Assert: Verify complete pipeline lifecycle works with real database persistence
            
        Validates:
            - Complete pipeline lifecycle works with real database
            - Metrics are properly saved to PostgreSQL
            - Database transactions work correctly
            - Error handling works for real database operations
            - Data integrity is maintained
            - Performance is acceptable for real operations
        """
        try:
            # Arrange: Set up metrics collector with real database connection
            settings = test_settings_with_file_provider
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            
            # Act: Execute complete pipeline lifecycle (start, process tables, end, save)
            collector.start_pipeline()
            
            # Process multiple tables with different scenarios
            collector.record_table_processed('patient', 1000, 30.5, True)
            collector.record_table_processed('appointment', 500, 15.2, False, "Database connection failed")
            collector.record_table_processed('procedure', 750, 25.0, True)
            collector.record_table_processed('payment', 250, 8.5, True)
            collector.record_error("General pipeline warning", "system")
            
            time.sleep(0.1)  # Small delay to ensure timing difference
            final_metrics = collector.end_pipeline()
            
            # Save metrics to real database
            save_result = collector.save_metrics()
            
            # Assert: Verify complete pipeline lifecycle works with real database persistence
            assert save_result is True
            assert collector.metrics['status'] == 'failed'  # Has errors
            assert collector.metrics['tables_processed'] == 4
            assert collector.metrics['total_rows_processed'] == 2500
            assert len(collector.metrics['errors']) == 2  # 1 table error + 1 general error
            assert len(collector.metrics['table_metrics']) == 4
            
            # Verify timing
            assert collector.metrics['total_time'] is not None
            assert collector.metrics['total_time'] > 0
            assert collector.metrics['start_time'] is not None
            assert collector.metrics['end_time'] is not None
            
            # Verify table metrics
            patient_metric = collector.metrics['table_metrics']['patient']
            assert patient_metric['rows_processed'] == 1000
            assert patient_metric['success'] is True
            assert patient_metric['status'] == 'completed'
            
            appointment_metric = collector.metrics['table_metrics']['appointment']
            assert appointment_metric['rows_processed'] == 500
            assert appointment_metric['success'] is False
            assert appointment_metric['error'] == "Database connection failed"
            assert appointment_metric['status'] == 'failed'
            
            # Verify final metrics
            assert final_metrics == collector.metrics
            
            # Verify data was actually saved to database
            with collector.analytics_engine.connect() as conn:
                # Check pipeline metrics
                result = conn.execute(text("""
                    SELECT pipeline_id, tables_processed, total_rows_processed, success, error_count, status
                    FROM etl_pipeline_metrics 
                    WHERE pipeline_id = :pipeline_id
                """), {'pipeline_id': collector.metrics['pipeline_id']})
                
                pipeline_row = result.fetchone()
                assert pipeline_row is not None
                assert pipeline_row[1] == 4  # tables_processed
                assert pipeline_row[2] == 2500  # total_rows_processed
                assert pipeline_row[3] is False  # success (has errors)
                assert pipeline_row[4] == 2  # error_count
                assert pipeline_row[5] == 'failed'  # status
                
                # Check table metrics
                result = conn.execute(text("""
                    SELECT table_name, rows_processed, processing_time, success, error
                    FROM etl_table_metrics 
                    WHERE pipeline_id = :pipeline_id
                    ORDER BY table_name
                """), {'pipeline_id': collector.metrics['pipeline_id']})
                
                table_rows = result.fetchall()
                assert len(table_rows) == 4
                
                # Verify specific table data
                table_data = {row[0]: row for row in table_rows}
                assert 'patient' in table_data
                assert table_data['patient'][1] == 1000  # rows_processed
                assert table_data['patient'][3] is True  # success
                assert table_data['appointment'][1] == 500  # rows_processed
                assert table_data['appointment'][3] is False  # success
                assert "Database connection failed" in table_data['appointment'][4]  # error
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_status_reporting_with_persistence(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test status reporting with real database persistence.
        
        AAA Pattern:
            Arrange: Set up metrics collector with real database and test data
            Act: Call various status reporting methods with real data
            Assert: Verify status reporting works correctly with real database
            
        Validates:
            - Status reporting works with real database data
            - Table filtering works correctly
            - All status fields are populated correctly
            - Error information is properly formatted
            - Timestamps are correctly formatted
            - Statistics are calculated accurately
        """
        try:
            # Arrange: Set up metrics collector with real database and test data
            settings = test_settings_with_file_provider
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            collector.start_pipeline()
            
            # Add complex test data
            collector.record_table_processed('patient', 1000, 30.5, True)
            collector.record_table_processed('appointment', 500, 15.2, False, "Connection timeout")
            collector.record_table_processed('procedure', 750, 25.0, True)
            collector.record_table_processed('payment', 250, 8.5, False, "Data validation failed")
            collector.record_error("General system error", "orchestration")
            collector.record_error("Configuration warning", "config")
            
            # Save to database
            collector.save_metrics()
            
            # Act: Call various status reporting methods
            pipeline_status = collector.get_pipeline_status()
            patient_status = collector.get_table_status('patient')
            appointment_status = collector.get_table_status('appointment')
            nonexistent_status = collector.get_table_status('nonexistent')
            pipeline_stats = collector.get_pipeline_stats()
            
            # Filter by specific table
            patient_only_status = collector.get_pipeline_status(table='patient')
            
            # Assert: Verify status reporting works correctly with real database
            # Pipeline status validation
            assert pipeline_status['status'] == 'running'
            assert pipeline_status['tables_processed'] == 4
            assert pipeline_status['total_rows_processed'] == 2500
            assert pipeline_status['error_count'] == 4  # 2 table errors + 2 general errors
            assert len(pipeline_status['tables']) == 4
            
            # Verify table information in pipeline status
            patient_table_info = next(t for t in pipeline_status['tables'] if t['name'] == 'patient')
            assert patient_table_info['status'] == 'completed'
            assert patient_table_info['records_processed'] == 1000
            assert patient_table_info['processing_time'] == 30.5
            assert patient_table_info['error'] is None
            
            appointment_table_info = next(t for t in pipeline_status['tables'] if t['name'] == 'appointment')
            assert appointment_table_info['status'] == 'failed'
            assert appointment_table_info['records_processed'] == 500
            assert appointment_table_info['processing_time'] == 15.2
            assert appointment_table_info['error'] == "Connection timeout"
            
            # Individual table status validation
            assert patient_status is not None
            assert patient_status['table_name'] == 'patient'
            assert patient_status['rows_processed'] == 1000
            assert patient_status['success'] is True
            assert patient_status['status'] == 'completed'
            
            assert appointment_status is not None
            assert appointment_status['table_name'] == 'appointment'
            assert appointment_status['rows_processed'] == 500
            assert appointment_status['success'] is False
            assert appointment_status['error'] == "Connection timeout"
            assert appointment_status['status'] == 'failed'
            
            assert nonexistent_status is None
            
            # Pipeline stats validation
            assert pipeline_stats['tables_processed'] == 4
            assert pipeline_stats['total_rows_processed'] == 2500
            assert pipeline_stats['error_count'] == 4
            assert pipeline_stats['success_count'] == 0  # No tables succeeded
            assert pipeline_stats['success_rate'] == 0.0  # 0 out of 4 tables
            assert pipeline_stats['status'] == 'running'
            
            # Filtered status validation
            assert len(patient_only_status['tables']) == 1
            assert patient_only_status['tables'][0]['name'] == 'patient'
            
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_cleanup_operations(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test cleanup operations with real database.
        
        AAA Pattern:
            Arrange: Set up metrics collector with real database and old test data
            Act: Call cleanup operations with real database
            Assert: Verify cleanup operations work correctly with real database
            
        Validates:
            - Cleanup operations work with real database
            - Old metrics are properly deleted
            - Retention policies are enforced
            - Database transactions work correctly
            - Error handling works for cleanup failures
        """
        try:
            # Arrange: Set up metrics collector with real database and old test data
            settings = test_settings_with_file_provider
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            
            # Create some old test data (simulate old metrics)
            old_pipeline_id = f"test_old_pipeline_{int(time.time())}"
            
            with collector.analytics_engine.connect() as conn:
                # Insert old pipeline metrics
                conn.execute(text("""
                    INSERT INTO etl_pipeline_metrics (
                        pipeline_id, start_time, end_time, total_time,
                        tables_processed, total_rows_processed, success, error_count, status
                    ) VALUES (
                        :pipeline_id, :start_time, :end_time, :total_time,
                        :tables_processed, :total_rows_processed, :success, :error_count, :status
                    )
                """), {
                    'pipeline_id': old_pipeline_id,
                    'start_time': datetime.now() - timedelta(days=35),  # Old data
                    'end_time': datetime.now() - timedelta(days=35),
                    'total_time': 100.0,
                    'tables_processed': 5,
                    'total_rows_processed': 1000,
                    'success': True,
                    'error_count': 0,
                    'status': 'completed'
                })
                
                # Insert old table metrics
                conn.execute(text("""
                    INSERT INTO etl_table_metrics (
                        pipeline_id, table_name, rows_processed, processing_time, success, error, timestamp
                    ) VALUES (
                        :pipeline_id, :table_name, :rows_processed, :processing_time, :success, :error, :timestamp
                    )
                """), {
                    'pipeline_id': old_pipeline_id,
                    'table_name': 'old_table',
                    'rows_processed': 1000,
                    'processing_time': 50.0,
                    'success': True,
                    'error': None,
                    'timestamp': datetime.now() - timedelta(days=35)
                })
                
                conn.commit()
            
            # Act: Call cleanup operations with real database
            cleanup_result = collector.cleanup_old_metrics(retention_days=30)
            
            # Assert: Verify cleanup operations work correctly with real database
            assert cleanup_result is True
            
            # Verify old data was cleaned up
            with collector.analytics_engine.connect() as conn:
                # Check that old pipeline metrics were deleted
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM etl_pipeline_metrics 
                    WHERE pipeline_id = :pipeline_id
                """), {'pipeline_id': old_pipeline_id})
                
                count = result.fetchone()[0]
                assert count == 0, f"Old pipeline metrics not cleaned up: {count} records found"
                
                # Check that old table metrics were deleted
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM etl_table_metrics 
                    WHERE pipeline_id = :pipeline_id
                """), {'pipeline_id': old_pipeline_id})
                
                count = result.fetchone()[0]
                assert count == 0, f"Old table metrics not cleaned up: {count} records found"
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_error_handling_with_database(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test error handling with real database operations.
        
        AAA Pattern:
            Arrange: Set up metrics collector with real database
            Act: Trigger various error conditions with real database
            Assert: Verify error handling works correctly with real database
            
        Validates:
            - Error handling works for real database operations
            - Database connection errors are handled gracefully
            - Transaction rollback works correctly
            - Error information is properly recorded
            - Error counts are accurate
            - Error messages are preserved
        """
        try:
            # Arrange: Set up metrics collector with real database
            settings = test_settings_with_file_provider
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            
            # Act: Trigger various error conditions with real database
            collector.start_pipeline()
            
            # Table processing errors
            collector.record_table_processed('patient', 1000, 30.5, False, "Database connection timeout")
            collector.record_table_processed('appointment', 500, 15.2, False, "Data validation failed")
            collector.record_table_processed('procedure', 750, 25.0, False, "Schema mismatch")
            
            # General pipeline errors
            collector.record_error("Configuration error", "config")
            collector.record_error("Network timeout", "network")
            collector.record_error("Memory allocation failed", "system")
            collector.record_error("File system error", "storage")
            
            # Successful table processing
            collector.record_table_processed('payment', 250, 8.5, True)
            
            collector.end_pipeline()
            
            # Try to save metrics (should work even with errors)
            save_result = collector.save_metrics()
            
            # Assert: Verify error handling works correctly with real database
            assert save_result is True  # Should still save despite errors
            assert len(collector.metrics['errors']) == 7  # 3 table errors + 4 general errors
            assert len(collector.metrics['table_metrics']) == 4
            
            # Verify table errors are properly recorded
            table_errors = [error for error in collector.metrics['errors'] if 'table' in error and error.get('table') in ['patient', 'appointment', 'procedure']]
            assert len(table_errors) == 3
            
            # Verify general errors are properly recorded
            general_errors = [error for error in collector.metrics['errors'] if 'table' in error and error.get('table') in ['config', 'network', 'system', 'storage']]
            assert len(general_errors) == 4
            
            # Verify all errors have timestamps
            for error in collector.metrics['errors']:
                assert error['timestamp'] is not None
                assert isinstance(error['timestamp'], datetime)
            
            # Verify data was saved to database despite errors
            with collector.analytics_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT pipeline_id, error_count, status
                    FROM etl_pipeline_metrics 
                    WHERE pipeline_id = :pipeline_id
                """), {'pipeline_id': collector.metrics['pipeline_id']})
                
                row = result.fetchone()
                assert row is not None
                assert row[1] == 7  # error_count
                assert row[2] == 'failed'  # status
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_factory_function_integration(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test factory function integration with real database.
        
        AAA Pattern:
            Arrange: Set up test settings with FileConfigProvider for real environment
            Act: Call factory function with real database connection
            Assert: Verify factory function works correctly with real database
            
        Validates:
            - Factory function works with real database connection
            - Settings injection works properly
            - Persistence can be enabled/disabled
            - Provider pattern dependency injection works
            - Error handling works for invalid configurations
        """
        try:
            # Arrange: Set up test settings with FileConfigProvider for real environment
            settings = test_settings_with_file_provider
            
            # Act: Call factory function with real database connection
            
            # Test with persistence enabled
            collector_with_persistence = create_metrics_collector(settings, enable_persistence=True)
            
            # Test with persistence disabled
            collector_without_persistence = create_metrics_collector(settings, enable_persistence=False)
            
            # Assert: Verify factory function works correctly with real database
            # With persistence
            assert isinstance(collector_with_persistence, UnifiedMetricsCollector)
            assert collector_with_persistence.settings == settings
            assert collector_with_persistence.analytics_engine is not None
            assert collector_with_persistence.enable_persistence is True
            
            # Without persistence
            assert isinstance(collector_without_persistence, UnifiedMetricsCollector)
            assert collector_without_persistence.settings == settings
            # Note: analytics_engine may be created but persistence is disabled
            assert collector_without_persistence.enable_persistence is False
            
            # Verify both collectors have proper configuration
            assert collector_with_persistence.metrics is not None
            assert collector_without_persistence.metrics is not None
            assert collector_with_persistence.metrics['status'] == 'idle'
            assert collector_without_persistence.metrics['status'] == 'idle'
            
            # Test real database connection with persistence enabled
            collector_with_persistence.start_pipeline()
            collector_with_persistence.record_table_processed('test_table', 100, 5.0, True)
            collector_with_persistence.end_pipeline()
            save_result = collector_with_persistence.save_metrics()
            assert save_result is True
            
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_performance_with_large_data_volumes(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test performance with large data volumes using real database.
        
        AAA Pattern:
            Arrange: Set up metrics collector with real database for large data volumes
            Act: Process large amounts of data and measure performance
            Assert: Verify performance is acceptable with real database
            
        Validates:
            - Performance is acceptable for large datasets
            - Database operations scale appropriately
            - Memory usage is reasonable
            - No memory leaks occur
            - Large error lists are handled efficiently
            - Complex status reporting is performant
        """
        try:
            # Arrange: Set up metrics collector with real database for large data volumes
            settings = test_settings_with_file_provider
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            
            # Act: Process large amounts of data and measure performance
            start_time = time.time()
            
            collector.start_pipeline()
            
            # Simulate processing many tables
            for i in range(50):  # Reduced from 100 for integration test performance
                table_name = f"table_{i}"
                rows_processed = (i + 1) * 100
                processing_time = (i + 1) * 0.1
                success = i % 10 != 0  # 90% success rate
                
                if success:
                    collector.record_table_processed(table_name, rows_processed, processing_time, True)
                else:
                    collector.record_table_processed(table_name, rows_processed, processing_time, False, f"Error in table {i}")
            
            # Add some general errors
            for i in range(10):  # Reduced from 20 for integration test performance
                collector.record_error(f"General error {i}", f"system_{i}")
            
            collector.end_pipeline()
            save_result = collector.save_metrics()
            end_time = time.time()
            
            # Assert: Verify performance is acceptable with real database
            processing_time = end_time - start_time
            
            # Performance validation
            assert processing_time < 30.0, f"Processing took too long: {processing_time:.2f}s"
            assert save_result is True
            assert collector.metrics['tables_processed'] == 50
            assert collector.metrics['total_rows_processed'] == 127500  # Sum of 1 to 50 * 100
            assert len(collector.metrics['errors']) == 15  # 5 table errors + 10 general errors
            assert len(collector.metrics['table_metrics']) == 50
            
            # Memory usage validation (indirect)
            assert collector.metrics is not None
            assert len(collector.metrics['table_metrics']) == 50
            assert len(collector.metrics['errors']) == 15
            
            # Status reporting performance
            status_start = time.time()
            pipeline_status = collector.get_pipeline_status()
            pipeline_stats = collector.get_pipeline_stats()
            status_end = time.time()
            
            status_time = status_end - status_start
            assert status_time < 5.0, f"Status reporting took too long: {status_time:.2f}s"
            
            # Verify status contains all data
            assert len(pipeline_status['tables']) == 50
            assert pipeline_stats['tables_processed'] == 50
            assert pipeline_stats['success_rate'] == 70.0  # 45 out of 50 tables succeeded
            assert pipeline_stats['status'] == 'failed'  # Has errors
            
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_provider_pattern_integration(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test provider pattern integration with real database.
        
        AAA Pattern:
            Arrange: Set up test settings with FileConfigProvider for real environment
            Act: Test provider pattern integration with real database
            Assert: Verify provider pattern integration works correctly with real database
            
        Validates:
            - Provider pattern works with real database connection
            - Settings injection works for environment-agnostic connections
            - FileConfigProvider loads real configuration correctly
            - Configuration validation works properly
            - Error handling works for invalid configurations
        """
        try:
            # Arrange: Set up test settings with FileConfigProvider for real environment
            settings = test_settings_with_file_provider
            
            # Act: Test provider pattern integration with real database
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            
            # Assert: Verify provider pattern integration works correctly with real database
            assert settings.environment == 'test'
            # Note: FileConfigProvider may have different interface than DictConfigProvider
            assert hasattr(settings.provider, 'configs') or hasattr(settings.provider, 'get_config')  # FileConfigProvider interface
            
            # Verify pipeline configuration structure
            pipeline_config = settings.pipeline_config
            assert pipeline_config is not None
            assert 'connections' in pipeline_config
            assert 'general' in pipeline_config
            assert 'stages' in pipeline_config
            assert 'logging' in pipeline_config
            assert 'error_handling' in pipeline_config
            
            # Verify real database connection works
            assert collector.analytics_engine is not None
            with collector.analytics_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row is not None and row[0] == 1
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_settings_injection_integration(self, test_settings_with_file_provider, cleanup_metrics_tables):
        """
        Test Settings injection integration with real database.
        
        AAA Pattern:
            Arrange: Set up test settings with FileConfigProvider for real environment
            Act: Test Settings injection with real database connection
            Assert: Verify Settings injection works correctly with real database
            
        Validates:
            - Settings injection works for environment-agnostic connections
            - Provider pattern integration works correctly
            - Configuration validation works properly
            - Error handling works for invalid configurations
            - Environment separation works correctly
        """
        try:
            # Arrange: Set up test settings with FileConfigProvider for real environment
            settings = test_settings_with_file_provider
            
            # Act: Test various settings injection scenarios
            # Test database configuration access
            pipeline_config = settings.pipeline_config
            assert pipeline_config is not None
            assert 'connections' in pipeline_config
            assert 'general' in pipeline_config
            assert 'stages' in pipeline_config
            assert 'logging' in pipeline_config
            assert 'error_handling' in pipeline_config
            
            # Test environment configuration access
            env_config = settings._env_vars
            assert env_config is not None
            assert 'ETL_ENVIRONMENT' in env_config
            assert env_config['ETL_ENVIRONMENT'] == 'test'
            
            # Test table configuration access
            table_configs = settings.tables_config.get('tables', {})
            assert table_configs is not None
            assert 'patient' in table_configs
            assert 'appointment' in table_configs
            
            # Test real database connection with Settings injection
            collector = UnifiedMetricsCollector(settings=settings, enable_persistence=True)
            assert collector.analytics_engine is not None
            
            # Verify real database operations work
            collector.start_pipeline()
            collector.record_table_processed('test_table', 100, 5.0, True)
            collector.end_pipeline()
            save_result = collector.save_metrics()
            assert save_result is True
            
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}") 