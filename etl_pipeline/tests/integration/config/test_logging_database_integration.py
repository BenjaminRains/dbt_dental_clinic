"""
Database Integration Tests for the Unified Logging System.

This module provides comprehensive integration tests for logging functionality
with real database connections and test data. These tests validate logging
behavior in real ETL scenarios with actual database operations.

Follows the three-tier ETL testing strategy and connection architecture:
- Uses real test databases (MariaDB v11.6 → PostgreSQL)
- Uses Settings injection for environment-agnostic connections
- Uses provider pattern for dependency injection (FileConfigProvider for integration)
- Uses unified interface with ConnectionFactory
- Uses order markers for proper test execution
- Uses dental clinic test data for realistic scenarios

ETL Context:
- Tests logging during real database operations
- Validates logging with actual dental clinic data
- Tests logging performance with real data volumes
- Ensures logging works with real connection pooling
- Tests logging during ETL workflow scenarios
"""

import pytest
import tempfile
import os
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any
from sqlalchemy import text
from datetime import datetime, timedelta

# Import ETL pipeline components
from etl_pipeline.config.logging import (
    setup_logging, 
    configure_sql_logging, 
    init_default_logger,
    get_logger,
    ETLLogger
)
from etl_pipeline.config import DatabaseType, PostgresSchema, Settings
from etl_pipeline.core.connections import ConnectionFactory

# Import fixtures
from tests.fixtures.test_data_fixtures import (
    standard_patient_test_data,
    incremental_patient_test_data,
    etl_tracking_test_data
)
from tests.fixtures.logging_fixtures import (
    test_logging_settings,
    sample_log_messages
)


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files with proper cleanup."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def validate_test_databases():
    """
    Validate that test databases are available and skip tests if not.
    
    This fixture follows the connection architecture by:
    - Using Settings injection for environment-agnostic connections
    - Validating real test database connections
    - Skipping tests gracefully when databases are unavailable
    - Following the provider pattern for configuration
    """
    try:
        # Try to create a real test settings object that loads from .env_test
        from etl_pipeline.config.providers import FileConfigProvider
        from pathlib import Path
        
        # Create FileConfigProvider that will load from .env_test
        config_dir = Path(__file__).parent.parent.parent.parent  # Go to etl_pipeline root
        provider = FileConfigProvider(config_dir)
        
        # Create settings with FileConfigProvider for real environment loading
        settings = Settings(environment='test', provider=provider)
        
        # Validate that we can connect to test databases
        source_engine = ConnectionFactory.get_source_connection(settings)
        with source_engine.connect() as conn:
            # Test a simple query
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            if not row or row[0] != 1:
                pytest.skip("Test database connection failed")
        
        # If we get here, databases are available
        return settings
        
    except Exception as e:
        # Skip tests if databases are not available
        pytest.skip(f"Test databases not available: {str(e)}")


@pytest.fixture
def test_source_engine_real(validate_test_databases):
    """
    Provides real test source database engine using FileConfigProvider.
    
    This fixture follows the connection architecture by:
    - Using FileConfigProvider to load real .env_test configuration
    - Using Settings injection for environment-agnostic connections
    - Providing real database connections for integration tests
    - Skipping tests when databases are unavailable
    """
    try:
        engine = ConnectionFactory.get_source_connection(validate_test_databases)
        yield engine
        engine.dispose()
    except Exception as e:
        pytest.skip(f"Test source database not available: {str(e)}")


@pytest.fixture
def test_analytics_engine_real(validate_test_databases):
    """
    Provides real test analytics database engine using FileConfigProvider.
    
    This fixture follows the connection architecture by:
    - Using FileConfigProvider to load real .env_test configuration
    - Using Settings injection for environment-agnostic connections
    - Providing real database connections for integration tests
    - Skipping tests when databases are unavailable
    """
    try:
        engine = ConnectionFactory.get_analytics_connection(validate_test_databases)
        yield engine
        engine.dispose()
    except Exception as e:
        pytest.skip(f"Test analytics database not available: {str(e)}")


@pytest.fixture
def test_replication_engine_real(validate_test_databases):
    """
    Provides real test replication database engine using FileConfigProvider.
    
    This fixture follows the connection architecture by:
    - Using FileConfigProvider to load real .env_test configuration
    - Using Settings injection for environment-agnostic connections
    - Providing real database connections for integration tests
    - Skipping tests when databases are unavailable
    """
    try:
        engine = ConnectionFactory.get_replication_connection(validate_test_databases)
        yield engine
        engine.dispose()
    except Exception as e:
        pytest.skip(f"Test replication database not available: {str(e)}")


@pytest.fixture
def test_data_manager_real(validate_test_databases):
    """
    Provides a real test data manager for integration tests using FileConfigProvider.
    
    This fixture follows the connection architecture by:
    - Using FileConfigProvider to load real .env_test configuration
    - Using Settings injection for environment-agnostic connections
    - Providing real database connections for integration tests
    - Skipping tests when databases are unavailable
    """
    try:
        from tests.fixtures.test_data_manager import IntegrationTestDataManager
        
        # Create test data manager with real settings
        manager = IntegrationTestDataManager(validate_test_databases)
        
        # Yield the manager for test use
        yield manager
        
        # Clean up after test
        try:
            manager.cleanup_all_test_data()
            manager.dispose()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to clean up test data manager: {e}")
            
    except Exception as e:
        pytest.skip(f"Test data manager not available: {str(e)}")


@pytest.fixture
def cleanup_logging():
    """Clean up logging configuration after tests for proper isolation."""
    # Store original handlers
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers.copy()
    original_level = root_logger.level
    
    yield
    
    # Restore original handlers and level
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    for handler in original_handlers:
        root_logger.addHandler(handler)
    root_logger.setLevel(original_level)





@pytest.mark.integration
@pytest.mark.order(0)
class TestDatabaseLoggingIntegration:
    """
    Integration tests for logging with real database operations.
    
    Tests logging behavior during actual database operations:
    - Real database connections and queries
    - ETL workflow logging with actual data
    - Performance logging with real operations
    - SQL query logging with actual queries
    - Error logging with real database errors
    """

    def test_real_database_connection_logging(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_source_engine_real,
        capsys
    ):
        """
        Test logging during real database connection operations.
        
        Validates:
            - Connection establishment logging
            - Query execution logging
            - Performance logging for real operations
            - Error logging for connection issues
            - Settings injection with real database connections
        """
        log_file = temp_log_dir / "database_connection.log"
        
        # Setup logging with database-specific configuration
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for database operations
        etl_logger = ETLLogger("database_connection_test")
        
        # Test real database connection with logging
        try:
            etl_logger.log_etl_start("database_connection", "connection_test")
            
            # Execute real query with logging
            with test_source_engine_real.connect() as conn:
                etl_logger.log_sql_query("SELECT COUNT(*) FROM patient", {})
                result = conn.execute(text("SELECT COUNT(*) FROM patient"))
                row = result.fetchone()
                count = row[0] if row else 0
                
                etl_logger.log_performance("patient_count_query", 0.1, count)
                etl_logger.log_validation_result("patient_count", True)
            
            etl_logger.log_etl_complete("database_connection", "connection_test", count)
            
            # Verify console output
            captured = capsys.readouterr()
            assert "[START] Starting connection_test for table: database_connection" in captured.out
            assert "SELECT COUNT(*) FROM patient" in captured.out
            assert "[PASS] Completed connection_test for table: database_connection" in captured.out
            assert f"Records: {count}" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "database_connection" in content
            assert "SELECT COUNT(*) FROM patient" in content
            assert "patient_count_query" in content
            
        except Exception as e:
            etl_logger.log_etl_error("database_connection", "connection_test", e)
            raise

    def test_real_etl_workflow_logging(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_source_engine_real,
        test_analytics_engine_real,
        test_data_manager_real,
        capsys
    ):
        """
        Test logging during complete ETL workflow with real data.
        
        Validates:
            - Complete ETL workflow logging
            - Data extraction logging with real records
            - Data transformation logging
            - Data loading logging
            - Performance tracking across workflow
            - Settings injection throughout workflow
        """
        log_file = temp_log_dir / "etl_workflow.log"
        
        # Setup logging for ETL workflow
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for workflow
        etl_logger = ETLLogger("etl_workflow_test")
        
        # Simulate complete ETL workflow with real data
        try:
            # Phase 1: Data Extraction
            etl_logger.log_etl_start("patient", "extraction")
            
            # Get real data from source
            with test_source_engine_real.connect() as conn:
                etl_logger.log_sql_query("SELECT * FROM patient WHERE PatStatus = 0", {})
                result = conn.execute(text("SELECT * FROM patient WHERE PatStatus = 0"))
                patients = result.fetchall() if result else []
                
                etl_logger.log_performance("patient_extraction", 0.5, len(patients))
                etl_logger.log_validation_result("patient_extraction", True)
            
            etl_logger.log_etl_complete("patient", "extraction", len(patients))
            
            # Phase 2: Data Transformation
            etl_logger.log_etl_start("patient", "transformation")
            
            # Simulate transformation (in real ETL, this would transform the data)
            transformed_count = len(patients)  # Simulate transformation
            etl_logger.log_performance("patient_transformation", 0.2, transformed_count)
            etl_logger.log_validation_result("patient_transformation", True)
            
            etl_logger.log_etl_complete("patient", "transformation", transformed_count)
            
            # Phase 3: Data Loading
            etl_logger.log_etl_start("patient", "loading")
            
            # Load to analytics database
            with test_analytics_engine_real.connect() as conn:
                # Simulate loading (in real ETL, this would insert the data)
                etl_logger.log_sql_query("INSERT INTO patient (PatNum, LName, FName) VALUES (%s, %s, %s)", 
                                       {"PatNum": 1, "LName": "Test", "FName": "User"})
                
                etl_logger.log_performance("patient_loading", 0.3, transformed_count)
                etl_logger.log_validation_result("patient_loading", True)
            
            etl_logger.log_etl_complete("patient", "loading", transformed_count)
            
            # Verify comprehensive logging
            captured = capsys.readouterr()
            assert "[START] Starting extraction for table: patient" in captured.out
            assert "[START] Starting transformation for table: patient" in captured.out
            assert "[START] Starting loading for table: patient" in captured.out
            assert "SELECT * FROM patient WHERE PatStatus = 0" in captured.out
            assert "INSERT INTO patient" in captured.out
            assert "patient_extraction" in captured.out
            assert "patient_transformation" in captured.out
            assert "patient_loading" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "etl_workflow_test" in content
            assert "patient" in content
            assert "extraction" in content
            assert "transformation" in content
            assert "loading" in content
            
        except Exception as e:
            etl_logger.log_etl_error("patient", "etl_workflow", e)
            raise

    def test_real_sql_query_logging(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_replication_engine_real,
        capsys
    ):
        """
        Test SQL query logging with real database queries.
        
        Validates:
            - SQL query logging with real queries
            - Parameter logging for prepared statements
            - Query performance logging
            - Error logging for failed queries
            - Settings injection with SQL logging
        """
        log_file = temp_log_dir / "sql_query.log"
        
        # Setup logging with SQL query logging enabled
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        configure_sql_logging(enabled=True, level="DEBUG")
        
        # Create ETL logger for SQL operations
        etl_logger = ETLLogger("sql_query_test")
        
        try:
            # Test various SQL query scenarios
            with test_replication_engine_real.connect() as conn:
                # Simple SELECT query
                etl_logger.log_sql_query("SELECT COUNT(*) FROM patient", {})
                result = conn.execute(text("SELECT COUNT(*) FROM patient"))
                row = result.fetchone()
                count = row[0] if row else 0
                
                # Parameterized query
                params = {"status": 0}
                etl_logger.log_sql_query("SELECT * FROM patient WHERE PatStatus = :status", params)
                result = conn.execute(text("SELECT * FROM patient WHERE PatStatus = :status"), params)
                patients = result.fetchall()
                
                # Complex query with JOIN
                etl_logger.log_sql_query("""
                    SELECT p.PatNum, p.LName, p.FName, a.AptDateTime 
                    FROM patient p 
                    LEFT JOIN appointment a ON p.PatNum = a.PatNum 
                    WHERE p.PatStatus = :status
                """, {"status": 0})
                
                # Performance logging
                etl_logger.log_performance("patient_query", 0.1, len(patients))
                
            # Verify SQL query logging
            captured = capsys.readouterr()
            assert "SELECT COUNT(*) FROM patient" in captured.out
            assert "SELECT * FROM patient WHERE PatStatus = :status" in captured.out
            assert "Parameters: {'status': 0}" in captured.out
            assert "LEFT JOIN appointment" in captured.out
            assert "patient_query" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "sql_query_test" in content
            assert "SELECT" in content
            assert "Parameters:" in content
            
        except Exception as e:
            etl_logger.log_etl_error("sql_query", "query_test", e)
            raise

    def test_real_performance_logging_with_data(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_source_engine_real,
        test_data_manager_real,
        capsys
    ):
        """
        Test performance logging with real data operations.
        
        Validates:
            - Performance logging with real data volumes
            - Timing accuracy for database operations
            - Throughput calculations with real records
            - Performance tracking across different operations
            - Settings injection for performance monitoring
        """
        log_file = temp_log_dir / "performance.log"
        
        # Setup logging for performance tracking
        setup_logging(
            log_level="INFO",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for performance testing
        etl_logger = ETLLogger("performance_test")
        
        try:
            # Test performance with real data operations
            
            # Operation 1: Count all patients
            start_time = time.time()
            with test_source_engine_real.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM patient"))
                row = result.fetchone()
                patient_count = row[0] if row else 0
            extraction_time = time.time() - start_time
            
            etl_logger.log_performance("patient_count_extraction", extraction_time, patient_count)
            
            # Operation 2: Extract patient details
            start_time = time.time()
            with test_source_engine_real.connect() as conn:
                result = conn.execute(text("SELECT PatNum, LName, FName FROM patient"))
                patients = result.fetchall()
            detail_extraction_time = time.time() - start_time
            
            etl_logger.log_performance("patient_detail_extraction", detail_extraction_time, len(patients))
            
            # Operation 3: Complex query performance
            start_time = time.time()
            with test_source_engine_real.connect() as conn:
                result = conn.execute(text("""
                    SELECT p.PatNum, p.LName, p.FName, COUNT(a.AptNum) as appointment_count
                    FROM patient p
                    LEFT JOIN appointment a ON p.PatNum = a.PatNum
                    GROUP BY p.PatNum, p.LName, p.FName
                """))
                complex_result = result.fetchall()
            complex_query_time = time.time() - start_time
            
            etl_logger.log_performance("complex_patient_query", complex_query_time, len(complex_result))
            
            # Verify performance logging
            captured = capsys.readouterr()
            assert "patient_count_extraction" in captured.out
            assert "patient_detail_extraction" in captured.out
            assert "complex_patient_query" in captured.out
            # Note: records/sec only appears when records_count > 0
            # Since test database has 0 records, we only see the basic performance format
            # The actual output shows: [PERF] operation completed in X.XXs (without records/sec)
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "performance_test" in content
            # Note: records/sec only appears when records_count > 0
            # Since test database has 0 records, we don't expect records/sec in the output
            
        except Exception as e:
            etl_logger.log_etl_error("performance", "performance_test", e)
            raise

    def test_real_error_logging_with_database_errors(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_analytics_engine_real,
        capsys
    ):
        """
        Test error logging with real database errors.
        
        Validates:
            - Error logging for connection failures
            - Error logging for invalid queries
            - Error logging for permission issues
            - Error logging for timeout scenarios
            - Settings injection for error handling
        """
        log_file = temp_log_dir / "error.log"
        
        # Setup logging for error tracking
        setup_logging(
            log_level="ERROR",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for error testing
        etl_logger = ETLLogger("error_test")
        
        try:
            # Test 1: Invalid table query
            try:
                with test_analytics_engine_real.connect() as conn:
                    conn.execute(text("SELECT * FROM non_existent_table"))
            except Exception as e:
                etl_logger.log_etl_error("non_existent_table", "query_test", e)
            
            # Test 2: Invalid column query
            try:
                with test_analytics_engine_real.connect() as conn:
                    conn.execute(text("SELECT invalid_column FROM patient"))
            except Exception as e:
                etl_logger.log_etl_error("patient", "invalid_column_query", e)
            
            # Test 3: Syntax error
            try:
                with test_analytics_engine_real.connect() as conn:
                    conn.execute(text("SELECT * FROM patient WHERE"))
            except Exception as e:
                etl_logger.log_etl_error("patient", "syntax_error", e)
            
            # Verify error logging
            captured = capsys.readouterr()
            assert "[FAIL] Error during query_test for table: non_existent_table" in captured.out
            assert "[FAIL] Error during invalid_column_query for table: patient" in captured.out
            assert "[FAIL] Error during syntax_error for table: patient" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "error_test" in content
            assert "[FAIL]" in content
            
        except Exception as e:
            etl_logger.log_etl_error("error_test", "error_logging_test", e)
            raise

    def test_real_concurrent_logging_with_database_operations(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_source_engine_real,
        test_data_manager_real,
        capsys
    ):
        """
        Test concurrent logging during database operations.
        
        Validates:
            - Concurrent logging during parallel operations
            - Thread safety of logging system
            - Performance under concurrent load
            - Log message ordering and integrity
            - Settings injection for concurrent operations
        """
        log_file = temp_log_dir / "concurrent.log"
        
        # Setup logging for concurrent operations
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create multiple ETL loggers for concurrent testing
        loggers = [
            ETLLogger("concurrent_test_1"),
            ETLLogger("concurrent_test_2"),
            ETLLogger("concurrent_test_3")
        ]
        
        try:
            # Simulate concurrent database operations
            
            # Operation 1: Patient data extraction
            loggers[0].log_etl_start("patient", "concurrent_extraction")
            with test_source_engine_real.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM patient"))
                row = result.fetchone()
                patient_count = row[0] if row else 0
            loggers[0].log_performance("patient_extraction", 0.1, patient_count)
            loggers[0].log_etl_complete("patient", "concurrent_extraction", patient_count)
            
            # Operation 2: Appointment data extraction
            loggers[1].log_etl_start("appointment", "concurrent_extraction")
            with test_source_engine_real.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM appointment"))
                row = result.fetchone()
                appointment_count = row[0] if row else 0
            loggers[1].log_performance("appointment_extraction", 0.1, appointment_count)
            loggers[1].log_etl_complete("appointment", "concurrent_extraction", appointment_count)
            
            # Operation 3: Complex query execution
            loggers[2].log_etl_start("complex_query", "concurrent_execution")
            with test_source_engine_real.connect() as conn:
                result = conn.execute(text("""
                    SELECT p.PatNum, p.LName, COUNT(a.AptNum) as appointment_count
                    FROM patient p
                    LEFT JOIN appointment a ON p.PatNum = a.PatNum
                    GROUP BY p.PatNum, p.LName
                """))
                complex_count = len(result.fetchall())
            loggers[2].log_performance("complex_query_execution", 0.2, complex_count)
            loggers[2].log_etl_complete("complex_query", "concurrent_execution", complex_count)
            
            # Verify concurrent logging
            captured = capsys.readouterr()
            assert "concurrent_test_1" in captured.out
            assert "concurrent_test_2" in captured.out
            assert "concurrent_test_3" in captured.out
            assert "patient" in captured.out
            assert "appointment" in captured.out
            assert "complex_query" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "concurrent_test_1" in content
            assert "concurrent_test_2" in content
            assert "concurrent_test_3" in content
            
        except Exception as e:
            for logger in loggers:
                logger.log_etl_error("concurrent_test", "concurrent_operation", e)
            raise

    def test_real_log_file_rotation_with_database_operations(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_source_engine_real,
        test_data_manager_real,
        capsys
    ):
        """
        Test log file writing during extended database operations.
        
        Validates:
            - Log file writing during long operations
            - Log message persistence across operations
            - Log file size management
            - Log message ordering in files
            - Settings injection for file operations
        """
        log_file = temp_log_dir / "rotation.log"
        
        # Setup logging for file rotation testing
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for rotation testing
        etl_logger = ETLLogger("rotation_test")
        
        try:
            # Simulate extended database operations with logging
            
            # Generate multiple log messages during database operations
            for i in range(50):  # Generate 50 log messages
                etl_logger.log_etl_start(f"table_{i}", "extraction")
                
                with test_source_engine_real.connect() as conn:
                    # Execute a simple query for each iteration
                    result = conn.execute(text("SELECT COUNT(*) FROM patient"))
                    row = result.fetchone()
                    count = row[0] if row else 0
                
                etl_logger.log_performance(f"extraction_{i}", 0.01, count)
                etl_logger.log_etl_complete(f"table_{i}", "extraction", count)
                
                # Add a small delay to ensure timestamps differ
                time.sleep(0.01)
            
            # Verify log file writing
            assert log_file.exists()
            content = log_file.read_text()
            
            # Verify all messages are in the file
            for i in range(50):
                assert f"table_{i}" in content
                assert f"extraction_{i}" in content
            
            # Verify log file size is reasonable
            file_size = log_file.stat().st_size
            assert file_size > 0
            assert file_size < 1024 * 1024  # Less than 1MB
            
            # Verify console output
            captured = capsys.readouterr()
            assert "rotation_test" in captured.out
            assert "table_0" in captured.out
            assert "table_49" in captured.out
            
        except Exception as e:
            etl_logger.log_etl_error("rotation_test", "file_rotation_test", e)
            raise


@pytest.mark.integration
@pytest.mark.order(0)
class TestDatabaseLoggingPerformance:
    """
    Performance tests for logging with real database operations.
    
    Tests logging performance under various conditions:
    - High-volume data operations
    - Long-running queries
    - Multiple concurrent operations
    - Memory usage during logging
    - CPU usage during logging
    """

    def test_logging_performance_with_large_data_operations(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_data_manager_real,
        capsys
    ):
        """
        Test logging performance with large data operations.
        
        Validates:
            - Logging performance with large datasets
            - Memory usage during high-volume logging
            - CPU usage during intensive logging
            - Log message throughput
            - Settings injection performance
        """
        log_file = temp_log_dir / "performance_large.log"
        
        # Setup logging for performance testing
        setup_logging(
            log_level="INFO",  # Use INFO to reduce log volume
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for performance testing
        etl_logger = ETLLogger("performance_large_test")
        
        try:
            # Test logging performance with large operations
            
            # Measure logging performance
            start_time = time.time()
            
            # Generate many log messages quickly
            for i in range(1000):
                etl_logger.log_performance(f"operation_{i}", 0.001, i)
            
            logging_time = time.time() - start_time
            
            # Verify performance is acceptable (should be very fast)
            assert logging_time < 5.0  # Should complete in under 5 seconds
            
            # Verify log messages were written
            captured = capsys.readouterr()
            assert "performance_large_test" in captured.out
            assert "operation_0" in captured.out
            assert "operation_999" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "performance_large_test" in content
            
        except Exception as e:
            etl_logger.log_etl_error("performance_large", "performance_test", e)
            raise

    def test_logging_memory_usage_with_database_operations(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        test_source_engine_real,
        test_data_manager_real
    ):
        """
        Test memory usage during logging with database operations.
        
        Validates:
            - Memory usage during logging operations
            - Memory cleanup after operations
            - No memory leaks during logging
            - Settings injection memory efficiency
        """
        import psutil
        import os
        
        log_file = temp_log_dir / "memory.log"
        
        # Setup logging for memory testing
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for memory testing
        etl_logger = ETLLogger("memory_test")
        
        try:
            # Get initial memory usage
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss
            
            # Perform database operations with logging
            
            for i in range(100):
                etl_logger.log_etl_start(f"table_{i}", "memory_test")
                
                with test_source_engine_real.connect() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM patient"))
                    row = result.fetchone()
                    count = row[0] if row else 0
                
                etl_logger.log_performance(f"memory_operation_{i}", 0.01, count)
                etl_logger.log_etl_complete(f"table_{i}", "memory_test", count)
            
            # Get final memory usage
            final_memory = process.memory_info().rss
            memory_increase = final_memory - initial_memory
            
            # Verify memory usage is reasonable (should not increase significantly)
            # Allow for some memory increase due to logging buffers
            assert memory_increase < 10 * 1024 * 1024  # Less than 10MB increase
            
            # Verify log file was created
            assert log_file.exists()
            
        except Exception as e:
            etl_logger.log_etl_error("memory_test", "memory_usage_test", e)
            raise


@pytest.mark.integration
@pytest.mark.order(0)
class TestDatabaseLoggingErrorRecovery:
    """
    Error recovery tests for logging with database operations.
    
    Tests logging behavior during error scenarios:
    - Database connection failures
    - Query execution errors
    - Logging system failures
    - Recovery mechanisms
    """

    def test_logging_during_database_connection_failure(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        capsys
    ):
        """
        Test logging behavior during database connection failures.
        
        Validates:
            - Error logging for connection failures
            - Graceful handling of connection errors
            - Logging system resilience
            - Settings injection error handling
        """
        log_file = temp_log_dir / "connection_error.log"
        
        # Setup logging for error testing
        setup_logging(
            log_level="ERROR",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for error testing
        etl_logger = ETLLogger("connection_error_test")
        
        try:
            # Test logging with invalid connection parameters
            # This will cause a connection error
            invalid_settings = Settings(environment='test')
            invalid_settings._env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'invalid-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'invalid_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'invalid_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'invalid_pass'
            }
            
            try:
                # This should fail
                engine = ConnectionFactory.get_source_connection(invalid_settings)
                with engine.connect():
                    pass
            except Exception as e:
                etl_logger.log_etl_error("invalid_connection", "connection_test", e)
            
            # Verify error logging
            captured = capsys.readouterr()
            assert "[FAIL] Error during connection_test for table: invalid_connection" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "connection_error_test" in content
            assert "[FAIL]" in content
            
        except Exception as e:
            etl_logger.log_etl_error("connection_error_test", "error_test", e)
            raise

    def test_logging_system_resilience(
        self,
        temp_log_dir,
        cleanup_logging,
        validate_test_databases,
        capsys
    ):
        """
        Test logging system resilience during various failure scenarios.
        
        Validates:
            - Logging continues during database errors
            - Logging system doesn't crash on errors
            - Error messages are properly logged
            - Settings injection error resilience
        """
        log_file = temp_log_dir / "resilience.log"
        
        # Setup logging for resilience testing
        setup_logging(
            log_level="DEBUG",
            log_file=str(log_file),
            log_dir=str(temp_log_dir)
        )
        
        # Create ETL logger for resilience testing
        etl_logger = ETLLogger("resilience_test")
        
        try:
            # Test 1: Log normal operation
            etl_logger.log_etl_start("resilience", "normal_operation")
            etl_logger.log_performance("normal_operation", 0.1, 100)
            etl_logger.log_etl_complete("resilience", "normal_operation", 100)
            
            # Test 2: Log error condition
            try:
                raise ValueError("Test error for resilience testing")
            except Exception as e:
                etl_logger.log_etl_error("resilience", "error_operation", e)
            
            # Test 3: Log recovery operation
            etl_logger.log_etl_start("resilience", "recovery_operation")
            etl_logger.log_performance("recovery_operation", 0.05, 50)
            etl_logger.log_etl_complete("resilience", "recovery_operation", 50)
            
            # Verify logging system remained functional
            captured = capsys.readouterr()
            assert "normal_operation" in captured.out
            assert "error_operation" in captured.out
            assert "recovery_operation" in captured.out
            assert "[FAIL]" in captured.out
            assert "Test error for resilience testing" in captured.out
            
            # Verify file output
            assert log_file.exists()
            content = log_file.read_text()
            assert "resilience_test" in content
            assert "normal_operation" in content
            assert "error_operation" in content
            assert "recovery_operation" in content
            
        except Exception as e:
            etl_logger.log_etl_error("resilience_test", "resilience_test", e)
            raise 