"""
Integration Test Fixtures

This module provides pytest fixtures for integration tests using the new architectural patterns.
These fixtures use the standardized test data manager and new ConnectionFactory methods.

All fixtures follow the new architectural guidelines:
- Use ConnectionFactory with Settings injection
- Integrate with Settings class and provider pattern
- Use standardized test data with proper enum usage
- Provide proper cleanup and error handling
- Follow unified interface patterns from connection architecture
- Use FileConfigProvider for real database connections
"""

import pytest
import logging
from typing import Generator, Tuple
from sqlalchemy import text
from pathlib import Path

from etl_pipeline.config import DatabaseType, PostgresSchema, Settings
from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.core.connections import ConnectionFactory
from .test_data_manager import IntegrationTestDataManager

logger = logging.getLogger(__name__)

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
        config_dir = Path(__file__).parent.parent.parent  # Go to etl_pipeline root
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
def validate_test_databases():
    """
    Validate that test databases are available and skip tests if not.
    
    This fixture follows the connection architecture by:
    - Using FileConfigProvider to load real .env_test configuration
    - Using Settings injection for environment-agnostic connections
    - Validating real test database connections
    - Skipping tests gracefully when databases are unavailable
    """
    try:
        # Create FileConfigProvider that will load from .env_test
        config_dir = Path(__file__).parent.parent.parent  # Go to etl_pipeline root
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
def test_data_manager(validate_test_databases) -> Generator[IntegrationTestDataManager, None, None]:
    """
    Provides a test data manager for integration tests.
    
    This fixture:
    - Creates test database connections using new ConnectionFactory with Settings injection
    - Provides standardized test data management
    - Automatically cleans up test data after tests
    - Uses provider pattern for configuration (FileConfigProvider for integration tests)
    
    Usage:
        def test_something(test_data_manager):
            test_data_manager.setup_patient_data()
            # ... test logic ...
            # Cleanup is automatic
    """
    manager = IntegrationTestDataManager(validate_test_databases)
    
    # Yield the manager for test use
    yield manager
    
    # Clean up after test
    try:
        manager.cleanup_all_test_data()
        manager.dispose()
    except Exception as e:
        logger.warning(f"Failed to clean up test data manager: {e}")

@pytest.fixture
def populated_test_databases(test_data_manager) -> IntegrationTestDataManager:
    """
    Provides databases with comprehensive test data.
    
    This fixture:
    - Sets up patient and appointment data in SOURCE and REPLICATION databases
    - SOURCE database contains original data (test_opendental)
    - REPLICATION database contains replicated data (test_opendental_replication) - what PostgresSchema reads from
    - ANALYTICS database starts empty - what PostgresSchema writes to
    - Automatically cleans up after tests
    - Uses DatabaseType enum for type safety
    
    Usage:
        def test_with_data(populated_test_databases):
            # Databases already have test data
            # ... test logic ...
    """
    # Set up standard test data in SOURCE database only
    # Let the pipeline copy data to REPLICATION database
    # ANALYTICS database starts empty (PostgresSchema will populate it)
    test_data_manager.setup_patient_data(
        include_all_fields=True, 
        database_types=[DatabaseType.SOURCE]
    )
    test_data_manager.setup_appointment_data(
        database_types=[DatabaseType.SOURCE]
    )
    test_data_manager.setup_procedure_data(
        database_types=[DatabaseType.SOURCE]
    )
    
    logger.info("Set up populated test databases with standard test data (SOURCE and REPLICATION only)")
    
    return test_data_manager

@pytest.fixture
def test_database_engines(validate_test_databases) -> Generator[Tuple, None, None]:
    """
    Provides test database engines using unified connection methods with Settings injection.
    
    Returns:
        Tuple of (replication_engine, analytics_engine)
    
    Usage:
        def test_something(test_database_engines):
            replication_engine, analytics_engine = test_database_engines
            # ... test logic ...
    """
    try:
        # Use unified ConnectionFactory methods with Settings injection
        replication_engine = ConnectionFactory.get_replication_connection(validate_test_databases)
        analytics_engine = ConnectionFactory.get_analytics_connection(validate_test_databases)
        
        logger.info("Created test database engines using unified connection methods with Settings injection")
        
        yield (replication_engine, analytics_engine)
        
        replication_engine.dispose()
        analytics_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to create test database engines: {e}")
        raise

@pytest.fixture
def test_source_engine(validate_test_databases):
    """
    Provides test source database engine using unified connection method with Settings injection.
    """
    try:
        engine = ConnectionFactory.get_source_connection(validate_test_databases)
        logger.info("Created test source engine using unified connection method with Settings injection")
        yield engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Failed to create test source engine: {e}")
        raise

@pytest.fixture
def test_replication_engine(validate_test_databases):
    """
    Provides test replication database engine using unified connection method with Settings injection.
    """
    try:
        engine = ConnectionFactory.get_replication_connection(validate_test_databases)
        logger.info("Created test replication engine using unified connection method with Settings injection")
        yield engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Failed to create test replication engine: {e}")
        raise

@pytest.fixture
def test_analytics_engine(validate_test_databases):
    """
    Provides test analytics database engine using unified connection method with Settings injection.
    """
    try:
        engine = ConnectionFactory.get_analytics_connection(validate_test_databases)
        logger.info("Created test analytics engine using unified connection method with Settings injection")
        yield engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Failed to create test analytics engine: {e}")
        raise

@pytest.fixture
def test_raw_engine(validate_test_databases):
    """
    Provides test raw schema engine using unified connection method with Settings injection.
    """
    try:
        engine = ConnectionFactory.get_analytics_raw_connection(validate_test_databases)
        logger.info("Created test raw schema engine using unified connection method with Settings injection")
        yield engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Failed to create test raw schema engine: {e}")
        raise

@pytest.fixture
def test_staging_engine(validate_test_databases):
    """
    Provides test staging schema engine using unified connection method with Settings injection.
    """
    try:
        engine = ConnectionFactory.get_analytics_staging_connection(validate_test_databases)
        logger.info("Created test staging schema engine using unified connection method with Settings injection")
        yield engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Failed to create test staging schema engine: {e}")
        raise

@pytest.fixture
def test_intermediate_engine(validate_test_databases):
    """
    Provides test intermediate schema engine using unified connection method with Settings injection.
    """
    try:
        engine = ConnectionFactory.get_analytics_intermediate_connection(validate_test_databases)
        logger.info("Created test intermediate schema engine using unified connection method with Settings injection")
        yield engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Failed to create test intermediate schema engine: {e}")
        raise

@pytest.fixture
def test_marts_engine(validate_test_databases):
    """
    Provides test marts schema engine using unified connection method with Settings injection.
    """
    try:
        engine = ConnectionFactory.get_analytics_marts_connection(validate_test_databases)
        logger.info("Created test marts schema engine using unified connection method with Settings injection")
        yield engine
        engine.dispose()
    except Exception as e:
        logger.error(f"Failed to create test marts schema engine: {e}")
        raise

@pytest.fixture
def setup_patient_table(test_data_manager) -> Tuple:
    """
    Sets up patient table with test data for integration tests.
    
    Returns:
        Tuple of (patient_count, appointment_count) for verification
    
    Usage:
        def test_patient_data(setup_patient_table):
            patient_count, appointment_count = setup_patient_table
            # ... test logic ...
    """
    # Set up patient data in SOURCE and REPLICATION databases using DatabaseType enum
    test_data_manager.setup_patient_data(
        include_all_fields=True,
        database_types=[DatabaseType.SOURCE, DatabaseType.REPLICATION]
    )
    
    # Get counts for verification using DatabaseType enum
    patient_count = test_data_manager.get_patient_count(DatabaseType.SOURCE)
    appointment_count = test_data_manager.get_appointment_count(DatabaseType.SOURCE)
    
    return (patient_count, appointment_count)

@pytest.fixture
def setup_etl_tracking(test_analytics_engine):
    """
    Sets up ETL tracking tables in analytics database.
    
    Usage:
        def test_etl_tracking(setup_etl_tracking):
            # ... test logic with ETL tracking ...
    """
    # Create ETL tracking tables if they don't exist
    with test_analytics_engine.connect() as conn:
        # Create etl_tracking table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_tracking (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL,
                last_processed_at TIMESTAMP,
                rows_processed INTEGER DEFAULT 0,
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.commit()
    
    yield
    
    # Clean up ETL tracking data
    with test_analytics_engine.connect() as conn:
        conn.execute(text("DELETE FROM etl_tracking"))
        conn.commit() 

@pytest.fixture(autouse=True)
def reset_analytics_tables(test_raw_engine):
    """
    Truncate raw.patient and raw.appointment tables in the analytics database before each test.
    Ensures test isolation and prevents unique constraint violations.
    """
    print(f"[pytest fixture] Truncating raw.patient and raw.appointment before test (engine id: {id(test_raw_engine)})")
    
    # First, verify the tables exist and get their current row counts
    with test_raw_engine.connect() as conn:
        # Check if tables exist and get row counts
        try:
            patient_count = conn.execute(text('SELECT COUNT(*) FROM raw.patient')).scalar()
            appointment_count = conn.execute(text('SELECT COUNT(*) FROM raw.appointment')).scalar()
            print(f"[pytest fixture] Before truncation - patient: {patient_count}, appointment: {appointment_count}")
        except Exception as e:
            print(f"[pytest fixture] Error checking table counts: {e}")
            # Tables might not exist yet, which is fine for new tests
            pass
    
    # Truncate the tables with explicit transaction handling
    with test_raw_engine.begin() as conn:
        try:
            conn.execute(text('TRUNCATE TABLE raw.patient RESTART IDENTITY CASCADE;'))
            conn.execute(text('TRUNCATE TABLE raw.appointment RESTART IDENTITY CASCADE;'))
            print("[pytest fixture] Successfully truncated tables")
        except Exception as e:
            print(f"[pytest fixture] Error truncating tables: {e}")
            # If tables don't exist, that's fine - they'll be created during the test
            pass
    
    # Verify the tables are empty
    with test_raw_engine.connect() as conn:
        try:
            patient_count = conn.execute(text('SELECT COUNT(*) FROM raw.patient')).scalar()
            appointment_count = conn.execute(text('SELECT COUNT(*) FROM raw.appointment')).scalar()
            print(f"[pytest fixture] After truncation - patient: {patient_count}, appointment: {appointment_count}")
        except Exception as e:
            print(f"[pytest fixture] Error verifying truncation: {e}")
            pass
    
    # Dispose the engine to ensure fresh connections
    test_raw_engine.dispose()
    yield 