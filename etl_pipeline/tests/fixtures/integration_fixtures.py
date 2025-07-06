"""
Integration Test Fixtures

This module provides pytest fixtures for integration tests using the new architectural patterns.
These fixtures use the standardized test data manager and new ConnectionFactory methods.

All fixtures follow the new architectural guidelines:
- Use ConnectionFactory with test methods
- Integrate with Settings class
- Use standardized test data
- Provide proper cleanup
"""

import pytest
import logging
from typing import Generator, Tuple
from sqlalchemy import text

from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from .test_data_manager import IntegrationTestDataManager

logger = logging.getLogger(__name__)

@pytest.fixture
def test_data_manager(test_settings) -> Generator[IntegrationTestDataManager, None, None]:
    """
    Provides a test data manager for integration tests.
    
    This fixture:
    - Creates test database connections using new ConnectionFactory
    - Provides standardized test data management
    - Automatically cleans up test data after tests
    
    Usage:
        def test_something(test_data_manager):
            test_data_manager.setup_patient_data()
            # ... test logic ...
            # Cleanup is automatic
    """
    manager = IntegrationTestDataManager(test_settings)
    
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
    
    Usage:
        def test_with_data(populated_test_databases):
            # Databases already have test data
            # ... test logic ...
    """
    # Set up standard test data in SOURCE and REPLICATION databases only
    # ANALYTICS database starts empty (PostgresSchema will populate it)
    test_data_manager.setup_patient_data(
        include_all_fields=True, 
        database_types=[DatabaseType.SOURCE, DatabaseType.REPLICATION]
    )
    test_data_manager.setup_appointment_data(
        database_types=[DatabaseType.SOURCE, DatabaseType.REPLICATION]
    )
    
    logger.info("Set up populated test databases with standard test data (SOURCE and REPLICATION only)")
    
    return test_data_manager



@pytest.fixture
def test_database_engines(test_settings) -> Generator[Tuple, None, None]:
    """
    Provides test database engines using explicit test connection methods.
    
    Returns:
        Tuple of (replication_engine, analytics_engine) for backward compatibility
    
    Usage:
        def test_something(test_database_engines):
            replication_engine, analytics_engine = test_database_engines
            # ... test logic ...
    """
    try:
        # Use explicit test connection methods as per connection_environment_separation.md
        replication_engine = ConnectionFactory.get_mysql_replication_test_connection(test_settings)
        analytics_engine = ConnectionFactory.get_postgres_analytics_test_connection(test_settings)
        
        logger.info("Created test database engines using explicit test connection methods")
        
        yield (replication_engine, analytics_engine)
        
        # Clean up connections
        replication_engine.dispose()
        analytics_engine.dispose()
        
    except Exception as e:
        logger.error(f"Failed to create test database engines: {e}")
        raise

@pytest.fixture
def test_source_engine(test_settings):
    """
    Provides test source database engine using explicit test connection method.
    
    Usage:
        def test_source_connection(test_source_engine):
            # ... test logic with source database ...
    """
    try:
        engine = ConnectionFactory.get_opendental_source_test_connection(test_settings)
        
        logger.info("Created test source engine using explicit test connection method")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"FAILED: Failed to create test source engine: {e}")
        raise

@pytest.fixture
def test_replication_engine(test_settings):
    """
    Provides test replication database engine using explicit test connection method.
    
    Usage:
        def test_replication_connection(test_replication_engine):
            # ... test logic with replication database ...
    """
    try:
        engine = ConnectionFactory.get_mysql_replication_test_connection(test_settings)
        
        logger.info("SUCCESS: Created test replication engine using explicit test connection method")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"FAILED: Failed to create test replication engine: {e}")
        raise

@pytest.fixture
def test_analytics_engine(test_settings):
    """
    Provides test analytics database engine using explicit test connection method.
    
    Usage:
        def test_analytics_connection(test_analytics_engine):
            # ... test logic with analytics database ...
    """
    try:
        engine = ConnectionFactory.get_postgres_analytics_test_connection(test_settings)
        
        logger.info("SUCCESS: Created test analytics engine using explicit test connection method")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"FAILED: Failed to create test analytics engine: {e}")
        raise

@pytest.fixture
def test_raw_engine(test_settings):
    """
    Provides test raw schema engine using explicit test connection method.
    
    Usage:
        def test_raw_schema(test_raw_engine):
            # ... test logic with raw schema ...
    """
    try:
        # Use the basic test analytics connection and specify raw schema
        engine = ConnectionFactory.get_postgres_analytics_test_connection(test_settings)
        
        logger.info("SUCCESS: Created test raw schema engine using explicit test connection method")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"FAILED: Failed to create test raw schema engine: {e}")
        raise

@pytest.fixture
def test_staging_engine(test_settings):
    """
    Provides test staging schema engine using explicit test connection method.
    
    Usage:
        def test_staging_schema(test_staging_engine):
            # ... test logic with staging schema ...
    """
    try:
        # Use the basic test analytics connection and specify staging schema
        engine = ConnectionFactory.get_postgres_analytics_test_connection(test_settings)
        
        logger.info("SUCCESS: Created test staging schema engine using explicit test connection method")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"FAILED: Failed to create test staging schema engine: {e}")
        raise

@pytest.fixture
def test_intermediate_engine(test_settings):
    """
    Provides test intermediate schema engine using explicit test connection method.
    
    Usage:
        def test_intermediate_schema(test_intermediate_engine):
            # ... test logic with intermediate schema ...
    """
    try:
        # Use the basic test analytics connection and specify intermediate schema
        engine = ConnectionFactory.get_postgres_analytics_test_connection(test_settings)
        
        logger.info("SUCCESS: Created test intermediate schema engine using explicit test connection method")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"FAILED: Failed to create test intermediate schema engine: {e}")
        raise

@pytest.fixture
def test_marts_engine(test_settings):
    """
    Provides test marts schema engine using explicit test connection method.
    
    Usage:
        def test_marts_schema(test_marts_engine):
            # ... test logic with marts schema ...
    """
    try:
        # Use the basic test analytics connection and specify marts schema
        engine = ConnectionFactory.get_postgres_analytics_test_connection(test_settings)
        
        logger.info("SUCCESS: Created test marts schema engine using explicit test connection method")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"FAILED: Failed to create test marts schema engine: {e}")
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
    # Set up patient data in SOURCE and REPLICATION databases
    test_data_manager.setup_patient_data(
        include_all_fields=True,
        database_types=[DatabaseType.SOURCE, DatabaseType.REPLICATION]
    )
    
    # Get counts for verification
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