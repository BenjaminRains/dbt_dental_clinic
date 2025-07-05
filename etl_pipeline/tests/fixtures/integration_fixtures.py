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
    Provides databases with all standard test data pre-populated.
    
    This fixture:
    - Sets up patient and appointment data in all test databases
    - Provides a ready-to-use test environment
    - Automatically cleans up after tests
    
    Usage:
        def test_with_data(populated_test_databases):
            # Databases already have test data
            # ... test logic ...
    """
    # Set up standard test data
    test_data_manager.setup_patient_data(include_all_fields=True)
    test_data_manager.setup_appointment_data()
    
    logger.info("✅ Set up populated test databases with standard test data")
    
    return test_data_manager

@pytest.fixture
def minimal_test_databases(test_data_manager) -> IntegrationTestDataManager:
    """
    Provides databases with minimal test data (only required fields).
    
    This fixture:
    - Sets up minimal patient data (faster setup)
    - Useful for tests that don't need all fields
    - Automatically cleans up after tests
    
    Usage:
        def test_minimal_data(minimal_test_databases):
            # Databases have minimal test data
            # ... test logic ...
    """
    # Set up minimal test data
    test_data_manager.setup_patient_data(include_all_fields=False)
    
    logger.info("✅ Set up minimal test databases with required fields only")
    
    return test_data_manager

@pytest.fixture
def incremental_test_databases(test_data_manager) -> IntegrationTestDataManager:
    """
    Provides databases with incremental test data for incremental loading tests.
    
    This fixture:
    - Sets up patient data with newer timestamps
    - Useful for testing incremental loading functionality
    - Automatically cleans up after tests
    
    Usage:
        def test_incremental_loading(incremental_test_databases):
            # Databases have incremental test data
            # ... test logic ...
    """
    # Set up incremental test data
    test_data_manager.setup_patient_data(include_all_fields=True)
    test_data_manager.setup_incremental_patient_data()
    
    logger.info("✅ Set up incremental test databases with newer timestamps")
    
    return test_data_manager

@pytest.fixture
def test_database_engines(test_settings) -> Tuple:
    """
    Provides test database engines using new ConnectionFactory.
    
    Returns:
        Tuple of (replication_engine, analytics_engine) for backward compatibility
    
    Usage:
        def test_something(test_database_engines):
            replication_engine, analytics_engine = test_database_engines
            # ... test logic ...
    """
    try:
        # Use new ConnectionFactory with test methods
        replication_engine = ConnectionFactory.get_replication_test_connection(test_settings)
        analytics_engine = ConnectionFactory.get_analytics_test_connection(test_settings)
        
        logger.info("✅ Created test database engines using new ConnectionFactory")
        
        yield (replication_engine, analytics_engine)
        
        # Clean up connections
        replication_engine.dispose()
        analytics_engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test database engines: {e}")
        raise

@pytest.fixture
def test_source_engine(test_settings):
    """
    Provides test source database engine using new ConnectionFactory.
    
    Usage:
        def test_source_connection(test_source_engine):
            # ... test logic with source database ...
    """
    try:
        engine = ConnectionFactory.get_source_test_connection(test_settings)
        
        logger.info("✅ Created test source engine using new ConnectionFactory")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test source engine: {e}")
        raise

@pytest.fixture
def test_replication_engine(test_settings):
    """
    Provides test replication database engine using new ConnectionFactory.
    
    Usage:
        def test_replication_connection(test_replication_engine):
            # ... test logic with replication database ...
    """
    try:
        engine = ConnectionFactory.get_replication_test_connection(test_settings)
        
        logger.info("✅ Created test replication engine using new ConnectionFactory")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test replication engine: {e}")
        raise

@pytest.fixture
def test_analytics_engine(test_settings):
    """
    Provides test analytics database engine using new ConnectionFactory.
    
    Usage:
        def test_analytics_connection(test_analytics_engine):
            # ... test logic with analytics database ...
    """
    try:
        engine = ConnectionFactory.get_analytics_test_connection(test_settings)
        
        logger.info("✅ Created test analytics engine using new ConnectionFactory")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test analytics engine: {e}")
        raise

@pytest.fixture
def test_raw_engine(test_settings):
    """
    Provides test raw schema engine using new ConnectionFactory.
    
    Usage:
        def test_raw_schema(test_raw_engine):
            # ... test logic with raw schema ...
    """
    try:
        engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.RAW)
        
        logger.info("✅ Created test raw schema engine using new ConnectionFactory")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test raw schema engine: {e}")
        raise

@pytest.fixture
def test_staging_engine(test_settings):
    """
    Provides test staging schema engine using new ConnectionFactory.
    
    Usage:
        def test_staging_schema(test_staging_engine):
            # ... test logic with staging schema ...
    """
    try:
        engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.STAGING)
        
        logger.info("✅ Created test staging schema engine using new ConnectionFactory")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test staging schema engine: {e}")
        raise

@pytest.fixture
def test_intermediate_engine(test_settings):
    """
    Provides test intermediate schema engine using new ConnectionFactory.
    
    Usage:
        def test_intermediate_schema(test_intermediate_engine):
            # ... test logic with intermediate schema ...
    """
    try:
        engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.INTERMEDIATE)
        
        logger.info("✅ Created test intermediate schema engine using new ConnectionFactory")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test intermediate schema engine: {e}")
        raise

@pytest.fixture
def test_marts_engine(test_settings):
    """
    Provides test marts schema engine using new ConnectionFactory.
    
    Usage:
        def test_marts_schema(test_marts_engine):
            # ... test logic with marts schema ...
    """
    try:
        engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.MARTS)
        
        logger.info("✅ Created test marts schema engine using new ConnectionFactory")
        
        yield engine
        
        engine.dispose()
        
    except Exception as e:
        logger.error(f"❌ Failed to create test marts schema engine: {e}")
        raise

# Legacy compatibility fixtures (for existing tests during migration)
@pytest.fixture
def setup_patient_table(test_data_manager) -> Tuple:
    """
    Legacy fixture for backward compatibility.
    
    Returns:
        Tuple of (replication_engine, analytics_engine) with patient data set up
    
    Usage:
        def test_something(setup_patient_table):
            replication_engine, analytics_engine = setup_patient_table
            # ... test logic ...
    """
    # Set up patient data
    test_data_manager.setup_patient_data(include_all_fields=True)
    
    # Return engines for backward compatibility
    return (test_data_manager.replication_engine, test_data_manager.analytics_engine)

@pytest.fixture
def setup_etl_tracking(test_analytics_engine):
    """
    Legacy fixture for ETL tracking setup.
    
    Sets up ETL tracking tables in the analytics database.
    
    Usage:
        def test_etl_tracking(setup_etl_tracking):
            # ... test logic with ETL tracking ...
    """
    try:
        with test_analytics_engine.connect() as conn:
            # Create ETL tracking table if it doesn't exist
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS etl_load_status (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    load_status VARCHAR(50) NOT NULL,
                    last_loaded TIMESTAMP,
                    records_processed INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Insert test tracking data
            conn.execute(text("""
                INSERT INTO etl_load_status (table_name, load_status, last_loaded, records_processed)
                VALUES ('patient', 'success', '2023-01-01 10:00:00', 3)
                ON CONFLICT DO NOTHING
            """))
            
            conn.commit()
            
        logger.info("✅ Set up ETL tracking tables")
        
        return test_analytics_engine
        
    except Exception as e:
        logger.error(f"❌ Failed to set up ETL tracking: {e}")
        raise 