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
    
    logger.info("✅ Set up populated test databases with standard test data (SOURCE and REPLICATION only)")
    
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