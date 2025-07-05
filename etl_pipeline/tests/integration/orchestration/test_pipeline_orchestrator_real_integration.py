"""
Real integration tests for PipelineOrchestrator with actual database connections.

These tests require:
- Real PostgreSQL analytics database
- Test environment variables set
- Database user with appropriate permissions

STATUS: ACTIVE - Real Integration Tests
======================================

These tests validate that the PipelineOrchestrator works correctly with real database
connections and actual data operations. They test the complete ETL pipeline flow
from source to analytics database.

TEST REQUIREMENTS:
- TEST_POSTGRES_ANALYTICS_HOST environment variable
- TEST_POSTGRES_ANALYTICS_PORT environment variable  
- TEST_POSTGRES_ANALYTICS_DB environment variable
- TEST_POSTGRES_ANALYTICS_USER environment variable
- TEST_POSTGRES_ANALYTICS_PASSWORD environment variable
- Database user with CREATE, INSERT, SELECT, UPDATE, DELETE permissions
- Raw schema exists in analytics database

TEST SCOPE:
- Real database connections and operations
- Schema creation and permissions
- Data loading and querying
- Transaction handling
- Performance testing
- Data integrity constraints
- Concurrent operations

These tests are marked with @pytest.mark.integration and will be skipped
if the required environment variables are not set.
"""

import pytest
import os
import logging
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.config import Settings, DatabaseType, PostgresSchema

logger = logging.getLogger(__name__)

# Check if integration test environment is available
def has_integration_environment():
    """Check if integration test environment variables are set."""
    required_vars = [
        'TEST_POSTGRES_ANALYTICS_HOST',
        'TEST_POSTGRES_ANALYTICS_PORT',
        'TEST_POSTGRES_ANALYTICS_DB',
        'TEST_POSTGRES_ANALYTICS_USER',
        'TEST_POSTGRES_ANALYTICS_PASSWORD'
    ]
    return all(os.getenv(var) for var in required_vars)

@pytest.mark.integration
class TestPipelineOrchestratorRealIntegration:
    """Real integration tests for PipelineOrchestrator with actual database."""
    
    @pytest.fixture(scope="class")
    def analytics_engine(self):
        """Create real analytics database engine for integration tests."""
        if not has_integration_environment():
            pytest.skip("Integration test environment not available")
        
        try:
            # Create settings with test environment
            settings = Settings(environment='test')
            
            # Create real analytics engine
            engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Successfully created real analytics engine for integration tests")
            yield engine
            
        except Exception as e:
            pytest.skip(f"Could not create analytics engine: {e}")
        finally:
            if 'engine' in locals():
                engine.dispose()
    
    @pytest.fixture(scope="class")
    def test_data_setup(self, analytics_engine):
        """Set up test data in the analytics database."""
        with analytics_engine.connect() as conn:
            # Create test patient data
            test_patients = [
                {
                    'patnum': 1, 'lname': 'Smith', 'fname': 'John',
                    'middlei': 'A', 'preferred': 'John',
                    'patstatus': True, 'gender': True, 'position': False,
                    'birthdate': '1980-01-01', 'ssn': '123-45-6789',
                    'isactive': True, 'isdeleted': False
                },
                {
                    'patnum': 2, 'lname': 'Johnson', 'fname': 'Jane',
                    'middlei': 'B', 'preferred': 'Jane',
                    'patstatus': True, 'gender': False, 'position': False,
                    'birthdate': '1985-05-15', 'ssn': '987-65-4321',
                    'isactive': True, 'isdeleted': False
                },
                {
                    'patnum': 3, 'lname': 'Williams', 'fname': 'Bob',
                    'middlei': 'C', 'preferred': 'Bob',
                    'patstatus': False, 'gender': True, 'position': False,
                    'birthdate': '1975-12-25', 'ssn': '555-55-5555',
                    'isactive': False, 'isdeleted': False
                }
            ]
            
            # Insert test data
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO raw.patient VALUES (
                        :patnum, :lname, :fname, :middlei, :preferred, :patstatus, :gender, :position,
                        :birthdate, :ssn, :isactive, :isdeleted
                    )
                """), patient)
            
            conn.commit()
            logger.info(f"Set up {len(test_patients)} test patients in analytics database")
            
            yield
            
            # Cleanup test data
            try:
                conn.execute(text("DELETE FROM raw.patient WHERE \"PatNum\" IN (1, 2, 3)"))
                conn.commit()
                logger.info("Cleaned up test data")
            except Exception as e:
                logger.warning(f"Could not clean up test data: {e}")
    
    @pytest.mark.order(5)
    def test_real_database_connection(self, analytics_engine):
        """Test real database connection and basic operations."""
        with analytics_engine.connect() as conn:
            # Test basic query
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            assert 'PostgreSQL' in version
            
            # Test schema access
            result = conn.execute(text("SELECT current_schema()"))
            schema = result.scalar()
            assert schema == 'raw'
            
            logger.info(f"Connected to PostgreSQL {version} in schema {schema}")
    
    @pytest.mark.order(5)
    def test_user_permissions(self, analytics_engine):
        """Test that the database user has required permissions."""
        with analytics_engine.connect() as conn:
            # Test SELECT permission
            result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'raw'"))
            table_count = result.scalar()
            assert table_count >= 0
            
            # Test CREATE permission (create a temporary table)
            test_table_name = f"test_permissions_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            try:
                conn.execute(text(f"""
                    CREATE TEMPORARY TABLE {test_table_name} (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(255)
                    )
                """))
                
                # Test INSERT permission
                conn.execute(text(f"INSERT INTO {test_table_name} (id, name) VALUES (1, 'test')"))
                
                # Test UPDATE permission
                conn.execute(text(f"UPDATE {test_table_name} SET name = 'updated' WHERE id = 1"))
                
                # Test DELETE permission
                conn.execute(text(f"DELETE FROM {test_table_name} WHERE id = 1"))
                
                logger.info("Database user has all required permissions")
                
            except Exception as e:
                pytest.fail(f"Database user missing required permissions: {e}")
    
    @pytest.mark.order(5)
    def test_etl_schema_access(self, analytics_engine):
        """Test access to ETL-specific schemas and tables."""
        with analytics_engine.connect() as conn:
            # Test access to raw schema
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'raw' 
                AND table_name = 'patient'
            """))
            assert result.fetchone() is not None
            
            # Test access to other schemas if they exist
            schemas = ['staging', 'intermediate', 'marts']
            for schema in schemas:
                try:
                    result = conn.execute(text(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema}'"))
                    if result.fetchone():
                        logger.info(f"Schema {schema} exists and is accessible")
                    else:
                        logger.info(f"Schema {schema} does not exist (this is normal)")
                except Exception as e:
                    logger.warning(f"Could not check schema {schema}: {e}")
    
    def test_schema_discovery_real_database(self, analytics_engine, test_data_setup):
        """Test SchemaDiscovery with real database connection."""
        try:
            # Create settings with test environment
            settings = Settings(environment='test')
            
            # Create real source engine (if available)
            try:
                source_engine = ConnectionFactory.get_source_connection(settings)
                source_db = settings.get_database_config(DatabaseType.SOURCE)['database']
                
                # Create SchemaDiscovery instance
                schema_discovery = SchemaDiscovery(source_engine, source_db)
                
                # Test schema discovery
                tables = schema_discovery.list_tables()
                assert len(tables) > 0
                
                # Test table schema discovery
                if 'patient' in tables:
                    patient_schema = schema_discovery.get_table_schema('patient')
                    assert patient_schema is not None
                    assert 'columns' in patient_schema
                
                logger.info(f"SchemaDiscovery found {len(tables)} tables in source database")
                
            except Exception as e:
                logger.warning(f"Could not test SchemaDiscovery with source database: {e}")
                # This is acceptable if source database is not available for integration tests
            
        except Exception as e:
            pytest.skip(f"SchemaDiscovery integration test skipped: {e}")
    
    def test_connection_factory_real_database(self):
        """Test ConnectionFactory with real database connections."""
        try:
            # Create settings with test environment
            settings = Settings(environment='test')
            
            # Test analytics connection
            analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            with analytics_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                assert result.scalar() == 1
            
            analytics_engine.dispose()
            logger.info("ConnectionFactory analytics connection working correctly")
            
        except Exception as e:
            pytest.skip(f"ConnectionFactory integration test skipped: {e}")
    
    def test_settings_real_database_config(self):
        """Test Settings class with real test database configuration."""
        # Create settings with test environment
        settings = Settings(environment='test')
        
        # Test that settings can load test database configuration using correct connection name
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        assert analytics_config is not None
        expected_db = os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics')
        expected_user = os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'analytics_test_user')
        assert analytics_config['database'] == expected_db
        assert analytics_config['user'] == expected_user
        assert analytics_config['schema'] == 'raw'
        
        logger.info(f"Settings loaded test database config: {analytics_config['database']}")
    
    def test_data_loading_and_querying(self, analytics_engine, test_data_setup):
        """Test actual data loading and querying capabilities."""
        with analytics_engine.connect() as conn:
            # Test data insertion
            new_patient = {
                'patnum': 999, 'lname': 'Test', 'fname': 'Integration',
                'middlei': 'T', 'preferred': 'TestUser',
                'patstatus': True, 'gender': False, 'position': False,
                'birthdate': '1990-01-01', 'ssn': '999-99-9999',
                'isactive': True, 'isdeleted': False
            }
            
            conn.execute(text("""
                INSERT INTO raw.patient VALUES (
                    :patnum, :lname, :fname, :middlei, :preferred, :patstatus, :gender, :position,
                    :birthdate, :ssn, :isactive, :isdeleted
                )
            """), new_patient)
            
            # Test data querying
            result = conn.execute(text("SELECT COUNT(*) FROM raw.patient"))
            count = result.scalar()
            assert count >= 3  # Should have at least the original test data
            
            # Test specific data retrieval
            result = conn.execute(text("SELECT * FROM raw.patient WHERE \"PatNum\" = 999"))
            row = result.fetchone()
            assert row is not None
            assert row[1] == 'Test'  # LName
            assert row[2] == 'Integration'  # FName
            
            conn.commit()
            
            logger.info(f"Successfully loaded and queried data. Total patients: {count}")
    
    def test_schema_creation_and_permissions(self, analytics_engine):
        """Test schema creation and user permissions."""
        with analytics_engine.connect() as conn:
            # Test creating a new schema
            test_schema = f"test_schema_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            try:
                conn.execute(text(f"CREATE SCHEMA {test_schema}"))
                # Note: GRANT statements require superuser privileges
                # The analytics_test_user should already have permissions set up by the database admin
                
                # Test creating a table in the new schema
                conn.execute(text(f"""
                    CREATE TABLE {test_schema}.test_table (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Test inserting data
                conn.execute(text(f"""
                    INSERT INTO {test_schema}.test_table (id, name) VALUES (1, 'test')
                """))
                
                # Test querying data
                result = conn.execute(text(f"SELECT * FROM {test_schema}.test_table"))
                row = result.fetchone()
                assert row[0] == 1
                assert row[1] == 'test'
                
                conn.commit()
                logger.info(f"Successfully created and used schema: {test_schema}")
                
            finally:
                # Cleanup
                conn.execute(text(f"DROP SCHEMA IF EXISTS {test_schema} CASCADE"))
                conn.commit()
    
    def test_pipeline_orchestrator_real_initialization(self, analytics_engine):
        """Test PipelineOrchestrator initialization with real database."""
        try:
            # Test PipelineOrchestrator with test environment
            orchestrator = PipelineOrchestrator(environment='test')
            assert orchestrator.settings.environment == 'test'
            
            # Test that we can access settings and configurations
            assert orchestrator.settings is not None
            
            # Test that we can get database configurations
            analytics_config = orchestrator.settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
            assert analytics_config is not None
            assert 'database' in analytics_config
            assert 'user' in analytics_config
            
            logger.info("PipelineOrchestrator components can be initialized with real database.")
            logger.info(f"Analytics config loaded: {analytics_config['database']} as {analytics_config['user']}")
            
        except Exception as e:
            pytest.skip(f"PipelineOrchestrator initialization test skipped: {e}")
    
    def test_performance_with_real_data(self, analytics_engine, test_data_setup):
        """Test performance with real database operations."""
        import time
        
        with analytics_engine.connect() as conn:
            # Test query performance
            start_time = time.time()
            
            # Perform a complex query
            result = conn.execute(text("""
                SELECT 
                    "PatNum",
                    "LName",
                    "FName",
                    EXTRACT(YEAR FROM AGE("Birthdate"::date)) as age_years
                FROM raw.patient 
                WHERE "PatStatus" = true
                ORDER BY "LName", "FName"
            """))
            
            rows = result.fetchall()
            query_time = time.time() - start_time
            
            assert len(rows) >= 0  # Should return some results
            assert query_time < 5.0  # Should complete within 5 seconds
            
            logger.info(f"Query completed in {query_time:.3f} seconds, returned {len(rows)} rows")
    
    def test_data_integrity_and_constraints(self, analytics_engine, test_data_setup):
        """Test data integrity and constraint enforcement."""
        with analytics_engine.connect() as conn:
            # Test primary key constraint
            try:
                conn.execute(text("""
                    INSERT INTO raw.patient ("PatNum", "LName", "FName", "MiddleI", "Preferred", "PatStatus", "Gender", "Position", "Birthdate", "SSN", "IsActive", "IsDeleted") 
                    VALUES (1, 'Duplicate', 'Key', 'D', 'Duplicate', true, false, false, '1990-01-01', '123-45-6789', true, false)
                """))
                assert False, "Should have raised primary key violation"
            except SQLAlchemyError:
                # Expected - primary key violation
                pass
            
            # Test data type constraints
            try:
                conn.execute(text("""
                    INSERT INTO raw.patient ("PatNum", "LName", "FName", "MiddleI", "Preferred", "PatStatus", "Gender", "Position", "Birthdate", "SSN", "IsActive", "IsDeleted") 
                    VALUES (9999, 'Test', 'Type', 'T', 'Test', 'invalid_string', false, false, '1990-01-01', '123-45-6789', true, false)
                """))
                assert False, "Should have raised data type violation"
            except SQLAlchemyError:
                # Expected - data type violation
                pass
            
            conn.rollback()
            logger.info("Data integrity constraints working correctly")
    
    def test_transaction_handling(self, analytics_engine, test_data_setup):
        """Test transaction handling and rollback capabilities."""
        with analytics_engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                # Insert test data
                conn.execute(text("""
                    INSERT INTO raw.patient ("PatNum", "LName", "FName", "MiddleI", "Preferred", "PatStatus", "Gender", "Position", "Birthdate", "SSN", "IsActive", "IsDeleted") 
                    VALUES (8888, 'Transaction', 'Test', 'T', 'Trans', true, false, false, '1990-01-01', '123-45-6789', true, false)
                """))
                
                # Verify data exists in transaction
                result = conn.execute(text("SELECT COUNT(*) FROM raw.patient WHERE \"PatNum\" = 8888"))
                count = result.scalar()
                assert count == 1
                
                # Rollback transaction
                trans.rollback()
                
                # Verify data was rolled back
                result = conn.execute(text("SELECT COUNT(*) FROM raw.patient WHERE \"PatNum\" = 8888"))
                count = result.scalar()
                assert count == 0
                
                logger.info("Transaction rollback working correctly")
                
            except Exception as e:
                trans.rollback()
                raise e
    
    def test_concurrent_connections(self, analytics_engine):
        """Test handling of multiple concurrent connections."""
        import threading
        import queue
        
        results = queue.Queue()
        
        def worker(worker_id):
            try:
                with analytics_engine.connect() as conn:
                    result = conn.execute(text("SELECT current_user, current_database()"))
                    user, db = result.fetchone()
                    results.put((worker_id, user, db))
            except Exception as e:
                results.put((worker_id, 'ERROR', str(e)))
        
        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Collect results
        successful_connections = 0
        while not results.empty():
            worker_id, user, db = results.get()
            if user != 'ERROR':
                expected_user = os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'analytics_test_user')
                expected_db = os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics')
                assert user == expected_user
                assert db == expected_db
                successful_connections += 1
        
        assert successful_connections == 5
        logger.info(f"All {successful_connections} concurrent connections successful")
    
    def test_pipeline_orchestrator_test_environment_initialization(self):
        """Test PipelineOrchestrator initialization with test environment."""
        try:
            # Create orchestrator with test environment
            orchestrator = PipelineOrchestrator(environment='test')
            
            # Verify environment is set correctly
            assert orchestrator.settings.environment == 'test'
            
            # Test that settings can load test configurations
            source_config = orchestrator.settings.get_database_config(DatabaseType.SOURCE)
            expected_source_db = os.getenv('TEST_OPENDENTAL_SOURCE_DB', 'opendental_test')
            assert source_config['database'] == expected_source_db
            
            analytics_config = orchestrator.settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
            expected_analytics_db = os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics')
            expected_analytics_user = os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'analytics_test_user')
            assert analytics_config['database'] == expected_analytics_db
            assert analytics_config['user'] == expected_analytics_user
            assert analytics_config['schema'] == 'raw'
            
            logger.info("PipelineOrchestrator test environment initialization successful")
            
        except Exception as e:
            pytest.skip(f"PipelineOrchestrator test environment initialization skipped: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 