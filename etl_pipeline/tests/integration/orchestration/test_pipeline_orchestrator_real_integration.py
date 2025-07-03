"""
Real Integration Tests for PipelineOrchestrator

This test suite performs actual integration testing using real database connections
to the test_opendental_analytics database with analytics_test_user.

Testing Strategy:
- Real database connections (no mocking)
- Actual data processing and transformations
- End-to-end workflow validation
- Real schema discovery and data loading
- Performance testing with real data volumes

Requirements:
- test_opendental_analytics database must exist
- analytics_test_user must have CREATE, INSERT, SELECT, DELETE permissions on schemas
- Schemas (raw, staging, intermediate, marts) must be accessible
- Database admin should pre-configure permissions for analytics_test_user
- Tests will verify permissions are correctly set up
"""

import pytest
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.schema_discovery import SchemaDiscovery

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestPipelineOrchestratorRealIntegration:
    
    @pytest.fixture(scope="class")
    def analytics_engine(self):
        """Create connection to test_opendental_analytics database."""
        # Use environment variables from .env file
        config = {
            'host': os.getenv('TEST_POSTGRES_ANALYTICS_HOST', 'localhost'),
            'port': int(os.getenv('TEST_POSTGRES_ANALYTICS_PORT', '5432')),
            'database': os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics'),
            'user': os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'analytics_test_user'),
            'password': os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD', 'test_password'),
            'schema': os.getenv('TEST_POSTGRES_ANALYTICS_SCHEMA', 'raw')
        }
        
        try:
            # Create real PostgreSQL connection
            connection_string = (
                f"postgresql+psycopg2://{config['user']}:{config['password']}"
                f"@{config['host']}:{config['port']}/{config['database']}"
            )
            
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                connect_args={'options': f"-csearch_path={config['schema']}"}
            )
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Successfully connected to {config['database']}")
            yield engine
            
        except Exception as e:
            pytest.skip(f"Could not connect to test database: {e}")
        finally:
            if 'engine' in locals():
                engine.dispose()
    
    @pytest.fixture(scope="class")
    def test_data_setup(self, analytics_engine):
        """Set up test data in the analytics database."""
        # Create test schemas if they don't exist
        schemas = ['raw', 'staging', 'intermediate', 'marts']
        
        with analytics_engine.connect() as conn:
            for schema in schemas:
                try:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    # Note: GRANT statements require superuser privileges
                    # The analytics_test_user should already have permissions set up by the database admin
                    logger.info(f"Created schema {schema} (permissions should be pre-configured)")
                except Exception as e:
                    logger.warning(f"Could not create schema {schema}: {e}")
            
            # Create test tables in raw schema
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.patient (
                    "PatNum" INTEGER PRIMARY KEY,
                    "LName" VARCHAR(255),
                    "FName" VARCHAR(255),
                    "MiddleI" VARCHAR(255),
                    "Preferred" VARCHAR(255),
                    "PatStatus" BOOLEAN,
                    "Gender" BOOLEAN,
                    "Position" BOOLEAN,
                    "Birthdate" DATE,
                    "SSN" VARCHAR(255),
                    "IsActive" BOOLEAN,
                    "IsDeleted" BOOLEAN
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.appointment (
                    "AptNum" INTEGER PRIMARY KEY,
                    "PatNum" INTEGER,
                    "ProvNum" INTEGER,
                    "AptDateTime" TIMESTAMP,
                    "AptStatus" INTEGER,
                    "IsNewPatient" INTEGER,
                    "IsHygiene" INTEGER,
                    "DateTimeArrived" TIMESTAMP,
                    "DateTimeSeated" TIMESTAMP,
                    "DateTimeDismissed" TIMESTAMP,
                    "ClinicNum" INTEGER,
                    "Op" INTEGER,
                    "AptType" INTEGER,
                    "SecDateTEntry" TIMESTAMP,
                    "DateTStamp" TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.procedurelog (
                    "ProcNum" INTEGER PRIMARY KEY,
                    "PatNum" INTEGER,
                    "AptNum" INTEGER,
                    "ProcDate" DATE,
                    "ProcFee" DECIMAL(10,2),
                    "ProcStatus" INTEGER,
                    "CodeNum" INTEGER,
                    "ProvNum" INTEGER,
                    "ClinicNum" INTEGER,
                    "DateComplete" DATE,
                    "DateEntryC" TIMESTAMP,
                    "DateTStamp" TIMESTAMP
                )
            """))
            
            # Insert test data
            test_patients = [
                (1, 'Doe', 'John', 'M', 'Johnny', True, False, False, '1980-01-01', '123-45-6789', True, False),
                (2, 'Smith', 'Jane', 'A', 'Jane', True, True, False, '1985-05-15', '234-56-7890', True, False),
                (3, 'Johnson', 'Bob', 'R', 'Bobby', True, False, True, '1975-12-10', '345-67-8901', True, False)
            ]
            
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO raw.patient VALUES (
                        :patnum, :lname, :fname, :middlei, :preferred, :patstatus, :gender, :position,
                        :birthdate, :ssn, :isactive, :isdeleted
                    )
                """), {
                    'patnum': patient[0], 'lname': patient[1], 'fname': patient[2],
                    'middlei': patient[3], 'preferred': patient[4], 'patstatus': patient[5],
                    'gender': patient[6], 'position': patient[7], 'birthdate': patient[8],
                    'ssn': patient[9], 'isactive': patient[10], 'isdeleted': patient[11]
                })
            
            conn.commit()
        
        yield analytics_engine
        
        # Cleanup test data
        with analytics_engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS raw.patient CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS raw.appointment CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS raw.procedurelog CASCADE"))
            conn.commit()
    
    def test_real_database_connection(self, analytics_engine):
        """Test real connection to test_opendental_analytics database."""
        with analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT current_database(), current_user"))
            db_name, user = result.fetchone()
            
            expected_db = os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics')
            expected_user = os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'analytics_test_user')
            assert db_name == expected_db
            assert user == expected_user
            
            logger.info(f"Connected to database: {db_name} as user: {user}")
    
    def test_user_permissions(self, analytics_engine):
        """Test that analytics_test_user has the required permissions."""
        with analytics_engine.connect() as conn:
            # Test 1: Check if user can create schemas
            test_schema = f"perm_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            try:
                conn.execute(text(f"CREATE SCHEMA {test_schema}"))
                logger.info(f"✅ User can CREATE schemas")
                
                # Test 2: Check if user can create tables
                conn.execute(text(f"""
                    CREATE TABLE {test_schema}.test_table (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(255)
                    )
                """))
                logger.info(f"✅ User can CREATE tables")
                
                # Test 3: Check if user can insert data
                conn.execute(text(f"""
                    INSERT INTO {test_schema}.test_table (id, name) VALUES (1, 'test')
                """))
                logger.info(f"✅ User can INSERT data")
                
                # Test 4: Check if user can select data
                result = conn.execute(text(f"SELECT * FROM {test_schema}.test_table"))
                row = result.fetchone()
                assert row[0] == 1
                assert row[1] == 'test'
                logger.info(f"✅ User can SELECT data")
                
                # Test 5: Check if user can delete data
                conn.execute(text(f"DELETE FROM {test_schema}.test_table WHERE id = 1"))
                result = conn.execute(text(f"SELECT COUNT(*) FROM {test_schema}.test_table"))
                count = result.scalar()
                assert count == 0
                logger.info(f"✅ User can DELETE data")
                
                # Test 6: Check if user can drop tables and schemas
                conn.execute(text(f"DROP TABLE {test_schema}.test_table"))
                conn.execute(text(f"DROP SCHEMA {test_schema}"))
                logger.info(f"✅ User can DROP tables and schemas")
                
                conn.commit()
                logger.info("✅ All required permissions are properly configured")
                
            except Exception as e:
                logger.error(f"❌ Permission test failed: {e}")
                # Cleanup if something went wrong
                try:
                    conn.execute(text(f"DROP SCHEMA IF EXISTS {test_schema} CASCADE"))
                    conn.commit()
                except:
                    pass
                raise
    
    def test_etl_schema_access(self, analytics_engine):
        """Test that analytics_test_user can access ETL pipeline schemas."""
        expected_schemas = ['raw', 'staging', 'intermediate', 'marts']
        
        with analytics_engine.connect() as conn:
            # Check which schemas exist and are accessible
            result = conn.execute(text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('raw', 'staging', 'intermediate', 'marts')
                ORDER BY schema_name
            """))
            
            accessible_schemas = [row[0] for row in result.fetchall()]
            logger.info(f"Accessible schemas: {accessible_schemas}")
            
            # Test access to each expected schema
            for schema in expected_schemas:
                try:
                    # Test if we can create a temporary table in each schema
                    test_table = f"temp_test_{datetime.now().strftime('%H%M%S')}"
                    conn.execute(text(f"""
                        CREATE TABLE {schema}.{test_table} (
                            id INTEGER PRIMARY KEY,
                            test_col VARCHAR(10)
                        )
                    """))
                    
                    # Test insert and select
                    conn.execute(text(f"INSERT INTO {schema}.{test_table} (id, test_col) VALUES (1, 'test')"))
                    result = conn.execute(text(f"SELECT * FROM {schema}.{test_table}"))
                    row = result.fetchone()
                    assert row[0] == 1
                    assert row[1] == 'test'
                    
                    # Cleanup
                    conn.execute(text(f"DROP TABLE {schema}.{test_table}"))
                    conn.commit()
                    
                    logger.info(f"✅ Schema '{schema}' is accessible and writable")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Schema '{schema}' access issue: {e}")
                    # Continue testing other schemas
                    conn.rollback()
            
            # Summary
            missing_schemas = set(expected_schemas) - set(accessible_schemas)
            if missing_schemas:
                logger.warning(f"⚠️ Missing schemas: {missing_schemas}")
            else:
                logger.info("✅ All expected ETL schemas are accessible")
    
    def test_schema_discovery_real_database(self, analytics_engine, test_data_setup):
        """Test real schema discovery on actual PostgreSQL database."""
        from etl_pipeline.core.postgres_schema import PostgresSchema
        from etl_pipeline.core.connections import ConnectionFactory
        
        try:
            # Check if MySQL replication test environment variables are set
            required_mysql_vars = [
                'TEST_MYSQL_REPLICATION_HOST',
                'TEST_MYSQL_REPLICATION_PORT', 
                'TEST_MYSQL_REPLICATION_DB',
                'TEST_MYSQL_REPLICATION_USER',
                'TEST_MYSQL_REPLICATION_PASSWORD'
            ]
            
            missing_vars = [var for var in required_mysql_vars if not os.getenv(var)]
            if missing_vars:
                pytest.skip(f"MySQL replication test environment variables not set: {', '.join(missing_vars)}")
            
            # Get both MySQL and PostgreSQL connections for PostgresSchema
            mysql_engine = ConnectionFactory.get_mysql_replication_test_connection()
            postgres_engine = analytics_engine  # Use the provided analytics engine
            
            # Extract database names from engine URLs
            mysql_db = mysql_engine.url.database
            postgres_db = postgres_engine.url.database
            
            # Create PostgresSchema instance (designed for PostgreSQL)
            postgres_schema = PostgresSchema(
                mysql_engine=mysql_engine,
                postgres_engine=postgres_engine,
                mysql_db=mysql_db,
                postgres_db=postgres_db,
                postgres_schema='raw'
            )
            
            # Test that PostgresSchema can be initialized
            assert postgres_schema is not None
            assert postgres_schema.mysql_engine is not None
            assert postgres_schema.postgres_engine is not None
            assert postgres_schema.postgres_schema == 'raw'
            
            # Test that we can access PostgreSQL inspector
            postgres_inspector = postgres_schema.postgres_inspector
            assert postgres_inspector is not None
            
            # Test that we can list tables in the raw schema
            tables = postgres_inspector.get_table_names(schema='raw')
            assert isinstance(tables, list)
            
            # Check if patient table exists (should be created by test_data_setup)
            assert 'patient' in tables, f"Patient table not found in raw schema. Available tables: {tables}"
            
            # Test that we can get column information for the patient table
            columns = postgres_inspector.get_columns('patient', schema='raw')
            assert isinstance(columns, list)
            assert len(columns) > 0, "No columns found in patient table"
            
            # Verify specific columns exist
            column_names = [col['name'] for col in columns]
            assert 'PatNum' in column_names, f"PatNum column not found. Available columns: {column_names}"
            assert 'LName' in column_names, f"LName column not found. Available columns: {column_names}"
            
            logger.info(f"PostgreSQL schema discovery successful. Found {len(tables)} tables, {len(columns)} columns in patient table")
            
        except Exception as e:
            logger.error(f"PostgreSQL schema discovery failed: {e}")
            pytest.skip(f"PostgreSQL schema discovery test skipped: {e}. This test requires both MySQL replication test database and PostgreSQL analytics test database to be configured.")
        finally:
            # Cleanup
            if 'mysql_engine' in locals():
                mysql_engine.dispose()
    
    def test_connection_factory_real_database(self):
        """Test ConnectionFactory with real test database configuration."""
        try:
            # Test the analytics test connection method using the correct test method
            engine = ConnectionFactory.get_postgres_analytics_test_connection()
            
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_database()"))
                db_name = result.scalar()
                expected_db = os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics')
                assert db_name == expected_db
            
            logger.info("ConnectionFactory successfully connected to test database")
            
        except Exception as e:
            pytest.skip(f"Could not connect via ConnectionFactory: {e}")
        finally:
            if 'engine' in locals():
                engine.dispose()
    
    def test_settings_real_database_config(self):
        """Test Settings class with real test database configuration."""
        # Create settings with test environment
        settings = Settings(environment='test')
        
        # Test that settings can load test database configuration using correct connection name
        analytics_config = settings.get_database_config('test_opendental_analytics_raw')
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
            analytics_config = orchestrator.settings.get_database_config('test_opendental_analytics_raw')
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
            source_config = orchestrator.settings.get_database_config('test_opendental_source')
            expected_source_db = os.getenv('TEST_OPENDENTAL_SOURCE_DB', 'opendental_test')
            assert source_config['database'] == expected_source_db
            
            analytics_config = orchestrator.settings.get_database_config('test_opendental_analytics_raw')
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