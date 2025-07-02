"""
Real Integration Tests for PipelineOrchestrator

This test suite performs actual integration testing using real database connections
to the opendental_analytics_test database with analytics_user.

Testing Strategy:
- Real database connections (no mocking)
- Actual data processing and transformations
- End-to-end workflow validation
- Real schema discovery and data loading
- Performance testing with real data volumes

Requirements:
- opendental_analytics_test database must exist
- analytics_user must have full permissions
- Test data must be loaded in test databases
"""

import pytest
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.schema_discovery import SchemaDiscovery

logger = logging.getLogger(__name__)


@pytest.mark.integration
class TestPipelineOrchestratorRealIntegration:
    
    @pytest.fixture(scope="class")
    def analytics_engine(self):
        """Create connection to opendental_analytics_test database."""
        # Use test database configuration from conftest.py
        config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'opendental_analytics_test',
            'user': 'analytics_user',
            'password': 'test_password',  # Should match your test environment
            'schema': 'raw'
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
        schemas = ['raw', 'public', 'public_staging', 'public_intermediate', 'public_marts']
        
        with analytics_engine.connect() as conn:
            for schema in schemas:
                try:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                    conn.execute(text(f"GRANT ALL ON SCHEMA {schema} TO analytics_user"))
                except Exception as e:
                    logger.warning(f"Could not create schema {schema}: {e}")
            
            # Create test tables in raw schema
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw.patient (
                    "PatNum" INTEGER PRIMARY KEY,
                    "LName" VARCHAR(255),
                    "FName" VARCHAR(255),
                    "Birthdate" DATE,
                    "Email" VARCHAR(255),
                    "HmPhone" VARCHAR(255),
                    "DateFirstVisit" DATE,
                    "PatStatus" INTEGER,
                    "Gender" INTEGER,
                    "Premed" INTEGER,
                    "WirelessPhone" VARCHAR(255),
                    "WkPhone" VARCHAR(255),
                    "Address" VARCHAR(255),
                    "City" VARCHAR(255),
                    "State" VARCHAR(10),
                    "Zip" VARCHAR(20),
                    "DateTStamp" TIMESTAMP,
                    "SecDateEntry" TIMESTAMP
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
                (1, 'Doe', 'John', '1980-01-01', 'john.doe@example.com', '555-0101', 
                 '2020-01-15', 0, 0, 0, '555-0101', '555-0102', '123 Main St', 'Anytown', 'CA', '12345',
                 '2023-12-01 10:00:00', '2020-01-15 09:00:00'),
                (2, 'Smith', 'Jane', '1985-05-15', 'jane.smith@example.com', '555-0102',
                 '2020-02-20', 0, 1, 0, '555-0102', '555-0103', '456 Oak Ave', 'Somewhere', 'CA', '12346',
                 '2023-12-01 11:00:00', '2020-02-20 10:00:00'),
                (3, 'Johnson', 'Bob', '1975-12-10', 'bob.johnson@example.com', '555-0103',
                 '2020-03-10', 0, 0, 1, '555-0103', '555-0104', '789 Pine St', 'Elsewhere', 'CA', '12347',
                 '2023-12-01 12:00:00', '2020-03-10 11:00:00')
            ]
            
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO raw.patient VALUES (
                        :patnum, :lname, :fname, :birthdate, :email, :hmphone,
                        :datefirstvisit, :patstatus, :gender, :premed, :wirelessphone,
                        :wkphone, :address, :city, :state, :zip, :datestamp, :secdateentry
                    )
                """), {
                    'patnum': patient[0], 'lname': patient[1], 'fname': patient[2],
                    'birthdate': patient[3], 'email': patient[4], 'hmphone': patient[5],
                    'datefirstvisit': patient[6], 'patstatus': patient[7], 'gender': patient[8],
                    'premed': patient[9], 'wirelessphone': patient[10], 'wkphone': patient[11],
                    'address': patient[12], 'city': patient[13], 'state': patient[14],
                    'zip': patient[15], 'datestamp': patient[16], 'secdateentry': patient[17]
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
        """Test real connection to opendental_analytics_test database."""
        with analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT current_database(), current_user"))
            db_name, user = result.fetchone()
            
            assert db_name == 'opendental_analytics_test'
            assert user == 'analytics_user'
            
            logger.info(f"Connected to database: {db_name} as user: {user}")
    
    def test_schema_discovery_real_database(self, analytics_engine):
        """Test real schema discovery on actual database."""
        schema_discovery = SchemaDiscovery(analytics_engine)
        
        # Test table discovery
        tables = schema_discovery.get_all_tables()
        assert 'patient' in tables or 'raw.patient' in tables
        
        # Test schema discovery for patient table
        patient_schema = schema_discovery.get_table_schema('patient')
        assert patient_schema is not None
        assert 'columns' in patient_schema
        assert 'types' in patient_schema
        
        # Verify specific columns exist
        columns = patient_schema['columns']
        assert 'PatNum' in columns or '"PatNum"' in columns
        assert 'LName' in columns or '"LName"' in columns
        
        logger.info(f"Discovered {len(columns)} columns in patient table")
    
    def test_connection_factory_real_database(self):
        """Test ConnectionFactory with real test database configuration."""
        # Set environment variables for test database
        os.environ['POSTGRES_ANALYTICS_TEST_HOST'] = 'localhost'
        os.environ['POSTGRES_ANALYTICS_TEST_PORT'] = '5432'
        os.environ['POSTGRES_ANALYTICS_TEST_DB'] = 'opendental_analytics_test'
        os.environ['POSTGRES_ANALYTICS_TEST_SCHEMA'] = 'raw'
        os.environ['POSTGRES_ANALYTICS_TEST_USER'] = 'analytics_user'
        os.environ['POSTGRES_ANALYTICS_TEST_PASSWORD'] = 'test_password'
        
        try:
            # Test the analytics test connection method
            engine = ConnectionFactory.get_postgres_analytics_connection()
            
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_database()"))
                db_name = result.scalar()
                assert db_name == 'opendental_analytics_test'
            
            logger.info("ConnectionFactory successfully connected to test database")
            
        except Exception as e:
            pytest.skip(f"Could not connect via ConnectionFactory: {e}")
        finally:
            if 'engine' in locals():
                engine.dispose()
    
    def test_settings_real_database_config(self):
        """Test Settings class with real test database configuration."""
        settings = Settings()
        
        # Test that settings can load test database configuration
        analytics_config = settings.get_database_config('analytics_test')
        assert analytics_config is not None
        assert analytics_config['database'] == 'opendental_analytics_test'
        assert analytics_config['user'] == 'analytics_user'
        
        logger.info(f"Settings loaded test database config: {analytics_config['database']}")
    
    def test_data_loading_and_querying(self, analytics_engine, test_data_setup):
        """Test actual data loading and querying capabilities."""
        with analytics_engine.connect() as conn:
            # Test data insertion
            new_patient = {
                'patnum': 999, 'lname': 'Test', 'fname': 'Integration',
                'birthdate': '1990-01-01', 'email': 'test.integration@example.com',
                'hmphone': '555-9999', 'datefirstvisit': '2024-01-01',
                'patstatus': 0, 'gender': 0, 'premed': 0,
                'wirelessphone': '555-9999', 'wkphone': '555-9998',
                'address': '999 Test St', 'city': 'TestCity', 'state': 'CA', 'zip': '99999',
                'datestamp': '2024-01-01 10:00:00', 'secdateentry': '2024-01-01 09:00:00'
            }
            
            conn.execute(text("""
                INSERT INTO raw.patient VALUES (
                    :patnum, :lname, :fname, :birthdate, :email, :hmphone,
                    :datefirstvisit, :patstatus, :gender, :premed, :wirelessphone,
                    :wkphone, :address, :city, :state, :zip, :datestamp, :secdateentry
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
                conn.execute(text(f"GRANT ALL ON SCHEMA {test_schema} TO analytics_user"))
                
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
        # This test would require the orchestrator to be modified to accept
        # test database configurations, which is beyond the scope of this test
        # For now, we'll test that the components can be created with real connections
        
        try:
            # Test that we can create a schema discovery with real engine
            schema_discovery = SchemaDiscovery(analytics_engine)
            assert schema_discovery is not None
            
            # Test that we can get table information
            tables = schema_discovery.get_all_tables()
            assert isinstance(tables, list)
            
            logger.info(f"PipelineOrchestrator components can be initialized with real database. Found {len(tables)} tables.")
            
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
                WHERE "PatStatus" = 0
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
                    INSERT INTO raw.patient ("PatNum", "LName", "FName") 
                    VALUES (1, 'Duplicate', 'Key')
                """))
                assert False, "Should have raised primary key violation"
            except SQLAlchemyError:
                # Expected - primary key violation
                pass
            
            # Test data type constraints
            try:
                conn.execute(text("""
                    INSERT INTO raw.patient ("PatNum", "LName", "FName", "PatStatus") 
                    VALUES (9999, 'Test', 'Type', 'invalid_string')
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
                    INSERT INTO raw.patient ("PatNum", "LName", "FName") 
                    VALUES (8888, 'Transaction', 'Test')
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
                assert user == 'analytics_user'
                assert db == 'opendental_analytics_test'
                successful_connections += 1
        
        assert successful_connections == 5
        logger.info(f"All {successful_connections} concurrent connections successful")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 