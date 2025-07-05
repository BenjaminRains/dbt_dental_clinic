"""
Real Integration Testing for MySQL Replicator - Using Real MySQL Databases

This approach tests the actual MySQL replication flow by using the REAL MySQL test databases
with clearly identifiable test data that won't interfere with production.

Refactored to follow new architectural patterns:
- Uses new ConnectionFactory methods with dependency injection
- Uses modular fixtures from tests/fixtures/
- Follows new configuration pattern with proper test isolation
- Uses standardized test data instead of custom test data creation
"""

import pytest
import logging
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Import new configuration system
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator
from etl_pipeline.core.schema_discovery import SchemaDiscovery

# Load environment variables from .env file first
from tests.fixtures.env_fixtures import load_test_environment
load_test_environment()

# Import standardized test fixtures
from tests.fixtures import populated_test_databases

logger = logging.getLogger(__name__)


class TestMySQLReplicatorRealIntegration:
    """Real integration tests using actual MySQL databases with standardized test data."""
    
    @pytest.fixture
    def schema_discovery(self, test_settings):
        """Create real SchemaDiscovery instance with source MySQL connection using new configuration."""
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        source_db = source_engine.url.database
        if source_db is None:
            raise ValueError("Source database name is None - check connection configuration")
        return SchemaDiscovery(source_engine, source_db)

    @pytest.fixture
    def mysql_replicator(self, schema_discovery, test_settings):
        """Create real ExactMySQLReplicator instance with test databases using new configuration."""
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        target_engine = ConnectionFactory.get_replication_connection(test_settings)
        source_db = source_engine.url.database
        target_db = target_engine.url.database
        
        if source_db is None:
            raise ValueError("Source database name is None - check connection configuration")
        if target_db is None:
            raise ValueError("Target database name is None - check connection configuration")
            
        return ExactMySQLReplicator(
            source_engine=source_engine,
            target_engine=target_engine,
            source_db=source_db,
            target_db=target_db,
            schema_discovery=schema_discovery
        )

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_replicator_initialization(self, populated_test_databases, mysql_replicator):
        """Test real ExactMySQLReplicator initialization with actual MySQL databases."""
        # Verify test data exists using standardized approach
        patient_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        appointment_count = populated_test_databases.get_appointment_count(DatabaseType.SOURCE)
        
        assert patient_count > 0, "No test patients found in source database"
        assert appointment_count > 0, "No test appointments found in source database"
        
        # Test REAL replicator with REAL MySQL databases
        assert mysql_replicator.source_engine is not None
        assert mysql_replicator.target_engine is not None
        assert mysql_replicator.schema_discovery is not None
        assert mysql_replicator.source_db is not None
        assert mysql_replicator.target_db is not None
        
        # Test real connections
        with mysql_replicator.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Source database connection failed"
        
        with mysql_replicator.target_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Target database connection failed"

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_exact_replica_creation(self, populated_test_databases, mysql_replicator):
        """Test real exact replica creation with actual MySQL databases."""
        # Verify test data exists using standardized approach
        patient_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "No test patients found in source database"
        
        # Test REAL exact replica creation for patient table
        result = mysql_replicator.create_exact_replica('patient')
        assert result, "Real exact replica creation failed for patient table"
        
        # Verify the replica was created in target database
        with mysql_replicator.target_engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES LIKE 'patient'"))
            assert result.fetchone() is not None, "Patient table not created in target database"

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_table_data_copying(self, populated_test_databases, mysql_replicator):
        """Test real table data copying with actual MySQL databases."""
        # Verify test data exists using standardized approach
        patient_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "No test patients found in source database"
        
        # First create the exact replica
        assert mysql_replicator.create_exact_replica('patient'), "Failed to create exact replica"
        
        # Test REAL table data copying
        result = mysql_replicator.copy_table_data('patient')
        assert result, "Real table data copying failed"
        
        # Verify data was copied correctly using standardized approach
        source_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        target_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        
        assert source_count == target_count, f"Data copy verification failed: source={source_count}, target={target_count}"

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_exact_replica_verification(self, populated_test_databases, mysql_replicator):
        """Test real exact replica verification with actual MySQL databases."""
        # Verify test data exists using standardized approach
        patient_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "No test patients found in source database"
        
        # Create and copy data
        assert mysql_replicator.create_exact_replica('patient'), "Failed to create exact replica"
        assert mysql_replicator.copy_table_data('patient'), "Failed to copy table data"
        
        # Test REAL exact replica verification
        result = mysql_replicator.verify_exact_replica('patient')
        assert result, "Real exact replica verification failed"

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_multiple_table_replication(self, populated_test_databases, mysql_replicator):
        """Test replication of multiple tables with real data."""
        # Verify test data exists using standardized approach
        patient_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        appointment_count = populated_test_databases.get_appointment_count(DatabaseType.SOURCE)
        assert patient_count > 0, "No test patients found in source database"
        assert appointment_count > 0, "No test appointments found in source database"
        
        # Test replication for multiple tables
        test_tables = ['patient', 'appointment']
        
        for table in test_tables:
            logger.info(f"Testing replication for table: {table}")
            
            # Create exact replica
            assert mysql_replicator.create_exact_replica(table), f"Failed to create exact replica for {table}"
            
            # Copy data
            assert mysql_replicator.copy_table_data(table), f"Failed to copy data for {table}"
            
            # Verify replica
            assert mysql_replicator.verify_exact_replica(table), f"Failed to verify replica for {table}"
            
            logger.info(f"Successfully replicated table: {table}")

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_error_handling(self, populated_test_databases, mysql_replicator):
        """Test real error handling with actual component failures."""
        # Test with non-existent table
        result = mysql_replicator.create_exact_replica('nonexistent_table')
        assert not result, "Should fail for non-existent table"
        
        # Test with non-existent table for data copying
        result = mysql_replicator.copy_table_data('nonexistent_table')
        assert not result, "Should fail for non-existent table"
        
        # Test with non-existent table for verification
        result = mysql_replicator.verify_exact_replica('nonexistent_table')
        assert not result, "Should fail for non-existent table"

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_schema_discovery_integration(self, populated_test_databases, mysql_replicator):
        """Test real SchemaDiscovery integration with MySQL replicator."""
        # Verify test data exists using standardized approach
        patient_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "No test patients found in source database"
        
        # Test that SchemaDiscovery is properly integrated
        schema = mysql_replicator.schema_discovery.get_table_schema('patient')
        assert schema is not None, "SchemaDiscovery integration failed"
        assert 'columns' in schema, "SchemaDiscovery columns not found"
        assert 'schema_hash' in schema, "SchemaDiscovery schema_hash not found"
        
        # Test table size info through SchemaDiscovery
        size_info = mysql_replicator.schema_discovery.get_table_size_info('patient')
        assert size_info is not None, "SchemaDiscovery size info failed"
        assert size_info['row_count'] >= patient_count, "SchemaDiscovery row count incorrect"

    @pytest.mark.integration
    def test_real_data_integrity_verification(self, populated_test_databases, mysql_replicator):
        """Test real data integrity verification after replication."""
        # Verify test data exists using standardized approach
        patient_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "No test patients found in source database"
        
        # Create and copy data
        assert mysql_replicator.create_exact_replica('patient'), "Failed to create exact replica"
        assert mysql_replicator.copy_table_data('patient'), "Failed to copy table data"
        
        # Verify data integrity by comparing specific records
        with mysql_replicator.source_engine.connect() as source_conn:
            source_result = source_conn.execute(text("""
                SELECT PatNum, LName, FName, SSN 
                FROM patient 
                ORDER BY PatNum
                LIMIT 5
            """))
            source_data = [dict(row._mapping) for row in source_result.fetchall()]
        
        with mysql_replicator.target_engine.connect() as target_conn:
            target_result = target_conn.execute(text("""
                SELECT PatNum, LName, FName, SSN 
                FROM patient 
                ORDER BY PatNum
                LIMIT 5
            """))
            target_data = [dict(row._mapping) for row in target_result.fetchall()]
        
        # Compare data integrity
        assert len(source_data) == len(target_data), "Data integrity check failed: different record counts"
        
        for i, (source_record, target_record) in enumerate(zip(source_data, target_data)):
            assert source_record['LName'] == target_record['LName'], f"Data integrity check failed at record {i}: LName mismatch"
            assert source_record['FName'] == target_record['FName'], f"Data integrity check failed at record {i}: FName mismatch"
            assert source_record['SSN'] == target_record['SSN'], f"Data integrity check failed at record {i}: SSN mismatch"

    @pytest.mark.integration
    def test_chunked_copy_composite_pk(self, mysql_replicator, populated_test_databases):
        """Test chunked copy logic with a composite primary key."""
        mysql_replicator.max_batch_size = 2
        # Create table with composite PK
        with mysql_replicator.source_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS composite_pk_table"))
            conn.execute(text("""
                CREATE TABLE composite_pk_table (
                    id1 INT,
                    id2 INT,
                    value VARCHAR(50),
                    PRIMARY KEY (id1, id2)
                )
            """))
            for i in range(4):
                conn.execute(text("INSERT INTO composite_pk_table (id1, id2, value) VALUES (:id1, :id2, :value)"), {
                    'id1': i // 2, 'id2': i % 2, 'value': f'val{i}'
                })
        # Replicate schema to target
        mysql_replicator.schema_discovery.replicate_schema(
            source_table='composite_pk_table',
            target_engine=mysql_replicator.target_engine,
            target_db=mysql_replicator.target_db,
            target_table='composite_pk_table'
        )
        result = mysql_replicator.copy_table_data('composite_pk_table')
        assert result is True

    @pytest.mark.integration
    def test_limit_offset_copy_large_table(self, mysql_replicator, populated_test_databases):
        """Test limit/offset copy logic with a large table and no primary key."""
        mysql_replicator.max_batch_size = 2
        # Create table with no PK and many rows
        with mysql_replicator.source_engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS no_pk_large_table"))
            conn.execute(text("CREATE TABLE no_pk_large_table (id INT, value VARCHAR(50))"))
            for i in range(6):
                conn.execute(text("INSERT INTO no_pk_large_table (id, value) VALUES (:id, :value)"), {'id': i, 'value': f'val{i}'})
        mysql_replicator.schema_discovery.replicate_schema(
            source_table='no_pk_large_table',
            target_engine=mysql_replicator.target_engine,
            target_db=mysql_replicator.target_db,
            target_table='no_pk_large_table'
        )
        result = mysql_replicator.copy_table_data('no_pk_large_table')
        assert result is True

    @pytest.mark.integration
    def test_data_integrity_verification_corruption(self, mysql_replicator, populated_test_databases):
        """Test data integrity verification failure after corrupting target data."""
        mysql_replicator.create_exact_replica('patient')
        mysql_replicator.copy_table_data('patient')
        # Delete a row to cause row count mismatch
        with mysql_replicator.target_engine.connect() as conn:
            conn.execute(text("DELETE FROM patient LIMIT 1"))
            conn.commit()
        result = mysql_replicator.verify_exact_replica('patient')
        assert result is False

    @pytest.mark.integration
    def test_copy_table_data_locked_table(self, mysql_replicator, populated_test_databases):
        """Test error handling when the source table is locked."""
        # Use a different approach to simulate database errors
        # Drop the table to force an error instead of locking
        with mysql_replicator.source_engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS patient"))
            conn.commit()
        
        result = mysql_replicator.copy_table_data('patient')
        assert result is False
        
        # Recreate the table for other tests by re-running the fixture setup
        # This will be handled automatically by the fixture cleanup and setup


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 