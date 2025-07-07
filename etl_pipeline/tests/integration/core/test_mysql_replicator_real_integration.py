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
        # Create simple test data directly in source database
        test_patients = [
            {'PatNum': 1001, 'LName': 'Test1', 'FName': 'John', 'PatStatus': 0, 'SSN': '111-11-1111'},
            {'PatNum': 1002, 'LName': 'Test2', 'FName': 'Jane', 'PatStatus': 0, 'SSN': '222-22-2222'},
            {'PatNum': 1003, 'LName': 'Test3', 'FName': 'Bob', 'PatStatus': 0, 'SSN': '333-33-3333'}
        ]
        
        # Insert test data into source database
        with mysql_replicator.source_engine.connect() as conn:
            # Clear any existing test data
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (1001, 1002, 1003)"))
            
            # Insert test data
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO patient (PatNum, LName, FName, PatStatus, SSN) 
                    VALUES (:PatNum, :LName, :FName, :PatStatus, :SSN)
                """), patient)
            conn.commit()
        
        # Verify test data was inserted
        with mysql_replicator.source_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1001, 1002, 1003)"))
            source_count = result.scalar()
            assert source_count == 3, f"Expected 3 test patients, found {source_count}"
        
        # First create the exact replica
        assert mysql_replicator.create_exact_replica('patient'), "Failed to create exact replica"
        
        # Test REAL table data copying
        result = mysql_replicator.copy_table_data('patient')
        assert result, "Real table data copying failed"
        
        # Verify data was copied correctly
        with mysql_replicator.target_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1001, 1002, 1003)"))
            target_count = result.scalar()
        
        assert source_count == target_count, f"Data copy verification failed: source={source_count}, target={target_count}"

    @pytest.mark.integration
    @pytest.mark.order(2)
    def test_real_exact_replica_verification(self, populated_test_databases, mysql_replicator):
        """Test real exact replica verification with actual MySQL databases."""
        # Create simple test data directly in source database
        test_patients = [
            {'PatNum': 2001, 'LName': 'Verify1', 'FName': 'John', 'PatStatus': 0, 'SSN': '444-44-4444'},
            {'PatNum': 2002, 'LName': 'Verify2', 'FName': 'Jane', 'PatStatus': 0, 'SSN': '555-55-5555'}
        ]
        
        # Insert test data into source database
        with mysql_replicator.source_engine.connect() as conn:
            # Clear any existing test data
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (2001, 2002)"))
            
            # Insert test data
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO patient (PatNum, LName, FName, PatStatus, SSN) 
                    VALUES (:PatNum, :LName, :FName, :PatStatus, :SSN)
                """), patient)
            conn.commit()
        
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
        # Create simple test data directly in source database
        test_patients = [
            {'PatNum': 3001, 'LName': 'Multi1', 'FName': 'John', 'PatStatus': 0, 'SSN': '666-66-6666'},
            {'PatNum': 3002, 'LName': 'Multi2', 'FName': 'Jane', 'PatStatus': 0, 'SSN': '777-77-7777'}
        ]
        
        test_appointments = [
            {'AptNum': 4001, 'PatNum': 3001, 'AptDateTime': '2023-01-01 10:00:00', 'AptStatus': 0, 'DateTStamp': '2023-01-01 10:00:00'},
            {'AptNum': 4002, 'PatNum': 3002, 'AptDateTime': '2023-01-02 11:00:00', 'AptStatus': 0, 'DateTStamp': '2023-01-02 11:00:00'}
        ]
        
        # Insert test data into source database
        with mysql_replicator.source_engine.connect() as conn:
            # Clear any existing test data
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (3001, 3002)"))
            conn.execute(text("DELETE FROM appointment WHERE AptNum IN (4001, 4002)"))
            
            # Insert patient data
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO patient (PatNum, LName, FName, PatStatus, SSN) 
                    VALUES (:PatNum, :LName, :FName, :PatStatus, :SSN)
                """), patient)
            
            # Insert appointment data
            for appointment in test_appointments:
                conn.execute(text("""
                    INSERT INTO appointment (AptNum, PatNum, AptDateTime, AptStatus, DateTStamp) 
                    VALUES (:AptNum, :PatNum, :AptDateTime, :AptStatus, :DateTStamp)
                """), appointment)
            conn.commit()
        
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
    def test_real_schema_discovery_integration(self, populated_test_databases, mysql_replicator):
        """Test that SchemaDiscovery is properly integrated with MySQL replicator."""
        # Test that SchemaDiscovery is properly integrated
        schema = mysql_replicator.schema_discovery.get_table_schema('patient')
        assert schema is not None, "SchemaDiscovery integration failed"
        assert 'columns' in schema, "SchemaDiscovery columns not found"
        assert 'schema_hash' in schema, "SchemaDiscovery schema_hash not found"
        
        # Test table size info through SchemaDiscovery (static config only)
        size_info = mysql_replicator.schema_discovery.get_table_size_info('patient')
        assert size_info is not None, "SchemaDiscovery size info failed"
        assert 'row_count' in size_info, "SchemaDiscovery row count missing"
        assert 'source' in size_info, "SchemaDiscovery source missing"
        
        # Verify that SchemaDiscovery returns static configuration data
        # (not dynamic test data - that's not its responsibility)
        assert size_info['source'] in ['static_configuration', 'error_fallback'], \
            f"SchemaDiscovery should return static config, got: {size_info['source']}"
        
        # Test that we can get copy strategy from static configuration
        copy_strategy = mysql_replicator.get_copy_strategy('patient')
        assert copy_strategy in ['single_transaction', 'chunked_transactions'], \
            f"Invalid copy strategy: {copy_strategy}"

    @pytest.mark.integration
    def test_real_data_integrity_verification(self, populated_test_databases, mysql_replicator):
        """Test real data integrity verification after replication."""
        # Create simple test data directly in source database
        test_patients = [
            {'PatNum': 5001, 'LName': 'Integrity1', 'FName': 'John', 'PatStatus': 0, 'SSN': '888-88-8888'},
            {'PatNum': 5002, 'LName': 'Integrity2', 'FName': 'Jane', 'PatStatus': 0, 'SSN': '999-99-9999'},
            {'PatNum': 5003, 'LName': 'Integrity3', 'FName': 'Bob', 'PatStatus': 0, 'SSN': '000-00-0000'}
        ]
        
        # Insert test data into source database
        with mysql_replicator.source_engine.connect() as conn:
            # Clear any existing test data
            conn.execute(text("DELETE FROM patient WHERE PatNum IN (5001, 5002, 5003)"))
            
            # Insert test data
            for patient in test_patients:
                conn.execute(text("""
                    INSERT INTO patient (PatNum, LName, FName, PatStatus, SSN) 
                    VALUES (:PatNum, :LName, :FName, :PatStatus, :SSN)
                """), patient)
            conn.commit()
        
        # Create and copy data
        assert mysql_replicator.create_exact_replica('patient'), "Failed to create exact replica"
        assert mysql_replicator.copy_table_data('patient'), "Failed to copy table data"
        
        # Verify data integrity by comparing specific records
        with mysql_replicator.source_engine.connect() as source_conn:
            source_result = source_conn.execute(text("""
                SELECT PatNum, LName, FName, SSN 
                FROM patient 
                WHERE PatNum IN (5001, 5002, 5003)
                ORDER BY PatNum
            """))
            source_data = [dict(row._mapping) for row in source_result.fetchall()]
        
        with mysql_replicator.target_engine.connect() as target_conn:
            target_result = target_conn.execute(text("""
                SELECT PatNum, LName, FName, SSN 
                FROM patient 
                WHERE PatNum IN (5001, 5002, 5003)
                ORDER BY PatNum
            """))
            target_data = [dict(row._mapping) for row in target_result.fetchall()]
        
        # Compare data integrity
        assert len(source_data) == len(target_data), "Data integrity check failed: different record counts"
        
        for i, (source_record, target_record) in enumerate(zip(source_data, target_data)):
            assert source_record['LName'] == target_record['LName'], f"Data integrity check failed at record {i}: LName mismatch"
            assert source_record['FName'] == target_record['FName'], f"Data integrity check failed at record {i}: FName mismatch"
            assert source_record['SSN'] == target_record['SSN'], f"Data integrity check failed at record {i}: SSN mismatch"

    @pytest.mark.integration
    def test_real_data_copy_verification(self, populated_test_databases, mysql_replicator):
        """Test data copying verification by querying database directly (not via SchemaDiscovery)."""
        # Get actual row counts from database before copy
        with mysql_replicator.source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
        
        # Perform the copy operation
        assert mysql_replicator.create_exact_replica('patient'), "Failed to create exact replica"
        assert mysql_replicator.copy_table_data('patient'), "Failed to copy table data"
        
        # Verify row counts by querying database directly
        with mysql_replicator.target_engine.connect() as conn:
            target_count = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
        
        # Assert that the copy worked correctly
        assert target_count == source_count, f"Row count mismatch: source={source_count}, target={target_count}"
        assert target_count > 0, "No data was copied"
        
        # Verify specific test data was copied correctly
        with mysql_replicator.source_engine.connect() as source_conn:
            source_data = source_conn.execute(text("""
                SELECT PatNum, LName, FName 
                FROM patient 
                ORDER BY PatNum 
                LIMIT 3
            """)).fetchall()
        
        with mysql_replicator.target_engine.connect() as target_conn:
            target_data = target_conn.execute(text("""
                SELECT PatNum, LName, FName 
                FROM patient 
                ORDER BY PatNum 
                LIMIT 3
            """)).fetchall()
        
        # Compare the actual data
        assert len(source_data) == len(target_data), "Data length mismatch"
        for i, (source_row, target_row) in enumerate(zip(source_data, target_data)):
            assert source_row.PatNum == target_row.PatNum, f"PatNum mismatch at row {i}"
            assert source_row.LName == target_row.LName, f"LName mismatch at row {i}"
            assert source_row.FName == target_row.FName, f"FName mismatch at row {i}"

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
        
        # Recreate the patient table for other tests
        # This is necessary because we dropped the table and fixtures only clean data, not recreate tables
        with mysql_replicator.source_engine.connect() as conn:
            # Recreate the patient table with the same schema as setup script
            create_patient_sql = """
                CREATE TABLE patient (
                    `PatNum` bigint(20) NOT NULL AUTO_INCREMENT,
                    `LName` varchar(100) DEFAULT '',
                    `FName` varchar(100) DEFAULT '',
                    `MiddleI` varchar(100) DEFAULT '',
                    `Preferred` varchar(100) DEFAULT '',
                    `PatStatus` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Gender` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Position` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Birthdate` date NOT NULL DEFAULT '0001-01-01',
                    `SSN` varchar(100) DEFAULT '',
                    `Address` varchar(100) DEFAULT '',
                    `Address2` varchar(100) DEFAULT '',
                    `City` varchar(100) DEFAULT '',
                    `State` varchar(100) DEFAULT '',
                    `Zip` varchar(100) DEFAULT '',
                    `HmPhone` varchar(30) DEFAULT '',
                    `WkPhone` varchar(30) DEFAULT '',
                    `WirelessPhone` varchar(30) DEFAULT '',
                    `Guarantor` bigint(20) NOT NULL DEFAULT 0,
                    `CreditType` char(1) DEFAULT '',
                    `Email` varchar(100) DEFAULT '',
                    `Salutation` varchar(100) DEFAULT '',
                    `EstBalance` double NOT NULL DEFAULT 0,
                    `PriProv` bigint(20) NOT NULL DEFAULT 0,
                    `SecProv` bigint(20) NOT NULL DEFAULT 0,
                    `FeeSched` bigint(20) NOT NULL DEFAULT 0,
                    `BillingType` bigint(20) NOT NULL DEFAULT 0,
                    `ImageFolder` varchar(100) DEFAULT '',
                    `AddrNote` text DEFAULT NULL,
                    `FamFinUrgNote` text DEFAULT NULL,
                    `MedUrgNote` varchar(255) DEFAULT '',
                    `ApptModNote` varchar(255) DEFAULT '',
                    `StudentStatus` char(1) DEFAULT '',
                    `SchoolName` varchar(255) NOT NULL DEFAULT '',
                    `ChartNumber` varchar(100) NOT NULL DEFAULT '',
                    `MedicaidID` varchar(20) DEFAULT '',
                    `Bal_0_30` double NOT NULL DEFAULT 0,
                    `Bal_31_60` double NOT NULL DEFAULT 0,
                    `Bal_61_90` double NOT NULL DEFAULT 0,
                    `BalOver90` double NOT NULL DEFAULT 0,
                    `InsEst` double NOT NULL DEFAULT 0,
                    `BalTotal` double NOT NULL DEFAULT 0,
                    `EmployerNum` bigint(20) NOT NULL DEFAULT 0,
                    `EmploymentNote` varchar(255) DEFAULT '',
                    `County` varchar(255) DEFAULT '',
                    `GradeLevel` tinyint(4) NOT NULL DEFAULT 0,
                    `Urgency` tinyint(4) NOT NULL DEFAULT 0,
                    `DateFirstVisit` date NOT NULL DEFAULT '0001-01-01',
                    `ClinicNum` bigint(20) NOT NULL DEFAULT 0,
                    `HasIns` varchar(255) DEFAULT '',
                    `TrophyFolder` varchar(255) DEFAULT '',
                    `PlannedIsDone` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Premed` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Ward` varchar(255) DEFAULT '',
                    `PreferConfirmMethod` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `PreferContactMethod` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `PreferRecallMethod` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `SchedBeforeTime` time DEFAULT NULL,
                    `SchedAfterTime` time DEFAULT NULL,
                    `SchedDayOfWeek` tinyint(3) unsigned NOT NULL DEFAULT 0,
                    `Language` varchar(100) DEFAULT '',
                    `AdmitDate` date NOT NULL DEFAULT '0001-01-01',
                    `Title` varchar(15) DEFAULT NULL,
                    `PayPlanDue` double NOT NULL DEFAULT 0,
                    `SiteNum` bigint(20) NOT NULL DEFAULT 0,
                    `DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
                    `ResponsParty` bigint(20) NOT NULL DEFAULT 0,
                    `CanadianEligibilityCode` tinyint(4) NOT NULL DEFAULT 0,
                    `AskToArriveEarly` int(11) NOT NULL DEFAULT 0,
                    `PreferContactConfidential` tinyint(4) NOT NULL DEFAULT 0,
                    `SuperFamily` bigint(20) NOT NULL DEFAULT 0,
                    `TxtMsgOk` tinyint(4) NOT NULL DEFAULT 0,
                    `SmokingSnoMed` varchar(32) NOT NULL DEFAULT '',
                    `Country` varchar(255) NOT NULL DEFAULT '',
                    `DateTimeDeceased` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
                    `BillingCycleDay` int(11) NOT NULL DEFAULT 1,
                    `SecUserNumEntry` bigint(20) NOT NULL DEFAULT 0,
                    `SecDateEntry` date NOT NULL DEFAULT '0001-01-01',
                    `HasSuperBilling` tinyint(4) NOT NULL DEFAULT 0,
                    `PatNumCloneFrom` bigint(20) NOT NULL DEFAULT 0,
                    `DiscountPlanNum` bigint(20) NOT NULL DEFAULT 0,
                    `HasSignedTil` tinyint(4) NOT NULL DEFAULT 0,
                    `ShortCodeOptIn` tinyint(4) NOT NULL DEFAULT 0,
                    `SecurityHash` varchar(255) NOT NULL DEFAULT '',
                    PRIMARY KEY (`PatNum`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            conn.execute(text(create_patient_sql))
            conn.commit()
            
        # Re-populate the table with test data using the fixture
        populated_test_databases.setup_patient_data(database_types=[DatabaseType.SOURCE])


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 