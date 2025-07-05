"""
Schema Discovery fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Schema discovery testing
- Test data management for schema discovery
- Schema discovery configuration
- Real database schema discovery testing
"""

import pytest
import logging
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List, Dict, Any

from etl_pipeline.core.schema_discovery import SchemaDiscovery, SchemaNotFoundError
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)

logger = logging.getLogger(__name__)


@pytest.fixture
def schema_discovery_test_settings():
    """Create test settings specifically for schema discovery testing.
    
    This fixture ensures we're using the test environment by:
    1. Setting ETL_ENVIRONMENT=test to ensure test environment detection
    2. Letting the configuration system load test variables from .env file
    3. Using the proper test connection methods
    """
    # Set test environment - this ensures the configuration system uses TEST_* variables
    # The actual test database connection details should come from .env file
    test_env_vars = {
        'ETL_ENVIRONMENT': 'test'  # This triggers test environment mode
    }
    
    # Create test settings - the configuration system will automatically:
    # 1. Detect test environment from ETL_ENVIRONMENT=test
    # 2. Load TEST_* variables from .env file
    # 3. Use test database connections
    settings = create_test_settings(env_vars=test_env_vars)
    
    yield settings
    
    # Cleanup: reset global settings after test
    reset_settings()


@pytest.fixture
def schema_discovery_test_data_manager(test_env_vars):
    """Manage test data in the real OpenDental database for schema discovery testing."""
    class TestDataManager:
        def __init__(self, env_vars):
            # Use the test MySQL replication connection for integration tests
            # Create test settings with proper environment and connection parameters
            # Ensure we're using test environment variables
            test_env_vars_with_environment = env_vars.copy()
            test_env_vars_with_environment['ETL_ENVIRONMENT'] = 'test'
            
            self.settings = create_test_settings(env_vars=test_env_vars_with_environment)
            # Use the explicit test connection method as specified in connection_environment_separation.md
            self.source_engine = ConnectionFactory.get_mysql_replication_test_connection()
            self.test_patients = []
            self.test_appointments = []
            self.test_procedures = []
        
        def create_test_data(self):
            """Create clearly identifiable test data in the real database."""
            logger.info("Creating test data in real OpenDental database...")
            
            # Create test patients with clearly identifiable names
            test_patients_data = [
                {
                    'PatNum': None,  # Will be auto-generated
                    'LName': 'TEST_PATIENT_001',
                    'FName': 'John',
                    'MiddleI': 'M',
                    'Preferred': 'Johnny',
                    'PatStatus': 0,
                    'Gender': 0,
                    'Position': 0,
                    'Birthdate': '1980-01-01',
                    'SSN': '123-45-6789'
                },
                {
                    'PatNum': None,
                    'LName': 'TEST_PATIENT_002', 
                    'FName': 'Jane',
                    'MiddleI': 'A',
                    'Preferred': 'Janey',
                    'PatStatus': 0,
                    'Gender': 1,
                    'Position': 0,
                    'Birthdate': '1985-05-15',
                    'SSN': '234-56-7890'
                },
                {
                    'PatNum': None,
                    'LName': 'TEST_PATIENT_003',
                    'FName': 'Bob',
                    'MiddleI': 'R',
                    'Preferred': 'Bobby',
                    'PatStatus': 0,
                    'Gender': 0,
                    'Position': 0,
                    'Birthdate': '1975-12-10',
                    'SSN': '345-67-8901'
                }
            ]
            
            # Insert test patients
            with self.source_engine.begin() as conn:
                for patient_data in test_patients_data:
                    params = {
                        'lname': patient_data['LName'],
                        'fname': patient_data['FName'],
                        'middlei': patient_data['MiddleI'],
                        'preferred': patient_data['Preferred'],
                        'patstatus': patient_data['PatStatus'],
                        'gender': patient_data['Gender'],
                        'position': patient_data['Position'],
                        'birthdate': patient_data['Birthdate'],
                        'ssn': patient_data['SSN']
                    }
                    result = conn.execute(text("""
                        INSERT INTO patient (LName, FName, MiddleI, Preferred, PatStatus, Gender, Position, Birthdate, SSN)
                        VALUES (:lname, :fname, :middlei, :preferred, :patstatus, :gender, :position, :birthdate, :ssn)
                    """), params)
                    
                    # Get the auto-generated PatNum
                    patient_data['PatNum'] = result.lastrowid
                    self.test_patients.append(patient_data)
            
            # Create test appointments
            test_appointments_data = [
                {
                    'AptNum': None,
                    'PatNum': self.test_patients[0]['PatNum'],
                    'AptDateTime': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
                    'AptStatus': 1,  # Scheduled
                    'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Notes': 'TEST_APPOINTMENT_001 - Regular checkup'
                },
                {
                    'AptNum': None,
                    'PatNum': self.test_patients[1]['PatNum'],
                    'AptDateTime': (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S'),
                    'AptStatus': 1,
                    'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Notes': 'TEST_APPOINTMENT_002 - Cleaning'
                }
            ]
            
            with self.source_engine.begin() as conn:
                for appointment_data in test_appointments_data:
                    params = {
                        'patnum': appointment_data['PatNum'],
                        'aptdatetime': appointment_data['AptDateTime'],
                        'aptstatus': appointment_data['AptStatus'],
                        'datestamp': appointment_data['DateTStamp'],
                        'notes': appointment_data['Notes']
                    }
                    result = conn.execute(text("""
                        INSERT INTO appointment (PatNum, AptDateTime, AptStatus, DateTStamp, Notes)
                        VALUES (:patnum, :aptdatetime, :aptstatus, :datestamp, :notes)
                    """), params)
                    
                    appointment_data['AptNum'] = result.lastrowid
                    self.test_appointments.append(appointment_data)
            
            # Create test procedures
            test_procedures_data = [
                {
                    'ProcNum': None,
                    'PatNum': self.test_patients[0]['PatNum'],
                    'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                    'ProcCode': 'TEST_PROC_001',
                    'ProcFee': 150.00,
                    'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                {
                    'ProcNum': None,
                    'PatNum': self.test_patients[1]['PatNum'],
                    'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                    'ProcCode': 'TEST_PROC_002',
                    'ProcFee': 200.00,
                    'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            ]
            
            with self.source_engine.begin() as conn:
                for procedure_data in test_procedures_data:
                    params = {
                        'patnum': procedure_data['PatNum'],
                        'procdate': procedure_data['ProcDate'],
                        'proccode': procedure_data['ProcCode'],
                        'procfee': procedure_data['ProcFee'],
                        'datestamp': procedure_data['DateTStamp']
                    }
                    result = conn.execute(text("""
                        INSERT INTO `procedure` (PatNum, ProcDate, ProcCode, ProcFee, DateTStamp)
                        VALUES (:patnum, :procdate, :proccode, :procfee, :datestamp)
                    """), params)
                    
                    procedure_data['ProcNum'] = result.lastrowid
                    self.test_procedures.append(procedure_data)
            
            logger.info(f"Created {len(self.test_patients)} test patients, {len(self.test_appointments)} appointments, {len(self.test_procedures)} procedures")
        
        def cleanup_test_data(self):
            """Remove all test data from the database."""
            logger.info("Cleaning up test data from real OpenDental database...")
            
            # Clean up test data
            try:
                with self.source_engine.connect() as conn:
                    # Delete test procedures first (due to foreign key constraints)
                    conn.execute(text("DELETE FROM `procedure` WHERE ProcNum IN (1, 2)"))
                    conn.execute(text("DELETE FROM appointment WHERE AptNum IN (1, 2)"))
                    conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                    conn.commit()
                    logger.info("Cleaned up test data from MySQL test database")
            except Exception as e:
                logger.warning(f"Failed to clean up MySQL test data: {e}")
        
        def verify_test_data_exists(self):
            """Verify that test data exists in the database."""
            with self.source_engine.connect() as conn:
                # Check test patients
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM patient 
                    WHERE LName LIKE 'TEST_PATIENT_%'
                """))
                patient_count = result.scalar()
                
                # Check test appointments
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM appointment 
                    WHERE Notes LIKE 'TEST_APPOINTMENT_%'
                """))
                appointment_count = result.scalar()
                
                # Check test procedures
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM `procedure` 
                    WHERE ProcCode LIKE 'TEST_PROC_%'
                """))
                procedure_count = result.scalar()
                
                logger.info(f"Found {patient_count} test patients, {appointment_count} test appointments, {procedure_count} test procedures")
                
                return patient_count > 0 and appointment_count > 0 and procedure_count > 0
    
    manager = TestDataManager(test_env_vars)
    manager.create_test_data()
    
    yield manager
    
    # Cleanup after tests
    manager.cleanup_test_data()


@pytest.fixture
def schema_discovery_instance(test_env_vars):
    """Create real SchemaDiscovery instance with real MySQL connection using new configuration system."""
    # Create test settings with proper environment and connection parameters
    # Ensure we're using test environment variables
    test_env_vars_with_environment = test_env_vars.copy()
    test_env_vars_with_environment['ETL_ENVIRONMENT'] = 'test'
    
    settings = create_test_settings(env_vars=test_env_vars_with_environment)
    
    # Use the explicit test connection method as specified in connection_environment_separation.md
    # For integration tests, we use the replication database (test_opendental_replication)
    source_engine = ConnectionFactory.get_mysql_replication_test_connection()
    
    # Extract database name from engine URL
    source_db = source_engine.url.database
    return SchemaDiscovery(source_engine, source_db)


@pytest.fixture
def mock_schema_discovery():
    """Mock SchemaDiscovery instance for unit testing."""
    from unittest.mock import MagicMock
    
    mock_discovery = MagicMock(spec=SchemaDiscovery)
    
    # Mock table schema
    mock_table_schema = {
        'table_name': 'test_table',
        'columns': [
            {'name': 'id', 'type': 'INT', 'is_nullable': False},
            {'name': 'name', 'type': 'VARCHAR(255)', 'is_nullable': True},
            {'name': 'created_at', 'type': 'DATETIME', 'is_nullable': False}
        ],
        'schema_hash': 'abc123'
    }
    
    mock_discovery.get_table_schema.return_value = mock_table_schema
    
    # Mock table size info
    mock_size_info = {
        'row_count': 100,
        'table_size_mb': 1.5,
        'index_size_mb': 0.5
    }
    
    mock_discovery.get_table_size_info.return_value = mock_size_info
    
    # Mock discover all tables
    mock_discovery.discover_all_tables.return_value = ['patient', 'appointment', 'procedure']
    
    return mock_discovery


@pytest.fixture
def sample_table_schemas():
    """Sample table schemas for testing."""
    return {
        'patient': {
            'table_name': 'patient',
            'columns': [
                {'name': 'PatNum', 'type': 'INT', 'is_nullable': False},
                {'name': 'LName', 'type': 'VARCHAR(100)', 'is_nullable': True},
                {'name': 'FName', 'type': 'VARCHAR(100)', 'is_nullable': True},
                {'name': 'Birthdate', 'type': 'DATE', 'is_nullable': True},
                {'name': 'SSN', 'type': 'VARCHAR(20)', 'is_nullable': True}
            ],
            'schema_hash': 'patient_hash_123'
        },
        'appointment': {
            'table_name': 'appointment',
            'columns': [
                {'name': 'AptNum', 'type': 'INT', 'is_nullable': False},
                {'name': 'PatNum', 'type': 'INT', 'is_nullable': False},
                {'name': 'AptDateTime', 'type': 'DATETIME', 'is_nullable': True},
                {'name': 'AptStatus', 'type': 'INT', 'is_nullable': True},
                {'name': 'Notes', 'type': 'TEXT', 'is_nullable': True}
            ],
            'schema_hash': 'appointment_hash_456'
        },
        'procedure': {
            'table_name': 'procedure',
            'columns': [
                {'name': 'ProcNum', 'type': 'INT', 'is_nullable': False},
                {'name': 'PatNum', 'type': 'INT', 'is_nullable': False},
                {'name': 'ProcDate', 'type': 'DATE', 'is_nullable': True},
                {'name': 'ProcCode', 'type': 'VARCHAR(20)', 'is_nullable': True},
                {'name': 'ProcFee', 'type': 'DECIMAL(10,2)', 'is_nullable': True}
            ],
            'schema_hash': 'procedure_hash_789'
        }
    }


@pytest.fixture
def sample_table_size_info():
    """Sample table size information for testing."""
    return {
        'patient': {
            'row_count': 1500,
            'table_size_mb': 2.5,
            'index_size_mb': 1.0
        },
        'appointment': {
            'row_count': 3000,
            'table_size_mb': 4.0,
            'index_size_mb': 1.5
        },
        'procedure': {
            'row_count': 5000,
            'table_size_mb': 6.0,
            'index_size_mb': 2.0
        }
    } 