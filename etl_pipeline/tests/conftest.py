"""
Shared test fixtures for ETL pipeline tests.

This module provides common fixtures used across all test modules,
including database mocks, test data generators, and configuration fixtures.
"""

import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, timedelta
from typing import Dict, List, Any
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text

# Import ETL pipeline components for testing
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.transformers.raw_to_public import RawToPublicTransformer
from etl_pipeline.loaders.postgres_loader import PostgresLoader


# =============================================================================
# DATABASE MOCKS AND CONNECTIONS
# =============================================================================

@pytest.fixture(scope="session")
def mock_database_engines():
    """Mock database engines for unit tests."""
    return {
        'source': MagicMock(spec=Engine),
        'replication': MagicMock(spec=Engine),
        'analytics': MagicMock(spec=Engine)
    }


@pytest.fixture
def mock_source_engine():
    """Mock source database engine."""
    engine = MagicMock(spec=Engine)
    engine.name = 'mysql'
    return engine


@pytest.fixture
def mock_replication_engine():
    """Mock MySQL replication database engine."""
    engine = MagicMock(spec=Engine)
    engine.name = 'mysql'
    # Mock URL attribute for SQLAlchemy compatibility (Lesson 4.1)
    engine.url = MagicMock()
    engine.url.database = 'opendental_replication'
    return engine


@pytest.fixture
def mock_analytics_engine():
    """Mock PostgreSQL analytics database engine."""
    engine = MagicMock(spec=Engine)
    engine.name = 'postgresql'
    # Mock URL attribute for SQLAlchemy compatibility (Lesson 4.1)
    engine.url = MagicMock()
    engine.url.database = 'opendental_analytics'
    return engine


@pytest.fixture
def mock_connection_factory():
    """Mock connection factory for testing."""
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock:
        mock.get_opendental_source_connection.return_value = MagicMock(spec=Engine)
        mock.get_mysql_replication_connection.return_value = MagicMock(spec=Engine)
        mock.get_postgres_analytics_connection.return_value = MagicMock(spec=Engine)
        yield mock


# =============================================================================
# TEST ENVIRONMENT CONFIGURATION
# =============================================================================

@pytest.fixture(scope="session")
def test_environment_config():
    """
    Test environment configuration that mirrors production structure.
    
    This fixture sets up the test environment to be as close to production
    as possible while maintaining safety and isolation.
    
    Key Principles:
    1. Use same users as production for realistic permission testing
    2. Use separate test databases to avoid production data contamination
    3. Use same ports and connection parameters as production
    4. Use different passwords for test environment security
    """
    return {
        'environment': 'test',
        'log_level': 'DEBUG',
        'batch_size': 100,  # Smaller for faster tests
        'max_retries': 2,   # Fewer retries for faster tests
        'retry_delay': 1,   # Shorter delays for faster tests
        
        # Database users match .env.template for realistic testing
        'users': {
            'readonly_user': {
                'purpose': 'Read-only access to production OpenDental',
                'permissions': ['SELECT'],
                'testing_strategy': 'MOCK_ONLY'
            },
            'replication_user': {
                'purpose': 'Full access to MySQL replication database',
                'permissions': ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP'],
                'testing_strategy': 'REAL_TEST_DATA'
            },
            'analytics_user': {
                'purpose': 'Full access to PostgreSQL analytics database',
                'permissions': ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP'],
                'testing_strategy': 'REAL_TEST_DATA'
            }
        },
        
        # Test database naming convention
        'database_naming': {
            'source_test': 'opendental_test',           # Mock only
            'replication_test': 'opendental_replication_test',  # Real test data
            'analytics_test': 'opendental_analytics_test'       # Real test data
        }
    }


# =============================================================================
# TEST DATABASE CONFIGURATION
# =============================================================================

@pytest.fixture(scope="session")
def test_database_config():
    """
    Test database configuration.
    
    Testing Strategy:
    - Source Database: MOCK ONLY (production, read-only)
    - Replication Database: REAL TEST DATA (local copy, full access)
    - Analytics Database: REAL TEST DATA (analytics warehouse, full access)
    
    Users match .env.template for realistic testing:
    - readonly_user: Read-only access to production (mocked)
    - replication_user: Full access to replication database
    - analytics_user: Full access to analytics database
    """
    return {
        'source': {
            'host': 'localhost',
            'port': 3306,
            'database': 'opendental_test',
            'user': 'readonly_user',  # Same as production for realistic testing
            'password': 'test_password',  # Different password for test environment
            'testing_strategy': 'MOCK_ONLY'  # Never write to production
        },
        'replication': {
            'host': 'localhost',
            'port': 3305,  # Same port as production replication
            'database': 'opendental_replication_test',  # Separate test database
            'user': 'replication_user',  # Same as production for realistic testing
            'password': 'test_password',  # Different password for test environment
            'testing_strategy': 'REAL_TEST_DATA'  # Full access for testing
        },
        'analytics': {
            'host': 'localhost',
            'port': 5432,  # Same port as production analytics
            'database': 'opendental_analytics_test',  # Separate test database
            'user': 'analytics_user',  # Same as production for realistic testing
            'password': 'test_password',  # Different password for test environment
            'schema': 'raw',
            'testing_strategy': 'REAL_TEST_DATA'  # Full access for testing
        }
    }


@pytest.fixture(scope="session")
def test_databases(test_database_config):
    """
    Set up test databases for the test session.
    
    This fixture creates test databases and loads sample data.
    It's scoped to session to avoid recreating databases for each test.
    
    Testing Strategy:
    - Source: Mock only (production safety)
    - Replication: Real test database with sample data
    - Analytics: Real test database with sample data
    """
    # TODO: Implement actual database setup for integration tests
    # For now, return the config
    yield test_database_config
    
    # Cleanup would go here
    # TODO: Drop test databases after session


@pytest.fixture(scope="session")
def test_replication_database(test_database_config):
    """
    Create test replication database with real test data.
    
    This database is used for testing the MySQL replicator functionality,
    schema copying, and data integrity verification.
    
    Uses replication_user (same as production) for realistic permission testing.
    """
    config = test_database_config['replication']
    
    # TODO: Implement actual database creation
    # 1. Create test database: opendental_replication_test
    # 2. Grant permissions to replication_user
    # 3. Load sample data for testing
    # 4. Verify user permissions match production
    
    yield config
    
    # TODO: Cleanup - drop test database
    # Ensure cleanup runs even if tests fail


@pytest.fixture(scope="session")
def test_analytics_database(test_database_config):
    """
    Create test analytics database with real test data.
    
    This database is used for testing the PostgreSQL loader functionality,
    schema transformations, and data type conversions.
    
    Uses analytics_user (same as production) for realistic permission testing.
    """
    config = test_database_config['analytics']
    
    # TODO: Implement actual database creation
    # 1. Create test database: opendental_analytics_test
    # 2. Create schemas: raw, public, public_staging, etc.
    # 3. Grant permissions to analytics_user
    # 4. Load sample data for testing
    # 5. Verify user permissions match production
    
    yield config
    
    # TODO: Cleanup - drop test database
    # Ensure cleanup runs even if tests fail


# =============================================================================
# TEST DATA GENERATORS
# =============================================================================

@pytest.fixture
def sample_patient_data():
    """Sample patient data for testing with PostgreSQL raw schema field names (quoted)."""
    return pd.DataFrame([
        {
            '"PatNum"': 1,
            '"LName"': 'Doe',
            '"FName"': 'John',
            '"Birthdate"': '1980-01-01',
            '"Email"': 'john.doe@example.com',
            '"HmPhone"': '555-0101',
            '"DateFirstVisit"': '2020-01-15',
            '"PatStatus"': 0,  # Active patient
            '"Gender"': 0,  # Male
            '"Premed"': 0,  # No premedication required
            '"WirelessPhone"': '555-0101',
            '"WkPhone"': '555-0102',
            '"Address"': '123 Main St',
            '"City"': 'Anytown',
            '"State"': 'CA',
            '"Zip"': '12345',
            '"DateTStamp"': '2023-12-01 10:00:00',
            '"SecDateEntry"': '2020-01-15 09:00:00'
        },
        {
            '"PatNum"': 2,
            '"LName"': 'Smith',
            '"FName"': 'Jane',
            '"Birthdate"': '1985-05-15',
            '"Email"': 'jane.smith@example.com',
            '"HmPhone"': '555-0102',
            '"DateFirstVisit"': '2020-02-20',
            '"PatStatus"': 0,  # Active patient
            '"Gender"': 1,  # Female
            '"Premed"': 0,  # No premedication required
            '"WirelessPhone"': '555-0102',
            '"WkPhone"': '555-0103',
            '"Address"': '456 Oak Ave',
            '"City"': 'Somewhere',
            '"State"': 'CA',
            '"Zip"': '12346',
            '"DateTStamp"': '2023-12-01 11:00:00',
            '"SecDateEntry"': '2020-02-20 10:00:00'
        },
        {
            '"PatNum"': 3,
            '"LName"': 'Johnson',
            '"FName"': 'Bob',
            '"Birthdate"': '1975-12-10',
            '"Email"': 'bob.johnson@example.com',
            '"HmPhone"': '555-0103',
            '"DateFirstVisit"': '2020-03-10',
            '"PatStatus"': 0,  # Active patient
            '"Gender"': 0,  # Male
            '"Premed"': 1,  # Premedication required
            '"WirelessPhone"': '555-0103',
            '"WkPhone"': '555-0104',
            '"Address"': '789 Pine St',
            '"City"': 'Elsewhere',
            '"State"': 'CA',
            '"Zip"': '12347',
            '"DateTStamp"': '2023-12-01 12:00:00',
            '"SecDateEntry"': '2020-03-10 11:00:00'
        },
        {
            '"PatNum"': 4,
            '"LName"': 'Williams',
            '"FName"': 'Alice',
            '"Birthdate"': '1990-08-22',
            '"Email"': 'alice.williams@example.com',
            '"HmPhone"': '555-0104',
            '"DateFirstVisit"': '2020-04-05',
            '"PatStatus"': 2,  # Inactive patient
            '"Gender"': 1,  # Female
            '"Premed"': 0,  # No premedication required
            '"WirelessPhone"': '555-0104',
            '"WkPhone"': '555-0105',
            '"Address"': '321 Elm St',
            '"City"': 'Nowhere',
            '"State"': 'CA',
            '"Zip"': '12348',
            '"DateTStamp"': '2023-12-01 13:00:00',
            '"SecDateEntry"': '2020-04-05 14:00:00'
        },
        {
            '"PatNum"': 5,
            '"LName"': 'Brown',
            '"FName"': 'Charlie',
            '"Birthdate"': '1982-03-18',
            '"Email"': 'charlie.brown@example.com',
            '"HmPhone"': '555-0105',
            '"DateFirstVisit"': '2020-05-12',
            '"PatStatus"': 0,  # Active patient
            '"Gender"': 0,  # Male
            '"Premed"': 0,  # No premedication required
            '"WirelessPhone"': '555-0105',
            '"WkPhone"': '555-0106',
            '"Address"': '654 Maple Dr',
            '"City"': 'Anywhere',
            '"State"': 'CA',
            '"Zip"': '12349',
            '"DateTStamp"': '2023-12-01 14:00:00',
            '"SecDateEntry"': '2020-05-12 15:00:00'
        }
    ])


@pytest.fixture
def sample_appointment_data():
    """Sample appointment data for testing with OpenDental source field names."""
    return pd.DataFrame([
        {
            'AptNum': 1,
            'PatNum': 1,
            'ProvNum': 1,
            'AptDateTime': '2023-01-15 09:00:00',
            'AptStatus': 2,  # Completed
            'IsNewPatient': 0,  # Existing patient
            'IsHygiene': 0,  # General appointment
            'DateTimeArrived': '2023-01-15 08:45:00',
            'DateTimeSeated': '2023-01-15 09:00:00',
            'DateTimeDismissed': '2023-01-15 10:30:00',
            'ClinicNum': 1,
            'Op': 1,  # Operatory
            'AptType': 1,  # Appointment type
            'SecDateTEntry': '2023-01-15 08:00:00',
            'DateTStamp': '2023-01-15 10:30:00'
        },
        {
            'AptNum': 2,
            'PatNum': 1,
            'ProvNum': 1,
            'AptDateTime': '2023-02-20 14:00:00',
            'AptStatus': 2,  # Completed
            'IsNewPatient': 0,  # Existing patient
            'IsHygiene': 1,  # Hygiene appointment
            'DateTimeArrived': '2023-02-20 13:45:00',
            'DateTimeSeated': '2023-02-20 14:00:00',
            'DateTimeDismissed': '2023-02-20 15:00:00',
            'ClinicNum': 1,
            'Op': 2,  # Operatory
            'AptType': 2,  # Appointment type
            'SecDateTEntry': '2023-02-20 13:00:00',
            'DateTStamp': '2023-02-20 15:00:00'
        },
        {
            'AptNum': 3,
            'PatNum': 2,
            'ProvNum': 2,
            'AptDateTime': '2023-03-10 10:00:00',
            'AptStatus': 1,  # Scheduled
            'IsNewPatient': 1,  # New patient
            'IsHygiene': 0,  # General appointment
            'DateTimeArrived': None,  # Future appointment
            'DateTimeSeated': None,   # Future appointment
            'DateTimeDismissed': None, # Future appointment
            'ClinicNum': 1,
            'Op': 1,  # Operatory
            'AptType': 3,  # Appointment type
            'SecDateTEntry': '2023-03-10 09:00:00',
            'DateTStamp': '2023-03-10 09:00:00'
        },
        {
            'AptNum': 4,
            'PatNum': 2,
            'ProvNum': 2,
            'AptDateTime': '2023-04-05 16:00:00',
            'AptStatus': 5,  # Broken/Missed
            'IsNewPatient': 0,  # Existing patient
            'IsHygiene': 0,  # General appointment
            'DateTimeArrived': None,  # Patient didn't show
            'DateTimeSeated': None,   # Patient didn't show
            'DateTimeDismissed': None, # Patient didn't show
            'ClinicNum': 1,
            'Op': 1,  # Operatory
            'AptType': 1,  # Appointment type
            'SecDateTEntry': '2023-04-05 15:00:00',
            'DateTStamp': '2023-04-05 16:30:00'
        },
        {
            'AptNum': 5,
            'PatNum': 3,
            'ProvNum': 1,
            'AptDateTime': '2023-05-12 11:00:00',
            'AptStatus': 2,  # Completed
            'IsNewPatient': 0,  # Existing patient
            'IsHygiene': 0,  # General appointment
            'DateTimeArrived': '2023-05-12 10:45:00',
            'DateTimeSeated': '2023-05-12 11:00:00',
            'DateTimeDismissed': '2023-05-12 12:30:00',
            'ClinicNum': 1,
            'Op': 3,  # Operatory
            'AptType': 1,  # Appointment type
            'SecDateTEntry': '2023-05-12 10:00:00',
            'DateTStamp': '2023-05-12 12:30:00'
        }
    ])


@pytest.fixture
def sample_procedure_data():
    """Sample procedure data for testing with OpenDental source field names."""
    return pd.DataFrame([
        {
            'ProcNum': 1,
            'PatNum': 1,
            'AptNum': 1,
            'ProcDate': '2023-01-15',
            'ProcFee': 150.00,
            'ProcStatus': 2,  # Completed
            'CodeNum': 1,
            'ProvNum': 1,
            'ClinicNum': 1,
            'DateComplete': '2023-01-15',
            'DateEntryC': '2023-01-15 09:00:00',
            'DateTStamp': '2023-01-15 09:00:00'
        },
        {
            'ProcNum': 2,
            'PatNum': 1,
            'AptNum': 2,
            'ProcDate': '2023-02-20',
            'ProcFee': 200.00,
            'ProcStatus': 2,  # Completed
            'CodeNum': 2,
            'ProvNum': 1,
            'ClinicNum': 1,
            'DateComplete': '2023-02-20',
            'DateEntryC': '2023-02-20 10:00:00',
            'DateTStamp': '2023-02-20 10:00:00'
        },
        {
            'ProcNum': 3,
            'PatNum': 2,
            'AptNum': 3,
            'ProcDate': '2023-03-10',
            'ProcFee': 75.00,
            'ProcStatus': 1,  # Treatment Planned
            'CodeNum': 3,
            'ProvNum': 2,
            'ClinicNum': 1,
            'DateComplete': None,  # Not completed yet
            'DateEntryC': '2023-03-10 11:00:00',
            'DateTStamp': '2023-03-10 11:00:00'
        },
        {
            'ProcNum': 4,
            'PatNum': 2,
            'AptNum': 4,
            'ProcDate': '2023-04-05',
            'ProcFee': 300.00,
            'ProcStatus': 2,  # Completed
            'CodeNum': 4,
            'ProvNum': 2,
            'ClinicNum': 1,
            'DateComplete': '2023-04-05',
            'DateEntryC': '2023-04-05 14:00:00',
            'DateTStamp': '2023-04-05 14:00:00'
        },
        {
            'ProcNum': 5,
            'PatNum': 3,
            'AptNum': 5,
            'ProcDate': '2023-05-12',
            'ProcFee': 125.00,
            'ProcStatus': 6,  # Ordered/Planned
            'CodeNum': 5,
            'ProvNum': 1,
            'ClinicNum': 1,
            'DateComplete': None,  # Not completed yet
            'DateEntryC': '2023-05-12 15:00:00',
            'DateTStamp': '2023-05-12 15:00:00'
        }
    ])


@pytest.fixture
def large_test_dataset():
    """
    Generate large securitylog dataset for performance testing.
    
    Uses the securitylog table structure from opendental_analytics database
    as it's the largest table and perfect for testing large dataset scenarios.
    
    Table structure from securitylog_pg_ddl.sql:
    - SecurityLogNum (serial4, primary key)
    - PermType (int2)
    - UserNum (int8)
    - LogDateTime (timestamp)
    - LogText (text)
    - PatNum (int8)
    - CompName (varchar(255))
    - FKey (int8)
    - LogSource (int2)
    - DefNum (int8)
    - DefNumError (int8)
    - DateTPrevious (timestamp)
    """
    def _generate_large_securitylog(count: int = 10000):
        """Generate large securitylog dataset for performance testing."""
        data = []
        for i in range(count):
            # Mix of different log types and sources
            perm_type = (i % 10) + 1  # 1-10 different permission types
            log_source = (i % 5) + 1  # 1-5 different log sources
            
            # Generate realistic log text
            log_actions = [
                'User login successful',
                'Patient record accessed',
                'Appointment scheduled',
                'Procedure completed',
                'Payment processed',
                'Insurance claim submitted',
                'Medical history updated',
                'Prescription written',
                'Lab results viewed',
                'Treatment plan created'
            ]
            log_text = f"{log_actions[i % len(log_actions)]} - ID: {i}"
            
            # Generate timestamps across a year
            base_date = datetime(2023, 1, 1) + timedelta(days=i % 365, hours=i % 24, minutes=i % 60)
            
            data.append({
                '"SecurityLogNum"': i + 1,  # PostgreSQL quoted field names
                '"PermType"': perm_type,
                '"UserNum"': (i % 20) + 1,  # 20 different users
                '"LogDateTime"': base_date.strftime('%Y-%m-%d %H:%M:%S'),
                '"LogText"': log_text,
                '"PatNum"': (i % 1000) + 1 if i % 3 != 0 else None,  # Some logs don't have patient
                '"CompName"': f'COMPUTER-{(i % 10) + 1:02d}',
                '"FKey"': (i % 10000) + 1 if i % 2 == 0 else None,  # Some logs don't have foreign key
                '"LogSource"': log_source,
                '"DefNum"': (i % 100) + 1 if i % 4 != 0 else None,  # Some logs don't have definition
                '"DefNumError"': (i % 50) + 1 if i % 10 == 0 else None,  # 10% have errors
                '"DateTPrevious"': (base_date - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') if i % 5 != 0 else None
            })

        return pd.DataFrame(data)
    
    return _generate_large_securitylog


# =============================================================================
# CONFIGURATION FIXTURES
# =============================================================================

@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    with patch('etl_pipeline.config.settings.Settings') as mock:
        settings_instance = MagicMock()
        
        # Mock database configurations
        settings_instance.get_database_config.return_value = {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        # Mock table configurations
        settings_instance.get_table_config.return_value = {
            'incremental': True,
            'primary_key': 'id',
            'batch_size': 1000,
            'estimated_size_mb': 50,
            'estimated_rows': 10000
        }
        
        # Mock incremental settings
        settings_instance.should_use_incremental.return_value = True
        
        # Mock table priority settings
        settings_instance.get_tables_by_priority.return_value = [
            'patient', 'appointment', 'procedurelog'
        ]
        
        mock.return_value = settings_instance
        yield settings_instance


@pytest.fixture
def test_config_file(tmp_path):
    """Create a temporary test configuration file."""
    config_content = """
[database]
source_host = localhost
source_port = 3306
source_database = opendental_test
source_user = test_user
source_password = test_pass

[table_config]
patient.incremental = true
patient.primary_key = PatientNum
patient.batch_size = 1000

appointment.incremental = true
appointment.primary_key = AptNum
appointment.batch_size = 1000
"""
    config_file = tmp_path / "test_config.ini"
    config_file.write_text(config_content)
    return str(config_file)


# =============================================================================
# TRANSFORMER FIXTURES
# =============================================================================

@pytest.fixture
def mock_raw_to_public_transformer(mock_analytics_engine):
    """Mock RawToPublicTransformer for testing."""
    transformer = MagicMock(spec=RawToPublicTransformer)
    transformer.transform_table.return_value = True
    transformer.transform_table_incremental.return_value = True
    return transformer


# =============================================================================
# IDEMPOTENCY TESTING FIXTURES
# =============================================================================

@pytest.fixture
def idempotency_test_data():
    """Test data specifically for idempotency testing with OpenDental source field names."""
    return {
        'initial_state': {
            'patient_count': 100,
            'appointment_count': 250,
            'last_modified': '2023-12-01 10:00:00'
        },
        'modified_row': {
            'table': 'patient',
            'id': 1,
            'changes': {
                'LName': 'Modified',
                'Email': 'modified@example.com'
            }
        },
        'new_row': {
            'table': 'patient',
            'data': {
                'PatNum': 101,
                'LName': 'New',
                'FName': 'Patient',
                'Email': 'new@example.com'
            }
        }
    }


@pytest.fixture
def incremental_test_scenarios():
    """Test scenarios for incremental processing."""
    return {
        'no_changes': {
            'description': 'No changes in source data',
            'expected_behavior': 'Skip processing or process zero rows',
            'source_changes': []
        },
        'single_row_change': {
            'description': 'Single row modified in source',
            'expected_behavior': 'Process only the changed row',
            'source_changes': [
                {'table': 'patient', 'id': 1, 'field': 'LName', 'old_value': 'Doe', 'new_value': 'Modified'}
            ]
        },
        'multiple_changes': {
            'description': 'Multiple rows changed in source',
            'expected_behavior': 'Process only changed rows',
            'source_changes': [
                {'table': 'patient', 'id': 1, 'field': 'LName', 'old_value': 'Doe', 'new_value': 'Modified'},
                {'table': 'patient', 'id': 2, 'field': 'Email', 'old_value': 'jane@example.com', 'new_value': 'jane.modified@example.com'},
                {'table': 'appointment', 'id': 1, 'field': 'AptStatus', 'old_value': 1, 'new_value': 2}
            ]
        },
        'schema_change': {
            'description': 'Schema change in source table',
            'expected_behavior': 'Revert to full load',
            'schema_changes': [
                {'table': 'patient', 'change': 'add_column', 'column': 'Phone2', 'type': 'VARCHAR(20)'}
            ]
        }
    }


# =============================================================================
# PERFORMANCE TESTING FIXTURES
# =============================================================================

@pytest.fixture
def performance_test_config():
    """
    Configuration for performance testing.
    
    Uses securitylog table (largest table) for realistic performance testing
    scenarios that match production data volumes.
    """
    return {
        'small_dataset': {
            'patient_count': 1000,
            'appointment_count': 5000,
            'securitylog_count': 10000,  # Small securitylog dataset
            'expected_duration_seconds': 30
        },
        'medium_dataset': {
            'patient_count': 10000,
            'appointment_count': 50000,
            'securitylog_count': 100000,  # Medium securitylog dataset
            'expected_duration_seconds': 120
        },
        'large_dataset': {
            'patient_count': 100000,
            'appointment_count': 500000,
            'securitylog_count': 1000000,  # Large securitylog dataset (1M records)
            'expected_duration_seconds': 600
        },
        'securitylog_performance': {
            'description': 'Securitylog table performance testing (largest table)',
            'test_scenarios': {
                'small': {'count': 10000, 'expected_seconds': 30},
                'medium': {'count': 100000, 'expected_seconds': 120},
                'large': {'count': 1000000, 'expected_seconds': 600},
                'xlarge': {'count': 5000000, 'expected_seconds': 1800}  # 5M records
            },
            'table_structure': 'securitylog_pg_ddl.sql',
            'database': 'opendental_analytics',
            'schema': 'raw'
        }
    }


# =============================================================================
# SECURITYLOG TEST DATA (LARGEST TABLE FOR PERFORMANCE TESTING)
# =============================================================================

@pytest.fixture
def sample_securitylog_data():
    """
    Sample securitylog data for testing with PostgreSQL analytics schema.
    
    This is the largest table in the system and perfect for performance testing.
    Uses the exact field names from securitylog_pg_ddl.sql.
    """
    return pd.DataFrame([
        {
            '"SecurityLogNum"': 1,
            '"PermType"': 1,  # Login permission
            '"UserNum"': 1,
            '"LogDateTime"': '2023-01-15 09:00:00',
            '"LogText"': 'User login successful - ID: 1',
            '"PatNum"': None,  # Login doesn't have patient
            '"CompName"': 'COMPUTER-01',
            '"FKey"': None,  # Login doesn't have foreign key
            '"LogSource"': 1,  # System login
            '"DefNum"': None,  # Login doesn't have definition
            '"DefNumError"': None,  # No error
            '"DateTPrevious"': None
        },
        {
            '"SecurityLogNum"': 2,
            '"PermType"': 2,  # Patient access permission
            '"UserNum"': 1,
            '"LogDateTime"': '2023-01-15 09:05:00',
            '"LogText"': 'Patient record accessed - ID: 2',
            '"PatNum"': 1,  # Patient 1 accessed
            '"CompName"': 'COMPUTER-01',
            '"FKey"': 1,  # Patient record foreign key
            '"LogSource"': 2,  # Patient module
            '"DefNum"': 1,  # Patient definition
            '"DefNumError"': None,  # No error
            '"DateTPrevious"': '2023-01-15 09:00:00'
        },
        {
            '"SecurityLogNum"': 3,
            '"PermType"': 3,  # Appointment permission
            '"UserNum"': 2,
            '"LogDateTime"': '2023-01-15 10:00:00',
            '"LogText"': 'Appointment scheduled - ID: 3',
            '"PatNum"': 2,  # Patient 2 appointment
            '"CompName"': 'COMPUTER-02',
            '"FKey"': 2,  # Appointment foreign key
            '"LogSource"': 3,  # Appointment module
            '"DefNum"': 2,  # Appointment definition
            '"DefNumError"': None,  # No error
            '"DateTPrevious"': '2023-01-15 09:30:00'
        },
        {
            '"SecurityLogNum"': 4,
            '"PermType"': 4,  # Procedure permission
            '"UserNum"': 1,
            '"LogDateTime"': '2023-01-15 11:00:00',
            '"LogText"': 'Procedure completed - ID: 4',
            '"PatNum"': 1,  # Patient 1 procedure
            '"CompName"': 'COMPUTER-01',
            '"FKey"': 3,  # Procedure foreign key
            '"LogSource"': 4,  # Procedure module
            '"DefNum"': 3,  # Procedure definition
            '"DefNumError"': None,  # No error
            '"DateTPrevious"': '2023-01-15 10:45:00'
        },
        {
            '"SecurityLogNum"': 5,
            '"PermType"': 5,  # Payment permission
            '"UserNum"': 3,
            '"LogDateTime"': '2023-01-15 12:00:00',
            '"LogText"': 'Payment processed - ID: 5',
            '"PatNum"': 3,  # Patient 3 payment
            '"CompName"': 'COMPUTER-03',
            '"FKey"': 4,  # Payment foreign key
            '"LogSource"': 5,  # Payment module
            '"DefNum"': 4,  # Payment definition
            '"DefNumError"': 1,  # Payment error occurred
            '"DateTPrevious"': '2023-01-15 11:30:00'
        }
    ])


@pytest.fixture
def large_securitylog_dataset():
    """
    Large securitylog dataset for performance testing.
    
    Generates realistic security log data with:
    - 100,000+ records (largest table)
    - Realistic log patterns and timestamps
    - Mix of different permission types and sources
    - Some records with errors for error handling tests
    - Distributed across multiple users and computers
    """
    def _generate_large_securitylog(count: int = 100000):
        """Generate large securitylog dataset for performance testing."""
        data = []
        for i in range(count):
            # Mix of different log types and sources
            perm_type = (i % 10) + 1  # 1-10 different permission types
            log_source = (i % 5) + 1  # 1-5 different log sources
            
            # Generate realistic log text based on permission type
            log_actions = {
                1: 'User login successful',
                2: 'Patient record accessed',
                3: 'Appointment scheduled',
                4: 'Procedure completed',
                5: 'Payment processed',
                6: 'Insurance claim submitted',
                7: 'Medical history updated',
                8: 'Prescription written',
                9: 'Lab results viewed',
                10: 'Treatment plan created'
            }
            log_text = f"{log_actions.get(perm_type, 'Unknown action')} - ID: {i}"
            
            # Generate timestamps across a year with realistic patterns
            base_date = datetime(2023, 1, 1) + timedelta(
                days=i % 365,
                hours=(i % 24),
                minutes=(i % 60),
                seconds=(i % 60)
            )
            
            # Some logs don't have all fields (realistic data)
            has_patient = i % 3 != 0  # 67% have patient
            has_foreign_key = i % 2 == 0  # 50% have foreign key
            has_definition = i % 4 != 0  # 75% have definition
            has_error = i % 10 == 0  # 10% have errors
            has_previous = i % 5 != 0  # 80% have previous timestamp
            
            data.append({
                '"SecurityLogNum"': i + 1,  # PostgreSQL quoted field names
                '"PermType"': perm_type,
                '"UserNum"': (i % 20) + 1,  # 20 different users
                '"LogDateTime"': base_date.strftime('%Y-%m-%d %H:%M:%S'),
                '"LogText"': log_text,
                '"PatNum"': (i % 1000) + 1 if has_patient else None,
                '"CompName"': f'COMPUTER-{(i % 10) + 1:02d}',
                '"FKey"': (i % 10000) + 1 if has_foreign_key else None,
                '"LogSource"': log_source,
                '"DefNum"': (i % 100) + 1 if has_definition else None,
                '"DefNumError"': (i % 50) + 1 if has_error else None,
                '"DateTPrevious"': (base_date - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S') if has_previous else None
            })
        return pd.DataFrame(data)
    
    return _generate_large_securitylog


# =============================================================================
# POSTGRESLOADER FIXTURES
# =============================================================================

@pytest.fixture
def sample_mysql_schema():
    """Sample MySQL schema for testing."""
    return {
        'columns': [
            {'name': 'id', 'type': 'int', 'nullable': False},
            {'name': 'name', 'type': 'varchar(255)', 'nullable': True},
            {'name': 'created_at', 'type': 'datetime', 'nullable': True}
        ],
        'primary_key': {'constrained_columns': ['id']},
        'incremental_columns': ['created_at']
    }


@pytest.fixture
def sample_table_data():
    """Sample table data for testing."""
    return [
        {'id': 1, 'name': 'John Doe', 'created_at': datetime(2023, 1, 1, 10, 0, 0)},
        {'id': 2, 'name': 'Jane Smith', 'created_at': datetime(2023, 1, 2, 11, 0, 0)},
        {'id': 3, 'name': 'Bob Johnson', 'created_at': datetime(2023, 1, 3, 12, 0, 0)}
    ]


@pytest.fixture
def postgres_loader(mock_replication_engine, mock_analytics_engine):
    """Create PostgresLoader instance with mocked engines."""
    with patch('etl_pipeline.loaders.postgres_loader.settings') as mock_settings:
        mock_settings.get_database_config.side_effect = lambda db: {
            'analytics': {'schema': 'raw'},
            'replication': {'schema': 'raw'}
        }.get(db, {})
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema') as mock_schema_class:
            mock_schema_adapter = MagicMock()
            mock_schema_class.return_value = mock_schema_adapter
            
            loader = PostgresLoader(
                replication_engine=mock_replication_engine,
                analytics_engine=mock_analytics_engine
            )
            loader.schema_adapter = mock_schema_adapter
            return loader


# =============================================================================
# UTILITY FIXTURES
# =============================================================================

@pytest.fixture
def temp_file_factory(tmp_path):
    """Factory for creating temporary files."""
    def _create_temp_file(content: str, filename: str = "temp.txt"):
        file_path = tmp_path / filename
        file_path.write_text(content)
        return str(file_path)
    
    return _create_temp_file


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    with patch('etl_pipeline.config.logging.logger') as mock:
        yield mock


@pytest.fixture
def mock_metrics_collector():
    """Mock metrics collector for testing."""
    collector = MagicMock()
    collector.record_processing_time.return_value = None
    collector.record_table_processed.return_value = None
    collector.record_error.return_value = None
    collector.get_metrics.return_value = {
        'tables_processed': 0,
        'total_processing_time': 0,
        'errors': 0
    }
    return collector


# =============================================================================
# TEST ENVIRONMENT SETUP
# =============================================================================

@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Set up test environment variables."""
    # Set test environment variables
    test_env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'localhost',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'opendental_test',
        'OPENDENTAL_SOURCE_USER': 'test_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
        'MYSQL_REPLICATION_HOST': 'localhost',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'replication_test',
        'MYSQL_REPLICATION_USER': 'test_user',
        'MYSQL_REPLICATION_PASSWORD': 'test_pass',
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics_test',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'test_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'test_pass'
    }
    
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)


# =============================================================================
# TEST CATEGORIES AND MARKERS
# =============================================================================

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, isolated)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (require databases)"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take longer to run"
    )
    config.addinivalue_line(
        "markers", "performance: Performance benchmarks"
    )
    config.addinivalue_line(
        "markers", "idempotency: Idempotency and incremental load tests"
    )
    config.addinivalue_line(
        "markers", "critical: Critical path tests for production"
    )
    config.addinivalue_line(
        "markers", "orchestration: Orchestration component tests"
    )
    config.addinivalue_line(
        "markers", "loaders: Data loader tests"
    )
    config.addinivalue_line(
        "markers", "transformers: Data transformation tests"
    )
    config.addinivalue_line(
        "markers", "postgres: mark test as requiring PostgreSQL"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle markers."""
    for item in items:
        # Add unit marker to tests that don't have any marker
        if not any(item.iter_markers()):
            item.add_marker(pytest.mark.unit)


@pytest.fixture(scope="session")
def test_environment():
    """Set up test environment variables."""
    # Set default PostgreSQL test configuration
    os.environ.setdefault('TEST_POSTGRES_HOST', 'localhost')
    os.environ.setdefault('TEST_POSTGRES_PORT', '5432')
    os.environ.setdefault('TEST_POSTGRES_USER', 'postgres')
    os.environ.setdefault('TEST_POSTGRES_PASSWORD', 'postgres')
    os.environ.setdefault('TEST_POSTGRES_DB', 'test_analytics')
    
    return {
        'postgres_host': os.environ.get('TEST_POSTGRES_HOST'),
        'postgres_port': os.environ.get('TEST_POSTGRES_PORT'),
        'postgres_user': os.environ.get('TEST_POSTGRES_USER'),
        'postgres_password': os.environ.get('TEST_POSTGRES_PASSWORD'),
        'postgres_db': os.environ.get('TEST_POSTGRES_DB'),
    }


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection for unit tests."""
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result
    return mock_conn, mock_result


@pytest.fixture
def mock_mysql_connection():
    """Mock MySQL connection for unit tests."""
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_conn.execute.return_value = mock_result
    return mock_conn, mock_result


# =============================================================================
# MYSQL REPLICATOR FIXTURES
# =============================================================================

@pytest.fixture
def sample_mysql_replicator_table_data():
    """Sample table data for MySQLReplicator tests."""
    return [
        {'PatNum': 1, 'LName': 'Doe', 'FName': 'John', 'Email': 'john@example.com'},
        {'PatNum': 2, 'LName': 'Smith', 'FName': 'Jane', 'Email': 'jane@example.com'},
        {'PatNum': 3, 'LName': 'Johnson', 'FName': 'Bob', 'Email': 'bob@example.com'}
    ]

@pytest.fixture
def sample_create_statement():
    """Sample CREATE TABLE statement for MySQLReplicator tests."""
    return """CREATE TABLE `patient` (
  `PatNum` int(11) NOT NULL AUTO_INCREMENT,
  `LName` varchar(255) NOT NULL DEFAULT '',
  `FName` varchar(255) NOT NULL DEFAULT '',
  `Birthdate` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `Email` varchar(255) NOT NULL DEFAULT '',
  `HmPhone` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`PatNum`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""

from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator

@pytest.fixture
def mock_target_engine():
    """Mock target database engine for MySQLReplicator tests."""
    engine = MagicMock(spec=Engine)
    engine.name = 'mysql'
    return engine

@pytest.fixture
def replicator(mock_source_engine, mock_target_engine):
    """Create ExactMySQLReplicator instance with mocked engines."""
    with patch('etl_pipeline.core.mysql_replicator.inspect') as mock_inspect:
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        replicator = ExactMySQLReplicator(
            source_engine=mock_source_engine,
            target_engine=mock_target_engine,
            source_db='test_source',
            target_db='test_target'
        )
        replicator.inspector = mock_inspector
        return replicator


# =============================================================================
# PRIORITYPROCESSOR FIXTURES
# =============================================================================

@pytest.fixture
def mock_priority_processor_settings():
    """Mock Settings instance for PriorityProcessor tests."""
    from etl_pipeline.config.settings import Settings
    settings = MagicMock(spec=Settings)
    settings.get_tables_by_priority.side_effect = lambda importance, **kwargs: {
        'critical': ['patient', 'appointment', 'procedurelog'],
        'important': ['payment', 'claim', 'insplan'],
        'audit': ['securitylog', 'entrylog'],
        'reference': ['zipcode', 'definition']
    }.get(importance, [])
    return settings


@pytest.fixture
def mock_priority_processor_table_processor():
    """Mock TableProcessor instance for PriorityProcessor tests."""
    from etl_pipeline.orchestration.table_processor import TableProcessor
    processor = MagicMock(spec=TableProcessor)
    processor.process_table.return_value = True
    return processor


@pytest.fixture
def priority_processor(mock_priority_processor_settings):
    """Create PriorityProcessor instance with mocked settings."""
    from etl_pipeline.orchestration.priority_processor import PriorityProcessor
    return PriorityProcessor(settings=mock_priority_processor_settings)


# =============================================================================
# PIPELINE ORCHESTRATOR FIXTURES
# =============================================================================

@pytest.fixture
def mock_components():
    """Mock pipeline components for PipelineOrchestrator tests."""
    return {
        'table_processor': MagicMock(),
        'priority_processor': MagicMock(),
        'metrics': MagicMock()
    }


@pytest.fixture
def orchestrator(mock_components):
    """Create REAL PipelineOrchestrator instance with mocked dependencies."""
    with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings:
        mock_settings.return_value = MagicMock()
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
            mock_processor_class.return_value = mock_components['table_processor']
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                mock_priority_class.return_value = mock_components['priority_processor']
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    mock_metrics_class.return_value = mock_components['metrics']
                    
                    # Create REAL orchestrator with mocked dependencies
                    from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
                    orchestrator = PipelineOrchestrator()
                    return orchestrator


@pytest.fixture
def sqlite_engines():
    """Create SQLite engines for integration testing."""
    import tempfile
    import os
    from sqlalchemy import create_engine
    
    # Create temporary SQLite databases
    raw_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    raw_db.close()
    public_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    public_db.close()
    
    # Create engines
    raw_engine = create_engine(f'sqlite:///{raw_db.name}')
    public_engine = create_engine(f'sqlite:///{public_db.name}')
    
    yield raw_engine, public_engine
    
    # Cleanup
    os.unlink(raw_db.name)
    os.unlink(public_db.name)


@pytest.fixture
def sqlite_compatible_orchestrator(sqlite_engines):
    """Create SQLite-compatible orchestrator for integration testing."""
    raw_engine, public_engine = sqlite_engines
    
    # Create a mock table processor that works with SQLite
    mock_table_processor = MagicMock()
    mock_table_processor.initialize_connections.return_value = True
    mock_table_processor.process_table.return_value = True
    mock_table_processor.cleanup = MagicMock()
    
    # Create a mock priority processor
    mock_priority_processor = MagicMock()
    mock_priority_processor.process_by_priority.return_value = {
        'high': {'success': ['patient'], 'failed': []}
    }
    
    # Create a mock metrics collector
    mock_metrics = MagicMock()
    
    with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings:
        mock_settings.return_value = MagicMock()
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
            mock_processor_class.return_value = mock_table_processor
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                mock_priority_class.return_value = mock_priority_processor
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                    mock_metrics_class.return_value = mock_metrics
                    
                    from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
                    orchestrator = PipelineOrchestrator()
                    return orchestrator 


# =============================================================================
# TABLEPROCESSOR FIXTURES
# =============================================================================

@pytest.fixture
def mock_table_processor_engines():
    """Mock database engines for TableProcessor tests with proper URL mocking."""
    # Create engines with proper URL mocking (Section 4.1 from debugging notes)
    mock_source = MagicMock(spec=Engine)
    mock_source.url = Mock()
    mock_source.url.database = 'source_db'
    
    mock_replication = MagicMock(spec=Engine)
    mock_replication.url = Mock()
    mock_replication.url.database = 'replication_db'
    
    mock_analytics = MagicMock(spec=Engine)
    mock_analytics.url = Mock()
    mock_analytics.url.database = 'analytics_db'
    
    return mock_source, mock_replication, mock_analytics


@pytest.fixture
def mock_table_processor_settings():
    """Mock Settings instance for TableProcessor tests."""
    settings = MagicMock(spec=Settings)
    
    # Mock database configurations with **kwargs (Section 12 from debugging notes)
    settings.get_database_config.side_effect = lambda db, **kwargs: {
        'database': f'{db}_database'
    }
    
    # Mock table configurations
    settings.get_table_config.return_value = {
        'estimated_rows': 1000,
        'estimated_size_mb': 10,
        'batch_size': 5000
    }
    
    # Mock incremental settings
    settings.should_use_incremental.return_value = True
    
    return settings


@pytest.fixture
def mock_table_processor_raw_to_public_transformer():
    """Mock RawToPublicTransformer for TableProcessor tests."""
    mock_transformer = MagicMock()
    mock_transformer.transform_table.return_value = True
    return mock_transformer


@pytest.fixture
def table_processor_standard_config():
    """Standard table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 1000,
        'estimated_size_mb': 10,
        'batch_size': 5000
    }


@pytest.fixture
def table_processor_large_config():
    """Large table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 2000000,
        'estimated_size_mb': 200,
        'batch_size': 5000
    }


@pytest.fixture
def table_processor_medium_large_config():
    """Medium-large table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 500000,
        'estimated_size_mb': 150,
        'batch_size': 5000
    }


@pytest.fixture
def table_processor_very_large_config():
    """Very large table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 5000000,
        'estimated_size_mb': 500,
        'batch_size': 10000
    }


@pytest.fixture
def mock_table_processor_components(mock_table_processor_engines, mock_table_processor_settings, mock_table_processor_raw_to_public_transformer):
    """Mock components for TableProcessor tests."""
    mock_source, mock_replication, mock_analytics = mock_table_processor_engines
    
    return {
        'source_engine': mock_source,
        'replication_engine': mock_replication,
        'analytics_engine': mock_analytics,
        'settings': mock_table_processor_settings,
        'raw_to_public_transformer': mock_table_processor_raw_to_public_transformer
    } 


# =============================================================================
# UNIFIED METRICS FIXTURES
# =============================================================================

@pytest.fixture
def mock_unified_metrics_connection():
    """Mock database connection with context manager support for unified metrics tests."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=None)
    return mock_conn


@pytest.fixture
def unified_metrics_collector_no_persistence():
    """Metrics collector without database persistence for unified metrics tests."""
    with patch('etl_pipeline.monitoring.unified_metrics.logger') as mock_logger:
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        collector = UnifiedMetricsCollector(enable_persistence=False)
        yield collector


# =============================================================================
# EXISTING UNIFIED METRICS FIXTURES (for backward compatibility) 