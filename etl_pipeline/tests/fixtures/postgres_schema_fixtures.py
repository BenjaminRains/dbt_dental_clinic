"""
PostgreSQL Schema fixtures for ETL pipeline tests.

This module contains fixtures related to:
- PostgreSQL schema testing
- Schema discovery testing
- PostgresSchema class testing
- Schema conversion testing
"""

import pytest
import logging
from unittest.mock import MagicMock, Mock
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, Any, List

# Import ETL pipeline components for testing
try:
    from etl_pipeline.core.postgres_schema import PostgresSchema as PostgresSchemaClass
    from etl_pipeline.core.schema_discovery import SchemaDiscovery
    from etl_pipeline.core.connections import ConnectionFactory
    POSTGRES_SCHEMA_AVAILABLE = True
except ImportError:
    POSTGRES_SCHEMA_AVAILABLE = False
    PostgresSchemaClass = None
    SchemaDiscovery = None
    ConnectionFactory = None

# Import new configuration system
try:
    from etl_pipeline.config import (
        create_test_settings, 
        DatabaseType, 
        PostgresSchema,
        reset_settings
    )
    NEW_CONFIG_AVAILABLE = True
except ImportError:
    NEW_CONFIG_AVAILABLE = False
    DatabaseType = None
    PostgresSchema = None

logger = logging.getLogger(__name__)


@pytest.fixture
def postgres_schema_test_settings():
    """Create test settings specifically for PostgreSQL schema testing.
    
    This fixture ensures we're using the test environment by:
    1. Setting ETL_ENVIRONMENT=test to ensure test environment detection
    2. Letting the configuration system load test variables from .env file
    3. Using the proper test connection methods
    """
    if not NEW_CONFIG_AVAILABLE:
        pytest.skip("New configuration system not available")
    
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
def mock_postgres_schema_engines():
    """Mock database engines for PostgreSQL schema testing."""
    if not POSTGRES_SCHEMA_AVAILABLE:
        pytest.skip("PostgreSQL schema components not available")
    
    # Create mock engines
    mysql_engine = MagicMock(spec=Engine)
    postgres_engine = MagicMock(spec=Engine)
    
    # Mock URL attributes for SQLAlchemy compatibility
    mysql_engine.url = Mock()
    mysql_engine.url.database = 'test_replication'
    postgres_engine.url = Mock()
    postgres_engine.url.database = 'test_analytics'
    
    return mysql_engine, postgres_engine


@pytest.fixture
def mock_schema_discovery():
    """Mock SchemaDiscovery for testing."""
    if not POSTGRES_SCHEMA_AVAILABLE:
        pytest.skip("SchemaDiscovery not available")
    
    discovery = MagicMock(spec=SchemaDiscovery)
    
    # Mock get_table_schema method
    def mock_get_table_schema(table_name):
        # Return different schemas based on table name
        if table_name == 'patient':
            return {
                'create_statement': '''
                    CREATE TABLE patient (
                        PatNum INT AUTO_INCREMENT PRIMARY KEY,
                        LName VARCHAR(100),
                        FName VARCHAR(100),
                        MiddleI VARCHAR(10),
                        Preferred VARCHAR(100),
                        PatStatus TINYINT,
                        Gender TINYINT,
                        Position TINYINT,
                        Birthdate DATE,
                        SSN VARCHAR(20),
                        IsActive TINYINT,
                        IsDeleted TINYINT
                    )
                '''
            }
        elif table_name == 'appointment':
            return {
                'create_statement': '''
                    CREATE TABLE appointment (
                        AptNum INT AUTO_INCREMENT PRIMARY KEY,
                        PatNum INT,
                        AptDateTime DATETIME,
                        AptStatus TINYINT,
                        DateTStamp DATETIME,
                        Notes TEXT,
                        IsConfirmed TINYINT,
                        IsCancelled TINYINT
                    )
                '''
            }
        elif table_name == 'boolean_test':
            return {
                'create_statement': '''
                    CREATE TABLE boolean_test (
                        ID INT AUTO_INCREMENT PRIMARY KEY,
                        Name VARCHAR(100),
                        IsActive TINYINT,
                        IsDeleted TINYINT,
                        Status TINYINT,
                        Priority TINYINT
                    )
                '''
            }
        else:
            return None
    
    discovery.get_table_schema = mock_get_table_schema
    return discovery


@pytest.fixture
def sample_mysql_schemas():
    """Sample MySQL schemas for testing schema conversion."""
    return {
        'patient': {
            'create_statement': '''
                CREATE TABLE patient (
                    PatNum INT AUTO_INCREMENT PRIMARY KEY,
                    LName VARCHAR(100),
                    FName VARCHAR(100),
                    MiddleI VARCHAR(10),
                    Preferred VARCHAR(100),
                    PatStatus TINYINT,
                    Gender TINYINT,
                    Position TINYINT,
                    Birthdate DATE,
                    SSN VARCHAR(20),
                    IsActive TINYINT,
                    IsDeleted TINYINT
                )
            '''
        },
        'appointment': {
            'create_statement': '''
                CREATE TABLE appointment (
                    AptNum INT AUTO_INCREMENT PRIMARY KEY,
                    PatNum INT,
                    AptDateTime DATETIME,
                    AptStatus TINYINT,
                    DateTStamp DATETIME,
                    Notes TEXT,
                    IsConfirmed TINYINT,
                    IsCancelled TINYINT
                )
            '''
        },
        'procedure': {
            'create_statement': '''
                CREATE TABLE `procedure` (
                    ProcNum INT AUTO_INCREMENT PRIMARY KEY,
                    PatNum INT,
                    ProcDate DATE,
                    ProcCode VARCHAR(20),
                    ProcFee DECIMAL(10,2),
                    DateTStamp DATETIME,
                    IsCompleted TINYINT,
                    IsPaid TINYINT
                )
            '''
        },
        'boolean_test': {
            'create_statement': '''
                CREATE TABLE boolean_test (
                    ID INT AUTO_INCREMENT PRIMARY KEY,
                    Name VARCHAR(100),
                    IsActive TINYINT,
                    IsDeleted TINYINT,
                    Status TINYINT,
                    Priority TINYINT
                )
            '''
        }
    }


@pytest.fixture
def expected_postgres_schemas():
    """Expected PostgreSQL schemas after conversion."""
    return {
        'patient': {
            'create_statement': '''
                CREATE TABLE raw.patient (
                    "PatNum" integer PRIMARY KEY,
                    "LName" character varying(100),
                    "FName" character varying(100),
                    "MiddleI" character varying(10),
                    "Preferred" character varying(100),
                    "PatStatus" boolean,
                    "Gender" boolean,
                    "Position" boolean,
                    "Birthdate" date,
                    "SSN" character varying(20),
                    "IsActive" boolean,
                    "IsDeleted" boolean
                )
            '''
        },
        'appointment': {
            'create_statement': '''
                CREATE TABLE raw.appointment (
                    "AptNum" integer PRIMARY KEY,
                    "PatNum" integer,
                    "AptDateTime" timestamp without time zone,
                    "AptStatus" boolean,
                    "DateTStamp" timestamp without time zone,
                    "Notes" text,
                    "IsConfirmed" boolean,
                    "IsCancelled" boolean
                )
            '''
        },
        'procedure': {
            'create_statement': '''
                CREATE TABLE raw.procedure (
                    "ProcNum" integer PRIMARY KEY,
                    "PatNum" integer,
                    "ProcDate" date,
                    "ProcCode" character varying(20),
                    "ProcFee" numeric(10,2),
                    "DateTStamp" timestamp without time zone,
                    "IsCompleted" boolean,
                    "IsPaid" boolean
                )
            '''
        },
        'boolean_test': {
            'create_statement': '''
                CREATE TABLE raw.boolean_test (
                    "ID" integer PRIMARY KEY,
                    "Name" character varying(100),
                    "IsActive" boolean,
                    "IsDeleted" boolean,
                    "Status" smallint,
                    "Priority" smallint
                )
            '''
        }
    }


@pytest.fixture
def postgres_schema_test_data():
    """Test data for PostgreSQL schema testing."""
    return {
        'test_patients': [
            {
                'PatNum': 1,
                'LName': 'POSTGRES_SCHEMA_TEST_PATIENT_001',
                'FName': 'John',
                'MiddleI': 'M',
                'Preferred': 'Johnny',
                'PatStatus': 0,
                'Gender': 0,
                'Position': 0,
                'Birthdate': '1980-01-01',
                'SSN': '123-45-6789',
                'IsActive': 1,
                'IsDeleted': 0
            },
            {
                'PatNum': 2,
                'LName': 'POSTGRES_SCHEMA_TEST_PATIENT_002',
                'FName': 'Jane',
                'MiddleI': 'A',
                'Preferred': 'Janey',
                'PatStatus': 0,
                'Gender': 1,
                'Position': 0,
                'Birthdate': '1985-05-15',
                'SSN': '234-56-7890',
                'IsActive': 1,
                'IsDeleted': 0
            }
        ],
        'test_appointments': [
            {
                'AptNum': 1,
                'PatNum': 1,
                'AptDateTime': '2024-01-15 10:00:00',
                'AptStatus': 1,
                'DateTStamp': '2024-01-14 09:00:00',
                'Notes': 'POSTGRES_SCHEMA_TEST_APPOINTMENT_001 - Regular checkup',
                'IsConfirmed': 1,
                'IsCancelled': 0
            },
            {
                'AptNum': 2,
                'PatNum': 2,
                'AptDateTime': '2024-01-16 14:30:00',
                'AptStatus': 1,
                'DateTStamp': '2024-01-14 09:00:00',
                'Notes': 'POSTGRES_SCHEMA_TEST_APPOINTMENT_002 - Cleaning',
                'IsConfirmed': 1,
                'IsCancelled': 0
            }
        ],
        'test_procedures': [
            {
                'ProcNum': 1,
                'PatNum': 1,
                'ProcDate': '2024-01-15',
                'ProcCode': 'POSTGRES_SCHEMA_TEST_PROC_001',
                'ProcFee': 150.00,
                'DateTStamp': '2024-01-15 10:00:00',
                'IsCompleted': 1,
                'IsPaid': 0
            },
            {
                'ProcNum': 2,
                'PatNum': 2,
                'ProcDate': '2024-01-16',
                'ProcCode': 'POSTGRES_SCHEMA_TEST_PROC_002',
                'ProcFee': 200.00,
                'DateTStamp': '2024-01-16 14:30:00',
                'IsCompleted': 1,
                'IsPaid': 1
            }
        ],
        'test_boolean_fields': [
            {
                'ID': 1,
                'Name': 'POSTGRES_SCHEMA_TEST_BOOL_001',
                'IsActive': 1,
                'IsDeleted': 0,
                'Status': 5,
                'Priority': 2
            },
            {
                'ID': 2,
                'Name': 'POSTGRES_SCHEMA_TEST_BOOL_002',
                'IsActive': 0,
                'IsDeleted': 1,
                'Status': 3,
                'Priority': 1
            }
        ]
    }


@pytest.fixture
def mock_postgres_schema_instance(mock_postgres_schema_engines):
    """Mock PostgresSchema instance for testing."""
    if not POSTGRES_SCHEMA_AVAILABLE:
        pytest.skip("PostgreSQL schema components not available")
    
    mysql_engine, postgres_engine = mock_postgres_schema_engines
    
    # Create mock PostgresSchema instance
    postgres_schema = MagicMock(spec=PostgresSchemaClass)
    postgres_schema.mysql_engine = mysql_engine
    postgres_schema.postgres_engine = postgres_engine
    postgres_schema.mysql_db = 'test_replication'
    postgres_schema.postgres_db = 'test_analytics'
    postgres_schema.postgres_schema = 'raw'
    
    # Mock methods
    def mock_adapt_schema(table_name, mysql_schema):
        # Return a mock PostgreSQL CREATE statement
        return f'CREATE TABLE raw.{table_name} ("ID" integer PRIMARY KEY)'
    
    def mock_create_postgres_table(table_name, mysql_schema):
        return True
    
    def mock_verify_schema(table_name, mysql_schema):
        return True
    
    def mock_convert_mysql_type(mysql_type, table_name, column_name):
        # Mock type conversion logic
        if mysql_type == 'TINYINT':
            if column_name in ['IsActive', 'IsDeleted', 'IsConfirmed', 'IsCancelled', 'IsCompleted', 'IsPaid']:
                return 'boolean'
            else:
                return 'smallint'
        elif mysql_type == 'INT':
            return 'integer'
        elif mysql_type == 'VARCHAR':
            return 'character varying'
        elif mysql_type == 'DATE':
            return 'date'
        elif mysql_type == 'DATETIME':
            return 'timestamp without time zone'
        elif mysql_type == 'DECIMAL':
            return 'numeric'
        elif mysql_type == 'TEXT':
            return 'text'
        else:
            return 'text'
    
    postgres_schema.adapt_schema = mock_adapt_schema
    postgres_schema.create_postgres_table = mock_create_postgres_table
    postgres_schema.verify_schema = mock_verify_schema
    postgres_schema._convert_mysql_type = mock_convert_mysql_type
    
    return postgres_schema


@pytest.fixture
def real_postgres_schema_instance(postgres_schema_test_settings):
    """Real PostgresSchema instance for integration testing using test environment.
    
    This fixture uses the test connection methods as specified in the connection
    environment separation documentation to ensure we're connecting to test databases.
    """
    if not POSTGRES_SCHEMA_AVAILABLE or not NEW_CONFIG_AVAILABLE:
        pytest.skip("PostgreSQL schema components or new configuration system not available")
    
    # Use test connection methods as specified in connection_environment_separation.md
    # These methods automatically use TEST_* environment variables from .env file
    mysql_engine = ConnectionFactory.get_mysql_replication_test_connection()
    postgres_engine = ConnectionFactory.get_postgres_analytics_test_connection()
    
    # Extract database names from engine URLs
    mysql_db = mysql_engine.url.database
    postgres_db = postgres_engine.url.database
    
    return PostgresSchemaClass(
        mysql_engine=mysql_engine,
        postgres_engine=postgres_engine,
        mysql_db=mysql_db,
        postgres_db=postgres_db,
        postgres_schema='raw'
    )


@pytest.fixture
def real_schema_discovery_instance(postgres_schema_test_settings):
    """Real SchemaDiscovery instance for integration testing using test environment.
    
    This fixture uses the test connection methods as specified in the connection
    environment separation documentation to ensure we're connecting to test databases.
    """
    if not POSTGRES_SCHEMA_AVAILABLE or not NEW_CONFIG_AVAILABLE:
        pytest.skip("SchemaDiscovery or new configuration system not available")
    
    # Use test connection method as specified in connection_environment_separation.md
    # This method automatically uses TEST_* environment variables from .env file
    mysql_engine = ConnectionFactory.get_mysql_replication_test_connection()
    mysql_db = mysql_engine.url.database
    return SchemaDiscovery(mysql_engine, mysql_db)


@pytest.fixture
def postgres_schema_test_tables():
    """List of test tables for PostgreSQL schema testing."""
    return ['patient', 'appointment', 'procedure', 'boolean_test']


@pytest.fixture
def postgres_schema_error_cases():
    """Error cases for PostgreSQL schema testing."""
    return {
        'malformed_schema': {
            'create_statement': 'CREATE TABLE test'  # Missing column definitions
        },
        'nonexistent_table': 'nonexistent_table',
        'invalid_mysql_type': 'INVALID_TYPE',
        'empty_schema': {
            'create_statement': ''
        }
    } 