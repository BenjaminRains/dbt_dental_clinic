"""
PostgreSQL Schema fixtures for ETL pipeline tests.

This module contains fixtures related to:
- PostgreSQL schema testing
- ConfigReader testing (replaces SchemaDiscovery)
- PostgresSchema class testing
- Schema conversion testing

Follows the connection architecture patterns where appropriate:
- Uses provider pattern for dependency injection
- Uses Settings injection for environment-agnostic schema testing
- Uses environment separation for test vs production schema testing
- Uses unified interface with ConnectionFactory
"""

import pytest
import logging
from unittest.mock import MagicMock, Mock, patch
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, Any, List

from etl_pipeline.config import create_test_settings
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.core import ConnectionFactory

# Import ETL pipeline components for testing
try:
    from etl_pipeline.core.postgres_schema import PostgresSchema as PostgresSchemaClass
    from etl_pipeline.config.config_reader import ConfigReader
    from etl_pipeline.core.connections import ConnectionFactory
    POSTGRES_SCHEMA_AVAILABLE = True
except ImportError:
    POSTGRES_SCHEMA_AVAILABLE = False
    PostgresSchemaClass = None
    ConfigReader = None
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
def test_postgres_schema_settings():
    """Test PostgreSQL schema settings using provider pattern for dependency injection."""
    # Create test provider with injected PostgreSQL schema configuration
    test_provider = DictConfigProvider(
        pipeline={
            'postgres_schema': {
                'default_schema': 'raw',
                'type_conversion': {
                    'TINYINT': 'boolean',
                    'INT': 'integer',
                    'VARCHAR': 'character varying',
                    'DATE': 'date',
                    'DATETIME': 'timestamp without time zone',
                    'DECIMAL': 'numeric',
                    'TEXT': 'text'
                }
            },
            'connections': {
                'source': {'pool_size': 5, 'connect_timeout': 10},
                'replication': {'pool_size': 10, 'max_overflow': 20},
                'analytics': {'application_name': 'etl_pipeline_test'}
            }
        },
        tables={
            'tables': {
                'patient': {
                    'priority': 'high',
                    'incremental_column': 'DateModified',
                    'batch_size': 1000,
                    'table_importance': 'critical'
                },
                'appointment': {
                    'priority': 'high',
                    'incremental_column': 'AptDateTime',
                    'batch_size': 500,
                    'table_importance': 'high'
                },
                'procedurelog': {
                    'priority': 'medium',
                    'incremental_column': 'ProcDate',
                    'batch_size': 2000,
                    'table_importance': 'high'
                }
            }
        },
        env={
            # OpenDental Source (Test) - following architecture naming
            'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
            'TEST_OPENDENTAL_SOURCE_PORT': '3306',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
            'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
            
            # MySQL Replication (Test) - following architecture naming
            'TEST_MYSQL_REPLICATION_HOST': 'localhost',
            'TEST_MYSQL_REPLICATION_PORT': '3305',
            'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
            'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
            'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
            
            # PostgreSQL Analytics (Test) - following architecture naming
            'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
            'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
        }
    )
    
    # Create test settings with provider injection
    return create_test_settings(
        pipeline_config=test_provider.configs['pipeline'],
        tables_config=test_provider.configs['tables'],
        env_vars=test_provider.configs['env']
    )


@pytest.fixture
def postgres_schema_test_settings():
    """Create test settings specifically for PostgreSQL schema testing (legacy version).
    
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
def mock_config_reader():
    """Mock ConfigReader for testing (replaces SchemaDiscovery)."""
    if not POSTGRES_SCHEMA_AVAILABLE:
        pytest.skip("ConfigReader not available")
    
    config_reader = MagicMock(spec=ConfigReader)
    
    # Mock get_table_config method
    def mock_get_table_config(table_name):
        # Return different configurations based on table name
        if table_name == 'patient':
            return {
                'priority': 'high',
                'incremental_column': 'DateModified',
                'batch_size': 1000,
                'table_importance': 'critical',
                'extraction_strategy': 'incremental'
            }
        elif table_name == 'appointment':
            return {
                'priority': 'high',
                'incremental_column': 'AptDateTime',
                'batch_size': 500,
                'table_importance': 'high',
                'extraction_strategy': 'incremental'
            }
        elif table_name == 'procedurelog':
            return {
                'priority': 'medium',
                'incremental_column': 'ProcDate',
                'batch_size': 2000,
                'table_importance': 'high',
                'extraction_strategy': 'incremental'
            }
        else:
            return {}
    
    config_reader.get_table_config = mock_get_table_config
    config_reader.get_tables_by_importance.return_value = ['patient', 'appointment']
    config_reader.get_tables_by_strategy.return_value = ['patient', 'appointment', 'procedurelog']
    config_reader.get_large_tables.return_value = ['procedurelog']
    config_reader.get_monitored_tables.return_value = ['patient']
    
    return config_reader


@pytest.fixture
def sample_mysql_schemas():
    """Sample MySQL schemas for testing schema conversion."""
    return {
        'patient': {
            'create_statement': '''
                CREATE TABLE patient (
                    `PatNum` INT AUTO_INCREMENT,
                    `LName` VARCHAR(100),
                    `FName` VARCHAR(100),
                    `MiddleI` VARCHAR(10),
                    `Preferred` VARCHAR(100),
                    `PatStatus` TINYINT,
                    `Gender` TINYINT,
                    `Position` TINYINT,
                    `Birthdate` DATE,
                    `SSN` VARCHAR(20),
                    `IsActive` TINYINT,
                    `IsDeleted` TINYINT,
                    PRIMARY KEY (`PatNum`)
                )
            '''
        },
        'appointment': {
            'create_statement': '''
                CREATE TABLE appointment (
                    `AptNum` INT AUTO_INCREMENT PRIMARY KEY,
                    `PatNum` INT,
                    `AptDateTime` DATETIME,
                    `AptStatus` TINYINT,
                    `DateTStamp` DATETIME,
                    `Notes` TEXT,
                    `IsConfirmed` TINYINT,
                    `IsCancelled` TINYINT
                )
            '''
        },
        'procedurelog': {
            'create_statement': '''
                CREATE TABLE `procedurelog` (
                    ProcNum INT AUTO_INCREMENT PRIMARY KEY,
                    PatNum INT,
                    AptNum INT,
                    ProcStatus TINYINT DEFAULT 0,
                    ProcFee DECIMAL(10,2) DEFAULT 0.00,
                    ProcFeeCur DECIMAL(10,2) DEFAULT 0.00,
                    ProcDate DATE,
                    CodeNum INT DEFAULT 0,
                    ProcNote TEXT,
                    DateTStamp DATETIME,
                    PRIMARY KEY (ProcNum)
                )
            '''
        },
        'boolean_test': {
            'create_statement': '''
                CREATE TABLE boolean_test (
                    `ID` INT AUTO_INCREMENT PRIMARY KEY,
                    `Name` VARCHAR(100),
                    `IsActive` TINYINT,
                    `IsDeleted` TINYINT,
                    `Status` TINYINT,
                    `Priority` TINYINT
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
        'procedurelog': {
            'create_statement': '''
                CREATE TABLE raw.procedurelog (
                    "ProcNum" integer PRIMARY KEY,
                    "PatNum" integer,
                    "AptNum" integer,
                    "ProcStatus" boolean DEFAULT false,
                    "ProcFee" numeric(10,2) DEFAULT 0.00,
                    "ProcFeeCur" numeric(10,2) DEFAULT 0.00,
                    "ProcDate" date,
                    "CodeNum" integer DEFAULT 0,
                    "ProcNote" text,
                    "DateTStamp" timestamp without time zone
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
def real_postgres_schema_instance_with_settings(test_postgres_schema_settings):
    """Real PostgresSchema instance for integration testing using Settings injection.
    
    This fixture uses the test connection methods as specified in the connection
    environment separation documentation to ensure we're connecting to test databases.
    """
    if not POSTGRES_SCHEMA_AVAILABLE or not NEW_CONFIG_AVAILABLE:
        pytest.skip("PostgreSQL schema components or new configuration system not available")
    
    # Mock ConnectionFactory to avoid real connections
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_connection_factory:
        mock_connection_factory.get_replication_connection.return_value = MagicMock()
        mock_connection_factory.get_analytics_raw_connection.return_value = MagicMock()
        
        # Create PostgresSchema instance with Settings injection
        if PostgresSchemaClass is not None and PostgresSchema is not None:
            return PostgresSchemaClass(
                settings=test_postgres_schema_settings,
                postgres_schema=PostgresSchema.RAW
            )
        else:
            pytest.skip("PostgresSchemaClass not available")


@pytest.fixture
def real_postgres_schema_instance(postgres_schema_test_settings):
    """Real PostgresSchema instance for integration testing using test environment (legacy version).
    
    This fixture uses the test connection methods as specified in the connection
    environment separation documentation to ensure we're connecting to test databases.
    """
    if not POSTGRES_SCHEMA_AVAILABLE or not NEW_CONFIG_AVAILABLE:
        pytest.skip("PostgreSQL schema components or new configuration system not available")
    
    # Use unified connection methods with test settings
    test_settings = create_test_settings()
    if ConnectionFactory is not None:
        mysql_engine = ConnectionFactory.get_replication_connection(test_settings)
        postgres_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
    else:
        pytest.skip("ConnectionFactory not available")
    
    # Extract database names from engine URLs
    mysql_db = mysql_engine.url.database
    postgres_db = postgres_engine.url.database
    
    if PostgresSchemaClass is not None and PostgresSchema is not None:
        return PostgresSchemaClass(
            settings=test_settings,
            postgres_schema=PostgresSchema.RAW
        )
    else:
        pytest.skip("PostgresSchemaClass not available")


@pytest.fixture
def real_config_reader_instance(test_postgres_schema_settings):
    """Real ConfigReader instance for integration testing using Settings injection.
    
    This fixture uses the test configuration as specified in the connection
    environment separation documentation to ensure we're using test configuration.
    """
    if not POSTGRES_SCHEMA_AVAILABLE or not NEW_CONFIG_AVAILABLE:
        pytest.skip("ConfigReader or new configuration system not available")
    
    # Create ConfigReader instance with test configuration
    if ConfigReader is not None:
        return ConfigReader()
    else:
        pytest.skip("ConfigReader not available")


@pytest.fixture
def postgres_schema_test_tables():
    """List of test tables for PostgreSQL schema testing."""
    return ['patient', 'appointment', 'procedurelog', 'boolean_test']


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


@pytest.fixture
def config_reader_with_settings(test_postgres_schema_settings):
    """ConfigReader with Settings injection for testing."""
    if not POSTGRES_SCHEMA_AVAILABLE:
        pytest.skip("ConfigReader not available")
    
    # Mock the ConfigReader to avoid real file operations
    with patch('etl_pipeline.config.config_reader.ConfigReader') as mock_config_reader_class:
        mock_config_reader = MagicMock()
        mock_config_reader_class.return_value = mock_config_reader
        
        # Configure mock ConfigReader with test data
        mock_config_reader.get_table_config.return_value = {
            'priority': 'high',
            'incremental_column': 'DateModified',
            'batch_size': 1000,
            'table_importance': 'critical'
        }
        mock_config_reader.get_tables_by_importance.return_value = ['patient', 'appointment']
        mock_config_reader.get_tables_by_strategy.return_value = ['patient', 'appointment', 'procedurelog']
        mock_config_reader.get_large_tables.return_value = ['procedurelog']
        mock_config_reader.get_monitored_tables.return_value = ['patient']
        
        return mock_config_reader


@pytest.fixture
def connection_factory_with_schema_settings(test_postgres_schema_settings):
    """ConnectionFactory with Settings injection for PostgreSQL schema testing."""
    # Mock the ConnectionFactory methods to return mock engines
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
        mock_factory.get_replication_connection.return_value = MagicMock()
        mock_factory.get_analytics_raw_connection.return_value = MagicMock()
        mock_factory.get_analytics_staging_connection.return_value = MagicMock()
        mock_factory.get_analytics_intermediate_connection.return_value = MagicMock()
        mock_factory.get_analytics_marts_connection.return_value = MagicMock()
        
        yield mock_factory


@pytest.fixture
def schema_configs_with_settings(test_postgres_schema_settings):
    """Test PostgreSQL schema configurations using Settings injection."""
    # Test schema configuration from settings
    schema_config = test_postgres_schema_settings.pipeline_config.get('postgres_schema', {})
    
    return {
        'default_schema': schema_config.get('default_schema', 'raw'),
        'type_conversion': schema_config.get('type_conversion', {
            'TINYINT': 'boolean',
            'INT': 'integer',
            'VARCHAR': 'character varying',
            'DATE': 'date',
            'DATETIME': 'timestamp without time zone',
            'DECIMAL': 'numeric',
            'TEXT': 'text'
        })
    } 