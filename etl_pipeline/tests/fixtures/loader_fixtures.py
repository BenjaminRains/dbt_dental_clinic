"""
Loader fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Data loaders (PostgresLoader, etc.)
- Sample table data
- Schema definitions
- Loading utilities

Follows the connection architecture patterns:
- Uses provider pattern for dependency injection
- Uses Settings injection for environment-agnostic connections
- Uses enums for type safety
- Uses unified interface with ConnectionFactory
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, Mock, patch
from typing import Dict, List, Any
from datetime import datetime, timedelta

from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.core import ConnectionFactory


@pytest.fixture
def test_settings():
    """Test settings using provider pattern for dependency injection."""
    # Create test provider with injected configuration
    test_provider = DictConfigProvider(
        pipeline={
            'connections': {
                'source': {'pool_size': 5, 'connect_timeout': 10},
                'replication': {'pool_size': 10, 'max_overflow': 20},
                'analytics': {'application_name': 'etl_pipeline_test'}
            }
        },
        tables={
            'tables': {
                'patient': {
                    'incremental_column': 'DateModified',
                    'batch_size': 1000,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'critical'
                },
                'appointment': {
                    'incremental_column': 'AptDateTime',
                    'batch_size': 500,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'high'
                },
                'procedurelog': {
                    'incremental_column': 'ProcDate',
                    'batch_size': 2000,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'high'
                }
            }
        },
        env={
            # Test environment variables (TEST_ prefixed)
            'TEST_OPENDENTAL_SOURCE_HOST': 'test-source-host',
            'TEST_OPENDENTAL_SOURCE_PORT': '3306',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
            'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
            
            'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
            'TEST_MYSQL_REPLICATION_PORT': '3306',
            'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
            'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
            'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
            
            'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
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
def postgres_loader(test_settings):
    """PostgresLoader instance using Settings injection and provider pattern.
    
    This fixture provides a fully mocked PostgresLoader for unit tests.
    For unit tests, we mock all dependencies to test isolated behavior.
    For integration tests, use real instances with test database connections.
    """
    # Create a fully mocked PostgresLoader for unit tests
    loader = MagicMock()
    
    # Mock all the attributes that PostgresLoader would have
    loader.settings = test_settings
    loader.replication_engine = MagicMock()
    loader.analytics_engine = MagicMock()
    loader.schema_adapter = MagicMock()
    loader.table_configs = {
        'patient': {'incremental_columns': ['DateModified'], 'batch_size': 1000},
        'appointment': {'incremental_columns': ['DateTStamp'], 'batch_size': 500}
    }
    
    # Mock the methods that tests will call
    loader.load_table.return_value = True
    loader.load_table_chunked.return_value = True
    loader.verify_load.return_value = True
    loader.get_table_config.return_value = {'incremental_columns': ['DateModified']}
    
    return loader


@pytest.fixture
def mock_replication_engine(test_settings):
    """Mock replication engine using Settings injection."""
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    return engine


@pytest.fixture
def mock_analytics_engine(test_settings):
    """Mock analytics engine using Settings injection."""
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    return engine


@pytest.fixture
def mock_source_engine(test_settings):
    """Mock source engine using Settings injection."""
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    return engine


@pytest.fixture
def sample_table_data():
    """Sample table data for testing."""
    return {
        'patient': pd.DataFrame({
            'PatNum': [1, 2, 3, 4, 5],
            'LName': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones'],
            'FName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'DateTStamp': [
                datetime.now() - timedelta(days=i) 
                for i in range(5)
            ],
            'Status': ['Active', 'Active', 'Inactive', 'Active', 'Active']
        }),
        'appointment': pd.DataFrame({
            'AptNum': [1, 2, 3, 4, 5],
            'PatNum': [1, 2, 1, 3, 4],
            'AptDateTime': [
                datetime.now() + timedelta(days=i) 
                for i in range(5)
            ],
            'AptStatus': ['Scheduled', 'Confirmed', 'Completed', 'Cancelled', 'Scheduled']
        }),
        'procedurelog': pd.DataFrame({
            'ProcNum': [1, 2, 3, 4, 5],
            'PatNum': [1, 2, 1, 3, 4],
            'ProcDate': [
                datetime.now() - timedelta(days=i*2) 
                for i in range(5)
            ],
            'ProcFee': [150.00, 200.00, 75.50, 300.00, 125.00],
            'ProcStatus': ['Complete', 'Complete', 'Complete', 'Complete', 'Complete']
        })
    }


@pytest.fixture
def sample_mysql_schema():
    """Sample MySQL schema for testing."""
    return {
        'patient': {
            'columns': [
                {'name': 'PatNum', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI'},
                {'name': 'LName', 'type': 'varchar(100)', 'null': 'YES', 'key': ''},
                {'name': 'FName', 'type': 'varchar(100)', 'null': 'YES', 'key': ''},
                {'name': 'DateTStamp', 'type': 'datetime', 'null': 'YES', 'key': ''},
                {'name': 'Status', 'type': 'varchar(50)', 'null': 'YES', 'key': ''}
            ],
            'primary_key': 'PatNum',
            'engine': 'InnoDB',
            'charset': 'utf8mb4'
        },
        'appointment': {
            'columns': [
                {'name': 'AptNum', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI'},
                {'name': 'PatNum', 'type': 'int(11)', 'null': 'YES', 'key': 'MUL'},
                {'name': 'AptDateTime', 'type': 'datetime', 'null': 'YES', 'key': ''},
                {'name': 'AptStatus', 'type': 'varchar(50)', 'null': 'YES', 'key': ''}
            ],
            'primary_key': 'AptNum',
            'engine': 'InnoDB',
            'charset': 'utf8mb4'
        },
        'procedurelog': {
            'columns': [
                {'name': 'ProcNum', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI'},
                {'name': 'PatNum', 'type': 'int(11)', 'null': 'YES', 'key': 'MUL'},
                {'name': 'ProcDate', 'type': 'date', 'null': 'YES', 'key': ''},
                {'name': 'ProcFee', 'type': 'decimal(10,2)', 'null': 'YES', 'key': ''},
                {'name': 'ProcStatus', 'type': 'varchar(50)', 'null': 'YES', 'key': ''}
            ],
            'primary_key': 'ProcNum',
            'engine': 'InnoDB',
            'charset': 'utf8mb4'
        }
    }


@pytest.fixture
def sample_postgres_schema():
    """Sample PostgreSQL schema for testing."""
    return {
        'patient': {
            'columns': [
                {'name': 'patnum', 'type': 'integer', 'nullable': False, 'primary_key': True},
                {'name': 'lname', 'type': 'character varying(100)', 'nullable': True, 'primary_key': False},
                {'name': 'fname', 'type': 'character varying(100)', 'nullable': True, 'primary_key': False},
                {'name': 'datestamp', 'type': 'timestamp without time zone', 'nullable': True, 'primary_key': False},
                {'name': 'status', 'type': 'character varying(50)', 'nullable': True, 'primary_key': False}
            ],
            'primary_key': 'patnum',
            'table_name': 'patient'
        },
        'appointment': {
            'columns': [
                {'name': 'aptnum', 'type': 'integer', 'nullable': False, 'primary_key': True},
                {'name': 'patnum', 'type': 'integer', 'nullable': True, 'primary_key': False},
                {'name': 'aptdatetime', 'type': 'timestamp without time zone', 'nullable': True, 'primary_key': False},
                {'name': 'aptstatus', 'type': 'character varying(50)', 'nullable': True, 'primary_key': False}
            ],
            'primary_key': 'aptnum',
            'table_name': 'appointment'
        }
    }


@pytest.fixture
def mock_loader_config():
    """Mock loader configuration for testing."""
    return {
        'batch_size': 1000,
        'parallel_jobs': 2,
        'max_retries': 3,
        'retry_delay': 5,
        'timeout': 300,
        'validation_enabled': True,
        'logging_enabled': True
    }


@pytest.fixture
def mock_loader_stats():
    """Mock loader statistics for testing."""
    return {
        'tables_processed': 5,
        'total_rows_loaded': 15000,
        'start_time': datetime.now() - timedelta(hours=1),
        'end_time': datetime.now(),
        'success_count': 4,
        'error_count': 1,
        'errors': [
            {
                'table': 'claim',
                'error': 'Connection timeout',
                'timestamp': datetime.now()
            }
        ]
    }


@pytest.fixture
def sample_create_statement():
    """Sample CREATE TABLE statement for testing."""
    return """
    CREATE TABLE `patient` (
        `PatNum` int(11) NOT NULL AUTO_INCREMENT,
        `LName` varchar(100) DEFAULT NULL,
        `FName` varchar(100) DEFAULT NULL,
        `DateTStamp` datetime DEFAULT NULL,
        `Status` varchar(50) DEFAULT NULL,
        PRIMARY KEY (`PatNum`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """


@pytest.fixture
def sample_drop_statement():
    """Sample DROP TABLE statement for testing."""
    return "DROP TABLE IF EXISTS `patient`;"


@pytest.fixture
def sample_insert_statement():
    """Sample INSERT statement for testing."""
    return """
    INSERT INTO `patient` (`PatNum`, `LName`, `FName`, `DateTStamp`, `Status`)
    VALUES (1, 'Smith', 'John', '2024-01-01 10:00:00', 'Active');
    """


@pytest.fixture
def sample_select_statement():
    """Sample SELECT statement for testing."""
    return "SELECT * FROM `patient` WHERE `Status` = 'Active';"


@pytest.fixture
def mock_loader_error():
    """Mock loader error for testing error handling."""
    class MockLoaderError(Exception):
        def __init__(self, message="Loader error", table=None, details=None):
            self.message = message
            self.table = table
            self.details = details
            super().__init__(self.message)
    
    return MockLoaderError


@pytest.fixture
def mock_validation_error():
    """Mock validation error for testing validation handling."""
    class MockValidationError(Exception):
        def __init__(self, message="Validation failed", field=None, value=None):
            self.message = message
            self.field = field
            self.value = value
            super().__init__(self.message)
    
    return MockValidationError


@pytest.fixture
def database_configs_with_enums(test_settings):
    """Test database configurations using enums for type safety."""
    # Test all database types using enums
    source_config = test_settings.get_database_config(DatabaseType.SOURCE)
    repl_config = test_settings.get_database_config(DatabaseType.REPLICATION)
    analytics_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
    
    return {
        'source': source_config,
        'replication': repl_config,
        'analytics': analytics_config
    }


@pytest.fixture
def schema_configs_with_enums(test_settings):
    """Test schema-specific configurations using enums."""
    raw_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
    staging_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.STAGING)
    intermediate_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.INTERMEDIATE)
    marts_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.MARTS)
    
    return {
        'raw': raw_config,
        'staging': staging_config,
        'intermediate': intermediate_config,
        'marts': marts_config
    }


@pytest.fixture
def connection_factory_with_settings(test_settings):
    """ConnectionFactory with Settings injection for testing."""
    # Mock the ConnectionFactory methods to return mock engines
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
        mock_factory.get_source_connection.return_value = MagicMock()
        mock_factory.get_replication_connection.return_value = MagicMock()
        mock_factory.get_analytics_raw_connection.return_value = MagicMock()
        mock_factory.get_analytics_staging_connection.return_value = MagicMock()
        mock_factory.get_analytics_intermediate_connection.return_value = MagicMock()
        mock_factory.get_analytics_marts_connection.return_value = MagicMock()
        
        yield mock_factory 