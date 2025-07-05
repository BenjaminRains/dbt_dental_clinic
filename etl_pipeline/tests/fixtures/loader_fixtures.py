"""
Loader fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Data loaders (PostgresLoader, etc.)
- Sample table data
- Schema definitions
- Loading utilities
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, Mock, patch
from typing import Dict, List, Any
from datetime import datetime, timedelta


@pytest.fixture
def postgres_loader(mock_replication_engine, mock_analytics_engine):
    """PostgresLoader instance with mocked engines."""
    try:
        from etl_pipeline.loaders.postgres_loader import PostgresLoader
        
        # Mock the PostgresSchema to prevent inspection of mock engines
        with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema') as mock_schema_class:
            mock_schema_adapter = MagicMock()
            mock_schema_class.return_value = mock_schema_adapter
            
            # Mock get_settings to return a mock settings object
            with patch('etl_pipeline.loaders.postgres_loader.get_settings') as mock_get_settings:
                mock_settings = MagicMock()
                mock_settings.get_database_config.side_effect = lambda db_type, schema=None: {
                    ('analytics', 'raw'): {'schema': 'raw'},
                    ('replication', None): {'schema': 'raw'}
                }.get((db_type, schema), {})
                mock_get_settings.return_value = mock_settings
                
                loader = PostgresLoader(
                    replication_engine=mock_replication_engine,
                    analytics_engine=mock_analytics_engine
                )
                return loader
    except ImportError:
        # Fallback mock loader
        loader = MagicMock()
        loader.replication_engine = mock_replication_engine
        loader.analytics_engine = mock_analytics_engine
        loader.schema_adapter = MagicMock()
        return loader


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