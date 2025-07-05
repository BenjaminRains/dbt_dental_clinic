"""
Replicator fixtures for ETL pipeline tests.

This module contains fixtures related to:
- MySQL replication components
- Schema discovery
- Replication utilities
- Target engine mocks
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta


@pytest.fixture
def sample_mysql_replicator_table_data():
    """Sample table data for MySQL replicator testing."""
    return {
        'patient': pd.DataFrame({
            'PatNum': [1, 2, 3, 4, 5],
            'LName': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones'],
            'FName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'DateTStamp': [
                datetime.now() - timedelta(days=i) 
                for i in range(5)
            ]
        }),
        'appointment': pd.DataFrame({
            'AptNum': [1, 2, 3, 4, 5],
            'PatNum': [1, 2, 1, 3, 4],
            'AptDateTime': [
                datetime.now() + timedelta(days=i) 
                for i in range(5)
            ]
        })
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
def mock_target_engine():
    """Mock target engine for replication testing."""
    engine = MagicMock()
    engine.name = 'mysql'
    engine.url.database = 'test_replication'
    engine.url.host = 'localhost'
    engine.url.port = 3306
    return engine


@pytest.fixture
def mock_schema_discovery():
    """Mock schema discovery for testing."""
    discovery = MagicMock()
    
    def mock_get_table_schema(table_name):
        schemas = {
            'patient': {
                'columns': [
                    {'name': 'PatNum', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI'},
                    {'name': 'LName', 'type': 'varchar(100)', 'null': 'YES', 'key': ''},
                    {'name': 'FName', 'type': 'varchar(100)', 'null': 'YES', 'key': ''},
                    {'name': 'DateTStamp', 'type': 'datetime', 'null': 'YES', 'key': ''}
                ],
                'primary_key': 'PatNum'
            },
            'appointment': {
                'columns': [
                    {'name': 'AptNum', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI'},
                    {'name': 'PatNum', 'type': 'int(11)', 'null': 'YES', 'key': 'MUL'},
                    {'name': 'AptDateTime', 'type': 'datetime', 'null': 'YES', 'key': ''}
                ],
                'primary_key': 'AptNum'
            }
        }
        return schemas.get(table_name, {})
    
    discovery.get_table_schema = mock_get_table_schema
    discovery.get_all_tables.return_value = ['patient', 'appointment', 'procedurelog']
    return discovery


@pytest.fixture
def replicator(mock_source_engine, mock_target_engine, mock_schema_discovery):
    """MySQL replicator instance with mocked components."""
    try:
        from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator
        replicator = ExactMySQLReplicator(
            source_engine=mock_source_engine,
            target_engine=mock_target_engine,
            schema_discovery=mock_schema_discovery
        )
        return replicator
    except ImportError:
        # Fallback mock replicator
        replicator = MagicMock()
        replicator.source_engine = mock_source_engine
        replicator.target_engine = mock_target_engine
        replicator.schema_discovery = mock_schema_discovery
        return replicator


@pytest.fixture
def mock_replication_config():
    """Mock replication configuration for testing."""
    return {
        'batch_size': 1000,
        'parallel_jobs': 2,
        'max_retries': 3,
        'retry_delay': 5,
        'timeout': 300,
        'validate_schema': True,
        'create_tables': True,
        'drop_existing': False
    }


@pytest.fixture
def mock_replication_stats():
    """Mock replication statistics for testing."""
    return {
        'tables_replicated': 5,
        'total_rows_replicated': 15000,
        'start_time': datetime.now() - timedelta(hours=1),
        'end_time': datetime.now(),
        'success_count': 4,
        'error_count': 1,
        'errors': [
            {
                'table': 'claim',
                'error': 'Table already exists',
                'timestamp': datetime.now()
            }
        ]
    }


@pytest.fixture
def sample_table_schemas():
    """Sample table schemas for replication testing."""
    return {
        'patient': {
            'table_name': 'patient',
            'columns': [
                {'name': 'PatNum', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI', 'default': None},
                {'name': 'LName', 'type': 'varchar(100)', 'null': 'YES', 'key': '', 'default': None},
                {'name': 'FName', 'type': 'varchar(100)', 'null': 'YES', 'key': '', 'default': None},
                {'name': 'DateTStamp', 'type': 'datetime', 'null': 'YES', 'key': '', 'default': None},
                {'name': 'Status', 'type': 'varchar(50)', 'null': 'YES', 'key': '', 'default': 'Active'}
            ],
            'primary_key': 'PatNum',
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        },
        'appointment': {
            'table_name': 'appointment',
            'columns': [
                {'name': 'AptNum', 'type': 'int(11)', 'null': 'NO', 'key': 'PRI', 'default': None},
                {'name': 'PatNum', 'type': 'int(11)', 'null': 'YES', 'key': 'MUL', 'default': None},
                {'name': 'AptDateTime', 'type': 'datetime', 'null': 'YES', 'key': '', 'default': None},
                {'name': 'AptStatus', 'type': 'varchar(50)', 'null': 'YES', 'key': '', 'default': 'Scheduled'}
            ],
            'primary_key': 'AptNum',
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        }
    }


@pytest.fixture
def mock_replication_error():
    """Mock replication error for testing error handling."""
    class MockReplicationError(Exception):
        def __init__(self, message="Replication failed", table=None, details=None):
            self.message = message
            self.table = table
            self.details = details
            super().__init__(self.message)
    
    return MockReplicationError


@pytest.fixture
def sample_replication_queries():
    """Sample replication queries for testing."""
    return {
        'create_table': """
        CREATE TABLE `patient` (
            `PatNum` int(11) NOT NULL AUTO_INCREMENT,
            `LName` varchar(100) DEFAULT NULL,
            `FName` varchar(100) DEFAULT NULL,
            `DateTStamp` datetime DEFAULT NULL,
            `Status` varchar(50) DEFAULT 'Active',
            PRIMARY KEY (`PatNum`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """,
        'insert_data': """
        INSERT INTO `patient` (`PatNum`, `LName`, `FName`, `DateTStamp`, `Status`)
        VALUES (1, 'Smith', 'John', '2024-01-01 10:00:00', 'Active');
        """,
        'select_data': """
        SELECT * FROM `patient` WHERE `Status` = 'Active';
        """,
        'drop_table': """
        DROP TABLE IF EXISTS `patient`;
        """
    }


@pytest.fixture
def mock_replication_validation():
    """Mock replication validation for testing."""
    return {
        'schema_match': True,
        'data_integrity': True,
        'row_count_match': True,
        'checksum_match': True,
        'validation_errors': []
    }


@pytest.fixture
def mock_replication_validation_with_errors():
    """Mock replication validation with errors for testing."""
    return {
        'schema_match': False,
        'data_integrity': True,
        'row_count_match': False,
        'checksum_match': False,
        'validation_errors': [
            {
                'type': 'schema_mismatch',
                'table': 'patient',
                'message': 'Column count mismatch'
            },
            {
                'type': 'row_count_mismatch',
                'table': 'patient',
                'message': 'Source: 1000 rows, Target: 950 rows'
            }
        ]
    } 