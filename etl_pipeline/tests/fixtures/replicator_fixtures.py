"""
Replicator fixtures for ETL pipeline tests.

This module contains fixtures related to:
- MySQL replication components with Settings injection
- ConfigReader for static configuration management
- Replication utilities with unified interface
- Target engine mocks with connection architecture patterns
- Replication configuration using enums and Settings
"""

import pytest
import pandas as pd
import logging
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta
from sqlalchemy import text

# Import connection architecture components
from etl_pipeline.config import DatabaseType, PostgresSchema, Settings
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator

logger = logging.getLogger(__name__)


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
def mock_source_engine():
    """Mock source database engine following connection architecture patterns."""
    engine = MagicMock()
    engine.name = 'mysql'
    # Create a mock URL object following connection architecture
    mock_url = MagicMock()
    mock_url.database = 'test_opendental'
    mock_url.host = 'localhost'
    mock_url.port = 3306
    engine.url = mock_url
    return engine


@pytest.fixture
def mock_target_engine():
    """Mock target engine for replication testing following connection architecture patterns."""
    engine = MagicMock()
    engine.name = 'mysql'
    # Create a mock URL object following connection architecture
    mock_url = MagicMock()
    mock_url.database = 'test_opendental_replication'
    mock_url.host = 'localhost'
    mock_url.port = 3306
    engine.url = mock_url
    return engine


@pytest.fixture
def mock_config_reader():
    """Mock ConfigReader for testing following connection architecture patterns.
    
    This fixture provides a mock ConfigReader that follows the connection architecture:
    - Uses static configuration instead of dynamic schema discovery
    - Provides the same interface as SchemaDiscovery for compatibility
    - Uses provider pattern for dependency injection
    - Supports unified interface for configuration access
    """
    config_reader = MagicMock()
    
    # Mock table configurations following connection architecture patterns
    table_configs = {
        'patient': {
            'primary_key': 'PatNum',
            'incremental_column': 'DateTStamp',
            'extraction_strategy': 'incremental',
            'batch_size': 1000,
            'estimated_size_mb': 50.0,
            'estimated_rows': 10000,
            'monitoring': {
                'alert_on_failure': True,
                'alert_on_slow_extraction': True
            },
            'is_modeled': True,
            'dependencies': []
        },
        'appointment': {
            'primary_key': 'AptNum',
            'incremental_column': 'AptDateTime',
            'extraction_strategy': 'incremental',
            'batch_size': 500,
            'estimated_size_mb': 25.0,
            'estimated_rows': 5000,
            'monitoring': {
                'alert_on_failure': True,
                'alert_on_slow_extraction': False
            },
            'is_modeled': True,
            'dependencies': ['patient']
        },
        'procedurelog': {
            'primary_key': 'ProcNum',
            'incremental_column': 'ProcDate',
            'extraction_strategy': 'incremental',
            'batch_size': 2000,
            'estimated_size_mb': 100.0,
            'estimated_rows': 20000,
            'monitoring': {
                'alert_on_failure': True,
                'alert_on_slow_extraction': True
            },
            'is_modeled': True,
            'dependencies': ['patient', 'appointment']
        }
    }
    
    def mock_get_table_config(table_name):
        """Mock get_table_config method following ConfigReader interface."""
        return table_configs.get(table_name, {})
    
    def mock_get_tables_by_strategy(strategy):
        """Mock get_tables_by_strategy method following ConfigReader interface."""
        tables = []
        for table_name, config in table_configs.items():
            if config.get('extraction_strategy') == strategy:
                tables.append(table_name)
        return tables
    
    def mock_get_large_tables(size_threshold_mb=100.0):
        """Mock get_large_tables method following ConfigReader interface."""
        tables = []
        for table_name, config in table_configs.items():
            if config.get('estimated_size_mb', 0) > size_threshold_mb:
                tables.append(table_name)
        return tables
    
    def mock_get_monitored_tables():
        """Mock get_monitored_tables method following ConfigReader interface."""
        tables = []
        for table_name, config in table_configs.items():
            if config.get('monitoring', {}).get('alert_on_failure', False):
                tables.append(table_name)
        return tables
    
    def mock_get_table_dependencies(table_name):
        """Mock get_table_dependencies method following ConfigReader interface."""
        config = table_configs.get(table_name, {})
        return config.get('dependencies', [])
    
    def mock_get_configuration_summary():
        """Mock get_configuration_summary method following ConfigReader interface."""
        return {
            'total_tables': len(table_configs),
            'extraction_strategies': {
                'incremental': 3
            },
            'size_ranges': {
                'small': 1,
                'medium': 1,
                'large': 1
            },
            'monitored_tables': 3,
            'modeled_tables': 3,
            'last_loaded': datetime.now().isoformat()
        }
    
    def mock_validate_configuration():
        """Mock validate_configuration method following ConfigReader interface."""
        return {
            'missing_batch_size': [],
            'missing_extraction_strategy': [],
            'missing_importance': [],
            'invalid_batch_size': [],
            'large_tables_without_monitoring': []
        }
    
    # Assign mock methods to config_reader
    config_reader.get_table_config = mock_get_table_config
    config_reader.get_tables_by_strategy = mock_get_tables_by_strategy
    config_reader.get_large_tables = mock_get_large_tables
    config_reader.get_monitored_tables = mock_get_monitored_tables
    config_reader.get_table_dependencies = mock_get_table_dependencies
    config_reader.get_configuration_summary = mock_get_configuration_summary
    config_reader.validate_configuration = mock_validate_configuration
    config_reader.get_configuration_path.return_value = "etl_pipeline/config/tables.yml"
    config_reader.get_last_loaded.return_value = datetime.now()
    config_reader.reload_configuration.return_value = True
    
    return config_reader


@pytest.fixture
def test_replication_env_vars():
    """Test replication environment variables following connection architecture naming convention.
    
    This fixture provides test environment variables for replication testing that conform to the connection architecture:
    - Uses TEST_ prefix for test environment variables
    - Follows the environment-specific variable naming convention
    - Matches the .env_test file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        # Environment declaration (required for fail-fast validation)
        'ETL_ENVIRONMENT': 'test',
        
        # OpenDental Source (Test) - following architecture naming
        'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
        
        # MySQL Replication (Test) - following architecture naming
        'TEST_MYSQL_REPLICATION_HOST': 'localhost',
        'TEST_MYSQL_REPLICATION_PORT': '3306',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass'
    }


@pytest.fixture
def test_replication_config_provider(test_replication_env_vars):
    """Test replication configuration provider following the provider pattern for dependency injection.
    
    This fixture implements the DictConfigProvider pattern as specified in the connection architecture:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected configuration for clean test isolation
    - Supports dependency injection for easy configuration swapping
    - Follows the provider pattern for configuration loading
    """
    return DictConfigProvider(
        pipeline={'connections': {
            'source': {'pool_size': 5, 'connect_timeout': 30},
            'replication': {'pool_size': 3, 'connect_timeout': 30}
        }},
        tables={'tables': {}},
        env=test_replication_env_vars
    )


@pytest.fixture
def test_replication_settings(test_replication_config_provider):
    """Test replication settings following connection architecture patterns.
    
    This fixture provides test settings for replication testing that conform to the connection architecture:
    - Uses Settings injection for environment-agnostic operation
    - Uses provider pattern for dependency injection
    - Supports unified interface for connection creation
    - Uses enums for type safety
    """
    return Settings(environment='test', provider=test_replication_config_provider)


@pytest.fixture
def replicator_with_settings(mock_source_engine, mock_target_engine, mock_config_reader, test_replication_settings):
    """MySQL replicator instance with Settings injection following connection architecture patterns.
    
    This fixture provides a replicator instance that follows the connection architecture:
    - Uses Settings injection for configuration
    - Uses ConfigReader for static configuration management
    - Uses provider pattern for dependency injection
    - Supports unified interface for connection creation
    - Uses enums for type safety
    """
    # Create a mock replicator to avoid real database connections
    replicator = MagicMock()
    replicator.source_engine = mock_source_engine
    replicator.target_engine = mock_target_engine
    replicator.config_reader = mock_config_reader  # Use ConfigReader instead of schema_discovery
    replicator.settings = test_replication_settings  # Settings injection
    
    # Mock table_configs to avoid real configuration loading
    replicator.table_configs = {
        'patient': {
            'incremental_column': 'DateTStamp',
            'batch_size': 1000,
            'table_importance': 'important'
        },
        'appointment': {
            'incremental_column': 'DateTStamp',
            'batch_size': 500,
            'table_importance': 'important'
        },
        'procedurelog': {
            'incremental_column': 'DateTStamp',
            'batch_size': 2000,
            'table_importance': 'standard'
        },
        'claim': {
            'incremental_column': 'DateTStamp',
            'batch_size': 1500,
            'table_importance': 'important'
        }
    }
    
    # Mock methods to avoid real database operations
    replicator.copy_table = MagicMock(return_value=True)
    replicator.copy_all_tables = MagicMock(return_value={
        'patient': True,
        'appointment': True,
        'procedurelog': True,
        'claim': False
    })
    replicator.copy_tables_by_importance = MagicMock(side_effect=lambda importance: {
        'patient': True,
        'appointment': True,
        'claim': True
    } if importance == 'important' else {
        'procedurelog': True
    } if importance == 'standard' else {})
    replicator.get_copy_method = MagicMock(side_effect=lambda table: {
        'claim': 'small',
        'appointment': 'medium',
        'procedurelog': 'large'
    }.get(table, 'medium'))
    
    return replicator


@pytest.fixture
def replicator(mock_source_engine, mock_target_engine, mock_config_reader):
    """MySQL replicator instance with mocked components (legacy support).
    
    This fixture provides backward compatibility for existing tests.
    For new tests, use replicator_with_settings instead.
    """
    try:
        from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
        replicator = SimpleMySQLReplicator()
        # Override engines with mocks for testing
        replicator.source_engine = mock_source_engine
        replicator.target_engine = mock_target_engine
        return replicator
    except ImportError:
        # Fallback mock replicator
        replicator = MagicMock()
        replicator.source_engine = mock_source_engine
        replicator.target_engine = mock_target_engine
        replicator.config_reader = mock_config_reader  # Use ConfigReader instead of schema_discovery
        return replicator


@pytest.fixture
def mock_replication_config():
    """Mock replication configuration for testing following connection architecture patterns."""
    return {
        'batch_size': 1000,
        'parallel_jobs': 2,
        'max_retries': 3,
        'retry_delay': 5,
        'timeout': 300,
        'validate_schema': True,
        'create_tables': True,
        'drop_existing': False,
        # Connection architecture specific settings
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.REPLICATION
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
    """Mock replication error for testing error handling using new exception classes."""
    # Use the actual DataExtractionError for consistency with the new exception system
    from etl_pipeline.exceptions import DataExtractionError
    
    def create_mock_replication_error(message="Replication failed", table_name=None, details=None):
        return DataExtractionError(
            message=message,
            table_name=table_name,
            extraction_strategy="incremental",
            batch_size=1000,
            details=details or {}
        )
    
    return create_mock_replication_error


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


@pytest.fixture
def database_types():
    """Database type enums for testing."""
    return DatabaseType


@pytest.fixture
def postgres_schemas():
    """PostgreSQL schema enums for testing."""
    return PostgresSchema


@pytest.fixture
def replication_test_cases():
    """Test cases for replication scenarios following connection architecture patterns."""
    return [
        {
            'name': 'patient_table_replication',
            'source_table': 'patient',
            'target_table': 'patient',
            'source_db_type': DatabaseType.SOURCE,
            'target_db_type': DatabaseType.REPLICATION,
            'expected_rows': 1000,
            'validation_required': True
        },
        {
            'name': 'appointment_table_replication',
            'source_table': 'appointment',
            'target_table': 'appointment',
            'source_db_type': DatabaseType.SOURCE,
            'target_db_type': DatabaseType.REPLICATION,
            'expected_rows': 500,
            'validation_required': True
        },
        {
            'name': 'procedurelog_table_replication',
            'source_table': 'procedurelog',
            'target_table': 'procedurelog',
            'source_db_type': DatabaseType.SOURCE,
            'target_db_type': DatabaseType.REPLICATION,
            'expected_rows': 2000,
            'validation_required': False
        }
    ]


@pytest.fixture
def replicator_with_real_settings(test_settings_with_file_provider):
    """
    Create SimpleMySQLReplicator with real settings for integration testing.
    This fixture follows the connection architecture by:
    - Using Settings injection for environment-agnostic connections
    - Using FileConfigProvider for real configuration loading
    - Creating real database connections using ConnectionFactory
    - Supporting real database replication testing
    - Using test environment configuration
    """
    try:
        logger.info("Creating SimpleMySQLReplicator with test settings...")
        
        # Create replicator with real settings loaded from .env_test
        replicator = SimpleMySQLReplicator(settings=test_settings_with_file_provider)
        
        # Validate that replicator has proper configuration
        assert replicator.settings is not None
        assert replicator.table_configs is not None
        assert len(replicator.table_configs) > 0
        
        logger.info(f"Replicator created with {len(replicator.table_configs)} table configurations")
        logger.info(f"Source engine: {replicator.source_engine}")
        logger.info(f"Target engine: {replicator.target_engine}")
        
        # Validate that we can connect to test databases
        logger.info("Testing source database connection...")
        with replicator.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            if not row or row[0] != 1:
                pytest.skip("Test source database connection failed")
        
        logger.info("Testing target database connection...")
        with replicator.target_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            if not row or row[0] != 1:
                pytest.skip("Test target database connection failed")
        
        logger.info("Successfully created SimpleMySQLReplicator with working database connections")
        return replicator
        
    except Exception as e:
        logger.error(f"Failed to create replicator: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        pytest.skip(f"Test databases not available: {str(e)}") 