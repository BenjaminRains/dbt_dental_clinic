"""
Shared test fixtures for ETL pipeline tests.

⚠️  DEPRECATION NOTICE ⚠️
==========================

This module is DEPRECATED and will be removed in a future version.
The fixtures have been refactored into modular, maintainable components.

NEW STRUCTURE:
- Use `tests/fixtures/` directory for organized, modular fixtures
- Import specific fixtures from their respective modules:
  * `from tests.fixtures.config_fixtures import test_pipeline_config`
  * `from tests.fixtures.env_fixtures import test_env_vars`
  * `from tests.fixtures.connection_fixtures import mock_source_engine`
  * etc.

MIGRATION GUIDE:
1. Replace direct fixture usage with imports from specific modules
2. Update test files to import from `tests.fixtures.*` modules
3. Remove dependency on this monolithic conftest.py

This module will be maintained for backward compatibility only.
New tests should use the modular fixture structure.

Updated for Configuration System Refactoring:
- Proper test isolation with reset_global_settings fixture
- Support for new enum-based DatabaseType and PostgresSchema
- Environment-aware configuration fixtures
- Factory pattern for test settings creation
"""

import os
import pytest
import pandas as pd
import logging
from unittest.mock import MagicMock, patch, Mock, mock_open
from datetime import datetime, timedelta
from typing import Dict, List, Any
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text

# Import ETL pipeline components for testing
try:
    from etl_pipeline.config.settings import Settings
except ImportError:
    # Fallback for new configuration system
    from etl_pipeline.config import create_settings
    Settings = None

from etl_pipeline.core.connections import ConnectionFactory

from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.config.logging import get_logger

# Import new configuration system components
try:
    from etl_pipeline.config import reset_settings, create_test_settings, DatabaseType, PostgresSchema
    NEW_CONFIG_AVAILABLE = True
except ImportError:
    # Fallback for backward compatibility
    NEW_CONFIG_AVAILABLE = False
    DatabaseType = None
    PostgresSchema = None

# Set up logger for this module using the project's logging configuration
logger = get_logger(__name__)

# =============================================================================
# CONFIGURATION SYSTEM FIXTURES (NEW)
# =============================================================================

@pytest.fixture(autouse=True)
def reset_global_settings():
    """Reset global settings before and after each test for proper isolation."""
    if NEW_CONFIG_AVAILABLE:
        reset_settings()
        yield
        reset_settings()
    else:
        # Fallback for old configuration system
        yield


@pytest.fixture
def test_env_vars():
    """Standard test environment variables."""
    return {
        'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
        'TEST_MYSQL_REPLICATION_HOST': 'localhost',
        'TEST_MYSQL_REPLICATION_PORT': '3306',
        'TEST_MYSQL_REPLICATION_DB': 'test_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_pass',
        'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_pass',
        'ETL_ENVIRONMENT': 'test'
    }


@pytest.fixture
def test_pipeline_config():
    """Standard test pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'test_pipeline',
            'environment': 'test',
            'batch_size': 1000,
            'parallel_jobs': 2
        },
        'connections': {
            'source': {
                'pool_size': 2,
                'connect_timeout': 30
            },
            'replication': {
                'pool_size': 2,
                'connect_timeout': 30
            },
            'analytics': {
                'pool_size': 2,
                'connect_timeout': 30
            }
        }
    }


@pytest.fixture
def test_tables_config():
    """Standard test tables configuration."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_column': 'DateTStamp',
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'batch_size': 100
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_column': 'AptDateTime',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 50
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_column': 'ProcDate',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 200
            }
        }
    }


@pytest.fixture
def test_settings(test_env_vars, test_pipeline_config, test_tables_config):
    """Create isolated test settings using new configuration system."""
    if NEW_CONFIG_AVAILABLE:
        return create_test_settings(
            pipeline_config=test_pipeline_config,
            tables_config=test_tables_config,
            env_vars=test_env_vars
        )
    else:
        # Fallback to old settings pattern
        with patch.dict(os.environ, test_env_vars):
            if Settings is not None:
                return Settings(environment='test')
            else:
                return create_settings(environment='test')


@pytest.fixture
def mock_env_test_settings(test_env_vars):
    """Create test settings with mocked environment variables."""
    if NEW_CONFIG_AVAILABLE:
        with patch.dict(os.environ, test_env_vars):
            from etl_pipeline.config import create_settings
            yield create_settings(environment='test')
    else:
        # Fallback to old settings pattern
        with patch.dict(os.environ, test_env_vars):
            if Settings is not None:
                yield Settings(environment='test')
            else:
                yield create_settings(environment='test')


@pytest.fixture
def production_env_vars():
    """Production environment variables for integration tests."""
    return {
        'OPENDENTAL_SOURCE_HOST': os.getenv('OPENDENTAL_SOURCE_HOST', 'localhost'),
        'OPENDENTAL_SOURCE_PORT': os.getenv('OPENDENTAL_SOURCE_PORT', '3306'),
        'OPENDENTAL_SOURCE_DB': os.getenv('OPENDENTAL_SOURCE_DB', 'opendental'),
        'OPENDENTAL_SOURCE_USER': os.getenv('OPENDENTAL_SOURCE_USER', 'user'),
        'OPENDENTAL_SOURCE_PASSWORD': os.getenv('OPENDENTAL_SOURCE_PASSWORD', 'password'),
        'MYSQL_REPLICATION_HOST': os.getenv('MYSQL_REPLICATION_HOST', 'localhost'),
        'MYSQL_REPLICATION_PORT': os.getenv('MYSQL_REPLICATION_PORT', '3306'),
        'MYSQL_REPLICATION_DB': os.getenv('MYSQL_REPLICATION_DB', 'opendental_replication'),
        'MYSQL_REPLICATION_USER': os.getenv('MYSQL_REPLICATION_USER', 'user'),
        'MYSQL_REPLICATION_PASSWORD': os.getenv('MYSQL_REPLICATION_PASSWORD', 'password'),
        'POSTGRES_ANALYTICS_HOST': os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
        'POSTGRES_ANALYTICS_PORT': os.getenv('POSTGRES_ANALYTICS_PORT', '5432'),
        'POSTGRES_ANALYTICS_DB': os.getenv('POSTGRES_ANALYTICS_DB', 'opendental_analytics'),
        'POSTGRES_ANALYTICS_SCHEMA': os.getenv('POSTGRES_ANALYTICS_SCHEMA', 'raw'),
        'POSTGRES_ANALYTICS_USER': os.getenv('POSTGRES_ANALYTICS_USER', 'user'),
        'POSTGRES_ANALYTICS_PASSWORD': os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'password'),
        'ETL_ENVIRONMENT': 'production'
    }


@pytest.fixture
def production_settings(production_env_vars):
    """Create production settings for integration tests."""
    if NEW_CONFIG_AVAILABLE:
        with patch.dict(os.environ, production_env_vars):
            from etl_pipeline.config import create_settings
            yield create_settings(environment='production')
    else:
        # Fallback to old settings pattern
        with patch.dict(os.environ, production_env_vars):
            yield Settings(environment='production')


# Database type fixtures for easy access
@pytest.fixture
def database_types():
    """Provide DatabaseType enum for tests."""
    if NEW_CONFIG_AVAILABLE:
        return DatabaseType
    else:
        # Fallback: return string constants
        class MockDatabaseType:
            SOURCE = 'source'
            REPLICATION = 'replication'
            ANALYTICS = 'analytics'
        return MockDatabaseType()


@pytest.fixture
def postgres_schemas():
    """Provide PostgresSchema enum for tests."""
    if NEW_CONFIG_AVAILABLE:
        return PostgresSchema
    else:
        # Fallback: return string constants
        class MockPostgresSchema:
            RAW = 'raw'
            STAGING = 'staging'
            INTERMEDIATE = 'intermediate'
            MARTS = 'marts'
        return MockPostgresSchema()


# =============================================================================
# DATABASE MOCKS AND CONNECTIONS (UPDATED)
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
# CONFIGURATION FIXTURES (UPDATED)
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
        settings_instance.get_tables_by_importance.return_value = [
            'patient', 'appointment', 'procedurelog'
        ]
        
        mock.return_value = settings_instance
        yield settings_instance


# =============================================================================
# TRANSFORMER FIXTURES
# =============================================================================

@pytest.fixture
def mock_table_processor_engines():
    """Create mock engines for TableProcessor tests."""
    mock_source = MagicMock(spec=Engine)
    mock_replication = MagicMock(spec=Engine)
    mock_analytics = MagicMock(spec=Engine)
    return mock_source, mock_replication, mock_analytics

@pytest.fixture
def table_processor_standard_config():
    """Standard table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 5000,
        'estimated_size_mb': 25,
        'batch_size': 1000,
        'priority': 'important'
    }

@pytest.fixture
def table_processor_large_config():
    """Large table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 1000001,  # > 1,000,000 to trigger chunked loading
        'estimated_size_mb': 101,   # > 100 to trigger chunked loading
        'batch_size': 5000,
        'priority': 'critical'
    }

@pytest.fixture
def table_processor_medium_large_config():
    """Medium-large table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 1000001,  # > 1,000,000 to trigger chunked loading
        'estimated_size_mb': 101,   # > 100 to trigger chunked loading
        'batch_size': 2500,
        'priority': 'important'
    }


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
# TEST ENVIRONMENT SETUP (UPDATED)
# =============================================================================

@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch, request):
    """Set up test environment variables only for unit tests."""
    # Only set test environment variables for unit tests, not integration tests
    # Integration tests should use real environment variables
    if request.node.get_closest_marker('integration'):
        # Skip setting test environment variables for integration tests
        return
    
    # Set test environment variables for unit tests only
    test_env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'localhost',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'test_opendental',
        'OPENDENTAL_SOURCE_USER': 'test_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
        'MYSQL_REPLICATION_HOST': 'localhost',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'MYSQL_REPLICATION_USER': 'replication_test_user',
        'MYSQL_REPLICATION_PASSWORD': 'test_pass',
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'analytics_test_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'test_pass'
    }
    
    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)


# =============================================================================
# TEST CATEGORIES AND MARKERS (UPDATED)
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
        "markers", "config: Configuration system tests"
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

@pytest.fixture
def mock_target_engine():
    """Mock target database engine for MySQLReplicator tests."""
    engine = MagicMock(spec=Engine)
    engine.name = 'mysql'
    return engine

@pytest.fixture
def mock_schema_discovery():
    """Mock SchemaDiscovery instance for MySQLReplicator tests."""
    mock_discovery = MagicMock(spec=SchemaDiscovery)
    return mock_discovery

@pytest.fixture
def replicator(mock_source_engine, mock_target_engine, mock_schema_discovery):
    """Create ExactMySQLReplicator instance with mocked engines and SchemaDiscovery."""
    replicator = ExactMySQLReplicator(
        source_engine=mock_source_engine,
        target_engine=mock_target_engine,
        source_db='test_source',
        target_db='test_target',
        schema_discovery=mock_schema_discovery
    )
    return replicator


# =============================================================================
# PRIORITYPROCESSOR FIXTURES
# =============================================================================

@pytest.fixture
def mock_priority_processor_settings():
    """Mock Settings instance for PriorityProcessor tests."""
    from etl_pipeline.config.settings import Settings
    settings = MagicMock(spec=Settings)
    settings.get_tables_by_importance.side_effect = lambda importance: {
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
    from etl_pipeline.core.schema_discovery import SchemaDiscovery
    
    # Create a mock schema discovery
    mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
    
    return PriorityProcessor(settings=mock_priority_processor_settings, schema_discovery=mock_schema_discovery)


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
    """Create PipelineOrchestrator instance with mocked components."""
    with patch('etl_pipeline.orchestration.pipeline_orchestrator.Settings') as mock_settings:
        mock_settings.return_value = MagicMock()
        
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConnectionFactory') as mock_connection_factory:
            mock_connection_factory.get_opendental_source_connection.return_value = MagicMock()
            
            with patch('etl_pipeline.orchestration.pipeline_orchestrator.SchemaDiscovery') as mock_schema_discovery_class:
                mock_schema_discovery = MagicMock()
                mock_schema_discovery_class.return_value = mock_schema_discovery
                
                with patch('etl_pipeline.orchestration.pipeline_orchestrator.TableProcessor') as mock_processor_class:
                    mock_processor_class.return_value = mock_components['table_processor']
                    
                    with patch('etl_pipeline.orchestration.pipeline_orchestrator.PriorityProcessor') as mock_priority_class:
                        mock_priority_class.return_value = mock_components['priority_processor']
                        
                        with patch('etl_pipeline.orchestration.pipeline_orchestrator.UnifiedMetricsCollector') as mock_metrics_class:
                            mock_metrics_class.return_value = mock_components['metrics']
                            
                            # Create REAL orchestrator with mocked dependencies
                            from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
                            orchestrator = PipelineOrchestrator()
                            
                            # Mock the initialize_connections method to avoid real database connections
                            with patch.object(orchestrator, 'initialize_connections') as mock_init_connections:
                                mock_init_connections.return_value = True
                                
                                # Manually set up the components and state as if initialize_connections was called
                                orchestrator.table_processor = mock_components['table_processor']
                                orchestrator.priority_processor = mock_components['priority_processor']
                                orchestrator.metrics_collector = mock_components['metrics']
                                orchestrator.schema_discovery = mock_schema_discovery
                                orchestrator._initialized = True
                                
                                return orchestrator


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
# CONFIGURATION TEST FIXTURES (UPDATED)
# =============================================================================

@pytest.fixture
def complete_config_environment():
    """Fixture providing a complete configuration environment."""
    env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'source_host',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'source_db',
        'OPENDENTAL_SOURCE_USER': 'source_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'source_pass',
        'MYSQL_REPLICATION_HOST': 'repl_host',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'repl_db',
        'MYSQL_REPLICATION_USER': 'repl_user',
        'MYSQL_REPLICATION_PASSWORD': 'repl_pass',
        'POSTGRES_ANALYTICS_HOST': 'analytics_host',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics_db',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }
    return env_vars


@pytest.fixture
def valid_pipeline_config():
    """Fixture providing a valid pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'dental_clinic_etl',
            'environment': 'production',
            'timezone': 'UTC',
            'max_retries': 3,
            'retry_delay_seconds': 300,
            'batch_size': 25000,
            'parallel_jobs': 6
        },
        'connections': {
            'source': {
                'pool_size': 6,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'read_timeout': 300,
                'write_timeout': 300
            },
            'replication': {
                'pool_size': 6,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'read_timeout': 300,
                'write_timeout': 300
            },
            'analytics': {
                'pool_size': 6,
                'pool_timeout': 30,
                'pool_recycle': 3600,
                'connect_timeout': 60,
                'application_name': 'dental_clinic_etl'
            }
        },
        'stages': {
            'extract': {
                'enabled': True,
                'timeout_minutes': 30,
                'error_threshold': 0.01
            },
            'load': {
                'enabled': True,
                'timeout_minutes': 45,
                'error_threshold': 0.01
            },
            'transform': {
                'enabled': True,
                'timeout_minutes': 60,
                'error_threshold': 0.01
            }
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file': {
                'enabled': True,
                'path': 'logs/pipeline.log',
                'max_size_mb': 100,
                'backup_count': 10
            },
            'console': {
                'enabled': True,
                'level': 'INFO'
            }
        },
        'error_handling': {
            'max_consecutive_failures': 3,
            'failure_notification_threshold': 2,
            'auto_retry': {
                'enabled': True,
                'max_attempts': 3,
                'delay_minutes': 5
            }
        }
    }


@pytest.fixture
def complete_tables_config():
    """Fixture providing complete tables configuration."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_key': 'DateTStamp',
                'incremental': True,
                'table_importance': 'critical',
                'batch_size': 1000,
                'extraction_strategy': 'incremental'
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_key': 'AptDateTime',
                'incremental': True,
                'table_importance': 'important',
                'batch_size': 500,
                'extraction_strategy': 'incremental'
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_key': 'ProcDate',
                'incremental': True,
                'table_importance': 'important',
                'batch_size': 2000,
                'extraction_strategy': 'incremental'
            },
            'securitylog': {
                'primary_key': 'SecurityLogNum',
                'incremental': False,
                'table_importance': 'reference',
                'batch_size': 5000,
                'extraction_strategy': 'full'
            }
        }
    }


@pytest.fixture
def minimal_pipeline_config():
    """Fixture providing a minimal pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'test_pipeline',
            'batch_size': 1000
        }
    }


@pytest.fixture
def invalid_pipeline_config():
    """Fixture providing an invalid pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'test_pipeline',
            'batch_size': 'invalid_string'  # Should be int
        },
        'connections': {
            'source': {
                'pool_size': -1  # Invalid negative value
            }
        }
    }


@pytest.fixture
def mock_settings_environment():
    """Fixture providing a context manager for mocking Settings environment."""
    def _mock_settings_environment(env_vars=None, pipeline_config=None, tables_config=None):
        """Create a context manager for mocking Settings environment."""
        patches = []
        
        # Mock environment variables
        if env_vars is not None:
            patches.append(patch.dict(os.environ, env_vars))
        
        # Mock file existence and loading
        if pipeline_config is not None or tables_config is not None:
            patches.append(patch('pathlib.Path.exists', return_value=True))
            
            # Configure YAML loading
            def yaml_side_effect(data):
                if 'pipeline' in str(data) and pipeline_config is not None:
                    return pipeline_config
                elif 'tables' in str(data) and tables_config is not None:
                    return tables_config
                return {}
            
            patches.append(patch('yaml.safe_load', side_effect=yaml_side_effect))
            patches.append(patch('builtins.open', mock_open(read_data='test: value')))
        
        # Always mock load_environment to avoid actual environment loading
        patches.append(patch('etl_pipeline.config.settings.Settings.load_environment'))
        
        return patches
    
    return _mock_settings_environment


# =============================================================================
# BACKWARD COMPATIBILITY FIXTURES
# =============================================================================

# These fixtures maintain backward compatibility with existing tests
# that may not have been updated to use the new configuration system

@pytest.fixture
def legacy_settings():
    """Legacy settings fixture for backward compatibility."""
    if NEW_CONFIG_AVAILABLE:
        # Use new system with fallback
        return create_test_settings()
    else:
        # Use old system
        return Settings(environment='test')


@pytest.fixture
def legacy_connection_factory():
    """Legacy connection factory fixture for backward compatibility."""
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock:
        # Mock both old and new connection methods
        mock.get_opendental_source_connection.return_value = MagicMock(spec=Engine)
        mock.get_mysql_replication_connection.return_value = MagicMock(spec=Engine)
        mock.get_postgres_analytics_connection.return_value = MagicMock(spec=Engine)
        
        # Mock new enum-based methods if available
        if NEW_CONFIG_AVAILABLE:
            mock.get_source_connection.return_value = MagicMock(spec=Engine)
            mock.get_replication_connection.return_value = MagicMock(spec=Engine)
            mock.get_analytics_connection.return_value = MagicMock(spec=Engine)
        
        yield mock 