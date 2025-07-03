"""
Shared test fixtures for ETL pipeline tests.

This module provides common fixtures used across all test modules,
including database mocks, test data generators, and configuration fixtures.
"""

import os
import pytest
import pandas as pd
import logging
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, timedelta
from typing import Dict, List, Any
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text

# Import ETL pipeline components for testing
from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory

from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.config.logging import get_logger

# Set up logger for this module using the project's logging configuration
logger = get_logger(__name__)

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
        'estimated_rows': 1000000,
        'estimated_size_mb': 100,
        'batch_size': 5000,
        'priority': 'critical'
    }

@pytest.fixture
def table_processor_medium_large_config():
    """Medium-large table configuration for TableProcessor tests."""
    return {
        'estimated_rows': 500000,
        'estimated_size_mb': 50,
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
# TEST ENVIRONMENT SETUP
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
# EXISTING UNIFIED METRICS FIXTURES (for backward compatibility) 