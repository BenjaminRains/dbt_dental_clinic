"""
Legacy fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Backward compatibility
- Legacy settings
- Legacy connection factory
- Migration utilities
"""

import pytest
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any


@pytest.fixture
def legacy_settings():
    """Legacy settings for backward compatibility testing."""
    return {
        'environment': 'test',
        'source_host': 'localhost',
        'source_port': 3306,
        'source_database': 'test_opendental',
        'source_username': 'test_user',
        'source_password': 'test_pass',
        'replication_host': 'localhost',
        'replication_port': 3306,
        'replication_database': 'test_replication',
        'replication_username': 'test_user',
        'replication_password': 'test_pass',
        'analytics_host': 'localhost',
        'analytics_port': 5432,
        'analytics_database': 'test_analytics',
        'analytics_schema': 'raw',
        'analytics_username': 'test_user',
        'analytics_password': 'test_pass'
    }


@pytest.fixture
def legacy_connection_factory():
    """Legacy connection factory for backward compatibility testing."""
    factory = MagicMock()
    
    def mock_get_legacy_connection(connection_type, **kwargs):
        mock_engine = MagicMock()
        if connection_type == 'source':
            mock_engine.name = 'mysql'
            mock_engine.url.database = 'test_opendental'
        elif connection_type == 'replication':
            mock_engine.name = 'mysql'
            mock_engine.url.database = 'test_replication'
        elif connection_type == 'analytics':
            mock_engine.name = 'postgresql'
            mock_engine.url.database = 'test_analytics'
        return mock_engine
    
    factory.get_connection = mock_get_legacy_connection
    return factory


@pytest.fixture
def legacy_config():
    """Legacy configuration for backward compatibility testing."""
    return {
        'pipeline': {
            'name': 'legacy_test_pipeline',
            'environment': 'test',
            'batch_size': 1000,
            'parallel_jobs': 2
        },
        'connections': {
            'source': {
                'host': 'localhost',
                'port': 3306,
                'database': 'test_opendental',
                'username': 'test_user',
                'password': 'test_pass'
            },
            'replication': {
                'host': 'localhost',
                'port': 3306,
                'database': 'test_replication',
                'username': 'test_user',
                'password': 'test_pass'
            },
            'analytics': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_analytics',
                'schema': 'raw',
                'username': 'test_user',
                'password': 'test_pass'
            }
        }
    }


@pytest.fixture
def legacy_table_config():
    """Legacy table configuration for backward compatibility testing."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_column': 'DateTStamp',
                'batch_size': 100
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_column': 'AptDateTime',
                'batch_size': 50
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_column': 'ProcDate',
                'batch_size': 200
            }
        }
    }


@pytest.fixture
def legacy_loader():
    """Legacy loader for backward compatibility testing."""
    loader = MagicMock()
    loader.load_table.return_value = True
    loader.load_all_tables.return_value = {'patient': True, 'appointment': True}
    return loader


@pytest.fixture
def legacy_transformer():
    """Legacy transformer for backward compatibility testing."""
    transformer = MagicMock()
    transformer.transform_table.return_value = True
    transformer.transform_all_tables.return_value = {'patient': True, 'appointment': True}
    return transformer


@pytest.fixture
def legacy_replicator():
    """Legacy replicator for backward compatibility testing."""
    replicator = MagicMock()
    replicator.replicate_table.return_value = True
    replicator.replicate_all_tables.return_value = {'patient': True, 'appointment': True}
    return replicator


@pytest.fixture
def legacy_orchestrator():
    """Legacy orchestrator for backward compatibility testing."""
    orchestrator = MagicMock()
    orchestrator.run_pipeline.return_value = True
    orchestrator.run_step.return_value = True
    return orchestrator


@pytest.fixture
def legacy_metrics():
    """Legacy metrics for backward compatibility testing."""
    metrics = MagicMock()
    metrics.collect_metrics.return_value = {
        'tables_processed': 5,
        'rows_processed': 15000,
        'processing_time': 1800.5
    }
    return metrics


@pytest.fixture
def legacy_validation():
    """Legacy validation for backward compatibility testing."""
    validation = MagicMock()
    validation.validate_table.return_value = True
    validation.validate_all_tables.return_value = {'patient': True, 'appointment': True}
    return validation


@pytest.fixture
def legacy_error_handler():
    """Legacy error handler for backward compatibility testing."""
    error_handler = MagicMock()
    error_handler.handle_error.return_value = True
    error_handler.log_error.return_value = None
    return error_handler


@pytest.fixture
def legacy_logger():
    """Legacy logger for backward compatibility testing."""
    logger = MagicMock()
    logger.info.return_value = None
    logger.error.return_value = None
    logger.warning.return_value = None
    logger.debug.return_value = None
    return logger


@pytest.fixture
def legacy_config_parser():
    """Legacy config parser for backward compatibility testing."""
    parser = MagicMock()
    parser.read_config.return_value = {
        'pipeline': {'name': 'legacy_test_pipeline'},
        'connections': {'source': {'host': 'localhost'}}
    }
    return parser


@pytest.fixture
def legacy_migration_utils():
    """Legacy migration utilities for backward compatibility testing."""
    utils = MagicMock()
    utils.migrate_config.return_value = True
    utils.validate_migration.return_value = True
    utils.rollback_migration.return_value = True
    return utils 