"""
Mock utilities for ETL pipeline tests.

This module contains:
- Common patch decorators
- Reusable mock builders
- Mock utilities
- Testing helpers

Follows the connection architecture patterns where appropriate:
- Uses provider pattern for dependency injection
- Uses Settings injection for environment-agnostic mocks
- Uses environment separation for test vs production mocks
- Uses unified interface with ConnectionFactory
"""

import pytest
from unittest.mock import MagicMock, Mock, patch, mock_open
from typing import Dict, List, Any, Callable, Optional
from functools import wraps

from etl_pipeline.config import create_test_settings
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.core import ConnectionFactory


def mock_database_connection_with_settings():
    """Decorator to mock database connections using Settings injection."""
    def decorator(func):
        @wraps(func)
        @patch('etl_pipeline.core.connections.ConnectionFactory')
        @patch('sqlalchemy.create_engine')
        def wrapper(mock_create_engine, mock_connection_factory, *args, **kwargs):
            # Mock the connection factory with Settings injection
            mock_factory = MagicMock()
            mock_factory.get_source_connection.return_value = create_mock_engine('mysql', 'test_opendental')
            mock_factory.get_replication_connection.return_value = create_mock_engine('mysql', 'test_opendental_replication')
            mock_factory.get_analytics_raw_connection.return_value = create_mock_engine('postgresql', 'test_opendental_analytics')
            mock_connection_factory.return_value = mock_factory
            
            # Mock the engine creation
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def mock_database_connection():
    """Decorator to mock database connections (legacy version)."""
    def decorator(func):
        @wraps(func)
        @patch('etl_pipeline.core.connections.ConnectionFactory')
        @patch('sqlalchemy.create_engine')
        def wrapper(mock_create_engine, mock_connection_factory, *args, **kwargs):
            # Mock the connection factory
            mock_factory = MagicMock()
            mock_connection_factory.return_value = mock_factory
            
            # Mock the engine creation
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def mock_file_operations():
    """Decorator to mock file operations."""
    def decorator(func):
        @wraps(func)
        @patch('builtins.open', new_callable=mock_open)
        @patch('os.path.exists')
        @patch('os.makedirs')
        def wrapper(mock_makedirs, mock_exists, mock_file, *args, **kwargs):
            # Mock file existence
            mock_exists.return_value = True
            
            # Mock directory creation
            mock_makedirs.return_value = None
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def mock_config_loading():
    """Decorator to mock configuration loading."""
    def decorator(func):
        @wraps(func)
        @patch('yaml.safe_load')
        @patch('builtins.open', new_callable=mock_open)
        def wrapper(mock_file, mock_yaml_load, *args, **kwargs):
            # Mock YAML loading
            mock_yaml_load.return_value = {
                'pipeline': {'name': 'test_pipeline'},
                'connections': {'source': {'host': 'localhost'}}
            }
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def mock_logging():
    """Decorator to mock logging operations."""
    def decorator(func):
        @wraps(func)
        @patch('etl_pipeline.config.logging.getLogger')
        def wrapper(mock_get_logger, *args, **kwargs):
            # Mock logger
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


def mock_environment_variables():
    """Decorator to mock environment variables."""
    def decorator(func):
        @wraps(func)
        @patch.dict('os.environ', {
            'ETL_ENVIRONMENT': 'test',
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
        })
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


def create_mock_engine(engine_type='mysql', database='test_db'):
    """Create a mock database engine."""
    engine = MagicMock()
    engine.name = engine_type
    engine.url.database = database
    engine.url.host = 'localhost'
    engine.url.port = 3306 if engine_type == 'mysql' else 5432
    return engine


def create_mock_connection():
    """Create a mock database connection."""
    connection = MagicMock()
    connection.execute.return_value = MagicMock()
    connection.close.return_value = None
    return connection


def create_mock_result_set(data=None):
    """Create a mock result set."""
    if data is None:
        data = [{'id': 1, 'name': 'test'}]
    
    result = MagicMock()
    result.fetchall.return_value = data
    result.fetchone.return_value = data[0] if data else None
    return result


def create_mock_settings_with_provider(environment='test'):
    """Create mock settings object using provider pattern."""
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
                    'extraction_strategy': 'incremental'
                }
            }
        },
        env={
            # Test environment variables (TEST_ prefixed)
            'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
            'TEST_OPENDENTAL_SOURCE_PORT': '3306',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
            'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
            
            'TEST_MYSQL_REPLICATION_HOST': 'localhost',
            'TEST_MYSQL_REPLICATION_PORT': '3306',
            'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
            'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
            'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
            
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


def create_mock_settings(environment='test'):
    """Create mock settings object (legacy version)."""
    settings = MagicMock()
    settings.environment = environment
    settings.source_host = 'localhost'
    settings.source_port = 3306
    settings.source_database = 'test_opendental'
    settings.source_username = 'test_user'
    settings.source_password = 'test_pass'
    return settings


def create_mock_config():
    """Create mock configuration object."""
    config = MagicMock()
    config.pipeline_name = 'test_pipeline'
    config.environment = 'test'
    config.batch_size = 1000
    config.parallel_jobs = 2
    return config


def create_mock_dataframe(columns: Optional[List[str]] = None, data: Optional[List[List[Any]]] = None):
    """Create a mock pandas DataFrame."""
    import pandas as pd
    
    if columns is None:
        columns = ['id', 'name', 'value']
    
    if data is None:
        data = [
            [1, 'test1', 100],
            [2, 'test2', 200],
            [3, 'test3', 300]
        ]
    
    # Fix the type issue by ensuring columns is properly typed
    df = pd.DataFrame(data)
    df.columns = columns
    return df


def create_mock_error_response(error_type='ConnectionError', message='Test error'):
    """Create a mock error response."""
    class MockError(Exception):
        def __init__(self, message):
            self.message = message
            super().__init__(self.message)
    
    return MockError(message)


def create_mock_metrics_collector_with_settings():
    """Create a mock metrics collector using Settings injection."""
    collector = MagicMock()
    collector.settings = create_mock_settings_with_provider()
    collector.collect_metrics.return_value = {
        'cpu_usage': 45.2,
        'memory_usage': 67.8,
        'processing_time': 1800.5
    }
    return collector


def create_mock_metrics_collector():
    """Create a mock metrics collector (legacy version)."""
    collector = MagicMock()
    collector.collect_metrics.return_value = {
        'cpu_usage': 45.2,
        'memory_usage': 67.8,
        'processing_time': 1800.5
    }
    return collector


def create_mock_validator():
    """Create a mock validator."""
    validator = MagicMock()
    validator.validate.return_value = True
    validator.get_errors.return_value = []
    return validator


def create_mock_transformer():
    """Create a mock transformer."""
    transformer = MagicMock()
    transformer.transform.return_value = True
    transformer.get_transformed_data.return_value = create_mock_dataframe()
    return transformer


def create_mock_loader():
    """Create a mock loader."""
    loader = MagicMock()
    loader.load.return_value = True
    loader.get_loaded_count.return_value = 1000
    return loader


def create_mock_replicator():
    """Create a mock replicator."""
    replicator = MagicMock()
    replicator.replicate.return_value = True
    replicator.get_replicated_tables.return_value = ['patient', 'appointment']
    return replicator


def create_mock_orchestrator():
    """Create a mock orchestrator."""
    orchestrator = MagicMock()
    orchestrator.run_pipeline.return_value = True
    orchestrator.get_pipeline_status.return_value = 'completed'
    return orchestrator


def mock_time_sleep():
    """Decorator to mock time.sleep for faster tests."""
    def decorator(func):
        @wraps(func)
        @patch('time.sleep')
        def wrapper(mock_sleep, *args, **kwargs):
            mock_sleep.return_value = None
            return func(*args, **kwargs)
        return wrapper
    return decorator


def mock_datetime_now():
    """Decorator to mock datetime.now for consistent testing."""
    def decorator(func):
        @wraps(func)
        @patch('datetime.datetime')
        def wrapper(mock_datetime, *args, **kwargs):
            from datetime import datetime
            mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def create_mock_context_manager():
    """Create a mock context manager."""
    context = MagicMock()
    context.__enter__.return_value = context
    context.__exit__.return_value = None
    return context


def create_mock_async_context_manager():
    """Create a mock async context manager."""
    context = MagicMock()
    context.__aenter__.return_value = context
    context.__aexit__.return_value = None
    return context


def mock_async_function():
    """Decorator to mock async functions."""
    def decorator(func):
        @wraps(func)
        @patch('asyncio.create_task')
        @patch('asyncio.gather')
        def wrapper(mock_gather, mock_create_task, *args, **kwargs):
            mock_create_task.return_value = MagicMock()
            mock_gather.return_value = [MagicMock()]
            return func(*args, **kwargs)
        return wrapper
    return decorator


def create_mock_queue():
    """Create a mock queue."""
    queue = MagicMock()
    queue.put.return_value = None
    queue.get.return_value = {'data': 'test'}
    queue.empty.return_value = False
    queue.qsize.return_value = 5
    return queue


def create_mock_thread_pool():
    """Create a mock thread pool."""
    pool = MagicMock()
    pool.submit.return_value = MagicMock()
    pool.map.return_value = [MagicMock()]
    pool.shutdown.return_value = None
    return pool


def create_mock_process_pool():
    """Create a mock process pool."""
    pool = MagicMock()
    pool.apply_async.return_value = MagicMock()
    pool.map.return_value = [MagicMock()]
    pool.close.return_value = None
    pool.join.return_value = None
    return pool


def create_mock_connection_factory_with_settings():
    """Create a mock ConnectionFactory using Settings injection."""
    factory = MagicMock()
    factory.get_source_connection.return_value = create_mock_engine('mysql', 'test_opendental')
    factory.get_replication_connection.return_value = create_mock_engine('mysql', 'test_opendental_replication')
    factory.get_analytics_raw_connection.return_value = create_mock_engine('postgresql', 'test_opendental_analytics')
    factory.get_analytics_staging_connection.return_value = create_mock_engine('postgresql', 'test_opendental_analytics')
    factory.get_analytics_intermediate_connection.return_value = create_mock_engine('postgresql', 'test_opendental_analytics')
    factory.get_analytics_marts_connection.return_value = create_mock_engine('postgresql', 'test_opendental_analytics')
    return factory


def create_mock_provider():
    """Create a mock provider for testing provider pattern integration."""
    provider = MagicMock()
    provider.get_config.return_value = {
        'pipeline': {
            'connections': {
                'source': {'pool_size': 5},
                'replication': {'pool_size': 10},
                'analytics': {'application_name': 'etl_pipeline_test'}
            }
        },
        'tables': {
            'tables': {
                'patient': {
                    'incremental_column': 'DateModified',
                    'batch_size': 1000
                }
            }
        },
        'env': {
            'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental'
        }
    }
    return provider 