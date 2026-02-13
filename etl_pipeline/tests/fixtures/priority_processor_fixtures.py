"""
Priority processor fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Priority processing components with Settings injection
- Table prioritization following connection architecture
- Processing queues with provider pattern integration
- Priority utilities with unified interface patterns
- Database type and schema enums for type safety
- Configuration provider pattern for dependency injection
"""

import pytest
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta

# Import new configuration system components
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema,
    create_test_settings
)
from etl_pipeline.config.providers import DictConfigProvider


@pytest.fixture
def database_types():
    """Database type enums for testing."""
    return DatabaseType


@pytest.fixture
def postgres_schemas():
    """PostgreSQL schema enums for testing."""
    return PostgresSchema


@pytest.fixture
def priority_processor_env_vars():
    """Priority processor environment variables following connection architecture naming convention.
    
    This fixture provides environment variables for priority processor testing that conform to the connection architecture:
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


@pytest.fixture
def priority_processor_pipeline_config():
    """Priority processor pipeline configuration following connection architecture patterns."""
    return {
        'general': {
            'pipeline_name': 'priority_processor_test',
            'environment': 'test',
            'batch_size': 1000,
            'parallel_jobs': 2
        },
        'connections': {
            'source': {
                'pool_size': 5,
                'connect_timeout': 30,
                'priority_processing': True
            },
            'replication': {
                'pool_size': 3,
                'connect_timeout': 30,
                'priority_processing': True
            },
            'analytics': {
                'pool_size': 4,
                'connect_timeout': 30,
                'priority_processing': True
            }
        },
        'priority_processor': {
            'enabled': True,
            'max_priority_levels': 5,
            'default_priority': 3,
            'priority_weights': {
                'critical': 5,
                'high': 4,
                'important': 3,
                'normal': 2,
                'low': 1
            },
            'batch_size': 1000,
            'timeout': 300
        }
    }


@pytest.fixture
def priority_processor_tables_config():
    """Priority processor tables configuration following connection architecture patterns."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_column': 'DateTStamp',
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'priority_level': 'critical',
                'priority': 5,
                'batch_size': 1000,
                'processing_order': 1
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_column': 'AptDateTime',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'priority_level': 'high',
                'priority': 4,
                'batch_size': 500,
                'processing_order': 2
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_column': 'ProcDate',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'priority_level': 'important',
                'priority': 3,
                'batch_size': 2000,
                'processing_order': 3
            },
            'claim': {
                'primary_key': 'ClaimNum',
                'incremental_column': 'DateSent',
                'extraction_strategy': 'incremental',
                'table_importance': 'normal',
                'priority_level': 'normal',
                'priority': 2,
                'batch_size': 1500,
                'processing_order': 4
            },
            'payment': {
                'primary_key': 'PayNum',
                'incremental_column': 'DatePay',
                'extraction_strategy': 'incremental',
                'table_importance': 'low',
                'priority_level': 'low',
                'priority': 1,
                'batch_size': 800,
                'processing_order': 5
            }
        }
    }


@pytest.fixture
def priority_processor_config_provider(priority_processor_pipeline_config, priority_processor_tables_config, priority_processor_env_vars):
    """Priority processor configuration provider following the provider pattern for dependency injection.
    
    This fixture implements the DictConfigProvider pattern as specified in the connection architecture:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected configuration for clean test isolation
    - Supports dependency injection for easy configuration swapping
    - Follows the provider pattern for configuration loading
    """
    return DictConfigProvider(
        pipeline=priority_processor_pipeline_config,
        tables=priority_processor_tables_config,
        env=priority_processor_env_vars
    )


@pytest.fixture
def priority_processor_settings(priority_processor_config_provider):
    """Priority processor settings following connection architecture patterns.
    
    This fixture provides Settings with priority processor configuration:
    - Uses provider pattern for dependency injection
    - Follows unified interface with Settings injection
    - Supports environment-agnostic operation
    - Includes proper environment validation
    """
    return Settings(environment='test', provider=priority_processor_config_provider)


@pytest.fixture
def mock_priority_processor_settings():
    """Mock priority processor settings for testing (legacy support)."""
    return {
        'enabled': True,
        'max_priority_levels': 5,
        'default_priority': 3,
        'priority_weights': {
            'critical': 5,
            'high': 4,
            'important': 3,
            'normal': 2,
            'low': 1
        },
        'batch_size': 1000,
        'timeout': 300
    }


@pytest.fixture
def mock_priority_processor_table_processor():
    """Mock table processor for priority processing testing."""
    processor = MagicMock()
    processor.process_table.return_value = True
    processor.get_processing_stats.return_value = {
        'tables_processed': 5,
        'rows_processed': 15000,
        'processing_time': 1800.5
    }
    return processor


@pytest.fixture
def priority_processor(priority_processor_settings):
    """Priority processor instance with Settings injection following connection architecture.
    
    This fixture creates a priority processor using the unified interface:
    - Uses Settings injection for environment-agnostic operation
    - Follows the provider pattern for configuration
    - Supports both clinic and test environments
    - Includes proper error handling and validation
    """
    # Create mock processor with Settings injection until PriorityProcessor is implemented
    processor = MagicMock()
    processor.settings = priority_processor_settings
    processor.process_priority_queue.return_value = True
    processor.get_processing_stats.return_value = {
        'tables_processed': 5,
        'rows_processed': 15000,
        'processing_time': 1800.5
    }
    return processor


@pytest.fixture
def sample_priority_queue():
    """Sample priority queue for testing with database type integration."""
    return [
        {
            'table_name': 'patient',
            'priority': 5,
            'priority_level': 'critical',
            'batch_size': 1000,
            'added_time': datetime.now() - timedelta(minutes=10),
            'database_type': DatabaseType.SOURCE.value,
            'schema': None
        },
        {
            'table_name': 'appointment',
            'priority': 4,
            'priority_level': 'high',
            'batch_size': 500,
            'added_time': datetime.now() - timedelta(minutes=5),
            'database_type': DatabaseType.SOURCE.value,
            'schema': None
        },
        {
            'table_name': 'procedurelog',
            'priority': 3,
            'priority_level': 'important',
            'batch_size': 2000,
            'added_time': datetime.now() - timedelta(minutes=15),
            'database_type': DatabaseType.SOURCE.value,
            'schema': None
        },
        {
            'table_name': 'claim',
            'priority': 2,
            'priority_level': 'normal',
            'batch_size': 1500,
            'added_time': datetime.now() - timedelta(minutes=20),
            'database_type': DatabaseType.REPLICATION.value,
            'schema': None
        },
        {
            'table_name': 'payment',
            'priority': 1,
            'priority_level': 'low',
            'batch_size': 800,
            'added_time': datetime.now() - timedelta(minutes=25),
            'database_type': DatabaseType.ANALYTICS.value,
            'schema': PostgresSchema.RAW.value
        }
    ]


@pytest.fixture
def mock_priority_table_config():
    """Mock priority table configuration for testing with database type integration."""
    return {
        'tables': {
            'patient': {
                'priority': 5,
                'priority_level': 'critical',
                'batch_size': 1000,
                'processing_order': 1,
                'database_type': DatabaseType.SOURCE.value,
                'schema': None
            },
            'appointment': {
                'priority': 4,
                'priority_level': 'high',
                'batch_size': 500,
                'processing_order': 2,
                'database_type': DatabaseType.SOURCE.value,
                'schema': None
            },
            'procedurelog': {
                'priority': 3,
                'priority_level': 'important',
                'batch_size': 2000,
                'processing_order': 3,
                'database_type': DatabaseType.SOURCE.value,
                'schema': None
            },
            'claim': {
                'priority': 2,
                'priority_level': 'normal',
                'batch_size': 1500,
                'processing_order': 4,
                'database_type': DatabaseType.REPLICATION.value,
                'schema': None
            },
            'payment': {
                'priority': 1,
                'priority_level': 'low',
                'batch_size': 800,
                'processing_order': 5,
                'database_type': DatabaseType.ANALYTICS.value,
                'schema': PostgresSchema.RAW.value
            }
        }
    }


@pytest.fixture
def mock_priority_processor_stats():
    """Mock priority processor statistics for testing."""
    return {
        'total_tables_queued': 15,
        'total_tables_processed': 12,
        'total_processing_time': 3600.5,
        'average_processing_time': 300.0,
        'priority_level_stats': {
            'critical': {'processed': 3, 'avg_time': 180.0},
            'high': {'processed': 4, 'avg_time': 240.0},
            'important': {'processed': 3, 'avg_time': 300.0},
            'normal': {'processed': 2, 'avg_time': 360.0},
            'low': {'processed': 0, 'avg_time': 0.0}
        },
        'queue_stats': {
            'current_queue_size': 3,
            'max_queue_size': 20,
            'average_wait_time': 45.5
        },
        'database_stats': {
            DatabaseType.SOURCE.value: {'processed': 8, 'avg_time': 200.0},
            DatabaseType.REPLICATION.value: {'processed': 2, 'avg_time': 300.0},
            DatabaseType.ANALYTICS.value: {'processed': 2, 'avg_time': 400.0}
        }
    }


@pytest.fixture
def mock_priority_scheduler():
    """Mock priority scheduler for testing with Settings injection."""
    scheduler = MagicMock()
    scheduler.schedule_table.return_value = True
    scheduler.get_next_table.return_value = {
        'table_name': 'patient',
        'priority': 5,
        'priority_level': 'critical',
        'database_type': DatabaseType.SOURCE.value,
        'schema': None
    }
    scheduler.get_queue_status.return_value = {
        'queue_size': 5,
        'highest_priority': 5,
        'lowest_priority': 1,
        'database_distribution': {
            DatabaseType.SOURCE.value: 3,
            DatabaseType.REPLICATION.value: 1,
            DatabaseType.ANALYTICS.value: 1
        }
    }
    return scheduler


@pytest.fixture
def mock_priority_validator():
    """Mock priority validator for testing with Settings injection."""
    validator = MagicMock()
    validator.validate_priority.return_value = True
    validator.validate_table_config.return_value = True
    validator.get_validation_errors.return_value = []
    validator.validate_database_config.return_value = True
    return validator


@pytest.fixture
def sample_priority_rules():
    """Sample priority rules for testing with database type integration."""
    return {
        'critical': {
            'description': 'Critical tables that must be processed first',
            'tables': ['patient', 'appointment'],
            'timeout': 180,
            'retry_count': 3,
            'database_types': [DatabaseType.SOURCE.value]
        },
        'high': {
            'description': 'High priority tables',
            'tables': ['procedurelog', 'claim'],
            'timeout': 300,
            'retry_count': 2,
            'database_types': [DatabaseType.SOURCE.value, DatabaseType.REPLICATION.value]
        },
        'important': {
            'description': 'Important tables',
            'tables': ['payment', 'insurance'],
            'timeout': 600,
            'retry_count': 1,
            'database_types': [DatabaseType.ANALYTICS.value]
        },
        'normal': {
            'description': 'Normal priority tables',
            'tables': ['document', 'task'],
            'timeout': 900,
            'retry_count': 1,
            'database_types': [DatabaseType.REPLICATION.value, DatabaseType.ANALYTICS.value]
        },
        'low': {
            'description': 'Low priority tables',
            'tables': ['log', 'temp'],
            'timeout': 1800,
            'retry_count': 0,
            'database_types': [DatabaseType.ANALYTICS.value]
        }
    }


@pytest.fixture
def mock_priority_monitor():
    """Mock priority monitor for testing with Settings injection."""
    monitor = MagicMock()
    monitor.start_monitoring.return_value = True
    monitor.stop_monitoring.return_value = True
    monitor.get_priority_metrics.return_value = {
        'queue_depth': 5,
        'processing_rate': 2.5,
        'average_wait_time': 45.5,
        'priority_distribution': {
            'critical': 1,
            'high': 2,
            'important': 1,
            'normal': 1,
            'low': 0
        },
        'database_distribution': {
            DatabaseType.SOURCE.value: 3,
            DatabaseType.REPLICATION.value: 1,
            DatabaseType.ANALYTICS.value: 1
        }
    }
    return monitor


@pytest.fixture
def mock_priority_error_handler():
    """Mock priority error handler for testing with Settings injection."""
    error_handler = MagicMock()
    error_handler.handle_priority_error.return_value = True
    error_handler.retry_table.return_value = True
    error_handler.escalate_priority.return_value = True
    error_handler.handle_database_error.return_value = True
    return error_handler


@pytest.fixture
def sample_priority_workflow():
    """Sample priority workflow for testing with database type integration."""
    return {
        'workflow_id': 'priority_workflow_001',
        'start_time': datetime.now() - timedelta(hours=1),
        'end_time': datetime.now(),
        'status': 'completed',
        'steps': [
            {
                'step': 'queue_analysis',
                'status': 'completed',
                'duration': 30,
                'tables_analyzed': 15
            },
            {
                'step': 'priority_assignment',
                'status': 'completed',
                'duration': 60,
                'priorities_assigned': 15
            },
            {
                'step': 'queue_processing',
                'status': 'completed',
                'duration': 3300,
                'tables_processed': 12
            }
        ],
        'priority_summary': {
            'critical_processed': 3,
            'high_processed': 4,
            'important_processed': 3,
            'normal_processed': 2,
            'low_processed': 0
        },
        'database_summary': {
            DatabaseType.SOURCE.value: {'processed': 8, 'avg_time': 200.0},
            DatabaseType.REPLICATION.value: {'processed': 2, 'avg_time': 300.0},
            DatabaseType.ANALYTICS.value: {'processed': 2, 'avg_time': 400.0}
        }
    }


@pytest.fixture
def mock_priority_optimizer():
    """Mock priority optimizer for testing with Settings injection."""
    optimizer = MagicMock()
    optimizer.optimize_queue.return_value = True
    optimizer.rebalance_priorities.return_value = True
    optimizer.get_optimization_suggestions.return_value = [
        {
            'table': 'patient',
            'suggestion': 'Increase batch size',
            'expected_improvement': 0.15,
            'database_type': DatabaseType.SOURCE.value,
            'schema': None
        }
    ]
    return optimizer 