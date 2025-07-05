"""
Configuration fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Pipeline configuration
- Table configuration  
- Settings creation and management
- Configuration validation
"""

import os
import pytest
from unittest.mock import patch
from typing import Dict, Any

# Import ETL pipeline components for testing
try:
    from etl_pipeline.config.settings import Settings
except ImportError:
    # Fallback for new configuration system
    from etl_pipeline.config import create_settings
    Settings = None

# Import new configuration system components
try:
    from etl_pipeline.config import reset_settings, create_test_settings, DatabaseType, PostgresSchema
    NEW_CONFIG_AVAILABLE = True
except ImportError:
    # Fallback for backward compatibility
    NEW_CONFIG_AVAILABLE = False
    DatabaseType = None
    PostgresSchema = None


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
def complete_config_environment():
    """Complete configuration environment with all required settings."""
    return {
        'pipeline_config': {
            'general': {
                'pipeline_name': 'complete_test_pipeline',
                'environment': 'test',
                'batch_size': 2000,
                'parallel_jobs': 4,
                'max_retries': 3,
                'retry_delay': 5
            },
            'connections': {
                'source': {
                    'pool_size': 5,
                    'connect_timeout': 60,
                    'read_timeout': 300
                },
                'replication': {
                    'pool_size': 3,
                    'connect_timeout': 45,
                    'read_timeout': 180
                },
                'analytics': {
                    'pool_size': 4,
                    'connect_timeout': 30,
                    'read_timeout': 120
                }
            },
            'logging': {
                'level': 'INFO',
                'format': 'detailed',
                'file_rotation': True
            }
        },
        'tables_config': {
            'tables': {
                'patient': {
                    'primary_key': 'PatNum',
                    'incremental_column': 'DateTStamp',
                    'extraction_strategy': 'incremental',
                    'table_importance': 'critical',
                    'batch_size': 500,
                    'parallel_processing': True
                },
                'appointment': {
                    'primary_key': 'AptNum',
                    'incremental_column': 'AptDateTime',
                    'extraction_strategy': 'incremental',
                    'table_importance': 'important',
                    'batch_size': 200,
                    'parallel_processing': True
                },
                'procedurelog': {
                    'primary_key': 'ProcNum',
                    'incremental_column': 'ProcDate',
                    'extraction_strategy': 'incremental',
                    'table_importance': 'important',
                    'batch_size': 300,
                    'parallel_processing': False
                },
                'claim': {
                    'primary_key': 'ClaimNum',
                    'incremental_column': 'DateSent',
                    'extraction_strategy': 'incremental',
                    'table_importance': 'standard',
                    'batch_size': 150
                }
            }
        }
    }


@pytest.fixture
def valid_pipeline_config():
    """Valid pipeline configuration for testing."""
    return {
        'general': {
            'pipeline_name': 'valid_test_pipeline',
            'environment': 'test',
            'batch_size': 1000,
            'parallel_jobs': 2,
            'max_retries': 3,
            'retry_delay': 5,
            'enable_monitoring': True,
            'enable_metrics': True
        },
        'connections': {
            'source': {
                'pool_size': 3,
                'connect_timeout': 30,
                'read_timeout': 120,
                'max_overflow': 5
            },
            'replication': {
                'pool_size': 2,
                'connect_timeout': 30,
                'read_timeout': 90,
                'max_overflow': 3
            },
            'analytics': {
                'pool_size': 4,
                'connect_timeout': 30,
                'read_timeout': 60,
                'max_overflow': 6
            }
        },
        'logging': {
            'level': 'INFO',
            'format': 'standard',
            'file_rotation': False,
            'console_output': True
        },
        'monitoring': {
            'enabled': True,
            'metrics_interval': 60,
            'health_check_interval': 30,
            'alert_threshold': 0.9
        }
    }


@pytest.fixture
def minimal_pipeline_config():
    """Minimal pipeline configuration for testing."""
    return {
        'general': {
            'pipeline_name': 'minimal_test_pipeline',
            'environment': 'test',
            'batch_size': 100
        },
        'connections': {
            'source': {
                'pool_size': 1
            },
            'replication': {
                'pool_size': 1
            },
            'analytics': {
                'pool_size': 1
            }
        }
    }


@pytest.fixture
def invalid_pipeline_config():
    """Invalid pipeline configuration for testing error handling."""
    return {
        'general': {
            'pipeline_name': '',  # Invalid: empty name
            'environment': 'invalid_env',  # Invalid: unknown environment
            'batch_size': -1,  # Invalid: negative batch size
            'parallel_jobs': 0  # Invalid: zero parallel jobs
        },
        'connections': {
            'source': {
                'pool_size': -5,  # Invalid: negative pool size
                'connect_timeout': 0  # Invalid: zero timeout
            }
        }
    }


@pytest.fixture
def complete_tables_config():
    """Complete tables configuration for testing."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_column': 'DateTStamp',
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'batch_size': 500,
                'parallel_processing': True,
                'validation_rules': ['not_null', 'unique'],
                'transformation_rules': ['clean_dates', 'standardize_names']
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_column': 'AptDateTime',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 200,
                'parallel_processing': True,
                'validation_rules': ['not_null', 'valid_date'],
                'transformation_rules': ['clean_datetime', 'timezone_convert']
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_column': 'ProcDate',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 300,
                'parallel_processing': False,
                'validation_rules': ['not_null', 'valid_amount'],
                'transformation_rules': ['clean_amounts', 'standardize_codes']
            },
            'claim': {
                'primary_key': 'ClaimNum',
                'incremental_column': 'DateSent',
                'extraction_strategy': 'incremental',
                'table_importance': 'standard',
                'batch_size': 150,
                'parallel_processing': True,
                'validation_rules': ['not_null'],
                'transformation_rules': ['clean_dates']
            },
            'payment': {
                'primary_key': 'PayNum',
                'incremental_column': 'DatePay',
                'extraction_strategy': 'incremental',
                'table_importance': 'standard',
                'batch_size': 100,
                'parallel_processing': False,
                'validation_rules': ['not_null', 'valid_amount'],
                'transformation_rules': ['clean_amounts']
            }
        }
    }


@pytest.fixture
def mock_settings_environment():
    """Mock settings environment for testing configuration loading."""
    def _mock_settings_environment(env_vars=None, pipeline_config=None, tables_config=None):
        """Create a mock settings environment with optional overrides."""
        base_env_vars = {
            'ETL_ENVIRONMENT': 'test',
            'OPENDENTAL_SOURCE_HOST': 'localhost',
            'OPENDENTAL_SOURCE_PORT': '3306',
            'OPENDENTAL_SOURCE_DB': 'test_opendental',
            'OPENDENTAL_SOURCE_USER': 'test_user',
            'OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
        }
        
        if env_vars:
            base_env_vars.update(env_vars)
        
        base_pipeline_config = {
            'general': {
                'pipeline_name': 'mock_test_pipeline',
                'environment': 'test',
                'batch_size': 1000
            }
        }
        
        if pipeline_config:
            base_pipeline_config.update(pipeline_config)
        
        base_tables_config = {
            'tables': {
                'patient': {
                    'primary_key': 'PatNum',
                    'extraction_strategy': 'incremental'
                }
            }
        }
        
        if tables_config:
            base_tables_config.update(tables_config)
        
        def yaml_side_effect(data):
            """Mock YAML loading behavior."""
            if 'pipeline_config' in str(data):
                return base_pipeline_config
            elif 'tables_config' in str(data):
                return base_tables_config
            else:
                return {}
        
        return {
            'env_vars': base_env_vars,
            'pipeline_config': base_pipeline_config,
            'tables_config': base_tables_config,
            'yaml_side_effect': yaml_side_effect
        }
    
    return _mock_settings_environment 