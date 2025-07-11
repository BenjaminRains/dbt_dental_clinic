"""
ConfigReader fixtures for ETL pipeline tests.

This module contains fixtures related to:
- ConfigReader initialization and configuration
- Test YAML configuration files
- Mock file system operations
- Configuration validation test cases
- Error scenarios for testing
"""

import os
import tempfile
import yaml
import pytest
from unittest.mock import patch, mock_open, MagicMock
from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime

from etl_pipeline.config.config_reader import ConfigReader


@pytest.fixture
def valid_tables_config():
    """Valid tables configuration for testing."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_column': 'DateTStamp',
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'batch_size': 1000,
                'estimated_size_mb': 50.0,
                'estimated_rows': 10000,
                'monitoring': {
                    'alert_on_failure': True,
                    'alert_on_slow_load': True,
                    'slow_load_threshold_minutes': 30
                },
                'dependencies': [],
                'is_modeled': True
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_column': 'AptDateTime',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 500,
                'estimated_size_mb': 25.0,
                'estimated_rows': 5000,
                'monitoring': {
                    'alert_on_failure': True,
                    'alert_on_slow_load': False,
                    'slow_load_threshold_minutes': 15
                },
                'dependencies': ['patient'],
                'is_modeled': True
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_column': 'ProcDate',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 2000,
                'estimated_size_mb': 150.0,
                'estimated_rows': 20000,
                'monitoring': {
                    'alert_on_failure': False,
                    'alert_on_slow_load': False,
                    'slow_load_threshold_minutes': 60
                },
                'dependencies': ['patient'],
                'is_modeled': True
            },
            'securitylog': {
                'primary_key': 'SecurityLogNum',
                'extraction_strategy': 'full_table',
                'table_importance': 'reference',
                'batch_size': 5000,
                'estimated_size_mb': 10.0,
                'estimated_rows': 1000,
                'monitoring': {
                    'alert_on_failure': False,
                    'alert_on_slow_load': False,
                    'slow_load_threshold_minutes': 10
                },
                'dependencies': [],
                'is_modeled': False
            },
            'definition': {
                'primary_key': 'DefNum',
                'extraction_strategy': 'full_table',
                'table_importance': 'standard',
                'batch_size': 100,
                'estimated_size_mb': 1.0,
                'estimated_rows': 100,
                'monitoring': {
                    'alert_on_failure': False,
                    'alert_on_slow_load': False,
                    'slow_load_threshold_minutes': 5
                },
                'dependencies': [],
                'is_modeled': False
            }
        }
    }


@pytest.fixture
def minimal_tables_config():
    """Minimal tables configuration for testing."""
    return {
        'tables': {
            'patient': {
                'table_importance': 'critical',
                'extraction_strategy': 'incremental'
            }
        }
    }


@pytest.fixture
def invalid_tables_config():
    """Invalid tables configuration for testing error scenarios."""
    return {
        'tables': {
            'patient': {
                # Missing required fields
                'table_importance': 'critical'
                # Missing extraction_strategy, batch_size, etc.
            },
            'appointment': {
                'table_importance': 'important',
                'extraction_strategy': 'incremental',
                'batch_size': -1,  # Invalid negative value
                'estimated_size_mb': 'invalid_string'  # Should be float
            },
            'procedurelog': {
                'table_importance': 'important',
                'extraction_strategy': 'incremental',
                'batch_size': 1000000,  # Too large
                'estimated_size_mb': 200.0  # Large table without monitoring
            }
        }
    }


@pytest.fixture
def empty_tables_config():
    """Empty tables configuration for testing edge cases."""
    return {
        'tables': {}
    }


@pytest.fixture
def malformed_yaml_config():
    """Malformed YAML configuration for testing error handling."""
    return """
    tables:
      patient:
        table_importance: critical
        extraction_strategy: incremental
        batch_size: 1000
      appointment:
        table_importance: important
        extraction_strategy: incremental
        batch_size: 500
        dependencies: [patient
        # Missing closing bracket - malformed YAML
    """


@pytest.fixture
def temp_config_file(valid_tables_config):
    """Temporary configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(valid_tables_config, f)
        config_path = f.name
    
    yield config_path
    
    # Clean up
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def temp_invalid_config_file(invalid_tables_config):
    """Temporary invalid configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(invalid_tables_config, f)
        config_path = f.name
    
    yield config_path
    
    # Clean up
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def temp_empty_config_file(empty_tables_config):
    """Temporary empty configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(empty_tables_config, f)
        config_path = f.name
    
    yield config_path
    
    # Clean up
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def mock_config_reader(valid_tables_config):
    """Mock ConfigReader that doesn't read files."""
    class MockConfigReader(ConfigReader):
        def __init__(self, config_path: str = "mock_config"):
            # Don't call parent constructor to avoid file loading
            self.config_path = config_path
            self.config = valid_tables_config
            self._last_loaded = datetime.now()
    
    return MockConfigReader()


@pytest.fixture
def mock_config_reader_with_invalid_config(invalid_tables_config):
    """Mock ConfigReader with invalid configuration."""
    class MockConfigReader(ConfigReader):
        def __init__(self, config_path: str = "mock_invalid_config"):
            # Don't call parent constructor to avoid file loading
            self.config_path = config_path
            self.config = invalid_tables_config
            self._last_loaded = datetime.now()
    
    return MockConfigReader()


@pytest.fixture
def mock_config_reader_with_empty_config(empty_tables_config):
    """Mock ConfigReader with empty configuration."""
    class MockConfigReader(ConfigReader):
        def __init__(self, config_path: str = "mock_empty_config"):
            # Don't call parent constructor to avoid file loading
            self.config_path = config_path
            self.config = empty_tables_config
            self._last_loaded = datetime.now()
    
    return MockConfigReader()


@pytest.fixture
def mock_file_system():
    """Mock file system operations for ConfigReader tests."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open') as mock_open_file:
        
        # Mock file existence
        mock_exists.return_value = True
        
        # Mock file reading
        mock_open_file.return_value.__enter__.return_value.read.return_value = "test content"
        
        yield mock_exists, mock_open_file


@pytest.fixture
def mock_yaml_loading():
    """Mock YAML loading operations."""
    with patch('yaml.safe_load') as mock_yaml_load:
        yield mock_yaml_load


@pytest.fixture
def config_reader_test_cases():
    """Test cases for ConfigReader methods."""
    return {
        'table_config': [
            {
                'name': 'existing_table',
                'table_name': 'patient',
                'expected_keys': ['primary_key', 'incremental_column', 'extraction_strategy', 'table_importance'],
                'should_exist': True
            },
            {
                'name': 'non_existing_table',
                'table_name': 'nonexistent_table',
                'expected_keys': [],
                'should_exist': False
            }
        ],
        'importance_levels': [
            {
                'name': 'critical_tables',
                'importance': 'critical',
                'expected_tables': ['patient'],
                'expected_count': 1
            },
            {
                'name': 'important_tables',
                'importance': 'important',
                'expected_tables': ['appointment', 'procedurelog'],
                'expected_count': 2
            },
            {
                'name': 'reference_tables',
                'importance': 'reference',
                'expected_tables': ['securitylog'],
                'expected_count': 1
            },
            {
                'name': 'standard_tables',
                'importance': 'standard',
                'expected_tables': ['definition'],
                'expected_count': 1
            },
            {
                'name': 'non_existing_importance',
                'importance': 'nonexistent',
                'expected_tables': [],
                'expected_count': 0
            }
        ],
        'extraction_strategies': [
            {
                'name': 'incremental_tables',
                'strategy': 'incremental',
                'expected_tables': ['patient', 'appointment', 'procedurelog'],
                'expected_count': 3
            },
            {
                'name': 'full_table_tables',
                'strategy': 'full_table',
                'expected_tables': ['securitylog', 'definition'],
                'expected_count': 2
            },
            {
                'name': 'non_existing_strategy',
                'strategy': 'nonexistent',
                'expected_tables': [],
                'expected_count': 0
            }
        ],
        'size_thresholds': [
            {
                'name': 'large_tables_50mb',
                'threshold': 50.0,
                'expected_tables': ['procedurelog'],
                'expected_count': 1
            },
            {
                'name': 'large_tables_10mb',
                'threshold': 10.0,
                'expected_tables': ['patient', 'appointment', 'procedurelog'],
                'expected_count': 3
            },
            {
                'name': 'large_tables_200mb',
                'threshold': 200.0,
                'expected_tables': [],
                'expected_count': 0
            }
        ]
    }


@pytest.fixture
def config_reader_error_cases():
    """Error cases for ConfigReader testing."""
    return {
        'file_not_found': {
            'error_type': FileNotFoundError,
            'error_message': 'Configuration file not found: nonexistent.yml',
            'config_path': 'nonexistent.yml'
        },
        'invalid_yaml': {
            'error_type': yaml.YAMLError,
            'error_message': 'Invalid YAML format',
            'yaml_content': 'invalid: yaml: content: ['
        },
        'missing_tables_section': {
            'error_type': ValueError,
            'error_message': 'Invalid configuration file: missing_tables.yml',
            'config_content': {'other_section': {}}
        },
        'empty_config': {
            'error_type': ValueError,
            'error_message': 'Invalid configuration file: empty.yml',
            'config_content': None
        }
    }


@pytest.fixture
def config_reader_validation_cases():
    """Validation test cases for ConfigReader."""
    return {
        'valid_configurations': [
            {
                'name': 'complete_valid_config',
                'config': {
                    'tables': {
                        'patient': {
                            'batch_size': 1000,
                            'extraction_strategy': 'incremental',
                            'table_importance': 'critical',
                            'estimated_size_mb': 50.0,
                            'monitoring': {'alert_on_failure': True}
                        }
                    }
                },
                'expected_issues': {}
            }
        ],
        'invalid_configurations': [
            {
                'name': 'missing_batch_size',
                'config': {
                    'tables': {
                        'patient': {
                            'extraction_strategy': 'incremental',
                            'table_importance': 'critical'
                        }
                    }
                },
                'expected_issues': {
                    'missing_batch_size': ['patient']
                }
            },
            {
                'name': 'missing_extraction_strategy',
                'config': {
                    'tables': {
                        'patient': {
                            'batch_size': 1000,
                            'table_importance': 'critical'
                        }
                    }
                },
                'expected_issues': {
                    'missing_extraction_strategy': ['patient']
                }
            },
            {
                'name': 'missing_importance',
                'config': {
                    'tables': {
                        'patient': {
                            'batch_size': 1000,
                            'extraction_strategy': 'incremental'
                        }
                    }
                },
                'expected_issues': {
                    'missing_importance': ['patient']
                }
            },
            {
                'name': 'invalid_batch_size',
                'config': {
                    'tables': {
                        'patient': {
                            'batch_size': -1,
                            'extraction_strategy': 'incremental',
                            'table_importance': 'critical'
                        }
                    }
                },
                'expected_issues': {
                    'invalid_batch_size': ['patient']
                }
            },
            {
                'name': 'large_table_without_monitoring',
                'config': {
                    'tables': {
                        'patient': {
                            'batch_size': 1000,
                            'extraction_strategy': 'incremental',
                            'table_importance': 'critical',
                            'estimated_size_mb': 100.0,
                            'monitoring': {'alert_on_failure': False}
                        }
                    }
                },
                'expected_issues': {
                    'large_tables_without_monitoring': ['patient']
                }
            }
        ]
    }


@pytest.fixture
def config_reader_performance_data():
    """Performance test data for ConfigReader."""
    return {
        'large_config': {
            'tables': {
                f'table_{i}': {
                    'primary_key': f'id_{i}',
                    'incremental_column': f'updated_{i}',
                    'extraction_strategy': 'incremental' if i % 2 == 0 else 'full_table',
                    'table_importance': ['critical', 'important', 'reference', 'standard'][i % 4],
                    'batch_size': 1000 + (i * 100),
                    'estimated_size_mb': 10.0 + (i * 5.0),
                    'estimated_rows': 1000 + (i * 1000),
                    'monitoring': {
                        'alert_on_failure': i % 3 == 0,
                        'alert_on_slow_load': i % 4 == 0
                    },
                    'dependencies': [f'table_{j}' for j in range(max(0, i-3), i)],
                    'is_modeled': i % 2 == 0
                }
                for i in range(100)  # 100 tables for performance testing
            }
        }
    }


@pytest.fixture
def config_reader_dependency_test_data():
    """Test data for dependency testing."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'batch_size': 1000,
                'dependencies': []
            },
            'appointment': {
                'primary_key': 'AptNum',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 500,
                'dependencies': ['patient']
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 2000,
                'dependencies': ['patient', 'appointment']
            },
            'claim': {
                'primary_key': 'ClaimNum',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 1500,
                'dependencies': ['patient', 'procedurelog']
            },
            'payment': {
                'primary_key': 'PayNum',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 800,
                'dependencies': ['patient', 'claim']
            }
        }
    }


@pytest.fixture
def mock_config_reader_with_dependency_data(config_reader_dependency_test_data):
    """Mock ConfigReader with dependency test data."""
    class MockConfigReader(ConfigReader):
        def __init__(self, config_path: str = "mock_deps_config"):
            # Don't call parent constructor to avoid file loading
            self.config_path = config_path
            self.config = config_reader_dependency_test_data
            self._last_loaded = datetime.now()
    
    return MockConfigReader()


 