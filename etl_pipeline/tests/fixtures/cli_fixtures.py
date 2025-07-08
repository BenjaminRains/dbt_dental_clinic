"""
CLI fixtures for ETL pipeline tests.

This module contains fixtures related to:
- CLI command testing
- Click test runner setup
- CLI configuration injection
- Temporary configuration files
- CLI output validation
- Database connection mocking for CLI tests
"""

import os
import pytest
import tempfile
import yaml
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from typing import Dict, Any, Optional

from click.testing import CliRunner

# Import CLI components
from etl_pipeline.cli.commands import inject_test_settings, clear_test_settings
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config import Settings


@pytest.fixture
def cli_runner():
    """Standard Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def cli_test_config():
    """Test configuration specifically for CLI testing."""
    return {
        'pipeline': {
            'connections': {
                'source': {
                    'pool_size': 5,
                    'connect_timeout': 15,
                    'read_timeout': 45
                },
                'replication': {
                    'pool_size': 10,
                    'max_overflow': 20,
                    'pool_timeout': 30
                },
                'analytics': {
                    'application_name': 'etl_pipeline_cli_test',
                    'pool_size': 5,
                    'max_overflow': 10
                }
            },
            'logging': {
                'level': 'DEBUG',
                'file': {
                    'path': '/tmp/cli_test.log',
                    'max_size': '10MB',
                    'backup_count': 5
                }
            },
            'performance': {
                'max_workers': 2,  # Reduced for CLI tests
                'batch_size': 1000,
                'chunk_size': 500
            }
        },
        'tables': {
            'tables': {
                'patient': {
                    'primary_key': 'PatNum',
                    'incremental_column': 'DateTStamp',
                    'batch_size': 1000,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'critical',
                    'estimated_size_mb': 500,
                    'estimated_rows': 100000,
                    'monitoring': True
                },
                'appointment': {
                    'primary_key': 'AptNum',
                    'incremental_column': 'AptDateTime',
                    'batch_size': 500,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'important',
                    'estimated_size_mb': 200,
                    'estimated_rows': 50000,
                    'monitoring': True
                },
                'procedurelog': {
                    'primary_key': 'ProcNum',
                    'incremental_column': 'ProcDate',
                    'batch_size': 200,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'important',
                    'estimated_size_mb': 100,
                    'estimated_rows': 25000,
                    'monitoring': False
                },
                'payment': {
                    'primary_key': 'PayNum',
                    'incremental_column': 'DatePay',
                    'batch_size': 100,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'reference',
                    'estimated_size_mb': 50,
                    'estimated_rows': 10000,
                    'monitoring': False
                }
            }
        }
    }


@pytest.fixture
def cli_test_env_vars():
    """Test environment variables for CLI testing."""
    return {
        # Test environment variables for database connections
        'TEST_OPENDENTAL_SOURCE_HOST': 'test-opendental-host',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_password',
        
        'TEST_MYSQL_REPLICATION_HOST': 'test-replication-host',
        'TEST_MYSQL_REPLICATION_PORT': '3306',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_password',
        
        'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_password',
        
        # Environment detection
        'ETL_ENVIRONMENT': 'test'
    }


@pytest.fixture
def cli_config_provider(cli_test_config, cli_test_env_vars):
    """CLI test configuration provider."""
    return DictConfigProvider(
        pipeline=cli_test_config['pipeline'],
        tables=cli_test_config['tables'],
        env=cli_test_env_vars
    )


@pytest.fixture
def cli_test_settings(cli_config_provider):
    """CLI test settings with injected configuration."""
    return Settings(environment='test', provider=cli_config_provider)


@pytest.fixture
def cli_test_config_reader():
    """Test config reader that doesn't read files."""
    from etl_pipeline.config.config_reader import ConfigReader
    
    # Create a subclass of ConfigReader that overrides the file loading
    class TestConfigReader(ConfigReader):
        def __init__(self):
            # Don't call parent constructor to avoid file loading
            self.config = {
                'tables': {
                    'patient': {
                        'table_importance': 'critical',
                        'extraction_strategy': 'incremental',
                        'estimated_size_mb': 10.0,
                        'monitoring': {'alert_on_failure': True},
                        'dependencies': []
                    },
                    'appointment': {
                        'table_importance': 'important',
                        'extraction_strategy': 'incremental',
                        'estimated_size_mb': 5.0,
                        'monitoring': {'alert_on_failure': True},
                        'dependencies': ['patient']
                    },
                    'procedurelog': {
                        'table_importance': 'reference',
                        'extraction_strategy': 'incremental',
                        'estimated_size_mb': 15.0,
                        'monitoring': {'alert_on_failure': False},
                        'dependencies': ['patient']
                    }
                }
            }
            self.config_path = "test_config"
            self._last_loaded = None
    
    return TestConfigReader()


@pytest.fixture
def cli_with_injected_config(cli_test_settings):
    """Fixture that injects test configuration into CLI commands."""
    # Inject test settings
    inject_test_settings(cli_test_settings)
    
    yield  # Provide the fixture
    
    # Clean up after test
    clear_test_settings()


@pytest.fixture
def temp_cli_config_file(cli_test_config):
    """Temporary configuration file for CLI file-based tests."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(cli_test_config, f)
        config_path = f.name
    
    yield config_path
    
    # Clean up
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def temp_tables_config_file(cli_test_config):
    """Temporary tables configuration file for CLI tests."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(cli_test_config['tables'], f)
        config_path = f.name
    
    yield config_path
    
    # Clean up
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def mock_cli_database_connections():
    """Mock database connections for CLI integration tests."""
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
        # Mock connection factory methods
        mock_factory.get_opendental_source_connection.return_value = MagicMock()
        mock_factory.get_mysql_replication_connection.return_value = MagicMock()
        mock_factory.get_opendental_analytics_raw_connection.return_value = MagicMock()
        
        # Mock test connection methods
        mock_factory.get_opendental_source_test_connection.return_value = MagicMock()
        mock_factory.get_mysql_replication_test_connection.return_value = MagicMock()
        mock_factory.get_opendental_analytics_test_connection.return_value = MagicMock()
        
        yield mock_factory


@pytest.fixture
def mock_cli_file_system():
    """Mock file system operations for CLI tests that need file access."""
    with patch('pathlib.Path.exists') as mock_exists, \
         patch('builtins.open') as mock_open:
        
        # Mock file existence
        mock_exists.return_value = True
        
        # Mock file reading
        mock_open.return_value.__enter__.return_value.read.return_value = "test content"
        
        yield mock_exists, mock_open


@pytest.fixture
def cli_expected_outputs():
    """Expected CLI output patterns for validation."""
    return {
        'help': {
            'main_help': ['ETL Pipeline CLI', 'run', 'status', 'test-connections'],
            'run_help': ['Run the ETL pipeline', '--dry-run', '--tables', '--full']
        },
        'dry_run': {
            'header': ['DRY RUN MODE - No changes will be made'],
            'processing': ['Would process all tables by priority', 'Would process specific tables'],
            'table_counts': ['CRITICAL:', 'IMPORTANT:', 'REFERENCE:'],
            'strategy': ['Parallel processing', 'Sequential processing'],
            'footer': ['Dry run completed - no changes made']
        },
        'status': {
            'table_format': ['Pipeline Status', 'Table', 'Status', 'Last Sync'],
            'json_format': ['"status"', '"tables"', '"last_update"'],
            'summary_format': ['Pipeline Status Summary', 'Overall Status']
        },
        'test_connections': {
            'header': ['Testing database connections'],
            'connections': ['OpenDental source connection', 'MySQL replication connection', 'PostgreSQL analytics connection'],
            'success': ['OK'],
            'failure': ['Connection failed', 'WARNING', 'Error']
        }
    }


@pytest.fixture
def cli_error_cases():
    """Common CLI error cases for testing."""
    return {
        'invalid_config': {
            'file_not_found': 'Configuration file not found',
            'invalid_yaml': 'Invalid YAML',
            'missing_required': 'Missing required configuration'
        },
        'invalid_parameters': {
            'invalid_parallel': 'Parallel workers must be between 1 and 20',
            'empty_tables': 'No tables specified',
            'invalid_table': 'Table not found in configuration'
        },
        'connection_errors': {
            'connection_failed': 'Failed to initialize connections',
            'timeout': 'Connection timeout',
            'authentication': 'Authentication failed'
        }
    }


@pytest.fixture
def cli_performance_thresholds():
    """Performance thresholds for CLI command testing."""
    return {
        'help_command': 1.0,      # 1 second
        'dry_run': 3.0,           # 3 seconds
        'test_connections': 5.0,  # 5 seconds
        'status_command': 2.0,    # 2 seconds
        'full_run_simulation': 10.0  # 10 seconds
    }


@pytest.fixture
def cli_test_scenarios():
    """Test scenarios for CLI command validation."""
    return {
        'basic_commands': [
            'help',
            'test-connections',
            'run --dry-run'
        ],
        'run_variations': [
            'run --dry-run --tables patient',
            'run --dry-run --tables patient appointment',
            'run --dry-run --full',
            'run --dry-run --parallel 2',
            'run --dry-run --force'
        ],
        'status_variations': [
            'status --config test.yml',
            'status --config test.yml --format json',
            'status --config test.yml --format summary',
            'status --config test.yml --table patient',
            'status --config test.yml --output status.json'
        ],
        'error_scenarios': [
            'run --config nonexistent.yml',
            'run --parallel 0',
            'run --parallel 21',
            'run --tables',
            'status --config invalid.yml'
        ]
    }


@pytest.fixture
def cli_output_validators():
    """Validators for CLI output patterns."""
    
    def validate_dry_run_output(output: str, expected_tables: Optional[list] = None):
        """Validate dry run command output."""
        assert "DRY RUN MODE - No changes will be made" in output
        
        if expected_tables:
            assert f"Would process {len(expected_tables)} specific tables" in output
            for table in expected_tables:
                assert table in output
        else:
            assert "Would process all tables by priority" in output
            assert "CRITICAL:" in output
            assert "IMPORTANT:" in output
            assert "REFERENCE:" in output
        
        assert "Dry run completed - no changes made" in output
    
    def validate_status_output(output: str, format_type: str = 'table'):
        """Validate status command output."""
        if format_type == 'json':
            try:
                json.loads(output)
            except json.JSONDecodeError:
                # Might be error message instead of JSON
                assert "error" in output.lower() or "failed" in output.lower()
        elif format_type == 'summary':
            assert "Pipeline Status Summary" in output
        else:  # table format
            assert "Pipeline Status" in output or "No tables found" in output
    
    def validate_connection_test_output(output: str):
        """Validate connection test output."""
        assert "Testing database connections" in output
        assert any(conn in output for conn in ["Source database", "Replication database", "Analytics database"])
    
    return {
        'validate_dry_run_output': validate_dry_run_output,
        'validate_status_output': validate_status_output,
        'validate_connection_test_output': validate_connection_test_output
    }


@pytest.fixture
def cli_integration_test_data():
    """Test data for CLI integration testing."""
    return {
        'sample_tables': ['patient', 'appointment', 'procedurelog', 'payment'],
        'sample_configs': {
            'minimal': {'tables': {'test': {'table_importance': 'critical'}}},
            'complete': cli_test_config,
            'invalid': {'invalid': 'config'}
        },
        'expected_exit_codes': {
            'success': 0,
            'error': 1,
            'abort': 2
        }
    }


@pytest.fixture
def cli_mock_orchestrator():
    """Mock PipelineOrchestrator for CLI testing."""
    with patch('etl_pipeline.cli.commands.PipelineOrchestrator') as mock_class:
        mock_orchestrator = MagicMock()
        mock_class.return_value = mock_orchestrator
        
        # Mock orchestrator methods
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.return_value = {
            'critical': {'success': ['patient'], 'failed': []},
            'important': {'success': ['appointment'], 'failed': []}
        }
        mock_orchestrator.run_pipeline_for_table.return_value = True
        mock_orchestrator.cleanup.return_value = None
        
        yield mock_orchestrator


@pytest.fixture
def cli_mock_metrics_collector():
    """Mock UnifiedMetricsCollector for CLI status testing."""
    with patch('etl_pipeline.cli.commands.UnifiedMetricsCollector') as mock_class:
        mock_collector = MagicMock()
        mock_class.return_value = mock_collector
        
        # Mock metrics methods
        mock_collector.get_pipeline_status.return_value = {
            'last_update': '2024-01-01 12:00:00',
            'status': 'running',
            'tables': [
                {
                    'name': 'patient',
                    'status': 'completed',
                    'last_sync': '2024-01-01 11:00:00',
                    'records_processed': 1000,
                    'error': None
                }
            ]
        }
        
        yield mock_collector 