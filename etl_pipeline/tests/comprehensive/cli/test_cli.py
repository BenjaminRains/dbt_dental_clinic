"""
Comprehensive tests for ETL Pipeline CLI

STATUS: COMPREHENSIVE TESTS - Full functionality testing with mocked dependencies
================================================================================

This module provides comprehensive testing for the ETL Pipeline CLI interface,
following the three-tier testing approach with provider pattern dependency injection.

TEST TYPE: Comprehensive Tests (test_cli.py)
- Purpose: Full functionality testing with mocked dependencies and provider pattern
- Scope: Complete component behavior, error handling, all methods
- Coverage: 90%+ target coverage (main test suite)
- Execution: < 5 seconds per component
- Provider Usage: DictConfigProvider for comprehensive test scenarios
- Markers: @pytest.mark.unit (default)

TESTED METHODS:
- run(): Main pipeline execution command with all options
- status(): Pipeline status reporting command with all formats
- test_connections(): Database connection validation command
- _execute_dry_run(): Dry run execution logic with all scenarios
- _display_status(): Status display formatting for all formats
- CLI entry points and subprocess execution

TESTING APPROACH:
- Comprehensive Testing: Full functionality with mocked dependencies
- Provider Pattern: DictConfigProvider for dependency injection
- Mocked Dependencies: All external components mocked
- File I/O Testing: Temporary files and configuration handling
- Subprocess Testing: Real CLI invocation testing
- Error Scenarios: Comprehensive error handling testing
- Performance Testing: Execution time validation

MOCKED COMPONENTS:
- PipelineOrchestrator: Main pipeline orchestration
- ConnectionFactory: Database connection management
- UnifiedMetricsCollector: Status and metrics reporting
- Settings: Configuration management
- File Operations: YAML loading, file writing, temporary files
- Click Context: CLI context and output
- Subprocess: CLI execution testing

PROVIDER PATTERN USAGE:
- DictConfigProvider: Injected test configuration
- FileConfigProvider: Real configuration file testing
- Environment Variables: Mocked through provider
- Configuration Validation: Complete configuration testing

This test suite ensures the CLI commands work correctly with comprehensive
coverage of all scenarios, error conditions, and user interactions.
"""
import os
import sys
import subprocess
import logging
import tempfile
import yaml
import json
import time
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock, mock_open, call

import pytest
from click.testing import CliRunner
from click import ClickException

# Import CLI components
from etl_pipeline.cli.main import cli
from etl_pipeline.cli.commands import run, status, test_connections, _execute_dry_run, _display_status

# Import provider pattern components
from etl_pipeline.config.providers import DictConfigProvider, FileConfigProvider
from etl_pipeline.config.settings import DatabaseType, PostgresSchema

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestCLIComprehensive:
    """
    Comprehensive tests for ETL CLI commands.
    
    This class tests the complete CLI functionality including:
    - All command options and flags
    - Configuration file handling
    - Error scenarios and edge cases
    - Subprocess execution
    - File I/O operations
    - Performance characteristics
    
    Uses comprehensive mocking and provider pattern for complete testing.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        
        # Create comprehensive test provider with injected configuration
        self.test_provider = DictConfigProvider(
            pipeline={
                'connections': {
                    'source': {'pool_size': 5, 'connect_timeout': 15},
                    'replication': {'pool_size': 10, 'max_overflow': 20},
                    'analytics': {'application_name': 'etl_pipeline_test'}
                },
                'logging': {'level': 'DEBUG', 'file': {'path': 'test.log'}},
                'performance': {'max_workers': 4, 'batch_size': 1000}
            },
            tables={
                'tables': {
                    'patient': {
                        'incremental_column': 'DateModified',
                        'batch_size': 1000,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical',
                        'estimated_size_mb': 500,
                        'estimated_rows': 100000
                    },
                    'appointment': {
                        'incremental_column': 'DateModified',
                        'batch_size': 500,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'important',
                        'estimated_size_mb': 200,
                        'estimated_rows': 50000
                    },
                    'procedure': {
                        'incremental_column': 'DateModified',
                        'batch_size': 200,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'audit',
                        'estimated_size_mb': 100,
                        'estimated_rows': 25000
                    },
                    'payment': {
                        'incremental_column': 'DateModified',
                        'batch_size': 100,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'reference',
                        'estimated_size_mb': 50,
                        'estimated_rows': 10000
                    }
                }
            },
            env={
                'OPENDENTAL_SOURCE_HOST': 'test-source-host',
                'OPENDENTAL_SOURCE_DB': 'test_opendental',
                'OPENDENTAL_SOURCE_USER': 'test_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'MYSQL_REPLICATION_HOST': 'test-repl-host',
                'MYSQL_REPLICATION_DB': 'test_opendental_repl',
                'POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'ETL_ENVIRONMENT': 'test'
            }
        )
    
    @pytest.mark.unit
    def test_cli_help_comprehensive(self):
        """Test comprehensive CLI help functionality."""
        # Test main help
        result = self.runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'ETL Pipeline CLI' in result.output
        assert 'run' in result.output
        assert 'status' in result.output
        assert 'test-connections' in result.output
        
        # Test run command help
        result = self.runner.invoke(cli, ['run', '--help'])
        assert result.exit_code == 0
        assert 'Run the ETL pipeline' in result.output
        assert '--config' in result.output
        assert '--tables' in result.output
        assert '--full' in result.output
        assert '--force' in result.output
        assert '--parallel' in result.output
        assert '--dry-run' in result.output
        
        # Test status command help
        result = self.runner.invoke(cli, ['status', '--help'])
        assert result.exit_code == 0
        assert 'Show current status' in result.output
        assert '--config' in result.output
        assert '--format' in result.output
        assert '--table' in result.output
        assert '--watch' in result.output
        assert '--output' in result.output
        
        # Test test-connections command help
        result = self.runner.invoke(cli, ['test-connections', '--help'])
        assert result.exit_code == 0
        assert 'Test all database connections' in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_all_options(self, mock_orchestrator_class):
        """Test run command with all possible options."""
        # Create temporary config file for testing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config_data = {
                'pipeline': {
                    'connections': {'source': {'pool_size': 5}},
                    'logging': {'level': 'DEBUG'}
                },
                'tables': {
                    'tables': {
                        'patient': {'table_importance': 'critical'},
                        'appointment': {'table_importance': 'important'}
                    }
                }
            }
            yaml.dump(config_data, f)
            config_path = f.name

        try:
            # Setup mock orchestrator
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.process_tables_by_priority.return_value = {
                'critical': {'success': ['patient'], 'failed': []},
                'important': {'success': ['appointment'], 'failed': []}
            }
            
            # Test run command with all options
            result = self.runner.invoke(cli, [
                'run', 
                '--config', config_path,
                '--tables', 'patient', '--tables', 'appointment',
                '--full',
                '--force',
                '--parallel', '8'
            ])
            
            # Verify results
            assert result.exit_code == 0
            assert "Full pipeline mode: Forcing full refresh for all tables" in result.output
            assert "Processing 2 specific tables: patient, appointment" in result.output
            assert "Pipeline completed successfully" in result.output
            
            # Verify orchestrator was called correctly
            mock_orchestrator_class.assert_called_once_with(config_path=config_path)
            assert mock_orchestrator.run_pipeline_for_table.call_count == 2
            mock_orchestrator.run_pipeline_for_table.assert_any_call('patient', force_full=True)
            mock_orchestrator.run_pipeline_for_table.assert_any_call('appointment', force_full=True)
        finally:
            # Clean up temporary file
            os.unlink(config_path)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_with_config_file(self, mock_orchestrator_class):
        """Test run command with configuration file."""
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config_data = {
                'pipeline': {
                    'connections': {'source': {'pool_size': 5}},
                    'logging': {'level': 'DEBUG'}
                },
                'tables': {
                    'tables': {
                        'patient': {'table_importance': 'critical'}
                    }
                }
            }
            yaml.dump(config_data, f)
            config_path = f.name

        try:
            # Setup mock orchestrator
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.process_tables_by_priority.return_value = {
                'critical': {'success': ['patient'], 'failed': []}
            }
            
            # Test run command with config file
            result = self.runner.invoke(cli, ['run', '--config', config_path])
            
            # Verify results
            assert result.exit_code == 0
            assert "Pipeline completed successfully" in result.output
            
            # Verify orchestrator was called with config file
            mock_orchestrator_class.assert_called_once_with(config_path=config_path)
            
        finally:
            # Clean up temporary file
            os.unlink(config_path)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_invalid_config_file(self, mock_orchestrator_class):
        """Test run command with invalid configuration file."""
        # Test with non-existent config file
        result = self.runner.invoke(cli, ['run', '--config', 'nonexistent.yaml'])
        assert result.exit_code != 0
        
        # Test with invalid YAML config file that will cause orchestrator to fail
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            config_path = f.name

        try:
            # Mock orchestrator to fail when it tries to load the invalid config
            mock_orchestrator_class.side_effect = Exception("Invalid YAML configuration")
            
            result = self.runner.invoke(cli, ['run', '--config', config_path])
            assert result.exit_code != 0
            assert "Invalid YAML configuration" in result.output
        finally:
            os.unlink(config_path)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_invalid_config_file_yaml_error(self, mock_orchestrator_class):
        """Test run command with malformed YAML configuration file."""
        # Test with malformed YAML that will cause parsing error
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("pipeline:\n  connections:\n    source:\n      pool_size: invalid_value\n")
            config_path = f.name

        try:
            # Mock orchestrator to raise exception on invalid config
            mock_orchestrator_class.side_effect = Exception("Invalid configuration")
            
            result = self.runner.invoke(cli, ['run', '--config', config_path])
            assert result.exit_code != 0
            assert "Invalid configuration" in result.output
        finally:
            os.unlink(config_path)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_edge_cases(self, mock_orchestrator_class):
        """Test run command edge cases and error conditions."""
        # Test with empty tables list
        result = self.runner.invoke(cli, ['run', '--tables'])
        assert result.exit_code != 0
        
        # Test with invalid parallel workers
        result = self.runner.invoke(cli, ['run', '--parallel', '0'])
        assert result.exit_code != 0
        assert "Parallel workers must be between 1 and 20" in result.output
        
        result = self.runner.invoke(cli, ['run', '--parallel', '21'])
        assert result.exit_code != 0
        assert "Parallel workers must be between 1 and 20" in result.output
        
        # Test with non-numeric parallel workers
        result = self.runner.invoke(cli, ['run', '--parallel', 'invalid'])
        assert result.exit_code != 0
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_exception_handling_comprehensive(self, mock_orchestrator_class):
        """Test comprehensive exception handling in run command."""
        # Test connection initialization failure
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = False
        
        result = self.runner.invoke(cli, ['run'])
        assert result.exit_code != 0
        assert "Failed to initialize connections" in result.output
        
        # Test table processing failure
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.run_pipeline_for_table.return_value = False
        
        result = self.runner.invoke(cli, ['run', '--tables', 'patient'])
        assert result.exit_code != 0
        assert "Failed to process table: patient" in result.output
        
        # Test priority processing failure
        mock_orchestrator.run_pipeline_for_table.return_value = True
        mock_orchestrator.process_tables_by_priority.return_value = {
            'critical': {'success': [], 'failed': ['patient', 'appointment']}
        }
        
        result = self.runner.invoke(cli, ['run'])
        assert result.exit_code != 0
        assert "Failed to process tables in critical" in result.output
        
        # Test orchestrator initialization failure
        mock_orchestrator_class.side_effect = Exception("Orchestrator error")
        
        result = self.runner.invoke(cli, ['run'])
        assert result.exit_code != 0
        assert "Orchestrator error" in result.output


class TestCLIDryRunComprehensive:
    """
    Comprehensive tests for CLI dry run functionality.
    
    Tests the _execute_dry_run function with all scenarios and edge cases.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_all_scenarios(self, mock_orchestrator_class):
        """Test dry run with all possible scenarios."""
        # Create temporary config file for testing
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            config_data = {
                'pipeline': {
                    'connections': {'source': {'pool_size': 5}},
                    'logging': {'level': 'DEBUG'}
                },
                'tables': {
                    'tables': {
                        'patient': {'table_importance': 'critical'},
                        'appointment': {'table_importance': 'important'}
                    }
                }
            }
            yaml.dump(config_data, f)
            config_path = f.name

        try:
            # Setup mock orchestrator
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            
            # Mock settings for comprehensive table information
            mock_settings = MagicMock()
            mock_orchestrator.settings = mock_settings
            mock_settings.get_tables_by_importance.side_effect = lambda importance: {
                'critical': ['patient', 'appointment'],
                'important': ['procedure', 'payment'],
                'audit': ['audit_log'],
                'reference': ['zipcode', 'carrier']
            }.get(importance, [])
            
            # Test dry run with all options
            result = self.runner.invoke(cli, [
                'run', '--dry-run', 
                '--config', config_path,
                '--tables', 'patient', '--tables', 'appointment',
                '--full',
                '--force',
                '--parallel', '8'
            ])
            
            # Verify results
            assert result.exit_code == 0
            assert "DRY RUN MODE - No changes will be made" in result.output
            assert f"Config file: {config_path}" in result.output
            assert "Parallel workers: 8" in result.output
            assert "Force full run: True" in result.output
            assert "Full pipeline mode: True" in result.output
            assert "All connections successful" in result.output
            assert "Would process 2 specific tables" in result.output
            assert "1. patient" in result.output
            assert "2. appointment" in result.output
            assert "Sequential processing of specified tables" in result.output
            assert "Full refresh mode: All data will be re-extracted" in result.output
            assert "Dry run completed - no changes made" in result.output
            
            # Verify that process_tables_by_priority was NOT called (dry run mode)
            mock_orchestrator.process_tables_by_priority.assert_not_called()
        finally:
            # Clean up temporary file
            os.unlink(config_path)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_connection_failures(self, mock_orchestrator_class):
        """Test dry run with various connection failure scenarios."""
        # Test connection initialization failure
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = False
        
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        assert result.exit_code == 0  # Dry run should not fail, just warn
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Connection test failed" in result.output
        assert "Pipeline would fail during execution" in result.output
        
        # Test connection initialization exception
        mock_orchestrator.initialize_connections.side_effect = Exception("Connection error")
        
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        assert result.exit_code == 0  # Dry run should not fail, just warn
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Connection test failed: Connection error" in result.output
        assert "Pipeline would fail during execution" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_table_information_errors(self, mock_orchestrator_class):
        """Test dry run with table information retrieval errors."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        
        # Mock settings with errors
        mock_settings = MagicMock()
        mock_orchestrator.settings = mock_settings
        mock_settings.get_tables_by_importance.side_effect = Exception("Settings error")
        
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Error getting tables - Settings error" in result.output


class TestCLIStatusComprehensive:
    """
    Comprehensive tests for CLI status command.
    
    Tests the status command with all formats, options, and scenarios.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_all_formats(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test status command with all output formats."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_opendental_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'last_update': '2024-01-01 12:00:00',
            'status': 'running',
            'tables': [
                {
                    'name': 'patient',
                    'status': 'completed',
                    'last_sync': '2024-01-01 11:00:00',
                    'records_processed': 1000,
                    'error': None
                },
                {
                    'name': 'appointment',
                    'status': 'failed',
                    'last_sync': '2024-01-01 10:00:00',
                    'records_processed': 500,
                    'error': 'Connection timeout'
                }
            ]
        }
        
        # Test table format (default)
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--format', 'table'
        ])
        assert result.exit_code == 0
        assert "Pipeline Status" in result.output
        assert "patient" in result.output
        assert "appointment" in result.output
        assert "completed" in result.output
        assert "failed" in result.output
        
        # Test JSON format
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--format', 'json'
        ])
        assert result.exit_code == 0
        assert '"status": "running"' in result.output
        assert '"name": "patient"' in result.output
        
        # Test summary format
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--format', 'summary'
        ])
        assert result.exit_code == 0
        assert "Pipeline Status Summary" in result.output
        assert "Total Tables: 2" in result.output
        assert "Last Update: 2024-01-01 12:00:00" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_output_file(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test status command with output file generation."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_opendental_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'status': 'running',
            'tables': []
        }
        
        # Test JSON output to file
        with patch('builtins.open', mock_open()) as mock_output_file:
            result = self.runner.invoke(cli, [
                'status', '--config', 'test_config.yaml', 
                '--format', 'json', '--output', 'status_report.json'
            ])
            
            assert result.exit_code == 0
            assert "Status report written to" in result.output
            
            # Verify file was opened for writing
            mock_output_file.assert_called_with('status_report.json', 'w')
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_specific_table(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test status command with specific table filtering."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_opendental_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'status': 'running',
            'tables': [{'name': 'patient', 'status': 'completed'}]
        }
        
        # Test with specific table
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--table', 'patient'
        ])
        
        assert result.exit_code == 0
        mock_metrics.get_pipeline_status.assert_called_once_with(table='patient')
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_command_error_handling(self, mock_conn_factory, mock_metrics_class):
        """Test status command error handling."""
        # Test with invalid config file
        result = self.runner.invoke(cli, ['status', '--config', 'nonexistent.yaml'])
        assert result.exit_code != 0
        
        # Test with metrics collection failure
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_opendental_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.side_effect = Exception("Metrics error")
        
        with patch('builtins.open', mock_open(read_data='{"pipeline": {}}')):
            result = self.runner.invoke(cli, ['status', '--config', 'test_config.yaml'])
            assert result.exit_code != 0
            assert "Metrics error" in result.output


class TestCLIConnectionTestingComprehensive:
    """
    Comprehensive tests for CLI connection testing command.
    
    Tests the test_connections command with all scenarios.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_test_connections_all_scenarios(self, mock_conn_factory):
        """Test connection testing with all scenarios."""
        # Setup mock engines
        mock_source_engine = MagicMock()
        mock_repl_engine = MagicMock()
        mock_analytics_engine = MagicMock()
        
        # Setup mock connection context managers
        mock_source_conn = MagicMock()
        mock_repl_conn = MagicMock()
        mock_analytics_conn = MagicMock()
        
        mock_source_engine.connect.return_value.__enter__.return_value = mock_source_conn
        mock_source_engine.connect.return_value.__exit__.return_value = None
        mock_repl_engine.connect.return_value.__enter__.return_value = mock_repl_conn
        mock_repl_engine.connect.return_value.__exit__.return_value = None
        mock_analytics_engine.connect.return_value.__enter__.return_value = mock_analytics_conn
        mock_analytics_engine.connect.return_value.__exit__.return_value = None
        
        # Setup connection factory
        mock_conn_factory.get_opendental_source_connection.return_value = mock_source_engine
        mock_conn_factory.get_mysql_replication_connection.return_value = mock_repl_engine
        mock_conn_factory.get_opendental_analytics_raw_connection.return_value = mock_analytics_engine
        
        # Test successful connection testing
        result = self.runner.invoke(cli, ['test-connections'])
        
        # Verify results
        assert result.exit_code == 0
        assert "✅ OpenDental source connection: OK" in result.output
        assert "✅ MySQL replication connection: OK" in result.output
        assert "✅ PostgreSQL analytics connection: OK" in result.output
        
        # Verify engines were disposed
        mock_source_engine.dispose.assert_called_once()
        mock_repl_engine.dispose.assert_called_once()
        mock_analytics_engine.dispose.assert_called_once()
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_source_connection_failure(self, mock_conn_factory):
        """Test source connection failure scenario."""
        mock_conn_factory.get_opendental_source_connection.side_effect = Exception("Source connection failed")
        
        result = self.runner.invoke(cli, ['test-connections'])
        assert result.exit_code != 0
        assert "Source connection failed" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_replication_connection_failure(self, mock_conn_factory):
        """Test replication connection failure scenario."""
        mock_conn_factory.get_mysql_replication_connection.side_effect = Exception("Replication connection failed")
        
        result = self.runner.invoke(cli, ['test-connections'])
        assert result.exit_code != 0
        assert "Replication connection failed" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_analytics_connection_failure(self, mock_conn_factory):
        """Test analytics connection failure scenario."""
        mock_conn_factory.get_opendental_analytics_raw_connection.side_effect = Exception("Analytics connection failed")
        
        result = self.runner.invoke(cli, ['test-connections'])
        assert result.exit_code != 0
        assert "Analytics connection failed" in result.output


class TestCLISubprocessComprehensive:
    """
    Comprehensive tests for CLI subprocess execution.
    
    Tests CLI execution as it would be used in real environments.
    """
    
    @pytest.mark.unit
    def test_cli_via_subprocess(self):
        """Test CLI via subprocess to ensure it works as a standalone command."""
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'etl_pipeline.cli.main', '--help'],
                capture_output=True,
                text=True,
                timeout=10
            )
            assert result.returncode == 0
            assert 'ETL Pipeline CLI' in result.stdout
        except subprocess.TimeoutExpired:
            pytest.fail("CLI command timed out")
        except FileNotFoundError:
            pytest.skip("CLI module not available as subprocess")
    
    @pytest.mark.unit
    def test_cli_entry_point(self):
        """Test CLI entry point functionality."""
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'etl_pipeline.cli', '--help'],
                capture_output=True,
                text=True,
                timeout=10
            )
            assert result.returncode == 0
            assert 'ETL Pipeline CLI' in result.stdout
        except subprocess.TimeoutExpired:
            pytest.fail("CLI entry point timed out")
        except FileNotFoundError:
            pytest.skip("CLI entry point not available")
    
    @pytest.mark.unit
    def test_cli_performance(self):
        """Test CLI performance characteristics."""
        import time
        
        # Test help command performance
        start_time = time.time()
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        end_time = time.time()
        
        assert result.exit_code == 0
        assert (end_time - start_time) < 1.0  # Should complete in under 1 second


class TestCLIHelperFunctionsComprehensive:
    """
    Comprehensive tests for CLI helper functions.
    
    Tests the internal helper functions with all scenarios.
    """
    
    @pytest.mark.unit
    def test_display_status_all_formats(self):
        """Test _display_status function with all formats and scenarios."""
        # Test table format with data
        status_data = {
            'tables': [
                {
                    'name': 'patient',
                    'status': 'completed',
                    'last_sync': '2024-01-01 11:00:00',
                    'records_processed': 1000,
                    'error': None
                },
                {
                    'name': 'appointment',
                    'status': 'failed',
                    'last_sync': '2024-01-01 10:00:00',
                    'records_processed': 500,
                    'error': 'Connection timeout'
                }
            ]
        }
        
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'table')
            
            # Verify table was displayed
            mock_echo.assert_called()
            calls = [call[0][0] for call in mock_echo.call_args_list]
            assert any('Pipeline Status' in call for call in calls)
            assert any('patient' in call for call in calls)
            assert any('appointment' in call for call in calls)
            assert any('completed' in call for call in calls)
            assert any('failed' in call for call in calls)
        
        # Test JSON format
        status_data = {
            'status': 'running',
            'tables': [{'name': 'patient', 'status': 'completed'}]
        }
        
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'json')
            
            # Verify JSON was displayed
            mock_echo.assert_called_once()
            output = mock_echo.call_args[0][0]
            assert '"status": "running"' in output
            assert '"name": "patient"' in output
        
        # Test summary format
        status_data = {
            'last_update': '2024-01-01 12:00:00',
            'status': 'running',
            'tables': [{'name': 'patient'}, {'name': 'appointment'}]
        }
        
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'summary')
            
            # Verify summary was displayed
            mock_echo.assert_called()
            calls = [call[0][0] for call in mock_echo.call_args_list]
            assert any('Pipeline Status Summary' in call for call in calls)
            assert any('Total Tables: 2' in call for call in calls)
            assert any('Last Update: 2024-01-01 12:00:00' in call for call in calls)
    
    @pytest.mark.unit
    def test_display_status_edge_cases(self):
        """Test _display_status function with edge cases."""
        # Test empty tables
        status_data = {'tables': []}
        
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'table')
            
            # Verify empty message was displayed
            mock_echo.assert_called_once()
            assert "No tables found in pipeline status" in mock_echo.call_args[0][0]
        
        # Test missing tables key
        status_data = {'status': 'running'}
        
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'table')
            
            # Verify empty message was displayed
            mock_echo.assert_called_once()
            assert "No tables found in pipeline status" in mock_echo.call_args[0][0]
        
        # Test invalid format
        status_data = {'tables': [{'name': 'patient'}]}
        
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'invalid_format')
            
            # Should default to table format
            mock_echo.assert_called()
            calls = [call[0][0] for call in mock_echo.call_args_list]
            assert any('Pipeline Status' in call for call in calls)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 