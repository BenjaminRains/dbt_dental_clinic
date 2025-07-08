"""
Unit tests for ETL Pipeline CLI

STATUS: UNIT TESTS - Pure unit testing with provider pattern
============================================================

This module provides pure unit tests for the ETL Pipeline CLI interface,
following the three-tier testing approach with provider pattern dependency injection.

TEST TYPE: Unit Tests (test_cli_unit.py)
- Purpose: Pure unit tests with comprehensive mocking and provider pattern
- Scope: Fast execution, isolated component behavior, no real connections
- Coverage: Core logic and edge cases for all CLI methods
- Execution: < 1 second per component
- Provider Usage: DictConfigProvider for injected test configuration
- Markers: @pytest.mark.unit

TESTED METHODS:
- run(): Main pipeline execution command
- status(): Pipeline status reporting command  
- test_connections(): Database connection validation command
- _execute_dry_run(): Dry run execution logic
- _display_status(): Status display formatting

TESTING APPROACH:
- Pure Unit Testing: Individual command testing with comprehensive mocking
- Provider Pattern: DictConfigProvider for dependency injection
- No Real Connections: All database operations mocked
- No File I/O: All file operations mocked
- Fast Execution: < 1 second per test
- Isolated Tests: No shared state between tests

MOCKED COMPONENTS:
- PipelineOrchestrator: Main pipeline orchestration
- ConnectionFactory: Database connection management
- UnifiedMetricsCollector: Status and metrics reporting
- Settings: Configuration management
- File Operations: YAML loading, file writing
- Click Context: CLI context and output

PROVIDER PATTERN USAGE:
- DictConfigProvider: Injected test configuration
- No FileConfigProvider: Pure unit tests don't use real files
- Environment Variables: Mocked through provider
- Configuration Isolation: Complete test isolation

This test suite ensures the CLI commands work correctly in isolation
with proper error handling and user interaction.
"""
import pytest
from unittest.mock import patch, MagicMock, Mock, mock_open, call
from click.testing import CliRunner
from click import ClickException
import yaml
import json
import tempfile
import os

# Import CLI components
from etl_pipeline.cli.main import cli
from etl_pipeline.cli.commands import run, status, test_connections, _execute_dry_run, _display_status

# Import provider pattern components
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import DatabaseType, PostgresSchema

# Configure logging for debugging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestCLIUnit:
    """
    Unit tests for ETL CLI commands.
    
    This class tests the core CLI functionality using pure unit testing:
    - Help command display
    - Pipeline execution with mocked components
    - Status reporting with mocked metrics
    - Connection testing with mocked connections
    
    Uses comprehensive mocking and provider pattern for complete isolation.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        
        # Create test provider with injected configuration
        self.test_provider = DictConfigProvider(
            pipeline={
                'connections': {
                    'source': {'pool_size': 5, 'connect_timeout': 15},
                    'replication': {'pool_size': 10, 'max_overflow': 20},
                    'analytics': {'application_name': 'etl_pipeline_test'}
                },
                'logging': {'level': 'DEBUG', 'file': {'path': 'test.log'}}
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
                'POSTGRES_ANALYTICS_SCHEMA': 'raw'
            }
        )
    
    @pytest.mark.unit
    def test_cli_help(self):
        """Test that CLI help works correctly."""
        result = self.runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'ETL Pipeline CLI' in result.output
        assert 'run' in result.output
        assert 'status' in result.output
        assert 'test-connections' in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_success(self, mock_orchestrator_class):
        """Test successful pipeline execution."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.return_value = {
            'critical': {'success': ['patient'], 'failed': []},
            'important': {'success': ['appointment'], 'failed': []}
        }
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code == 0
        assert "Pipeline completed successfully" in result.output
        
        # Verify orchestrator was called correctly
        mock_orchestrator_class.assert_called_once_with(config_path=None)
        mock_orchestrator.initialize_connections.assert_called_once()
        mock_orchestrator.process_tables_by_priority.assert_called_once_with(
            max_workers=4,
            force_full=False
        )
        mock_orchestrator.cleanup.assert_called_once()
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_with_specific_tables(self, mock_orchestrator_class):
        """Test pipeline execution with specific tables."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.run_pipeline_for_table.return_value = True
        
        # Test run command with specific tables
        result = self.runner.invoke(cli, ['run', '--tables', 'patient', '--tables', 'appointment'])
        
        # Verify results
        assert result.exit_code == 0
        assert "Processing 2 specific tables: patient, appointment" in result.output
        assert "Pipeline completed successfully" in result.output
        
        # Verify orchestrator was called correctly
        assert mock_orchestrator.run_pipeline_for_table.call_count == 2
        mock_orchestrator.run_pipeline_for_table.assert_any_call('patient', force_full=False)
        mock_orchestrator.run_pipeline_for_table.assert_any_call('appointment', force_full=False)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_with_full_flag(self, mock_orchestrator_class):
        """Test pipeline execution with --full flag."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.return_value = {
            'critical': {'success': ['patient'], 'failed': []}
        }
        
        # Test run command with --full flag
        result = self.runner.invoke(cli, ['run', '--full'])
        
        # Verify results
        assert result.exit_code == 0
        assert "Full pipeline mode: Forcing full refresh for all tables" in result.output
        assert "Pipeline completed successfully" in result.output
        
        # Verify orchestrator was called with force_full=True
        mock_orchestrator.process_tables_by_priority.assert_called_once_with(
            max_workers=4,
            force_full=True
        )
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_with_parallel_workers(self, mock_orchestrator_class):
        """Test pipeline execution with custom parallel workers."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.return_value = {
            'critical': {'success': ['patient'], 'failed': []}
        }
        
        # Test run command with custom parallel workers
        result = self.runner.invoke(cli, ['run', '--parallel', '8'])
        
        # Verify results
        assert result.exit_code == 0
        assert "Processing all tables by priority with 8 workers" in result.output
        
        # Verify orchestrator was called with correct workers
        mock_orchestrator.process_tables_by_priority.assert_called_once_with(
            max_workers=8,
            force_full=False
        )
    
    @pytest.mark.unit
    def test_run_command_invalid_parallel_workers(self):
        """Test pipeline execution with invalid parallel workers."""
        # Test with 0 workers
        result = self.runner.invoke(cli, ['run', '--parallel', '0'])
        assert result.exit_code != 0
        assert "Parallel workers must be between 1 and 20" in result.output
        
        # Test with 21 workers
        result = self.runner.invoke(cli, ['run', '--parallel', '21'])
        assert result.exit_code != 0
        assert "Parallel workers must be between 1 and 20" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_connection_failure(self, mock_orchestrator_class):
        """Test pipeline execution when connection initialization fails."""
        # Setup mock orchestrator to fail connection initialization
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = False
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "Failed to initialize connections" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_table_failure(self, mock_orchestrator_class):
        """Test pipeline execution when table processing fails."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.run_pipeline_for_table.return_value = False
        
        # Test run command with specific table
        result = self.runner.invoke(cli, ['run', '--tables', 'patient'])
        
        # Verify results
        assert result.exit_code != 0
        assert "Failed to process table: patient" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_priority_failure(self, mock_orchestrator_class):
        """Test pipeline execution when priority processing fails."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.return_value = {
            'critical': {'success': [], 'failed': ['patient', 'appointment']}
        }
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "Failed to process tables in critical" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_exception_handling(self, mock_orchestrator_class):
        """Test pipeline execution exception handling."""
        # Setup mock orchestrator to raise exception
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.side_effect = Exception("Test error")
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "Test error" in result.output


class TestCLIDryRunUnit:
    """
    Unit tests for CLI dry run functionality.
    
    Tests the _execute_dry_run function with comprehensive mocking.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        
        # Create test provider with injected configuration
        self.test_provider = DictConfigProvider(
            pipeline={'connections': {'source': {'pool_size': 5}}},
            tables={
                'tables': {
                    'patient': {'table_importance': 'critical', 'batch_size': 1000},
                    'appointment': {'table_importance': 'important', 'batch_size': 500},
                    'procedure': {'table_importance': 'audit', 'batch_size': 200},
                    'payment': {'table_importance': 'reference', 'batch_size': 100}
                }
            },
            env={'ETL_ENVIRONMENT': 'test'}
        )
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_success(self, mock_orchestrator_class):
        """Test successful dry run execution."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        
        # Mock settings for table information
        mock_settings = MagicMock()
        mock_orchestrator.settings = mock_settings
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient'],
            'important': ['appointment'],
            'audit': ['procedure'],
            'reference': ['payment']
        }.get(importance, [])
        
        # Test dry run command
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "All connections successful" in result.output
        assert "Would process all tables by priority" in result.output
        assert "CRITICAL: 1 tables" in result.output
        assert "IMPORTANT: 1 tables" in result.output
        assert "Total tables to process: 4" in result.output
        assert "Dry run completed - no changes made" in result.output
        
        # Verify that process_tables_by_priority was NOT called (dry run mode)
        mock_orchestrator.process_tables_by_priority.assert_not_called()
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_with_specific_tables(self, mock_orchestrator_class):
        """Test dry run execution with specific tables."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        
        # Test dry run command with specific tables
        result = self.runner.invoke(cli, [
            'run', '--dry-run', '--tables', 'patient', '--tables', 'appointment'
        ])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Would process 2 specific tables" in result.output
        assert "1. patient" in result.output
        assert "2. appointment" in result.output
        assert "Sequential processing of specified tables" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_connection_failure(self, mock_orchestrator_class):
        """Test dry run execution when connections fail."""
        # Setup mock orchestrator to fail connection initialization
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = False
        
        # Test dry run command
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Verify results
        assert result.exit_code == 0  # Dry run should not fail, just warn
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Connection test failed" in result.output
        assert "Pipeline would fail during execution" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_with_full_flag(self, mock_orchestrator_class):
        """Test dry run execution with --full flag."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        
        # Mock settings for table information
        mock_settings = MagicMock()
        mock_orchestrator.settings = mock_settings
        mock_settings.get_tables_by_importance.return_value = ['patient']
        
        # Test dry run command with --full flag
        result = self.runner.invoke(cli, ['run', '--dry-run', '--full'])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Full pipeline mode: True" in result.output
        assert "Full refresh mode: All data will be re-extracted" in result.output


class TestCLIStatusUnit:
    """
    Unit tests for CLI status command.
    
    Tests the status command with comprehensive mocking.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        
        # Create test provider with injected configuration
        self.test_provider = DictConfigProvider(
            pipeline={'connections': {'analytics': {'pool_size': 5}}},
            tables={'tables': {'patient': {'table_importance': 'critical'}}},
            env={'POSTGRES_ANALYTICS_HOST': 'test-host'}
        )
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_success(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test successful status command execution."""
        # Setup mock connection factory
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        # Setup mock metrics collector
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
                }
            ]
        }
        
        # Test status command
        result = self.runner.invoke(cli, ['status', '--config', 'test_config.yaml'])
        
        # Verify results
        assert result.exit_code == 0
        assert "Pipeline Status" in result.output
        assert "patient" in result.output
        
        # Verify mocks were called correctly
        mock_conn_factory.get_analytics_raw_connection.assert_called_once()
        mock_metrics_class.assert_called_once_with(analytics_engine=mock_analytics_engine, settings=mock_conn_factory.get_analytics_raw_connection.call_args[0][0])
        mock_metrics.get_pipeline_status.assert_called_once_with(table=None)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_json_format(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test status command with JSON format."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'status': 'running',
            'tables': []
        }
        
        # Test status command with JSON format
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--format', 'json'
        ])
        
        # Verify results
        assert result.exit_code == 0
        assert '"status": "running"' in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_summary_format(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test status command with summary format."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'last_update': '2024-01-01 12:00:00',
            'status': 'running',
            'tables': [{'name': 'patient'}]
        }
        
        # Test status command with summary format
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--format', 'summary'
        ])
        
        # Verify results
        assert result.exit_code == 0
        assert "Pipeline Status Summary" in result.output
        assert "Total Tables: 1" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_specific_table(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test status command with specific table filter."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'status': 'running',
            'tables': []
        }
        
        # Test status command with specific table
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--table', 'patient'
        ])
        
        # Verify results
        assert result.exit_code == 0
        mock_metrics.get_pipeline_status.assert_called_once_with(table='patient')
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    @patch('builtins.open', new_callable=mock_open)
    def test_status_command_with_output_file(self, mock_output_file, mock_config_file, mock_conn_factory, mock_metrics_class):
        """Test status command with output file."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'status': 'running',
            'tables': []
        }
        
        # Test status command with output file
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--output', 'status_report.json'
        ])
        
        # Verify results
        assert result.exit_code == 0
        assert "Status report written to" in result.output
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_metrics_failure(self, mock_file, mock_conn_factory, mock_metrics_class):
        """Test status command when metrics collection fails."""
        # Setup mocks
        mock_analytics_engine = MagicMock()
        mock_conn_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.side_effect = Exception("Metrics error")
        
        # Test status command
        result = self.runner.invoke(cli, ['status', '--config', 'test_config.yaml'])
        
        # Verify results
        assert result.exit_code != 0
        assert "Metrics error" in result.output


class TestCLIConnectionTestingUnit:
    """
    Unit tests for CLI connection testing command.
    
    Tests the test_connections command with comprehensive mocking.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_test_connections_success(self, mock_conn_factory):
        """Test successful connection testing."""
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
        mock_conn_factory.get_source_connection.return_value = mock_source_engine
        mock_conn_factory.get_replication_connection.return_value = mock_repl_engine
        mock_conn_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        # Test connection testing command
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
    def test_test_connections_failure(self, mock_conn_factory):
        """Test connection testing when connections fail."""
        # Setup connection factory to raise exception
        mock_conn_factory.get_source_connection.side_effect = Exception("Connection failed")
        
        # Test connection testing command
        result = self.runner.invoke(cli, ['test-connections'])
        
        # Verify results
        assert result.exit_code != 0
        assert "Connection failed" in result.output


class TestCLIEdgeCasesUnit:
    """
    Unit tests for CLI edge cases and error conditions.
    
    Tests various edge cases and error scenarios.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
    
    @pytest.mark.unit
    def test_missing_config_file(self):
        """Test behavior when config file is missing."""
        result = self.runner.invoke(cli, ['status', '--config', 'nonexistent.yaml'])
        assert result.exit_code != 0
    
    @pytest.mark.unit
    def test_invalid_format_option(self):
        """Test behavior with invalid format option."""
        result = self.runner.invoke(cli, [
            'status', '--config', 'test_config.yaml', '--format', 'invalid'
        ])
        assert result.exit_code != 0
    
    @pytest.mark.unit
    def test_cli_with_no_arguments(self):
        """Test CLI behavior with no arguments."""
        result = self.runner.invoke(cli, [])
        # When no arguments are provided, Click shows help and exits with code 2
        assert result.exit_code == 2
        assert "ETL Pipeline CLI" in result.output


class TestCLIHelperFunctionsUnit:
    """
    Unit tests for CLI helper functions.
    
    Tests the internal helper functions used by CLI commands.
    """
    
    @pytest.mark.unit
    def test_display_status_table_format(self):
        """Test _display_status function with table format."""
        status_data = {
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
        
        # Test table format display
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'table')
            
            # Verify table headers and data were displayed
            mock_echo.assert_called()
            calls = [call[0][0] for call in mock_echo.call_args_list]
            assert any('Table' in call for call in calls)
            assert any('patient' in call for call in calls)
    
    @pytest.mark.unit
    def test_display_status_json_format(self):
        """Test _display_status function with JSON format."""
        status_data = {
            'status': 'running',
            'tables': []
        }
        
        # Test JSON format display
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'json')
            
            # Verify JSON was displayed
            mock_echo.assert_called_once()
            output = mock_echo.call_args[0][0]
            assert '"status": "running"' in output
    
    @pytest.mark.unit
    def test_display_status_summary_format(self):
        """Test _display_status function with summary format."""
        status_data = {
            'last_update': '2024-01-01 12:00:00',
            'status': 'running',
            'tables': [{'name': 'patient'}]
        }
        
        # Test summary format display
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'summary')
            
            # Verify summary was displayed
            mock_echo.assert_called()
            calls = [call[0][0] for call in mock_echo.call_args_list]
            assert any('Pipeline Status Summary' in call for call in calls)
            assert any('Total Tables: 1' in call for call in calls)
    
    @pytest.mark.unit
    def test_display_status_empty_tables(self):
        """Test _display_status function with empty tables."""
        status_data = {
            'tables': []
        }
        
        # Test empty tables display
        with patch('click.echo') as mock_echo:
            _display_status(status_data, 'table')
            
            # Verify empty message was displayed
            mock_echo.assert_called_once()
            assert "No tables found in pipeline status" in mock_echo.call_args[0][0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])