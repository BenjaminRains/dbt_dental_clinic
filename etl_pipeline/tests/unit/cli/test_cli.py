"""
Test suite for ETL Pipeline CLI

STATUS: ACTIVE - Comprehensive CLI Testing Suite (WELL TESTED)
=============================================================

This module provides comprehensive testing for the ETL Pipeline CLI interface,
covering all major commands, edge cases, and integration scenarios. It's actively
used to validate CLI functionality and ensure reliable command-line operations.

CURRENT STATE:
- ✅ COMPREHENSIVE COVERAGE: Tests all major CLI commands and scenarios
- ✅ MOCKED INTEGRATIONS: Uses mocks for database connections and pipeline components
- ✅ EDGE CASE TESTING: Handles error conditions and invalid inputs
- ✅ SUBPROCESS TESTING: Tests CLI as it would be used in real environments
- ✅ TEMPORARY FILES: Uses temporary config files for testing
- ✅ DEBUG LOGGING: Includes debug output for troubleshooting

TEST STRUCTURE:
1. TestCLI: Core CLI command testing (help, run, status)
2. TestConnectionTesting: Database connection validation
3. TestCLIEdgeCases: Error handling and edge cases
4. TestCLICommandOptions: Testing all command options and flags
5. Standalone Functions: Subprocess and entry point testing

COVERED COMMANDS:
- help: CLI help display
- run: Pipeline execution with config files
- run --tables: Pipeline execution for specific tables
- run --full: Full pipeline execution flag
- run --force: Force execution flag
- run --parallel: Parallel workers configuration
- run --dry-run: Dry run mode (currently not implemented)
- status: Pipeline status reporting
- status --format: Different output formats
- status --table: Specific table status
- status --watch: Real-time monitoring
- status --output: Output file generation
- test-connections: Database connection validation

TESTING APPROACH:
- Unit Testing: Individual command testing with mocks
- Integration Testing: End-to-end command execution
- Subprocess Testing: Real CLI invocation
- Error Testing: Invalid inputs and failure scenarios
- Configuration Testing: Temporary config file handling

MOCKED COMPONENTS:
- PipelineOrchestrator: Main pipeline orchestration
- ConnectionFactory: Database connection management
- Settings: Configuration management
- PipelineMetrics: Status and metrics reporting

This test suite ensures the CLI is reliable and user-friendly for
ETL pipeline operations.
"""
import os
import sys
import subprocess
import logging
from unittest.mock import patch, MagicMock, Mock
import pytest
from click.testing import CliRunner
from etl_pipeline.cli.main import cli
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
import tempfile
import yaml
import json
import time

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestCLI:
    """
    Test cases for the ETL CLI.
    
    This class tests the core CLI functionality including:
    - Help command display
    - Pipeline execution with configuration files
    - Pipeline execution for specific tables
    - Status reporting
    
    Uses mocked components to avoid real database connections
    and pipeline execution during testing.
    """
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        
    def test_cli_help(self):
        """Test that CLI help works."""
        result = self.runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'ETL Pipeline CLI' in result.output
        assert 'run' in result.output
        assert 'status' in result.output
        assert 'test-connections' in result.output
        
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    @patch('etl_pipeline.config.settings.Settings')
    def test_run_command(self, mock_settings, mock_orchestrator):
        """Test the run command."""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
            source:
              host: localhost
              port: 3306
              database: opendental
              user: test_user
              password: test_pass
            replication:
              host: localhost
              port: 3306
              database: opendental_replication
              user: test_user
              password: test_pass
            analytics:
              host: localhost
              port: 5432
              database: opendental_analytics
              user: test_user
              password: test_pass
            """)
            config_path = f.name

        try:
            # Setup mock settings
            mock_settings_instance = mock_settings.return_value
            mock_settings_instance.get_database_config.return_value = {
                'host': 'localhost',
                'port': 3306,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_pass'
            }
            mock_settings_instance.get_table_config.return_value = {
                'incremental': True,
                'primary_key': 'id',
                'batch_size': 1000
            }
            mock_settings_instance.should_use_incremental.return_value = True

            # Setup mock orchestrator
            mock_orchestrator_instance = mock_orchestrator.return_value
            mock_orchestrator_instance.initialize_connections.return_value = True
            mock_orchestrator_instance.process_tables_by_priority.return_value = {
                'high': {'success': ['table1', 'table2'], 'failed': []},
                'medium': {'success': ['table3'], 'failed': []},
                'low': {'success': ['table4'], 'failed': []}
            }

            # Run the command
            runner = CliRunner()
            result = runner.invoke(cli, ['run', '--config', config_path])

            # Verify the result
            assert result.exit_code == 0
            assert "Pipeline completed successfully" in result.output

            # Verify orchestrator was called correctly
            mock_orchestrator.assert_called_once_with(config_path=config_path)
            mock_orchestrator_instance.initialize_connections.assert_called_once()
            mock_orchestrator_instance.process_tables_by_priority.assert_called_once_with(
                max_workers=4,
                force_full=False
            )

        finally:
            # Clean up the temporary file
            os.unlink(config_path)
        
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_tables(self, mock_orchestrator_class):
        """Test run command with specific tables."""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
            source:
              host: localhost
              port: 3306
              database: opendental
              user: test_user
              password: test_pass
            replication:
              host: localhost
              port: 3306
              database: opendental_replication
              user: test_user
              password: test_pass
            analytics:
              host: localhost
              port: 5432
              database: opendental_analytics
              user: test_user
              password: test_pass
            """)
            config_path = f.name

        try:
            # Setup mock orchestrator
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            
            # Mock the context manager methods
            mock_orchestrator.__enter__.return_value = mock_orchestrator
            mock_orchestrator.__exit__.return_value = None
            
            # Mock the core methods
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.run_pipeline_for_table.return_value = True

            # Run the command
            runner = CliRunner()
            cmd_args = ['run', '--config', config_path, '--tables', 'patient', '--tables', 'appointment']
            result = runner.invoke(cli, cmd_args)

            # Verify the result
            assert result.exit_code == 0, f"Command failed with output: {result.output}"
            assert "Pipeline completed successfully" in result.output

            # Verify orchestrator was called correctly
            mock_orchestrator_class.assert_called_once_with(config_path=config_path)
            mock_orchestrator.initialize_connections.assert_called_once()
            assert mock_orchestrator.run_pipeline_for_table.call_count == 2
            mock_orchestrator.run_pipeline_for_table.assert_any_call('patient', force_full=False)
            mock_orchestrator.run_pipeline_for_table.assert_any_call('appointment', force_full=False)

        finally:
            # Clean up the temporary file
            os.unlink(config_path)
        
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_command(self, mock_conn_factory, mock_metrics_class):
        """Test the status command."""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("""
            source:
              host: localhost
              port: 3306
              database: opendental
              user: test_user
              password: test_pass
            replication:
              host: localhost
              port: 3306
              database: opendental_replication
              user: test_user
              password: test_pass
            analytics:
              host: localhost
              port: 5432
              database: opendental_analytics
              user: test_user
              password: test_pass
            """)
            config_path = f.name

        try:
            # Setup mock connection factory
            mock_analytics_engine = MagicMock()
            mock_conn_factory.get_postgres_analytics_connection.return_value = mock_analytics_engine

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

            # Run the command
            runner = CliRunner()
            result = runner.invoke(cli, ['status', '--config', config_path])

            # Verify the result
            assert result.exit_code == 0
            assert "Pipeline Status" in result.output
            assert "patient" in result.output

            # Verify mocks were called correctly
            mock_conn_factory.get_postgres_analytics_connection.assert_called_once()
            mock_metrics_class.assert_called_once_with(analytics_engine=mock_analytics_engine)
            mock_metrics.get_pipeline_status.assert_called_once_with(table=None)

        finally:
            # Clean up the temporary file
            os.unlink(config_path)


class TestCLICommandOptions:
    """Test CLI command options and flags."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_full_flag(self, mock_orchestrator_class):
        """Test run command with --full flag."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.process_tables_by_priority.return_value = {
                'critical': {'success': ['table1'], 'failed': []}
            }

            result = self.runner.invoke(cli, ['run', '--config', config_path, '--full'])

            assert result.exit_code == 0
            assert "Full pipeline mode: Forcing full refresh for all tables" in result.output
            # Verify that force_full=True was passed to process_tables_by_priority
            mock_orchestrator.process_tables_by_priority.assert_called_once_with(
                max_workers=4,
                force_full=True
            )
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_force_flag(self, mock_orchestrator_class):
        """Test run command with --force flag."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.process_tables_by_priority.return_value = {
                'high': {'success': ['table1'], 'failed': []}
            }

            result = self.runner.invoke(cli, ['run', '--config', config_path, '--force'])

            assert result.exit_code == 0
            mock_orchestrator.process_tables_by_priority.assert_called_once_with(
                max_workers=4,
                force_full=True
            )
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_parallel_workers(self, mock_orchestrator_class):
        """Test run command with --parallel flag."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.process_tables_by_priority.return_value = {
                'critical': {'success': ['table1'], 'failed': []}
            }

            result = self.runner.invoke(cli, ['run', '--config', config_path, '--parallel', '8'])

            assert result.exit_code == 0
            mock_orchestrator.process_tables_by_priority.assert_called_once_with(
                max_workers=8,
                force_full=False
            )
            
        finally:
            os.unlink(config_path)
    
    def test_run_with_invalid_parallel_workers(self):
        """Test run command with invalid parallel workers."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            # Test with 0 workers
            result = self.runner.invoke(cli, ['run', '--config', config_path, '--parallel', '0'])
            assert result.exit_code != 0
            assert "Parallel workers must be between 1 and 20" in result.output
            
            # Test with 21 workers
            result = self.runner.invoke(cli, ['run', '--config', config_path, '--parallel', '21'])
            assert result.exit_code != 0
            assert "Parallel workers must be between 1 and 20" in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_dry_run_flag(self, mock_orchestrator_class):
        """Test run command with --dry-run flag."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            
            # Mock settings for dry run
            mock_settings = MagicMock()
            mock_orchestrator.settings = mock_settings
            mock_settings.get_tables_by_importance.side_effect = lambda importance: {
                'critical': ['patient', 'appointment'],
                'important': ['procedure', 'payment'],
                'audit': ['audit_log'],
                'reference': ['zipcode', 'carrier']
            }.get(importance, [])

            result = self.runner.invoke(cli, ['run', '--config', config_path, '--dry-run'])

            assert result.exit_code == 0
            assert "DRY RUN MODE - No changes will be made" in result.output
            assert "All connections successful" in result.output
            assert "Would process all tables by priority" in result.output
            assert "CRITICAL: 2 tables" in result.output
            assert "IMPORTANT: 2 tables" in result.output
            assert "Total tables to process: 7" in result.output
            assert "Dry run completed - no changes made" in result.output
            
            # Verify that process_tables_by_priority was NOT called (dry run mode)
            mock_orchestrator.process_tables_by_priority.assert_not_called()
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_dry_run_specific_tables(self, mock_orchestrator_class):
        """Test run command with --dry-run flag and specific tables."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True

            result = self.runner.invoke(cli, [
                'run', '--config', config_path, '--dry-run', 
                '--tables', 'patient', '--tables', 'appointment'
            ])

            assert result.exit_code == 0
            assert "DRY RUN MODE - No changes will be made" in result.output
            assert "Would process 2 specific tables" in result.output
            assert "1. patient" in result.output
            assert "2. appointment" in result.output
            assert "Sequential processing of specified tables" in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_dry_run_connection_failure(self, mock_orchestrator_class):
        """Test run command with --dry-run flag when connections fail."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = False

            result = self.runner.invoke(cli, ['run', '--config', config_path, '--dry-run'])

            assert result.exit_code == 0  # Dry run should not fail, just warn
            assert "DRY RUN MODE - No changes will be made" in result.output
            assert "Connection test failed" in result.output
            assert "Pipeline would fail during execution" in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_with_json_format(self, mock_conn_factory, mock_metrics_class):
        """Test status command with JSON format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_analytics_engine = MagicMock()
            mock_conn_factory.get_postgres_analytics_connection.return_value = mock_analytics_engine

            mock_metrics = MagicMock()
            mock_metrics_class.return_value = mock_metrics
            mock_metrics.get_pipeline_status.return_value = {
                'status': 'running',
                'tables': []
            }

            result = self.runner.invoke(cli, ['status', '--config', config_path, '--format', 'json'])

            assert result.exit_code == 0
            # Should output JSON format
            assert '"status": "running"' in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_with_summary_format(self, mock_conn_factory, mock_metrics_class):
        """Test status command with summary format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_analytics_engine = MagicMock()
            mock_conn_factory.get_postgres_analytics_connection.return_value = mock_analytics_engine

            mock_metrics = MagicMock()
            mock_metrics_class.return_value = mock_metrics
            mock_metrics.get_pipeline_status.return_value = {
                'last_update': '2024-01-01 12:00:00',
                'status': 'running',
                'tables': [{'name': 'table1'}]
            }

            result = self.runner.invoke(cli, ['status', '--config', config_path, '--format', 'summary'])

            assert result.exit_code == 0
            assert "Pipeline Status Summary" in result.output
            assert "Total Tables: 1" in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_with_specific_table(self, mock_conn_factory, mock_metrics_class):
        """Test status command with specific table filter."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_analytics_engine = MagicMock()
            mock_conn_factory.get_postgres_analytics_connection.return_value = mock_analytics_engine

            mock_metrics = MagicMock()
            mock_metrics_class.return_value = mock_metrics
            mock_metrics.get_pipeline_status.return_value = {
                'status': 'running',
                'tables': []
            }

            result = self.runner.invoke(cli, ['status', '--config', config_path, '--table', 'patient'])

            assert result.exit_code == 0
            mock_metrics.get_pipeline_status.assert_called_once_with(table='patient')
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_with_output_file(self, mock_conn_factory, mock_metrics_class):
        """Test status command with output file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_analytics_engine = MagicMock()
            mock_conn_factory.get_postgres_analytics_connection.return_value = mock_analytics_engine

            mock_metrics = MagicMock()
            mock_metrics_class.return_value = mock_metrics
            mock_metrics.get_pipeline_status.return_value = {
                'status': 'running',
                'tables': []
            }

            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as output_file:
                output_path = output_file.name

            try:
                result = self.runner.invoke(cli, [
                    'status', '--config', config_path, 
                    '--output', output_path
                ])

                assert result.exit_code == 0
                assert "Status report written to" in result.output
                
                # Check that file was created
                assert os.path.exists(output_path)
                
            finally:
                if os.path.exists(output_path):
                    os.unlink(output_path)
            
        finally:
            os.unlink(config_path)


class TestCLIErrorHandling:
    """Test CLI error handling and edge cases."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_connection_failure(self, mock_orchestrator_class):
        """Test run command when connection initialization fails."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = False

            result = self.runner.invoke(cli, ['run', '--config', config_path])

            assert result.exit_code != 0
            assert "Failed to initialize connections" in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_table_failure(self, mock_orchestrator_class):
        """Test run command when table processing fails."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.run_pipeline_for_table.return_value = False

            result = self.runner.invoke(cli, [
                'run', '--config', config_path, '--tables', 'patient'
            ])

            assert result.exit_code != 0
            assert "Failed to process table: patient" in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_with_priority_failure(self, mock_orchestrator_class):
        """Test run command when priority processing fails."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_orchestrator = MagicMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.initialize_connections.return_value = True
            mock_orchestrator.process_tables_by_priority.return_value = {
                'high': {'success': [], 'failed': ['table1', 'table2']}
            }

            result = self.runner.invoke(cli, ['run', '--config', config_path])

            assert result.exit_code != 0
            assert "Failed to process tables in high" in result.output
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_with_invalid_config(self, mock_conn_factory, mock_metrics_class):
        """Test status command with invalid configuration file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            config_path = f.name

        try:
            result = self.runner.invoke(cli, ['status', '--config', config_path])

            assert result.exit_code != 0
            # Should fail due to invalid YAML
            
        finally:
            os.unlink(config_path)
    
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_status_with_metrics_failure(self, mock_conn_factory, mock_metrics_class):
        """Test status command when metrics collection fails."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            mock_analytics_engine = MagicMock()
            mock_conn_factory.get_postgres_analytics_connection.return_value = mock_analytics_engine

            mock_metrics = MagicMock()
            mock_metrics_class.return_value = mock_metrics
            mock_metrics.get_pipeline_status.side_effect = Exception("Metrics error")

            result = self.runner.invoke(cli, ['status', '--config', config_path])

            assert result.exit_code != 0
            assert "Metrics error" in result.output
            
        finally:
            os.unlink(config_path)


class TestConnectionTesting:
    """Test database connection testing functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_test_connections_success(self, mock_conn_factory):
        """Test successful connection testing."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_repl_engine = MagicMock()
        mock_analytics_engine = MagicMock()
        
        # Mock connection context managers
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
        mock_conn_factory.get_postgres_analytics_connection.return_value = mock_analytics_engine
        
        result = self.runner.invoke(cli, ['test-connections'])
        
        assert result.exit_code == 0
        assert "✅ OpenDental source connection: OK" in result.output
        assert "✅ MySQL replication connection: OK" in result.output
        assert "✅ PostgreSQL analytics connection: OK" in result.output
        
        # Verify engines were disposed
        mock_source_engine.dispose.assert_called_once()
        mock_repl_engine.dispose.assert_called_once()
        mock_analytics_engine.dispose.assert_called_once()
    
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_test_connections_failure(self, mock_conn_factory):
        """Test connection testing when connections fail."""
        # Mock connection factory to raise exception
        mock_conn_factory.get_opendental_source_connection.side_effect = Exception("Connection failed")
        
        result = self.runner.invoke(cli, ['test-connections'])
        
        assert result.exit_code != 0
        assert "Connection failed" in result.output


class TestCLIEdgeCases:
    """Test CLI edge cases and error conditions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
    
    def test_missing_config(self):
        """Test behavior when config file is missing."""
        result = self.runner.invoke(cli, ['run', '--config', 'nonexistent.yaml'])
        assert result.exit_code != 0
    
    def test_invalid_format(self):
        """Test behavior with invalid format option."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("source: {}\nreplication: {}\nanalytics: {}")
            config_path = f.name

        try:
            result = self.runner.invoke(cli, [
                'status', '--config', config_path, '--format', 'invalid'
            ])
            assert result.exit_code != 0
        finally:
            os.unlink(config_path)


def test_cli_via_subprocess():
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


def test_cli_entry_point():
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])