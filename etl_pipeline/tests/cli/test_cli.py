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
4. Standalone Functions: Subprocess and entry point testing

COVERED COMMANDS:
- help: CLI help display
- run: Pipeline execution with config files
- run --tables: Pipeline execution for specific tables
- status: Pipeline status reporting
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
        
    @patch('etl_pipeline.core.unified_metrics.UnifiedMetricsCollector')
    @patch('etl_pipeline.core.connections.ConnectionFactory')
    def test_status_command(self, mock_conn_factory, mock_metrics_class):
        """Test status command with mocked metrics and connection factory."""
        # Set up the metrics mock
        mock_metrics = MagicMock()
        mock_metrics_class.return_value = mock_metrics
        mock_metrics.get_pipeline_status.return_value = {
            'last_update': '2024-02-20 10:00:00',
            'status': 'running',
            'tables': [
                {'name': 'patient', 'status': 'completed', 'last_sync': '2024-02-20 10:00:00', 'records_processed': 1000, 'error': None},
                {'name': 'appointment', 'status': 'in_progress', 'last_sync': '2024-02-20 09:55:00', 'records_processed': 500, 'error': None}
            ]
        }
        
        # Set up the connection factory mock
        mock_conn_factory.return_value = MagicMock()
        
        # Test status command
        result = self.runner.invoke(cli, ['status', '--config', 'etl_pipeline/config/tables.yml'])
        assert result.exit_code == 0
        assert 'Pipeline Status' in result.output
        assert 'patient' in result.output
        assert 'appointment' in result.output


class TestConnectionTesting:
    """
    Test cases for connection testing functionality.
    
    This class tests the database connection validation features:
    - Successful connection testing for all database types
    - Failed connection handling and error reporting
    - Connection factory integration
    
    Uses mocked connection factories to simulate both
    successful and failed connection scenarios.
    """
    
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_test_connections_success(self, mock_conn_factory):
        """Test that connection testing succeeds when all connections are OK."""
        from unittest.mock import MagicMock
        # Create a mock engine with a working connect method and context manager
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        # Set up mock factory to return the working engine
        mock_conn_factory.get_opendental_source_connection.return_value = mock_engine
        mock_conn_factory.get_mysql_replication_connection.return_value = mock_engine
        mock_conn_factory.get_postgres_analytics_connection.return_value = mock_engine
        # Run the command
        runner = CliRunner()
        result = runner.invoke(cli, ['test-connections'])
        # Print debug info
        logger.debug(f"CLI Output: {result.output}")
        logger.debug(f"Exit Code: {result.exit_code}")
        # Verify the result
        assert result.exit_code == 0, f"Expected exit code 0, got {result.exit_code}. Output: {result.output}"
        assert "OpenDental source connection: OK" in result.output
        assert "MySQL replication connection: OK" in result.output
        assert "PostgreSQL analytics connection: OK" in result.output

    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    def test_test_connections_failure(self, mock_conn_factory):
        """Test that connection testing fails when connections fail."""
        # Make the ConnectionFactory methods themselves raise exceptions
        mock_conn_factory.get_opendental_source_connection.side_effect = Exception("Connection failed")
        mock_conn_factory.get_mysql_replication_connection.side_effect = Exception("Connection failed")
        mock_conn_factory.get_postgres_analytics_connection.side_effect = Exception("Connection failed")
        # Run the command
        runner = CliRunner()
        result = runner.invoke(cli, ['test-connections'])
        # Print debug info
        logger.debug(f"CLI Output: {result.output}")
        logger.debug(f"Exit Code: {result.exit_code}")
        # Verify the result
        assert result.exit_code == 1, f"Expected exit code 1, got {result.exit_code}. Output: {result.output}"
        assert "Error: Connection failed" in result.output


class TestCLIEdgeCases:
    """Test edge cases and error handling."""
    
    def test_missing_config(self):
        """Test behavior with missing configuration."""
        runner = CliRunner()
        result = runner.invoke(cli, ['run', '--config', 'nonexistent.yml'])
        assert result.exit_code != 0
        assert 'Error' in result.output
        
    def test_invalid_format(self):
        """Test status command with invalid format."""
        runner = CliRunner()
        result = runner.invoke(cli, ['status', '--format', 'invalid'])
        assert result.exit_code != 0
        assert 'Error' in result.output


def test_cli_via_subprocess():
    """Test CLI via subprocess (similar to real usage)."""
    try:
        # Test help command
        result = subprocess.run([
            sys.executable, '-m', 'etl_pipeline.cli.__main__', '--help'
        ], capture_output=True, text=True, timeout=10)
        
        assert result.returncode == 0
        assert 'ETL Pipeline CLI' in result.stdout
        
    except subprocess.TimeoutExpired:
        pytest.fail("CLI command timed out")
    except Exception as e:
        pytest.fail(f"CLI subprocess test failed: {e}")


def test_cli_entry_point():
    """Test that the CLI entry point can be imported."""
    try:
        from etl_pipeline.cli.__main__ import main
        assert callable(main)
    except ImportError as e:
        pytest.fail(f"Failed to import CLI entry point: {e}")


if __name__ == '__main__':
    """Run tests directly."""
    pytest.main([__file__, '-v'])