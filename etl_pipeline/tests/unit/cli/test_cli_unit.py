"""
Unit tests for ETL Pipeline CLI (Dental Clinic ETL)

ETL CONTEXT: Dental Clinic nightly ETL pipeline, OpenDental (MariaDB v11.6) → MySQL Replication → PostgreSQL Analytics

TEST STRATEGY: Three-tier testing approach with provider pattern dependency injection and settings injection for environment-agnostic connections.

- Provider Pattern: Uses DictConfigProvider for pure unit test isolation (no file I/O, no real env vars)
- Settings Injection: All configuration via Settings(provider=DictConfigProvider) for type safety and test isolation
- Type Safety: Uses DatabaseType and PostgresSchema enums for all database config access
- FAIL FAST: Tests validate system fails if ETL_ENVIRONMENT is not set (critical security requirement)
- No Legacy Code: No compatibility or fallback logic; pure modern static config
- Dental Clinic Context: All tests assume OpenDental schema and typical dental clinic data flows

TESTED CLI COMMANDS:
- run: Main pipeline execution
- status: Pipeline status reporting
- test-connections: Database connection validation
- _execute_dry_run: Dry run logic
- _display_status: Status formatting

TEST ORGANIZATION:
- Unit tests: Purely mocked, no real connections, no file I/O
- Provider pattern: DictConfigProvider for all config/env
- Settings injection: All connections/config via Settings(provider=...)
- Type safety: All config access via enums
- ETL context: Dental clinic data flows, OpenDental schema

EXCEPTION HANDLING:
- Tests specific exception types (ConfigurationError, EnvironmentError, etc.)
- Tests user-friendly error messages with emojis
- Tests error context preservation
- Tests structured logging for monitoring

See docs/connection_architecture.md and docs/TESTING_PLAN.md for full architecture and testing strategy.
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

# Import custom exceptions for testing specific exception handling
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from etl_pipeline.exceptions.data import DataExtractionError, DataLoadingError
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Configure logging for debugging
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



class TestCLIUnit:
    """
    Unit tests for ETL CLI commands (Dental Clinic ETL, Provider Pattern).

    ETL Context:
        - Dental clinic nightly ETL pipeline (OpenDental → Analytics)
        - All config via provider pattern (DictConfigProvider)
        - All connections/config via Settings injection
        - Type safety with DatabaseType/PostgresSchema enums
        - No real connections, no file I/O, no env pollution
        - FAIL FAST on missing ETL_ENVIRONMENT
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
                        'batch_size': 1000,
                        'extraction_strategy': 'incremental',
                        'incremental_column': 'PatientNum',
                        'estimated_rows': 50000,
                        'estimated_size_mb': 25.5
                    },
                    'appointment': {
                        'batch_size': 500,
                        'extraction_strategy': 'incremental',
                        'incremental_column': 'AptNum',
                        'estimated_rows': 100000,
                        'estimated_size_mb': 45.2
                    }
                }
            },
            env={
                'ETL_ENVIRONMENT': 'test',
                # Source database (OpenDental) - TEST_ prefixed for test environment
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
                # Replication database (MySQL) - TEST_ prefixed for test environment
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                # Analytics database (PostgreSQL) - TEST_ prefixed for test environment
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        # Use Settings injection for all config access
        from etl_pipeline.config.settings import Settings
        self.test_settings = Settings(environment='test', provider=self.test_provider)
    
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
        assert "❌ Error: Parallel workers must be between 1 and 20" in result.output
        
        # Test with 21 workers
        result = self.runner.invoke(cli, ['run', '--parallel', '21'])
        assert result.exit_code != 0
        assert "❌ Error: Parallel workers must be between 1 and 20" in result.output
    
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
        assert "❌ Failed to initialize connections" in result.output
    
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
        assert "❌ Failed to process table: patient" in result.output
    
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
        assert "❌ Failed to process tables in critical: patient, appointment" in result.output
    
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
        assert "❌ Unexpected Error: Test error" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_configuration_error(self, mock_orchestrator_class):
        """Test pipeline execution with ConfigurationError."""
        # Setup mock orchestrator to raise ConfigurationError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.side_effect = ConfigurationError("Invalid config")
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Configuration Error: Invalid config" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_environment_error(self, mock_orchestrator_class):
        """Test pipeline execution with EnvironmentError."""
        # Setup mock orchestrator to raise EnvironmentError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.side_effect = EnvironmentError("Missing environment")
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Environment Error: Missing environment" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_data_extraction_error(self, mock_orchestrator_class):
        """Test pipeline execution with DataExtractionError."""
        # Setup mock orchestrator to raise DataExtractionError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.side_effect = DataExtractionError("Extraction failed")
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Data Extraction Error: Extraction failed" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_data_loading_error(self, mock_orchestrator_class):
        """Test pipeline execution with DataLoadingError."""
        # Setup mock orchestrator to raise DataLoadingError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.side_effect = DataLoadingError("Loading failed")
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Data Loading Error: Loading failed" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_database_connection_error(self, mock_orchestrator_class):
        """Test pipeline execution with DatabaseConnectionError."""
        # Setup mock orchestrator to raise DatabaseConnectionError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.side_effect = DatabaseConnectionError("Connection failed")
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Database Connection Error: Connection failed" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_database_transaction_error(self, mock_orchestrator_class):
        """Test pipeline execution with DatabaseTransactionError."""
        # Setup mock orchestrator to raise DatabaseTransactionError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.process_tables_by_priority.side_effect = DatabaseTransactionError("Transaction failed")
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Database Transaction Error: Transaction failed" in result.output

    @pytest.mark.unit
    def test_run_command_invalid_parallel_workers_updated(self):
        """Test pipeline execution with invalid parallel workers (updated error message)."""
        # Test with 0 workers
        result = self.runner.invoke(cli, ['run', '--parallel', '0'])
        assert result.exit_code != 0
        assert "❌ Error: Parallel workers must be between 1 and 20" in result.output
        
        # Test with 21 workers
        result = self.runner.invoke(cli, ['run', '--parallel', '21'])
        assert result.exit_code != 0
        assert "❌ Error: Parallel workers must be between 1 and 20" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_connection_failure_updated(self, mock_orchestrator_class):
        """Test pipeline execution when connection initialization fails (updated error message)."""
        # Setup mock orchestrator to fail connection initialization
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = False
        
        # Test run command
        result = self.runner.invoke(cli, ['run'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Failed to initialize connections" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_table_failure_updated(self, mock_orchestrator_class):
        """Test pipeline execution when table processing fails (updated error message)."""
        # Setup mock orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        mock_orchestrator.run_pipeline_for_table.return_value = False
        
        # Test run command with specific table
        result = self.runner.invoke(cli, ['run', '--tables', 'patient'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Failed to process table: patient" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_run_command_priority_failure_updated(self, mock_orchestrator_class):
        """Test pipeline execution when priority processing fails (updated error message)."""
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
        assert "❌ Failed to process tables in critical: patient, appointment" in result.output

    @pytest.mark.unit
    def test_fail_fast_on_missing_environment(self):
        """Test that system fails fast when ETL_ENVIRONMENT is not set (critical security requirement)."""
        import os
        from etl_pipeline.config.settings import Settings
        from etl_pipeline.exceptions.configuration import EnvironmentError
        # Remove ETL_ENVIRONMENT if present
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        try:
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                Settings()
        finally:
            if original_env is not None:
                os.environ['ETL_ENVIRONMENT'] = original_env


class TestCLIDryRunUnit:
    """
    Unit tests for CLI dry run functionality (Dental Clinic ETL, Provider Pattern).

    ETL Context:
        - Dental clinic ETL pipeline dry run logic
        - All config via provider pattern (DictConfigProvider)
        - All connections/config via Settings injection
        - Type safety with DatabaseType/PostgresSchema enums
        - No real connections, no file I/O, no env pollution
        - FAIL FAST on missing ETL_ENVIRONMENT
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
                    'procedurelog': {'table_importance': 'audit', 'batch_size': 200},
                    'payment': {'table_importance': 'reference', 'batch_size': 100}
                }
            },
            env={
                'ETL_ENVIRONMENT': 'test',
                # Source database (OpenDental) - TEST_ prefixed for test environment
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                # Replication database (MySQL) - TEST_ prefixed for test environment
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                # Analytics database (PostgreSQL) - TEST_ prefixed for test environment
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        # Use Settings injection for all config access
        from etl_pipeline.config.settings import Settings
        self.test_settings = Settings(environment='test', provider=self.test_provider)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_success(self, mock_orchestrator_class):
        """
        Test successful dry run execution.
        
        AAA Pattern:
            Arrange: Set up mock orchestrator with test settings
            Act: Execute dry run command
            Assert: Verify dry run output matches expected format
        """
        # Arrange: Set up mock orchestrator with test settings
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.return_value = True
        
        # Mock settings for table information
        mock_settings = MagicMock()
        mock_orchestrator.settings = mock_settings
        mock_settings.get_tables_by_importance.side_effect = lambda importance: {
            'critical': ['patient'],
            'important': ['appointment'],
            'audit': ['procedurelog'],
            'reference': ['payment']
        }.get(importance, [])
        
        # Act: Execute dry run command
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Assert: Verify dry run output matches expected format
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "All connections successful" in result.output
        assert "Would process all tables by priority" in result.output
        # Check for actual table counts from mock (not hardcoded values)
        assert "IMPORTANT: 1 tables" in result.output
        assert "AUDIT: 1 tables" in result.output
        assert "Total tables to process: 2" in result.output
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
        assert "❌ Connection test failed" in result.output
        assert "⚠️  Pipeline would fail during execution" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_configuration_error(self, mock_orchestrator_class):
        """Test dry run execution with ConfigurationError."""
        # Setup mock orchestrator to raise ConfigurationError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.side_effect = ConfigurationError("Invalid config")
        
        # Test dry run command
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Verify results
        assert result.exit_code == 0  # Dry run should not fail, just warn
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "❌ Configuration Error: Invalid config" in result.output
        assert "⚠️  Pipeline would fail during execution" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_environment_error(self, mock_orchestrator_class):
        """Test dry run execution with EnvironmentError."""
        # Setup mock orchestrator to raise EnvironmentError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.side_effect = EnvironmentError("Missing environment")
        
        # Test dry run command
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Verify results
        assert result.exit_code == 0  # Dry run should not fail, just warn
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "❌ Environment Error: Missing environment" in result.output
        assert "⚠️  Pipeline would fail during execution" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.PipelineOrchestrator')
    def test_dry_run_database_connection_error(self, mock_orchestrator_class):
        """Test dry run execution with DatabaseConnectionError."""
        # Setup mock orchestrator to raise DatabaseConnectionError
        mock_orchestrator = MagicMock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.initialize_connections.side_effect = DatabaseConnectionError("Connection failed")
        
        # Test dry run command
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Verify results
        assert result.exit_code == 0  # Dry run should not fail, just warn
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "❌ Database Connection Error: Connection failed" in result.output
        assert "⚠️  Pipeline would fail during execution" in result.output

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
    Unit tests for CLI status command (Dental Clinic ETL, Provider Pattern).

    ETL Context:
        - Dental clinic ETL pipeline status reporting
        - All config via provider pattern (DictConfigProvider)
        - All connections/config via Settings injection
        - Type safety with DatabaseType/PostgresSchema enums
        - No real connections, no file I/O, no env pollution
        - FAIL FAST on missing ETL_ENVIRONMENT
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        
        # Create test provider with injected configuration
        self.test_provider = DictConfigProvider(
            pipeline={'connections': {'analytics': {'pool_size': 5}}},
            tables={'tables': {'patient': {'table_importance': 'critical'}}},
            env={
                'ETL_ENVIRONMENT': 'test',
                # Source database (OpenDental) - TEST_ prefixed for test environment
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                # Replication database (MySQL) - TEST_ prefixed for test environment
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                # Analytics database (PostgreSQL) - TEST_ prefixed for test environment
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        # Use Settings injection for all config access
        from etl_pipeline.config.settings import Settings
        self.test_settings = Settings(environment='test', provider=self.test_provider)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory.get_analytics_raw_connection')
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_success(self, mock_file, mock_get_settings, mock_conn_factory):
        """
        Test successful status command execution using real UnifiedMetricsCollector constructor.
        
        AAA Pattern:
            Arrange: Set up mocks for settings and connection factory, patch get_pipeline_status
            Act: Execute status command
            Assert: Verify status command output and mock calls
        """
        # Arrange: Set up mocks for settings and connection factory
        mock_get_settings.return_value = self.test_settings
        mock_analytics_engine = MagicMock()
        mock_conn_factory.return_value = mock_analytics_engine

        # Patch only the get_pipeline_status method
        from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
        with patch.object(UnifiedMetricsCollector, 'get_pipeline_status', return_value={
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
        }):
            # Act: Execute status command
            result = self.runner.invoke(cli, ['status', '--config', 'test_config.yaml'])

        # Assert: Verify status command output and mock calls
        assert result.exit_code == 0
        assert "Pipeline Status" in result.output
        assert "patient" in result.output
        mock_get_settings.assert_called_once()
        mock_conn_factory.assert_called_once_with(self.test_settings)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_json_format(self, mock_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """Test status command with JSON format."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
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
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_summary_format(self, mock_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """Test status command with summary format."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
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
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_with_specific_table(self, mock_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """Test status command with specific table filter."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
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
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.open', new_callable=mock_open)
    def test_status_command_with_output_file(self, mock_output_file, mock_config_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """Test status command with output file."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
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
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_metrics_failure(self, mock_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """Test status command when metrics collection fails."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
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
        assert "❌ Unexpected Error: Metrics error" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_configuration_error(self, mock_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """Test status command with ConfigurationError."""
        # Setup mock get_settings to raise ConfigurationError
        mock_get_settings.side_effect = ConfigurationError("Invalid config")
        
        # Test status command
        result = self.runner.invoke(cli, ['status', '--config', 'test_config.yaml'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Configuration Error: Invalid config" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_environment_error(self, mock_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """Test status command with EnvironmentError."""
        # Setup mock get_settings to raise EnvironmentError
        mock_get_settings.side_effect = EnvironmentError("Missing environment")
        
        # Test status command
        result = self.runner.invoke(cli, ['status', '--config', 'test_config.yaml'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Environment Error: Missing environment" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.UnifiedMetricsCollector')
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    @patch('builtins.open', new_callable=mock_open, read_data='{"pipeline": {}}')
    def test_status_command_database_connection_error(self, mock_file, mock_get_settings, mock_conn_factory, mock_metrics_class):
        """
        Test status command with database connection error.
        
        AAA Pattern:
            Arrange: Set up mocks with database connection error in get_pipeline_status
            Act: Execute status command
            Assert: Verify error handling and exit code
        """
        # Arrange: Set up mocks with database connection error
        mock_get_settings.return_value = self.test_settings
        
        # Create a mock UnifiedMetricsCollector instance
        mock_monitor = MagicMock()
        mock_monitor.get_pipeline_status.side_effect = DatabaseConnectionError("Connection failed")
        
        # Patch the UnifiedMetricsCollector constructor to return our mock
        with patch('etl_pipeline.cli.commands.UnifiedMetricsCollector', return_value=mock_monitor):
            result = self.runner.invoke(cli, ['status', '--config', 'etl_pipeline/config/pipeline.yml'])
            print(f"Exit code: {result.exit_code}")
            print(f"Output: {result.output}")
            print(f"Exception: {result.exception}")
        
        # Assert: Verify error handling and exit code
        assert result.exit_code != 0
        assert "❌ Database Connection Error: Connection failed" in result.output


class TestCLIConnectionTestingUnit:
    """
    Unit tests for CLI connection testing command (Dental Clinic ETL, Provider Pattern).

    ETL Context:
        - Dental clinic ETL pipeline connection validation
        - All config via provider pattern (DictConfigProvider)
        - All connections/config via Settings injection
        - Type safety with DatabaseType/PostgresSchema enums
        - No real connections, no file I/O, no env pollution
        - FAIL FAST on missing ETL_ENVIRONMENT
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        # Create test provider with injected configuration (if needed for future expansion)
        from etl_pipeline.config.providers import DictConfigProvider
        from etl_pipeline.config.settings import Settings
        self.test_provider = DictConfigProvider(
            env={
                'ETL_ENVIRONMENT': 'test',
                # Source database (OpenDental) - TEST_ prefixed for test environment
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                # Replication database (MySQL) - TEST_ prefixed for test environment
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                # Analytics database (PostgreSQL) - TEST_ prefixed for test environment
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        self.test_settings = Settings(environment='test', provider=self.test_provider)
    
    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    def test_test_connections_success(self, mock_get_settings, mock_conn_factory):
        """Test successful connection testing."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
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
    @patch('etl_pipeline.cli.commands.get_settings')
    def test_test_connections_failure(self, mock_get_settings, mock_conn_factory):
        """Test connection testing when connections fail."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
        # Setup connection factory to raise exception
        mock_conn_factory.get_source_connection.side_effect = Exception("Connection failed")
        
        # Test connection testing command
        result = self.runner.invoke(cli, ['test-connections'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Unexpected Error: Connection failed" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    def test_test_connections_configuration_error(self, mock_get_settings, mock_conn_factory):
        """Test connection testing with ConfigurationError."""
        # Setup mock get_settings to raise ConfigurationError
        mock_get_settings.side_effect = ConfigurationError("Invalid config")
        
        # Test connection testing command
        result = self.runner.invoke(cli, ['test-connections'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Configuration Error: Invalid config" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    def test_test_connections_environment_error(self, mock_get_settings, mock_conn_factory):
        """Test connection testing with EnvironmentError."""
        # Setup mock get_settings to raise EnvironmentError
        mock_get_settings.side_effect = EnvironmentError("Missing environment")
        
        # Test connection testing command
        result = self.runner.invoke(cli, ['test-connections'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Environment Error: Missing environment" in result.output

    @pytest.mark.unit
    @patch('etl_pipeline.cli.commands.ConnectionFactory')
    @patch('etl_pipeline.cli.commands.get_settings')
    def test_test_connections_database_connection_error(self, mock_get_settings, mock_conn_factory):
        """Test connection testing with DatabaseConnectionError."""
        # Setup mock get_settings to return our test settings
        mock_get_settings.return_value = self.test_settings
        
        # Setup connection factory to raise DatabaseConnectionError
        mock_conn_factory.get_source_connection.side_effect = DatabaseConnectionError("Connection failed")
        
        # Test connection testing command
        result = self.runner.invoke(cli, ['test-connections'])
        
        # Verify results
        assert result.exit_code != 0
        assert "❌ Database Connection Error: Connection failed" in result.output


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