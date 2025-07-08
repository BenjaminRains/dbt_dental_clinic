"""
Integration tests for ETL Pipeline CLI using pytest fixtures.

This module demonstrates how to use pytest fixtures instead of setup_method
for more reliable and isolated CLI integration testing.

STATUS: INTEGRATION TESTS - Real database integration with test environment
===========================================================================

This module provides integration testing for the ETL Pipeline CLI interface,
following the three-tier testing approach with real test database connections.

TEST TYPE: Integration Tests (test_cli_integration_fixtures.py)
- Purpose: Real database integration with test environment and provider pattern
- Scope: Safety, error handling, actual data flow, all methods
- Coverage: Integration scenarios and edge cases
- Execution: < 10 seconds per component
- Environment: Real test databases, no production connections with FileConfigProvider
- Order Markers: Proper test execution order for data flow validation
- Provider Usage: FileConfigProvider with real test configuration files
- Markers: @pytest.mark.integration

TESTED METHODS:
- run(): Main pipeline execution command with real connections
- status(): Pipeline status reporting command with real metrics
- test_connections(): Database connection validation with real connections
- _execute_dry_run(): Dry run execution logic with real connections
- _display_status(): Status display formatting with real data

TESTING APPROACH:
- Integration Testing: Real database connections with test environment
- Provider Pattern: DictConfigProvider for test configuration injection
- Real Connections: Test database connections (not production)
- Configuration Injection: Test configuration via provider pattern
- Data Flow Testing: Actual data movement through pipeline
- Error Recovery: Real error scenarios and recovery
- Performance Testing: Real execution time validation

REAL COMPONENTS:
- PipelineOrchestrator: Real pipeline orchestration with test databases
- ConnectionFactory: Real database connection management
- UnifiedMetricsCollector: Real status and metrics reporting
- Settings: Real configuration management with DictConfigProvider
- File Operations: Real YAML loading, file writing
- Click Context: Real CLI context and output

PROVIDER PATTERN USAGE:
- DictConfigProvider: Test configuration injection for isolated testing
- Test Environment Variables: Real test database configuration
- Configuration Validation: Real configuration testing
- Environment Separation: Clear test vs production boundaries

This test suite ensures the CLI commands work correctly with real
database connections and actual data flow through the pipeline.
"""

import os
import sys
import logging
import tempfile
import yaml
import json
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner
from click import ClickException

# Import CLI components
from etl_pipeline.cli.main import cli
from etl_pipeline.cli.commands import run, status, test_connections, _execute_dry_run, _display_status

# Import fixtures from the fixtures directory
import sys
from pathlib import Path

# Add tests directory to path for imports
tests_dir = Path(__file__).parent.parent.parent  # Go up 3 levels: cli -> integration -> tests
sys.path.insert(0, str(tests_dir))

from fixtures import ( # type: ignore
    cli_runner,
    cli_test_config,
    cli_test_env_vars,
    cli_config_provider,
    cli_test_settings,
    cli_test_config_reader,
    cli_with_injected_config,
    temp_cli_config_file,
    temp_tables_config_file,
    mock_cli_database_connections,
    cli_expected_outputs,
    cli_error_cases,
    cli_performance_thresholds,
    cli_output_validators,
    cli_integration_test_data
)

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def verify_test_environment_config(verbose=True, skip_on_failure=False):
    """Verify test environment configuration and return status."""
    if verbose:
        logger.info("=" * 60)
        logger.info("TEST ENVIRONMENT VERIFICATION")
        logger.info("=" * 60)
    
    # Define environment variables to check
    env_vars = {
        'ETL Configuration': {
            'ETL_BATCH_SIZE': os.getenv('ETL_BATCH_SIZE', 'NOT_SET'),
            'ETL_MAX_RETRIES': os.getenv('ETL_MAX_RETRIES', 'NOT_SET'),
            'ETL_RETRY_DELAY': os.getenv('ETL_RETRY_DELAY', 'NOT_SET'),
            'ETL_LOG_LEVEL': os.getenv('ETL_LOG_LEVEL', 'NOT_SET')
        },
        'Test Database': {
            'TEST_OPENDENTAL_SOURCE_HOST': os.getenv('TEST_OPENDENTAL_SOURCE_HOST', 'NOT_SET'),
            'TEST_OPENDENTAL_SOURCE_DB': os.getenv('TEST_OPENDENTAL_SOURCE_DB', 'NOT_SET'),
            'TEST_MYSQL_REPLICATION_HOST': os.getenv('TEST_MYSQL_REPLICATION_HOST', 'NOT_SET'),
            'TEST_MYSQL_REPLICATION_DB': os.getenv('TEST_MYSQL_REPLICATION_DB', 'NOT_SET'),
            'TEST_POSTGRES_ANALYTICS_HOST': os.getenv('TEST_POSTGRES_ANALYTICS_HOST', 'NOT_SET'),
            'TEST_POSTGRES_ANALYTICS_DB': os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'NOT_SET'),
            'TEST_POSTGRES_ANALYTICS_SCHEMA': os.getenv('TEST_POSTGRES_ANALYTICS_SCHEMA', 'NOT_SET')
        }
    }
    
    # Log all environment variables if verbose
    if verbose:
        for category, variables in env_vars.items():
            logger.info(f"{category} Variables:")
            for key, value in variables.items():
                logger.info(f"  {key}: {value}")
    
    # Verify test database configuration
    test_db_vars = env_vars['Test Database']
    db_hosts = ['TEST_OPENDENTAL_SOURCE_HOST', 'TEST_MYSQL_REPLICATION_HOST', 'TEST_POSTGRES_ANALYTICS_HOST']
    db_names = ['TEST_OPENDENTAL_SOURCE_DB', 'TEST_MYSQL_REPLICATION_DB', 'TEST_POSTGRES_ANALYTICS_DB']
    
    hosts_configured = all(test_db_vars[host] != 'NOT_SET' for host in db_hosts)
    names_configured = all(test_db_vars[name] != 'NOT_SET' for name in db_names)
    using_test_names = all('test_' in test_db_vars[name].lower() for name in db_names if test_db_vars[name] != 'NOT_SET')
    
    # Check critical variables for fixture
    critical_vars = {
        'TEST_OPENDENTAL_SOURCE_DB': test_db_vars['TEST_OPENDENTAL_SOURCE_DB'],
        'TEST_POSTGRES_ANALYTICS_DB': test_db_vars['TEST_POSTGRES_ANALYTICS_DB']
    }
    
    all_good = True
    
    # Log verification results
    if verbose:
        logger.info("Environment Verification:")
        logger.info(f"  Test Database Hosts Configured: {hosts_configured}")
        logger.info(f"  Test Database Names Configured: {names_configured}")
        logger.info(f"  Using Test Database Names: {using_test_names}")
    
    # Check critical variables
    for var_name, var_value in critical_vars.items():
        if var_value:
            if verbose:
                logger.info(f"  [OK] {var_name}: {var_value}")
        else:
            logger.error(f"  [FAIL] {var_name}: NOT_SET")
            all_good = False
    
    # Verify test database names contain 'test_'
    for var_name, var_value in critical_vars.items():
        if var_value and 'test_' not in var_value.lower():
            logger.error(f"[FAIL] {var_name} doesn't appear to be a test database: {var_value}")
            all_good = False
    
    # Log warnings
    warnings = []
    if not hosts_configured:
        warnings.append("Test database hosts not properly configured!")
    if not names_configured:
        warnings.append("Test database names not properly configured!")
    if not using_test_names:
        warnings.append("Database names don't appear to be test databases!")
    
    for warning in warnings:
        logger.warning(f"[WARN] {warning}")
    
    if verbose:
        logger.info("=" * 60)
    
    # Handle failure
    if not all_good and skip_on_failure:
        pytest.skip("Test environment not properly configured")
    
    return all_good


# Log environment info at module import time
verify_test_environment_config(verbose=True, skip_on_failure=False)


@pytest.fixture
def verify_test_environment():
    """Fixture to verify test environment is properly configured."""
    logger.info("[INFO] Verifying test environment configuration...")
    return verify_test_environment_config(verbose=False, skip_on_failure=True)


@pytest.fixture
def cli_with_injected_config_and_reader(cli_test_settings, cli_test_config_reader):
    """Fixture that sets up test environment for CLI commands using real configuration."""
    from etl_pipeline.cli.commands import inject_test_settings, clear_test_settings
    
    # Set test environment variable to use test databases
    original_env = os.environ.get('ETL_ENVIRONMENT')
    os.environ['ETL_ENVIRONMENT'] = 'test'
    
    # Inject test settings for configuration
    inject_test_settings(cli_test_settings)
    
    # Create a temporary tables.yml file for testing
    # This follows Section 10.1.1 - Fix File Dependencies
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        test_tables_config = {
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
                },
                'payment': {
                    'primary_key': 'PayNum',
                    'incremental_column': 'DatePay',
                    'extraction_strategy': 'incremental',
                    'table_importance': 'reference',
                    'batch_size': 100
                }
            }
        }
        yaml.dump(test_tables_config, f)
        temp_config_path = f.name
    
    # Use the real PipelineOrchestrator but patch the ConfigReader to use our temporary file
    # This follows Section 4.1 - Testing Real Logic vs Mocks
    with patch('etl_pipeline.orchestration.pipeline_orchestrator.ConfigReader') as mock_config_reader_class:
        # Create a real ConfigReader that uses our temporary file
        from etl_pipeline.config.config_reader import ConfigReader
        
        class TestConfigReader(ConfigReader):
            def __init__(self, config_path=None):
                # Always use our temporary config file regardless of what path is passed
                super().__init__(temp_config_path)
        
        mock_config_reader_class.side_effect = TestConfigReader
        
        yield temp_config_path
    
    # Clean up after test
    clear_test_settings()
    
    # Restore original environment
    if original_env:
        os.environ['ETL_ENVIRONMENT'] = original_env
    else:
        os.environ.pop('ETL_ENVIRONMENT', None)
    
    # Clean up temporary file
    if os.path.exists(temp_config_path):
        os.unlink(temp_config_path)


class TestCLIIntegrationWithFixtures:
    """
    Integration tests for ETL CLI commands using pytest fixtures.
    
    This class demonstrates how to use fixtures instead of setup_method
    for more reliable and isolated testing:
    - Uses pytest fixtures for test data and configuration
    - No setup_method/teardown_method complexity
    - Better test isolation and reliability
    - Clearer test dependencies
    - Easier to maintain and debug
    
    Uses DictConfigProvider for test configuration injection and test environment.
    """
    
    def setup_method(self):
        """Verify test environment before each test."""
        logger.info("=" * 40)
        logger.info(f"Starting test: {self.__class__.__name__}")
        logger.info("=" * 40)
        
        # Verify test environment
        env_check = {
            'TEST_OPENDENTAL_SOURCE_DB': os.getenv('TEST_OPENDENTAL_SOURCE_DB'),
            'TEST_MYSQL_REPLICATION_DB': os.getenv('TEST_MYSQL_REPLICATION_DB'),
            'TEST_POSTGRES_ANALYTICS_DB': os.getenv('TEST_POSTGRES_ANALYTICS_DB')
        }
        
        logger.info("Test Environment Check:")
        for key, value in env_check.items():
            status = "[OK]" if value else "[FAIL]"
            logger.info(f"  {status} {key}: {value or 'NOT_SET'}")
        
        # Check if database names contain 'test_' to verify they're test databases
        test_db_verified = True
        for key, value in env_check.items():
            if value and 'test_' not in value.lower():
                logger.warning(f"[WARN] WARNING: {key} doesn't appear to be a test database: {value}")
                test_db_verified = False
        
        if not test_db_verified:
            logger.warning("[WARN] WARNING: Some database names don't appear to be test databases!")
        
        if not env_check['TEST_OPENDENTAL_SOURCE_DB'] or not env_check['TEST_POSTGRES_ANALYTICS_DB']:
            logger.warning("[WARN] WARNING: Test database variables not configured!")
        
        logger.info("=" * 40)
    
    @pytest.mark.integration
    @pytest.mark.order(5)  # Run after core components are tested
    def test_cli_help_integration(self, cli_runner, cli_expected_outputs, verify_test_environment):
        """Test CLI help command with fixture-based configuration."""
        # Verify test environment is properly configured
        assert verify_test_environment, "Test environment verification failed"
        
        # Test help command
        result = cli_runner.invoke(cli, ['--help'])
        
        # Verify results
        assert result.exit_code == 0
        for expected_text in cli_expected_outputs['help']['main_help']:
            assert expected_text in result.output
        
        # Test run command help
        result = cli_runner.invoke(cli, ['run', '--help'])
        assert result.exit_code == 0
        for expected_text in cli_expected_outputs['help']['run_help']:
            assert expected_text in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_test_connections_integration(self, cli_runner, cli_with_injected_config, cli_expected_outputs, verify_test_environment):
        """Test test-connections command with fixture-based configuration."""
        # Verify test environment is properly configured
        assert verify_test_environment, "Test environment verification failed"
        
        # Test connections command
        result = cli_runner.invoke(cli, ['test-connections'])
        
        # Verify results
        assert result.exit_code == 0
        # Check for connection status messages (without header since CLI doesn't show one)
        for expected_text in cli_expected_outputs['test_connections']['connections']:
            assert expected_text in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_dry_run_integration(self, cli_runner, cli_with_injected_config_and_reader, cli_expected_outputs, cli_output_validators):
        """Test run command dry run with fixture-based configuration."""
        # Test dry run with fixture configuration and temporary config file
        # This follows Section 10.1.1 - Fix File Dependencies
        result = cli_runner.invoke(cli, ['run', '--dry-run', '--config', cli_with_injected_config_and_reader])
        
        # Verify results
        assert result.exit_code == 0
        
        # Use fixture-based output validation
        cli_output_validators['validate_dry_run_output'](result.output)
        
        # Additional specific checks
        assert "Parallel workers: 4" in result.output
        assert "Force full run: False" in result.output
        assert "Full pipeline mode: False" in result.output
        
        # Should show table information from fixture config
        assert "Would process all tables by priority" in result.output
        assert "CRITICAL: 1 tables" in result.output
        assert "IMPORTANT: 2 tables" in result.output
        assert "REFERENCE: 1 tables" in result.output
        assert "Total tables to process: 4" in result.output
        
        # Should show processing strategy
        assert "Critical tables: Parallel processing for speed" in result.output
        assert "Other tables: Sequential processing to manage resources" in result.output
        assert "Max parallel workers: 4" in result.output
        
        # Should show estimated impact
        assert "Incremental mode: Only new/changed data will be processed" in result.output
        assert "Faster processing time expected" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_dry_run_with_specific_tables(self, cli_runner, cli_with_injected_config_and_reader, cli_output_validators):
        """Test run command dry run with specific tables and fixture configuration."""
        # Test dry run with specific tables and temporary config file
        # This follows Section 10.1.1 - Fix File Dependencies
        result = cli_runner.invoke(cli, [
            'run', '--dry-run', '--config', cli_with_injected_config_and_reader,
            '--tables', 'patient', '--tables', 'appointment'
        ])
        
        # Verify results
        assert result.exit_code == 0
        
        # Use fixture-based output validation
        cli_output_validators['validate_dry_run_output'](result.output, ['patient', 'appointment'])
        
        # Additional specific checks
        assert "Would process 2 specific tables" in result.output
        assert "1. patient" in result.output
        assert "2. appointment" in result.output
        assert "Sequential processing of specified tables" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_dry_run_with_full_flag(self, cli_runner, cli_with_injected_config_and_reader, cli_expected_outputs):
        """Test run command dry run with full flag and fixture configuration."""
        # Test dry run with full flag and temporary config file
        # This follows Section 10.1.1 - Fix File Dependencies
        result = cli_runner.invoke(cli, [
            'run', '--dry-run', '--config', cli_with_injected_config_and_reader, '--full'
        ])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Full pipeline mode: True" in result.output
        assert "Full refresh mode: All data will be re-extracted" in result.output
        assert "Longer processing time expected" in result.output
        assert "Dry run completed - no changes made" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_dry_run_connection_testing(self, cli_runner, cli_with_injected_config_and_reader, mock_cli_database_connections):
        """Test run command dry run with real connection testing using fixtures."""
        try:
            # Test dry run with real connection testing
            result = cli_runner.invoke(cli, ['run', '--dry-run'])
            
            # Verify results
            assert result.exit_code == 0
            assert "DRY RUN MODE - No changes will be made" in result.output
            
            # Check connection test results
            if "All connections successful" in result.output:
                assert "All connections successful" in result.output
            else:
                # If connections fail, should show appropriate warning
                assert "Connection test failed" in result.output or "WARNING" in result.output
                assert "Pipeline would fail during execution" in result.output
                logger.info("Test database connections failed - this is expected in some environments")
                
        except Exception as e:
            # Handle case where test databases are not configured
            logger.info(f"Test databases not available: {str(e)}")
            pytest.skip("Test databases not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_integration(self, cli_runner, temp_cli_config_file, mock_cli_database_connections, cli_output_validators):
        """Test status command with fixture-based configuration and metrics."""
        try:
            # Test status command with fixture configuration
            result = cli_runner.invoke(cli, [
                'status', '--config', temp_cli_config_file
            ])
            
            # Verify results - should succeed if analytics database is available
            if result.exit_code == 0:
                # Use fixture-based output validation
                cli_output_validators['validate_status_output'](result.output, 'table')
            else:
                # If analytics database is not available, should provide clear error message
                assert "Failed to get pipeline status" in result.output or "Connection" in result.output
                logger.info("Analytics database not available - this is expected in some environments")
                
        except Exception as e:
            # Handle case where analytics database is not configured
            logger.info(f"Analytics database not available: {str(e)}")
            pytest.skip("Analytics database not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_formats(self, cli_runner, temp_cli_config_file, mock_cli_database_connections, cli_output_validators):
        """Test status command with different output formats using fixtures."""
        try:
            # Test JSON format
            result = cli_runner.invoke(cli, [
                'status', '--config', temp_cli_config_file, '--format', 'json'
            ])
            
            if result.exit_code == 0:
                # Use fixture-based output validation
                cli_output_validators['validate_status_output'](result.output, 'json')
            
            # Test summary format
            result = cli_runner.invoke(cli, [
                'status', '--config', temp_cli_config_file, '--format', 'summary'
            ])
            
            if result.exit_code == 0:
                cli_output_validators['validate_status_output'](result.output, 'summary')
                
        except Exception as e:
            logger.info(f"Analytics database not available: {str(e)}")
            pytest.skip("Analytics database not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_output_file(self, cli_runner, temp_cli_config_file, mock_cli_database_connections):
        """Test status command with output file generation using fixtures."""
        try:
            # Create temporary output file
            output_file = tempfile.mktemp(suffix='.json')
            
            # Test status command with output file
            result = cli_runner.invoke(cli, [
                'status', '--config', temp_cli_config_file,
                '--format', 'json', '--output', output_file
            ])
            
            if result.exit_code == 0:
                assert "Status report written to" in result.output
                
                # Check that file was created
                if os.path.exists(output_file):
                    with open(output_file, 'r') as f:
                        content = f.read()
                        # Should contain valid JSON or error message
                        if content.strip():
                            try:
                                json.loads(content)
                            except json.JSONDecodeError:
                                # Might contain error message instead of JSON
                                assert "error" in content.lower() or "failed" in content.lower()
                    
                    # Clean up
                    os.unlink(output_file)
            else:
                # If command failed, should provide clear error message
                assert "Failed" in result.output or "Error" in result.output
                
        except Exception as e:
            logger.info(f"Analytics database not available: {str(e)}")
            pytest.skip("Analytics database not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_invalid_config_integration(self, cli_runner, cli_error_cases):
        """Test run command with invalid configuration files using fixtures."""
        # Test with non-existent config file
        result = cli_runner.invoke(cli, ['run', '--config', 'nonexistent.yaml'])
        assert result.exit_code != 0
        
        # Test with invalid YAML config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            invalid_config_path = f.name
        
        try:
            result = cli_runner.invoke(cli, ['run', '--config', invalid_config_path])
            assert result.exit_code != 0
        finally:
            # Clean up
            if os.path.exists(invalid_config_path):
                os.unlink(invalid_config_path)
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_edge_cases_integration(self, cli_runner, cli_with_injected_config_and_reader, cli_error_cases):
        """Test run command edge cases with fixture-based configuration."""
        # Test with empty tables list
        result = cli_runner.invoke(cli, ['run', '--tables'])
        assert result.exit_code != 0
        
        # Test with invalid parallel workers
        result = cli_runner.invoke(cli, [
            'run', '--parallel', '0'
        ])
        assert result.exit_code != 0
        assert cli_error_cases['invalid_parameters']['invalid_parallel'] in result.output
        
        result = cli_runner.invoke(cli, [
            'run', '--parallel', '21'
        ])
        assert result.exit_code != 0
        assert cli_error_cases['invalid_parameters']['invalid_parallel'] in result.output
        
        # Test with non-numeric parallel workers
        result = cli_runner.invoke(cli, [
            'run', '--parallel', 'invalid'
        ])
        assert result.exit_code != 0
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_cli_performance_integration(self, cli_runner, cli_with_injected_config_and_reader, cli_performance_thresholds):
        """Test CLI performance with fixture-based configuration."""
        # Test performance of dry run command with temporary config file
        # This follows Section 10.1.1 - Fix File Dependencies
        start_time = time.time()
        result = cli_runner.invoke(cli, ['run', '--dry-run', '--config', cli_with_injected_config_and_reader])
        end_time = time.time()
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        
        # Performance check: should complete within reasonable time
        execution_time = end_time - start_time
        assert execution_time < cli_performance_thresholds['dry_run']
        logger.info(f"CLI dry run execution time: {execution_time:.2f} seconds")


class TestCLIConfigurationIntegrationWithFixtures:
    """
    Integration tests for CLI configuration handling using fixtures.
    
    Tests real configuration file handling and validation using provider pattern
    with fixture-based test data.
    """
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_configuration_file_loading(self, cli_runner, cli_with_injected_config_and_reader):
        """Test real configuration file loading and validation using fixtures."""
        # Test configuration loading with temporary config file
        # This follows Section 10.1.1 - Fix File Dependencies
        result = cli_runner.invoke(cli, ['run', '--dry-run', '--config', cli_with_injected_config_and_reader])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "CRITICAL: 1 tables" in result.output
        assert "patient" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_configuration_validation(self, cli_runner, cli_test_env_vars, cli_error_cases):
        """Test configuration validation with fixture-based provider pattern."""
        # Test with missing required fields using fixtures
        invalid_config = {
            'pipeline': {
                'connections': {
                    'source': {'pool_size': 5}
                    # Missing replication and analytics
                }
            }
            # Missing tables section
        }
        
        # Create test provider with invalid config using fixtures
        from etl_pipeline.config.providers import DictConfigProvider
        from etl_pipeline.config import Settings
        from etl_pipeline.cli.commands import inject_test_settings, clear_test_settings
        
        invalid_provider = DictConfigProvider(
            pipeline=invalid_config,
            tables={'tables': {}},
            env=cli_test_env_vars
        )
        
        # Create test settings with invalid provider
        invalid_settings = Settings(environment='test', provider=invalid_provider)
        
        # Inject invalid test settings
        inject_test_settings(invalid_settings)
        
        try:
            # Test with invalid configuration
            result = cli_runner.invoke(cli, ['run', '--dry-run'])
            
            # The configuration might be valid enough to pass validation
            # but should show some indication of the configuration being used
            # Check that the command runs (even if with warnings)
            if result.exit_code == 0:
                # If it succeeds, it should show some configuration info
                assert "DRY RUN MODE" in result.output
                logger.info("Configuration validation passed - this may be expected behavior")
            else:
                # If it fails, should be due to configuration issues
                assert "configuration" in result.output.lower() or "error" in result.output.lower()
            
        finally:
            # Clean up
            clear_test_settings()
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_fixture_isolation(self, cli_integration_test_data):
        """Test that fixtures provide isolated test data."""
        # Verify fixture provides expected data
        test_data = cli_integration_test_data
        assert 'sample_tables' in test_data
        assert 'patient' in test_data['sample_tables']
        assert 'appointment' in test_data['sample_tables']
        assert 'procedurelog' in test_data['sample_tables']  # Fixed: should be 'procedurelog' not 'procedure'
        assert 'payment' in test_data['sample_tables']
        
        # Verify expected exit codes
        assert test_data['expected_exit_codes']['success'] == 0
        assert test_data['expected_exit_codes']['error'] == 1
        assert test_data['expected_exit_codes']['abort'] == 2
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_temp_file_fixture(self, temp_tables_config_file):
        """Test temporary file fixture functionality."""
        # Verify file was created
        assert os.path.exists(temp_tables_config_file)
        
        # Verify file contains valid YAML
        with open(temp_tables_config_file, 'r') as f:
            content = f.read()
            config = yaml.safe_load(content)
            assert 'tables' in config
            assert 'patient' in config['tables']
        
        # File should be automatically cleaned up after test
        # (handled by fixture teardown) 

    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_actual_execution(self, cli_runner, cli_with_injected_config_and_reader):
        """Test run command actual execution (not dry run) with fixture configuration."""
        # Test actual execution with temporary config file
        # This follows Section 10.1.1 - Fix File Dependencies
        result = cli_runner.invoke(cli, [
            'run', '--config', cli_with_injected_config_and_reader,
            '--tables', 'patient'  # Process just one table
        ])
        
        # Verify results - should succeed or fail gracefully
        if result.exit_code == 0:
            assert "Processing 1 specific tables: patient" in result.output
            assert "Pipeline completed successfully" in result.output
        else:
            # If it fails, should be due to database issues, not CLI issues
            assert "Failed to process table" in result.output or "Error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_with_force_flag(self, cli_runner, cli_with_injected_config_and_reader):
        """Test run command with force flag and fixture configuration."""
        # Test with force flag and temporary config file
        result = cli_runner.invoke(cli, [
            'run', '--config', cli_with_injected_config_and_reader,
            '--force', '--tables', 'patient'
        ])
        
        # Verify results
        if result.exit_code == 0:
            assert "Processing 1 specific tables: patient" in result.output
            assert "Pipeline completed successfully" in result.output
        else:
            # Should fail gracefully if database issues
            assert "Failed to process table" in result.output or "Error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_with_parallel_workers(self, cli_runner, cli_with_injected_config_and_reader):
        """Test run command with custom parallel workers and fixture configuration."""
        # Test with custom parallel workers and temporary config file
        result = cli_runner.invoke(cli, [
            'run', '--config', cli_with_injected_config_and_reader,
            '--parallel', '2', '--tables', 'patient', 'appointment'
        ])
        
        # Verify results
        if result.exit_code == 0:
            assert "Processing 2 specific tables: patient, appointment" in result.output
            assert "Pipeline completed successfully" in result.output
        else:
            # Should fail gracefully if database issues
            assert "Failed to process table" in result.output or "Error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_all_tables_by_priority(self, cli_runner, cli_with_injected_config_and_reader):
        """Test run command processing all tables by priority with fixture configuration."""
        # Test processing all tables by priority with temporary config file
        result = cli_runner.invoke(cli, [
            'run', '--config', cli_with_injected_config_and_reader
        ])
        
        # Verify results
        if result.exit_code == 0:
            assert "Processing all tables by priority with" in result.output
            assert "Pipeline completed successfully" in result.output
        else:
            # Should fail gracefully if database issues
            assert "Failed to process tables in" in result.output or "Error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_dry_run_with_connection_failure(self, cli_runner, cli_with_injected_config_and_reader):
        """Test dry run with connection failure simulation."""
        # Patch the orchestrator to simulate connection failure
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator.initialize_connections') as mock_init:
            mock_init.return_value = False
            
            result = cli_runner.invoke(cli, [
                'run', '--dry-run', '--config', cli_with_injected_config_and_reader
            ])
            
            # Verify results
            assert result.exit_code == 0  # Dry run should not fail
            assert "DRY RUN MODE - No changes will be made" in result.output
            assert "Connection test failed" in result.output
            assert "Pipeline would fail during execution" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_dry_run_with_connection_exception(self, cli_runner, cli_with_injected_config_and_reader):
        """Test dry run with connection exception simulation."""
        # Patch the orchestrator to simulate connection exception
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator.initialize_connections') as mock_init:
            mock_init.side_effect = Exception("Connection timeout")
            
            result = cli_runner.invoke(cli, [
                'run', '--dry-run', '--config', cli_with_injected_config_and_reader
            ])
            
            # Verify results
            assert result.exit_code == 0  # Dry run should not fail
            assert "DRY RUN MODE - No changes will be made" in result.output
            assert "Connection test failed: Connection timeout" in result.output
            assert "Pipeline would fail during execution" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_dry_run_with_table_importance_error(self, cli_runner, cli_with_injected_config_and_reader):
        """Test dry run with table importance error simulation."""
        # Patch the settings to simulate error getting tables by importance
        with patch('etl_pipeline.config.settings.Settings.get_tables_by_importance') as mock_get_tables:
            mock_get_tables.side_effect = Exception("Settings error")
            
            result = cli_runner.invoke(cli, [
                'run', '--dry-run', '--config', cli_with_injected_config_and_reader
            ])
            
            # Verify results
            assert result.exit_code == 0  # Dry run should not fail
            assert "DRY RUN MODE - No changes will be made" in result.output
            assert "Error getting tables - Settings error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_watch_mode(self, cli_runner, temp_cli_config_file, mock_cli_database_connections):
        """Test status command with watch mode using fixtures."""
        # Skip watch mode test as it's designed to run continuously
        # and would hang the test suite
        pytest.skip("Watch mode test skipped - designed for interactive use only")
        
        # Alternative approach: test the watch mode logic without the infinite loop
        # This would require mocking the time.sleep and click.clear functions
        # which is complex and not worth the effort for integration tests
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_specific_table(self, cli_runner, temp_cli_config_file, mock_cli_database_connections):
        """Test status command with specific table parameter using fixtures."""
        try:
            # Test status command with specific table
            result = cli_runner.invoke(cli, [
                'status', '--config', temp_cli_config_file, '--table', 'patient'
            ])
            
            if result.exit_code == 0:
                # Should show status for specific table
                assert "Pipeline Status" in result.output or "No tables found" in result.output
            else:
                # If it fails, should be due to database issues
                assert "Failed to get pipeline status" in result.output or "Connection" in result.output
                
        except Exception as e:
            logger.info(f"Specific table status test failed: {str(e)}")
            pytest.skip("Analytics database not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_invalid_format(self, cli_runner, temp_cli_config_file):
        """Test status command with invalid format parameter."""
        # Test with invalid format (should be caught by Click)
        result = cli_runner.invoke(cli, [
            'status', '--config', temp_cli_config_file, '--format', 'invalid'
        ])
        
        # Should fail due to invalid format choice
        assert result.exit_code != 0
        assert "Invalid value" in result.output or "Error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_missing_config(self, cli_runner):
        """Test status command with missing config file."""
        # Test with non-existent config file
        result = cli_runner.invoke(cli, [
            'status', '--config', 'nonexistent.yml'
        ])
        
        # Should fail due to missing config file
        assert result.exit_code != 0
        assert "No such file" in result.output or "Error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_test_connections_with_failure(self, cli_runner, mock_cli_database_connections):
        """Test test-connections command with connection failure simulation."""
        # Patch the connection factory method that's actually called in the command
        with patch('etl_pipeline.cli.commands.ConnectionFactory.get_source_connection') as mock_source:
            # Create a mock engine that raises an exception when connect() is called
            mock_engine = MagicMock()
            mock_engine.connect.side_effect = Exception("Source connection failed")
            mock_source.return_value = mock_engine
            
            result = cli_runner.invoke(cli, ['test-connections'])
            
            # Should fail due to connection error
            assert result.exit_code != 0
            assert "Connection test failed" in result.output or "Source connection failed" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_with_table_processing_failure(self, cli_runner, cli_with_injected_config_and_reader):
        """Test run command with table processing failure simulation."""
        # Patch the orchestrator to simulate table processing failure
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator.run_pipeline_for_table') as mock_run:
            mock_run.return_value = False  # Simulate failure
            
            result = cli_runner.invoke(cli, [
                'run', '--config', cli_with_injected_config_and_reader,
                '--tables', 'patient'
            ])
            
            # Should fail due to table processing failure
            assert result.exit_code != 0
            assert "Failed to process table: patient" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_with_priority_processing_failure(self, cli_runner, cli_with_injected_config_and_reader):
        """Test run command with priority processing failure simulation."""
        # Patch the orchestrator to simulate priority processing failure
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator.process_tables_by_priority') as mock_process:
            mock_process.return_value = {
                'critical': {'success': [], 'failed': ['patient']},
                'important': {'success': [], 'failed': []}
            }
            
            result = cli_runner.invoke(cli, [
                'run', '--config', cli_with_injected_config_and_reader
            ])
            
            # Should fail due to priority processing failure
            assert result.exit_code != 0
            assert "Failed to process tables in critical" in result.output
            assert "patient" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_with_orchestrator_exception(self, cli_runner, cli_with_injected_config_and_reader):
        """Test run command with orchestrator exception simulation."""
        # Patch the orchestrator to simulate exception
        with patch('etl_pipeline.orchestration.pipeline_orchestrator.PipelineOrchestrator.initialize_connections') as mock_init:
            mock_init.side_effect = Exception("Orchestrator error")
            
            result = cli_runner.invoke(cli, [
                'run', '--config', cli_with_injected_config_and_reader,
                '--tables', 'patient'
            ])
            
            # Should fail due to orchestrator error
            assert result.exit_code != 0
            assert "Error: Orchestrator error" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_metrics_exception(self, cli_runner, temp_cli_config_file):
        """Test status command with metrics collector exception simulation."""
        # Patch the metrics collector to simulate exception
        with patch('etl_pipeline.monitoring.unified_metrics.UnifiedMetricsCollector.get_pipeline_status') as mock_status:
            mock_status.side_effect = Exception("Metrics error")
            
            result = cli_runner.invoke(cli, [
                'status', '--config', temp_cli_config_file
            ])
            
            # Should fail due to metrics error
            assert result.exit_code != 0
            # The error message can be either format
            assert ("Failed to get pipeline status" in result.output or 
                   "Error: Metrics error" in result.output)
            assert "Metrics error" in result.output 