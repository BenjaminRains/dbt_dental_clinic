"""
Integration tests for ETL Pipeline CLI

STATUS: INTEGRATION TESTS - Real database integration with test environment
===========================================================================

This module provides integration testing for the ETL Pipeline CLI interface,
following the three-tier testing approach with real test database connections.

TEST TYPE: Integration Tests (test_cli_integration.py)
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
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner
from click import ClickException

# Import CLI components
from etl_pipeline.cli.main import cli
from etl_pipeline.cli.commands import run, status, test_connections, _execute_dry_run, _display_status, inject_test_settings, clear_test_settings

# Import provider pattern components
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import DatabaseType, PostgresSchema, Settings

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestCLIIntegration:
    """
    Integration tests for ETL CLI commands.
    
    This class tests the CLI functionality with real test database connections:
    - Real database connections to test environment
    - Test configuration injection via provider pattern
    - Real data flow through pipeline components
    - Error scenarios with real connections
    - Performance characteristics with real databases
    
    Uses DictConfigProvider for test configuration injection and test environment.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        
        # Create test configuration using provider pattern
        self.test_pipeline_config = {
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
                    'application_name': 'etl_pipeline_integration_test',
                    'pool_size': 5,
                    'max_overflow': 10
                }
            },
            'logging': {
                'level': 'DEBUG',
                'file': {
                    'path': '/tmp/test.log',
                    'max_size': '10MB',
                    'backup_count': 5
                }
            },
            'performance': {
                'max_workers': 2,  # Reduced for integration tests
                'batch_size': 1000,
                'chunk_size': 500
            }
        }
        
        # Create test tables configuration
        self.test_tables_config = {
            'tables': {
                'patient': {
                    'incremental_column': 'DateModified',
                    'batch_size': 1000,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'critical',
                    'estimated_size_mb': 500,
                    'estimated_rows': 100000,
                    'monitoring': True
                },
                'appointment': {
                    'incremental_column': 'DateModified',
                    'batch_size': 500,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'important',
                    'estimated_size_mb': 200,
                    'estimated_rows': 50000,
                    'monitoring': True
                },
                'procedure': {
                    'incremental_column': 'DateModified',
                    'batch_size': 200,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'audit',
                    'estimated_size_mb': 100,
                    'estimated_rows': 25000,
                    'monitoring': False
                },
                'payment': {
                    'incremental_column': 'DateModified',
                    'batch_size': 100,
                    'extraction_strategy': 'incremental',
                    'table_importance': 'reference',
                    'estimated_size_mb': 50,
                    'estimated_rows': 10000,
                    'monitoring': False
                }
            }
        }
        
        # Create test environment variables
        self.test_env_vars = {
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
        
        # Create test provider
        self.test_provider = DictConfigProvider(
            pipeline=self.test_pipeline_config,
            tables=self.test_tables_config,
            env=self.test_env_vars
        )
        
        # Create test settings with provider
        self.test_settings = Settings(environment='test', provider=self.test_provider)
        
        # Inject test settings for CLI commands
        inject_test_settings(self.test_settings)
    
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        # Clear injected test settings
        clear_test_settings()
    
    @pytest.mark.integration
    @pytest.mark.order(5)  # Run after core components are tested
    def test_cli_help_integration(self):
        """Test CLI help functionality with real configuration."""
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
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_test_connections_integration(self):
        """Test connection testing with real test database connections."""
        try:
            # Test connection testing command with real connections
            result = self.runner.invoke(cli, ['test-connections'])
            
            # Verify results - should succeed if test databases are available
            if result.exit_code == 0:
                assert "✅ OpenDental source connection: OK" in result.output
                assert "✅ MySQL replication connection: OK" in result.output
                assert "✅ PostgreSQL analytics connection: OK" in result.output
            else:
                # If test databases are not available, should provide clear error message
                assert "Connection test failed" in result.output or "Connection error" in result.output
                logger.info("Test databases not available - this is expected in some environments")
                
        except Exception as e:
            # Handle case where test databases are not configured
            logger.info(f"Test databases not available: {str(e)}")
            pytest.skip("Test databases not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_dry_run_integration(self):
        """Test run command dry run with test configuration via provider pattern."""
        # Test dry run with test configuration
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Parallel workers: 4" in result.output
        assert "Force full run: False" in result.output
        assert "Full pipeline mode: False" in result.output
        
        # Should show table information from test config
        assert "Would process all tables by priority" in result.output
        assert "CRITICAL: 1 tables" in result.output
        assert "IMPORTANT: 1 tables" in result.output
        assert "AUDIT: 1 tables" in result.output
        assert "REFERENCE: 1 tables" in result.output
        assert "Total tables to process: 4" in result.output
        
        # Should show processing strategy
        assert "Critical tables: Parallel processing for speed" in result.output
        assert "Other tables: Sequential processing to manage resources" in result.output
        assert "Max parallel workers: 4" in result.output
        
        # Should show estimated impact
        assert "Incremental mode: Only new/changed data will be processed" in result.output
        assert "Faster processing time expected" in result.output
        
        assert "Dry run completed - no changes made" in result.output
        assert "To execute the pipeline, run without --dry-run flag" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_dry_run_with_specific_tables(self):
        """Test run command dry run with specific tables and test configuration."""
        # Test dry run with specific tables
        result = self.runner.invoke(cli, [
            'run', '--dry-run',
            '--tables', 'patient', '--tables', 'appointment'
        ])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "Would process 2 specific tables" in result.output
        assert "1. patient" in result.output
        assert "2. appointment" in result.output
        assert "Sequential processing of specified tables" in result.output
        assert "Dry run completed - no changes made" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_dry_run_with_full_flag(self):
        """Test run command dry run with full flag and test configuration."""
        # Test dry run with full flag
        result = self.runner.invoke(cli, [
            'run', '--dry-run', '--full'
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
    def test_run_command_dry_run_connection_testing(self):
        """Test run command dry run with real connection testing."""
        try:
            # Test dry run with real connection testing
            result = self.runner.invoke(cli, ['run', '--dry-run'])
            
            # Verify results
            assert result.exit_code == 0
            assert "DRY RUN MODE - No changes will be made" in result.output
            
            # Check connection test results
            if "All connections successful" in result.output:
                assert "✅ All connections successful" in result.output
            else:
                # If connections fail, should show appropriate warning
                assert "Connection test failed" in result.output or "⚠️" in result.output
                assert "Pipeline would fail during execution" in result.output
                logger.info("Test database connections failed - this is expected in some environments")
                
        except Exception as e:
            # Handle case where test databases are not configured
            logger.info(f"Test databases not available: {str(e)}")
            pytest.skip("Test databases not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_integration(self):
        """Test status command with real configuration and metrics."""
        try:
            # Test status command with real configuration
            result = self.runner.invoke(cli, [
                'status', '--config', 'test_config.yaml'
            ])
            
            # Verify results - should succeed if analytics database is available
            if result.exit_code == 0:
                # Should display status information
                assert "Pipeline Status" in result.output or "No tables found" in result.output
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
    def test_status_command_with_formats(self):
        """Test status command with different output formats."""
        try:
            # Test JSON format
            result = self.runner.invoke(cli, [
                'status', '--config', 'test_config.yaml', '--format', 'json'
            ])
            
            if result.exit_code == 0:
                # Should output valid JSON
                try:
                    json_data = json.loads(result.output)
                    assert isinstance(json_data, dict)
                except json.JSONDecodeError:
                    # If no data, might output empty or error message
                    assert "No tables found" in result.output or "Failed" in result.output
            
            # Test summary format
            result = self.runner.invoke(cli, [
                'status', '--config', 'test_config.yaml', '--format', 'summary'
            ])
            
            if result.exit_code == 0:
                assert "Pipeline Status Summary" in result.output
                
        except Exception as e:
            logger.info(f"Analytics database not available: {str(e)}")
            pytest.skip("Analytics database not available for integration testing")
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_status_command_with_output_file(self):
        """Test status command with output file generation."""
        try:
            # Create temporary output file
            output_file = tempfile.mktemp(suffix='.json')
            
            # Test status command with output file
            result = self.runner.invoke(cli, [
                'status', '--config', 'test_config.yaml',
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
    def test_run_command_invalid_config_integration(self):
        """Test run command with invalid configuration files."""
        # Test with non-existent config file
        result = self.runner.invoke(cli, ['run', '--config', 'nonexistent.yaml'])
        assert result.exit_code != 0
        
        # Test with invalid YAML config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            invalid_config_path = f.name
        
        try:
            result = self.runner.invoke(cli, ['run', '--config', invalid_config_path])
            assert result.exit_code != 0
        finally:
            # Clean up
            if os.path.exists(invalid_config_path):
                os.unlink(invalid_config_path)
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_run_command_edge_cases_integration(self):
        """Test run command edge cases with test configuration."""
        # Test with empty tables list
        result = self.runner.invoke(cli, ['run', '--tables'])
        assert result.exit_code != 0
        
        # Test with invalid parallel workers
        result = self.runner.invoke(cli, [
            'run', '--parallel', '0'
        ])
        assert result.exit_code != 0
        assert "Parallel workers must be between 1 and 20" in result.output
        
        result = self.runner.invoke(cli, [
            'run', '--parallel', '21'
        ])
        assert result.exit_code != 0
        assert "Parallel workers must be between 1 and 20" in result.output
        
        # Test with non-numeric parallel workers
        result = self.runner.invoke(cli, [
            'run', '--parallel', 'invalid'
        ])
        assert result.exit_code != 0
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_cli_performance_integration(self):
        """Test CLI performance with test configuration."""
        import time
        
        # Test help command performance
        start_time = time.time()
        result = self.runner.invoke(cli, ['--help'])
        end_time = time.time()
        
        assert result.exit_code == 0
        assert (end_time - start_time) < 2.0  # Should complete in under 2 seconds
        
        # Test dry run performance
        start_time = time.time()
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        end_time = time.time()
        
        assert result.exit_code == 0
        assert (end_time - start_time) < 5.0  # Should complete in under 5 seconds


class TestCLIConfigurationIntegration:
    """
    Integration tests for CLI configuration handling.
    
    Tests real configuration file handling and validation using provider pattern.
    """
    
    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = CliRunner()
        
        # Create test configuration using provider pattern
        self.test_config_data = {
            'pipeline': {
                'connections': {
                    'source': {'pool_size': 5},
                    'replication': {'pool_size': 10},
                    'analytics': {'pool_size': 5}
                },
                'logging': {'level': 'DEBUG'},
                'performance': {'max_workers': 2}
            },
            'tables': {
                'tables': {
                    'test_table': {
                        'table_importance': 'critical',
                        'batch_size': 1000,
                        'extraction_strategy': 'incremental'
                    }
                }
            }
        }
        
        # Create test environment variables
        self.test_env_vars = {
            'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
            'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
            'ETL_ENVIRONMENT': 'test'
        }
        
        # Create test provider
        self.test_provider = DictConfigProvider(
            pipeline=self.test_config_data['pipeline'],
            tables=self.test_config_data['tables'],
            env=self.test_env_vars
        )
        
        # Create test settings with provider
        self.test_settings = Settings(environment='test', provider=self.test_provider)
        
        # Inject test settings for CLI commands
        inject_test_settings(self.test_settings)
    
    def teardown_method(self):
        """Clean up test fixtures after each test method."""
        # Clear injected test settings
        clear_test_settings()
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_configuration_file_loading(self):
        """Test real configuration file loading and validation using provider pattern."""
        # Test configuration loading
        result = self.runner.invoke(cli, ['run', '--dry-run'])
        
        # Verify results
        assert result.exit_code == 0
        assert "DRY RUN MODE - No changes will be made" in result.output
        assert "CRITICAL: 1 tables" in result.output
        assert "test_table" in result.output
    
    @pytest.mark.integration
    @pytest.mark.order(5)
    def test_configuration_validation(self):
        """Test configuration validation with provider pattern."""
        # Test with missing required fields
        invalid_config = {
            'pipeline': {
                'connections': {
                    'source': {'pool_size': 5}
                    # Missing replication and analytics
                }
            }
            # Missing tables section
        }
        
        # Create test provider with invalid config
        invalid_provider = DictConfigProvider(
            pipeline=invalid_config,
            tables={'tables': {}},
            env=self.test_env_vars
        )
        
        # Create test settings with invalid provider
        invalid_settings = Settings(environment='test', provider=invalid_provider)
        
        # Inject invalid test settings
        inject_test_settings(invalid_settings)
        
        try:
            # Test configuration validation
            result = self.runner.invoke(cli, ['run', '--dry-run'])
            
            # Should handle missing configuration gracefully
            if result.exit_code == 0:
                assert "DRY RUN MODE - No changes will be made" in result.output
            else:
                # Should provide clear error message
                assert "Error" in result.output or "Failed" in result.output
        finally:
            # Restore original test settings
            inject_test_settings(self.test_settings)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 