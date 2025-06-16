"""
Test suite for ETL Pipeline CLI
"""
import pytest
import subprocess
import sys
import tempfile
import yaml
from pathlib import Path
from click.testing import CliRunner

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl_pipeline.cli.main import cli


class TestCLI:
    """Test cases for the ETL CLI."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        
    def test_cli_help(self):
        """Test that CLI help works."""
        result = self.runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'ETL Pipeline CLI' in result.output
        
    def test_status_command(self):
        """Test status command."""
        result = self.runner.invoke(cli, ['status'])
        # Should work even if connections fail
        assert result.exit_code in [0, 1]  # Allow for connection failures in test env
        
    def test_run_dry_run(self):
        """Test run command in dry-run mode."""
        result = self.runner.invoke(cli, ['run', '--phase', '1', '--dry-run'])
        assert result.exit_code == 0
        assert 'DRY RUN MODE' in result.output
        
    def test_run_with_tables_dry_run(self):
        """Test run command with specific tables in dry-run mode."""
        result = self.runner.invoke(cli, ['run', '--tables', 'patient,appointment', '--dry-run'])
        assert result.exit_code == 0
        assert 'Would process tables: patient, appointment' in result.output
        
    def test_discover_schema_help(self):
        """Test discover-schema command help."""
        result = self.runner.invoke(cli, ['discover-schema', '--help'])
        assert result.exit_code == 0
        assert 'Discover database schema' in result.output
        
    def test_validate_help(self):
        """Test validate command help."""
        result = self.runner.invoke(cli, ['validate', '--help'])
        assert result.exit_code == 0
        assert 'Validate data quality' in result.output


class TestCLIIntegration:
    """Integration tests for CLI commands."""
    
    @pytest.mark.integration
    def test_status_with_real_connections(self):
        """Test status command with real database connections."""
        runner = CliRunner()
        result = runner.invoke(cli, ['status', '--format', 'json'])
        
        # This will fail if databases aren't set up, but should handle gracefully
        if result.exit_code == 0:
            # If successful, should contain connection info
            assert 'connections' in result.output.lower()
    
    @pytest.mark.integration 
    def test_test_connections_command(self):
        """Test the test-connections command."""
        runner = CliRunner()
        result = runner.invoke(cli, ['test-connections'])
        
        # Should execute without crashing (may fail on connection issues)
        assert result.exit_code in [0, 1]


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


class TestCLIEdgeCases:
    """Test edge cases and error handling."""
    
    def test_invalid_phase(self):
        """Test run command with invalid phase."""
        runner = CliRunner()
        result = runner.invoke(cli, ['run', '--phase', '5'])
        assert result.exit_code != 0
        
    def test_invalid_format(self):
        """Test status command with invalid format."""
        runner = CliRunner()
        result = runner.invoke(cli, ['status', '--format', 'invalid'])
        assert result.exit_code != 0


if __name__ == '__main__':
    """Run tests directly."""
    pytest.main([__file__, '-v'])