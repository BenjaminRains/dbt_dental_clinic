"""Test script for ETL CLI functionality."""

import subprocess
import sys
import tempfile
import yaml
from pathlib import Path

def create_test_config():
    """Create a test configuration file."""
    config = {
        'pipeline': {
            'name': 'test_pipeline',
            'version': '1.0',
            'execution': {
                'max_parallel_tables': 2,
                'retry_attempts': 3,
                'timeout_minutes': 30
            }
        },
        'monitoring': {
            'enabled': True,
            'log_level': 'INFO'
        },
        'notifications': {
            'email': {'enabled': False},
            'slack': {'enabled': False}
        }
    }
    
    # Create temp config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', 
                                     delete=False) as f:
        yaml.dump(config, f, default_flow_style=False)
        return f.name

def test_cli_command(cmd_args, config_file):
    """Test a CLI command."""
    full_cmd = ['pipenv', 'run', 'python', '-m', 'etl_pipeline.cli.main', 
                '--config', config_file] + cmd_args
    
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=30)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)

def main():
    """Run CLI tests."""
    print("🧪 Testing ETL Pipeline CLI")
    print("=" * 50)
    
    # Create test config
    config_file = create_test_config()
    print(f"📄 Created test config: {config_file}")
    
    # Test cases
    test_cases = [
        (['--help'], "Help command"),
        (['status', '--format', 'summary', '--dry-run'], "Status command"),
        (['run', '--dry-run', '--full'], "Run command (dry)"),
        (['extract', '--table', 'patient', '--dry-run'], "Extract command (dry)"),
        (['validate-data', '--table', 'patient', '--dry-run'], "Validate command (dry)"),
        (['patient-sync', '--dry-run'], "Patient sync (dry)"),
        (['appointment-metrics', '--dry-run'], "Appointment metrics (dry)"),
        (['compliance-check', '--dry-run'], "Compliance check (dry)"),
    ]
    
    passed = 0
    failed = 0
    
    for cmd_args, description in test_cases:
        print(f"\n🔍 Testing: {description}")
        success, stdout, stderr = test_cli_command(cmd_args, config_file)
        
        if success:
            print(f"  ✅ PASSED")
            passed += 1
        else:
            print(f"  ❌ FAILED")
            if stderr:
                print(f"     Error: {stderr[:200]}...")
            failed += 1
    
    # Cleanup
    Path(config_file).unlink()
    
    print(f"\n📊 Test Results:")
    print(f"  ✅ Passed: {passed}")
    print(f"  ❌ Failed: {failed}")
    print(f"  📈 Success Rate: {passed/(passed+failed)*100:.1f}%")
    
    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1) 