#!/usr/bin/env python3
"""
Simple test script to verify ETL setup is working.
Run this after installing the package to ensure everything works.
"""
import subprocess
import sys
import os
from pathlib import Path
import pytest

@pytest.mark.skip(reason="This is a standalone script, not a pytest test")
def test_command(cmd, description, expected_in_output=None):
    """Test a command and report results."""
    print(f"\n🔍 Testing: {description}")
    print(f"📝 Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            success = True
            if expected_in_output:
                success = expected_in_output in result.stdout
            
            if success:
                print(f"  ✅ PASSED")
                return True
            else:
                print(f"  ⚠️  PASSED but unexpected output")
                return False
        else:
            print(f"  ❌ FAILED (exit code: {result.returncode})")
            if result.stderr.strip():
                print(f"  ❗ Error: {result.stderr.strip()[:200]}...")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"  ⏰ TIMEOUT")
        return False
    except Exception as e:
        print(f"  💥 EXCEPTION: {str(e)}")
        return False

def main():
    """Run ETL setup tests."""
    print("🧪 Testing ETL Pipeline Setup")
    print("=" * 50)
    
    # Change to project root if needed
    if Path("dbt_project.yml").exists():
        print("✅ Found dbt project - in correct directory")
    else:
        print("⚠️  No dbt_project.yml found - make sure you're in the project root")
    
    # Test cases
    tests = [
        # Test existing working commands
        (["etl-init", "--help"], "ETL Init Help", "help"),
        (["etl-test-connections", "--help"], "ETL Test Connections Help", None),
        
        # Test new CLI
        (["etl-run", "--help"], "ETL Run Help", "ETL Pipeline CLI"),
        (["etl-run", "status"], "ETL Status", None),
        (["etl-run", "run", "--phase", "1", "--dry-run"], "ETL Run Dry Mode", "DRY RUN"),
        
        # Test Python module access
        ([sys.executable, "-c", "import etl_pipeline; print('Import OK')"], "Python Import", "Import OK"),
        ([sys.executable, "-m", "etl_pipeline.cli.__main__", "--help"], "CLI Module", "ETL Pipeline CLI"),
    ]
    
    passed = 0
    failed = 0
    
    for cmd, description, expected in tests:
        if test_command(cmd, description, expected):
            passed += 1
        else:
            failed += 1
    
    # Summary
    total = passed + failed
    success_rate = (passed / total * 100) if total > 0 else 0
    
    print(f"\n📊 Test Summary")
    print("=" * 50)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {success_rate:.1f}%")
    
    if failed == 0:
        print("\n🎉 All tests passed! Your ETL setup is working correctly.")
        print("\n📋 Available Commands:")
        print("  etl-init              - Initialize ETL environment")
        print("  etl-test-connections  - Test database connections")
        print("  etl-run --help        - Show CLI help")
        print("  etl-run status        - Show pipeline status")
        print("  etl-run run --dry-run - Test pipeline run")
        
    else:
        print(f"\n⚠️  {failed} test(s) failed.")
        print("\n💡 Troubleshooting:")
        print("  1. Make sure you've installed: pip install -e .")
        print("  2. Check your .env file exists and has correct settings")
        print("  3. Ensure you're in the project root directory")
        print("  4. Try running: pip install -e . --force-reinstall")
    
    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)