#!/usr/bin/env python3
"""
Simple test runner for ETL CLI functionality.
Run this to test your CLI without needing pytest.
"""
import subprocess
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def run_command(cmd, description):
    """Run a command and report results."""
    print(f"\n🔍 Testing: {description}")
    print(f"📝 Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"  ✅ PASSED")
            if result.stdout.strip():
                print(f"  📄 Output: {result.stdout.strip()[:200]}...")
            return True
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
    """Run CLI tests."""
    print("🧪 Testing ETL Pipeline CLI")
    print("=" * 60)
    
    # Change to project directory
    os.chdir(project_root)
    
    # Test cases
    test_cases = [
        # Basic help and info commands
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', '--help'], 
         "CLI Help"),
        
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', 'status'], 
         "Status Command"),
        
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', 'status', '--format', 'json'], 
         "Status JSON Format"),
        
        # Dry run commands
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', 'run', '--phase', '1', '--dry-run'], 
         "Run Phase 1 (Dry Run)"),
        
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', 'run', '--tables', 'patient', '--dry-run'], 
         "Run Specific Table (Dry Run)"),
        
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', 'validate', '--table', 'patient'], 
         "Validate Command"),
        
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', 'discover-schema', '--tables', 'patient'], 
         "Schema Discovery"),
        
        # Test connections (if available)
        ([sys.executable, '-m', 'etl_pipeline.cli.__main__', 'test-connections'], 
         "Test Connections"),
    ]
    
    # Also test the etl-run command if installed
    try:
        result = subprocess.run(['etl-run', '--help'], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            test_cases.extend([
                (['etl-run', '--help'], "ETL-Run Help"),
                (['etl-run', 'status'], "ETL-Run Status"),
                (['etl-run', 'run', '--phase', '1', '--dry-run'], "ETL-Run Phase 1 (Dry)"),
            ])
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("📝 Note: etl-run command not found (not installed as package)")
    
    # Run tests
    passed = 0
    failed = 0
    
    for cmd, description in test_cases:
        if run_command(cmd, description):
            passed += 1
        else:
            failed += 1
    
    # Summary
    total = passed + failed
    success_rate = (passed / total * 100) if total > 0 else 0
    
    print(f"\n📊 Test Summary")
    print("=" * 60)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {success_rate:.1f}%")
    
    if failed == 0:
        print("\n🎉 All tests passed! Your CLI is working correctly.")
    else:
        print(f"\n⚠️  {failed} test(s) failed. Check the errors above.")
        
        # Common troubleshooting tips
        print("\n💡 Troubleshooting Tips:")
        print("  • Make sure you've installed the package: pip install -e .")
        print("  • Check your .env file has the correct database credentials")
        print("  • Verify your database connections with: etl-test-connections")
        print("  • Some failures are expected if databases aren't set up")
    
    return failed == 0

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)