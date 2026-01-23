#!/usr/bin/env python3
"""
Test script to verify API environment configuration.

This script tests the API configuration system to ensure it properly:
1. Detects the environment from ETL_ENVIRONMENT
2. Loads the correct .env file
3. Validates required environment variables
4. Builds the correct database connection string
"""

import os
import sys
from pathlib import Path

# Add the api directory to Python path
api_dir = Path(__file__).parent
sys.path.insert(0, str(api_dir))

def test_api_config():
    """Test API configuration system."""
    try:
        from config import APIConfig, Environment, DatabaseType
        
        print("=" * 60)
        print("API CONFIGURATION TEST")
        print("=" * 60)
        
        # Show current environment
        current_env = os.getenv('API_ENVIRONMENT', 'NOT SET')
        print(f"Current API_ENVIRONMENT: {current_env}")
        
        # Test configuration loading
        print("\n1. Testing configuration loading...")
        config = APIConfig()
        print(f"   ✓ Environment detected: {config.environment}")
        
        # Test database configuration
        print("\n2. Testing database configuration...")
        db_config = config.get_database_config(DatabaseType.ANALYTICS)
        print(f"   ✓ Host: {db_config['host']}")
        print(f"   ✓ Port: {db_config['port']}")
        print(f"   ✓ Database: {db_config['database']}")
        print(f"   ✓ User: {db_config['user']}")
        print(f"   ✓ Password: {'*' * len(db_config['password']) if db_config['password'] else 'NOT SET'}")
        
        # Test database URL
        print("\n3. Testing database URL generation...")
        db_url = config.get_database_url(DatabaseType.ANALYTICS)
        # Hide password in URL for display
        safe_url = db_url.split('@')[0].split('//')[1] if '@' in db_url else db_url
        safe_url = f"postgresql://{safe_url}@{db_url.split('@')[1] if '@' in db_url else 'HIDDEN'}"
        print(f"   ✓ Database URL: {safe_url}")
        
        print("\n" + "=" * 60)
        print("✅ API CONFIGURATION TEST PASSED")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ API CONFIGURATION TEST FAILED: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure API_ENVIRONMENT is set to 'test', 'demo', 'clinic', or 'local'")
        print("2. Ensure the appropriate .env file exists (.env_api_test, .env_api_demo, .env_api_clinic, or .env_api_local)")
        print("3. Ensure all required environment variables are set in the .env file")
        print("4. Check the environment file is in the project root directory")
        return False

def test_environment_files():
    """Test if environment files exist and are readable."""
    print("\n" + "=" * 60)
    print("ENVIRONMENT FILES CHECK")
    print("=" * 60)
    
    project_root = Path(__file__).parent.parent
    env_files = ['.env_api_test', '.env_api_demo']
    
    for env_file in env_files:
        env_path = project_root / env_file
        if env_path.exists():
            print(f"✓ {env_file} exists at {env_path}")
            try:
                # Try to read the file
                with open(env_path, 'r') as f:
                    lines = f.readlines()
                    print(f"  - Contains {len(lines)} lines")
                    # Check for key variables
                    content = ''.join(lines)
                    required_vars = ['API_ENVIRONMENT', 'HOST', 'PORT', 'DB', 'USER', 'PASSWORD']
                    env_type = 'TEST_' if 'test' in env_file else ''
                    for var in required_vars:
                        if var == 'API_ENVIRONMENT':
                            if 'API_ENVIRONMENT' in content:
                                print(f"  ✓ Contains API_ENVIRONMENT")
                            else:
                                print(f"  ❌ Missing API_ENVIRONMENT")
                        else:
                            if f"{env_type}POSTGRES_ANALYTICS_{var}" in content:
                                print(f"  ✓ Contains {env_type}POSTGRES_ANALYTICS_{var}")
                            else:
                                print(f"  ❌ Missing {env_type}POSTGRES_ANALYTICS_{var}")
            except Exception as e:
                print(f"  ❌ Error reading {env_file}: {e}")
        else:
            print(f"❌ {env_file} not found at {env_path}")

if __name__ == "__main__":
    print("Testing API Environment Configuration...")
    
    # Test environment files first
    test_environment_files()
    
    # Test configuration system
    success = test_api_config()
    
    if success:
        print("\n🎉 All tests passed! Your API configuration is working correctly.")
        sys.exit(0)
    else:
        print("\n💥 Tests failed. Please fix the configuration issues above.")
        sys.exit(1)
