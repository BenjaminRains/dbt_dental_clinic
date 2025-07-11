#!/usr/bin/env python3
"""
Test script to debug environment variable loading issues.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def test_environment_loading():
    """Test different methods of loading environment variables."""
    
    print("=== Environment Variable Loading Test ===")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python executable: {sys.executable}")
    print()
    
    # Test 1: Check current environment variables
    print("1. Current environment variables:")
    print(f"   ETL_ENVIRONMENT: {os.getenv('ETL_ENVIRONMENT', 'NOT SET')}")
    print(f"   OPENDENTAL_SOURCE_HOST: {os.getenv('OPENDENTAL_SOURCE_HOST', 'NOT SET')}")
    print()
    
    # Test 2: Check if .env files exist
    print("2. Checking for .env files:")
    env_files = [
        ".env_production",
        "etl_pipeline/.env_production", 
        "etl_pipeline/.env_test"
    ]
    
    for env_file in env_files:
        if Path(env_file).exists():
            print(f"   ✅ {env_file} exists")
            # Show first few lines
            with open(env_file, 'r') as f:
                lines = f.readlines()[:3]
                for line in lines:
                    print(f"      {line.strip()}")
        else:
            print(f"   ❌ {env_file} does not exist")
    print()
    
    # Test 3: Try loading .env files manually
    print("3. Manually loading .env files:")
    
    for env_file in env_files:
        if Path(env_file).exists():
            print(f"   Loading {env_file}...")
            try:
                load_dotenv(env_file, override=True)
                print(f"   ✅ Successfully loaded {env_file}")
            except Exception as e:
                print(f"   ❌ Failed to load {env_file}: {e}")
    
    print()
    
    # Test 4: Check environment variables after loading
    print("4. Environment variables after loading:")
    print(f"   ETL_ENVIRONMENT: {os.getenv('ETL_ENVIRONMENT', 'NOT SET')}")
    print(f"   OPENDENTAL_SOURCE_HOST: {os.getenv('OPENDENTAL_SOURCE_HOST', 'NOT SET')}")
    print()
    
    # Test 5: Test the ETL pipeline's environment loading
    print("5. Testing ETL pipeline environment loading:")
    try:
        from etl_pipeline.etl_pipeline.config import get_settings
        settings = get_settings()
        print(f"   ✅ Successfully created settings with environment: {settings.environment}")
        print(f"   Environment variables in settings: {len(settings._env_vars)} variables")
    except Exception as e:
        print(f"   ❌ Failed to create settings: {e}")
    
    print()
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_environment_loading() 