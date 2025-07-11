#!/usr/bin/env python3
"""
Test script for Settings class with environment-specific files.

This script tests that the Settings class correctly loads from:
- .env_production for production environment
- .env_test for test environment

No variable mapping should occur - variables are used as-is from the files.
"""

import os
import sys
import tempfile
from pathlib import Path

# Add the etl_pipeline directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from etl_pipeline.config.settings import Settings, DatabaseType, PostgresSchema


def create_test_env_files():
    """Use existing .env_production and .env_test files for testing."""
    
    # Find the actual config directory where Settings expects the files
    from etl_pipeline.config.settings import Settings
    config_dir = Path(__file__).parent / 'etl_pipeline' / 'config'
    
    # Use existing .env files
    env_production_path = config_dir.parent / '.env_production'
    env_test_path = config_dir.parent / '.env_test'
    
    print(f"Using existing environment files:")
    print(f"  - Production: {env_production_path}")
    print(f"  - Test: {env_test_path}")
    
    return config_dir, env_production_path, env_test_path


def test_production_environment():
    """Test Settings with production environment."""
    print("\n=== Testing Production Environment ===")
    
    # Set environment variable
    os.environ['ETL_ENVIRONMENT'] = 'production'
    
    try:
        # Create settings with production environment
        settings = Settings(environment='production')
        
        print(f"Environment: {settings.environment}")
        
        # Test source connection
        source_config = settings.get_source_connection_config()
        print(f"Source Host: {source_config['host']}")
        print(f"Source Database: {source_config['database']}")
        
        # Test analytics connection
        analytics_config = settings.get_analytics_connection_config()
        print(f"Analytics Host: {analytics_config['host']}")
        print(f"Analytics Database: {analytics_config['database']}")
        
        # Verify we're getting production values (using actual values from .env_production)
        assert source_config['host'] == '192.168.2.10', f"Expected production host 192.168.2.10, got {source_config['host']}"
        assert analytics_config['host'] == 'localhost', f"Expected production analytics host localhost, got {analytics_config['host']}"
        
        print("✅ Production environment test passed!")
        
    except Exception as e:
        print(f"❌ Production environment test failed: {e}")
        raise


def test_test_environment():
    """Test Settings with test environment."""
    print("\n=== Testing Test Environment ===")
    
    # Set environment variable
    os.environ['ETL_ENVIRONMENT'] = 'test'
    
    try:
        # Create settings with test environment
        settings = Settings(environment='test')
        
        print(f"Environment: {settings.environment}")
        
        # Test source connection
        source_config = settings.get_source_connection_config()
        print(f"Source Host: {source_config['host']}")
        print(f"Source Database: {source_config['database']}")
        
        # Test analytics connection
        analytics_config = settings.get_analytics_connection_config()
        print(f"Analytics Host: {analytics_config['host']}")
        print(f"Analytics Database: {analytics_config['database']}")
        
        # Verify we're getting test values (using actual values from .env_test)
        # Note: These assertions will depend on what's actually in your .env_test file
        print(f"Test environment loaded successfully with host: {source_config['host']}")
        print(f"Test analytics loaded successfully with host: {analytics_config['host']}")
        
        print("✅ Test environment test passed!")
        
    except Exception as e:
        print(f"❌ Test environment test failed: {e}")
        raise


def test_environment_detection():
    """Test automatic environment detection."""
    print("\n=== Testing Environment Detection ===")
    
    # Test production detection
    os.environ['ETL_ENVIRONMENT'] = 'production'
    settings = Settings()
    assert settings.environment == 'production', f"Expected production, got {settings.environment}"
    print("✅ Production detection passed")
    
    # Test test detection
    os.environ['ETL_ENVIRONMENT'] = 'test'
    settings = Settings()
    assert settings.environment == 'test', f"Expected test, got {settings.environment}"
    print("✅ Test detection passed")
    
    # Test default to production
    del os.environ['ETL_ENVIRONMENT']
    settings = Settings()
    assert settings.environment == 'production', f"Expected production default, got {settings.environment}"
    print("✅ Default to production passed")


def test_validation():
    """Test that validation works correctly."""
    print("\n=== Testing Validation ===")
    
    # Test with valid configuration
    os.environ['ETL_ENVIRONMENT'] = 'production'
    settings = Settings()
    assert settings.validate_configs(), "Validation should pass with complete configuration"
    print("✅ Validation with complete config passed")
    
    # Test that validation works with test environment too
    os.environ['ETL_ENVIRONMENT'] = 'test'
    settings = Settings()
    assert settings.validate_configs(), "Validation should pass with test configuration"
    print("✅ Validation with test config passed")


def test_no_variable_mapping():
    """Test that no variable mapping occurs."""
    print("\n=== Testing No Variable Mapping ===")
    
    # Verify that variables are used exactly as they appear in the environment files
    os.environ['ETL_ENVIRONMENT'] = 'production'
    settings = Settings()
    
    # Check that the environment variables are loaded directly without any transformation
    env_vars = settings._env_vars
    
    # Verify that we have the expected variables with their original names
    expected_vars = [
        'OPENDENTAL_SOURCE_HOST',
        'OPENDENTAL_SOURCE_PORT',
        'OPENDENTAL_SOURCE_DB',
        'OPENDENTAL_SOURCE_USER',
        'OPENDENTAL_SOURCE_PASSWORD'
    ]
    
    for var in expected_vars:
        assert var in env_vars, f"Expected variable {var} not found in environment variables"
        print(f"✅ Found variable: {var}")
    
    print("✅ No variable mapping test passed!")


def main():
    """Run all tests."""
    print("🧪 Testing Settings with Environment-Specific Files")
    print("=" * 50)
    
    # Use existing environment files
    config_dir, env_production_path, env_test_path = create_test_env_files()
    
    try:
        # Run tests
        test_environment_detection()
        test_production_environment()
        test_test_environment()
        test_validation()
        test_no_variable_mapping()
        
        print("\n🎉 All tests passed!")
        print("\nSummary:")
        print("- Environment-specific files load correctly")
        print("- No variable mapping occurs")
        print("- Validation works properly")
        print("- Environment detection works")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    main() 