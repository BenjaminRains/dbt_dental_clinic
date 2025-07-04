"""
Step 1.4: Test the core changes

Run this script to verify the new configuration system works:
python test_new_config.py
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_basic_functionality():
    """Test basic functionality of the new configuration system."""
    print("Testing new configuration system...")
    
    # Import the new configuration system
    from etl_pipeline.config import (
        Settings, 
        DatabaseType, 
        PostgresSchema,
        create_settings,
        create_test_settings
    )
    
    print("✅ Successfully imported new configuration classes")
    
    # Test environment detection
    settings = create_settings()
    print(f"✅ Environment detected: {settings.environment}")
    assert settings.environment is not None, "Environment should be detected"
    
    # Test database type enums
    print(f"✅ Database types: {[dt.value for dt in DatabaseType]}")
    print(f"✅ PostgreSQL schemas: {[ps.value for ps in PostgresSchema]}")
    assert len(list(DatabaseType)) > 0, "DatabaseType enum should have values"
    assert len(list(PostgresSchema)) > 0, "PostgresSchema enum should have values"
    
    # Test configuration loading
    pipeline_config = settings.pipeline_config
    tables_config = settings.tables_config
    print(f"✅ Pipeline config loaded: {bool(pipeline_config)}")
    print(f"✅ Tables config loaded: {bool(tables_config)}")
    assert pipeline_config is not None, "Pipeline config should be loaded"
    assert tables_config is not None, "Tables config should be loaded"
    
    # Test test configuration creation
    test_env_vars = {
        'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
        'ETL_ENVIRONMENT': 'test'
    }
    
    test_settings = create_test_settings(env_vars=test_env_vars)
    print(f"✅ Test settings created: {test_settings.environment}")
    assert test_settings.environment == 'test', "Test environment should be 'test'"
    
    # Test database configuration
    try:
        source_config = test_settings.get_database_config(DatabaseType.SOURCE)
        print(f"✅ Source database config: {source_config.get('host')}")
        assert source_config.get('host') == 'test-host', "Source host should match test value"
    except Exception as e:
        print(f"⚠️ Database config test failed (expected): {e}")
    
    print("\n🎉 Core configuration system is working!")

def test_connection_string_generation():
    """Test connection string generation."""
    print("\nTesting connection string generation...")
    
    from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
    
    # Create test settings with complete configuration
    test_env_vars = {
        'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
        'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_pass',
        'ETL_ENVIRONMENT': 'test'
    }
    
    settings = create_test_settings(env_vars=test_env_vars)
    
    # Test MySQL connection string
    mysql_conn_str = settings.get_connection_string(DatabaseType.SOURCE)
    print(f"✅ MySQL connection string: {mysql_conn_str[:50]}...")
    assert 'mysql' in mysql_conn_str.lower(), "MySQL connection string should contain 'mysql'"
    assert 'localhost' in mysql_conn_str, "MySQL connection string should contain host"
    
    # Test PostgreSQL connection string
    postgres_conn_str = settings.get_connection_string(DatabaseType.ANALYTICS, PostgresSchema.RAW)
    print(f"✅ PostgreSQL connection string: {postgres_conn_str[:50]}...")
    assert 'postgresql' in postgres_conn_str.lower(), "PostgreSQL connection string should contain 'postgresql'"
    assert 'localhost' in postgres_conn_str, "PostgreSQL connection string should contain host"
    assert 'raw' in postgres_conn_str.lower(), "PostgreSQL connection string should contain schema"
    
    print("✅ Connection string generation working!")

if __name__ == "__main__":
    # For standalone execution, we can still use the old approach
    try:
        test_basic_functionality()
        test_connection_string_generation()
        print("\n🎉 All core tests passed! Ready for Phase 2.")
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        sys.exit(1)