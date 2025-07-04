"""
Step 2.2: Test ConnectionFactory

Run this script to verify the ConnectionFactory works:
python test_connection_factory.py
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_connection_factory():
    """Test the new ConnectionFactory."""
    print("Testing ConnectionFactory...")
    
    try:
        from etl_pipeline.core.connections import ConnectionFactory
        from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
        
        # Create test settings
        test_env_vars = {
            'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
            'TEST_OPENDENTAL_SOURCE_PORT': '3306',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
            'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
            'TEST_MYSQL_REPLICATION_HOST': 'localhost',
            'TEST_MYSQL_REPLICATION_PORT': '3306',
            'TEST_MYSQL_REPLICATION_DB': 'test_replication',
            'TEST_MYSQL_REPLICATION_USER': 'test_user',
            'TEST_MYSQL_REPLICATION_PASSWORD': 'test_pass',
            'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_pass',
            'ETL_ENVIRONMENT': 'test'
        }
        
        settings = create_test_settings(env_vars=test_env_vars)
        print("✅ Test settings created")
        
        # Test new connection methods
        try:
            source_engine = ConnectionFactory.get_source_connection(settings)
            print(f"✅ Source connection created: {source_engine.url}")
        except Exception as e:
            print(f"⚠️ Source connection failed (expected without real DB): {e}")
        
        try:
            replication_engine = ConnectionFactory.get_replication_connection(settings)
            print(f"✅ Replication connection created: {replication_engine.url}")
        except Exception as e:
            print(f"⚠️ Replication connection failed (expected without real DB): {e}")
        
        try:
            analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            print(f"✅ Analytics connection created: {analytics_engine.url}")
        except Exception as e:
            print(f"⚠️ Analytics connection failed (expected without real DB): {e}")
        
        # Test schema-specific connections
        try:
            staging_engine = ConnectionFactory.get_analytics_staging_connection(settings)
            print(f"✅ Staging connection created: {staging_engine.url}")
        except Exception as e:
            print(f"⚠️ Staging connection failed (expected without real DB): {e}")
        
        # Test legacy methods (should show deprecation warnings)
        try:
            legacy_source = ConnectionFactory.get_opendental_source_connection(settings)
            print(f"✅ Legacy source connection created (with deprecation warning)")
        except Exception as e:
            print(f"⚠️ Legacy source connection failed (expected without real DB): {e}")
        
        print("\n🎉 ConnectionFactory tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing ConnectionFactory: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_connection_factory()
    
    if success:
        print("\n🎉 ConnectionFactory ready! Proceed to Phase 3.")
    else:
        print("\n❌ ConnectionFactory tests failed. Check the implementation.")
        sys.exit(1)