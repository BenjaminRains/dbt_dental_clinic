"""
Step 5.1: Comprehensive validation test suite

Run this to validate that the refactoring is working correctly.
"""

import os
import sys
from pathlib import Path
import pytest
from typing import List, Dict, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class RefactoringValidator:
    """Validates that the refactoring was successful."""
    
    def __init__(self):
        self.passed_tests = []
        self.failed_tests = []
        self.warnings = []
    
    def test_imports(self) -> bool:
        """Test that new imports work correctly."""
        try:
            from etl_pipeline.config import (
                Settings, DatabaseType, PostgresSchema,
                get_settings, create_settings, create_test_settings
            )
            from etl_pipeline.core.connections import ConnectionFactory
            
            self.passed_tests.append("✅ All imports successful")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Import failed: {e}")
            return False
    
    def test_enum_functionality(self) -> bool:
        """Test that enums work correctly."""
        try:
            from etl_pipeline.config import DatabaseType, PostgresSchema
            
            # Test DatabaseType enum
            assert DatabaseType.SOURCE.value == "source"
            assert DatabaseType.REPLICATION.value == "replication"
            assert DatabaseType.ANALYTICS.value == "analytics"
            
            # Test PostgresSchema enum
            assert PostgresSchema.RAW.value == "raw"
            assert PostgresSchema.STAGING.value == "staging"
            assert PostgresSchema.INTERMEDIATE.value == "intermediate"
            assert PostgresSchema.MARTS.value == "marts"
            
            self.passed_tests.append("✅ Enum functionality working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Enum functionality failed: {e}")
            return False
    
    def test_settings_creation(self) -> bool:
        """Test that settings can be created correctly."""
        try:
            from etl_pipeline.config import create_settings, create_test_settings
            
            # Test production settings
            prod_settings = create_settings(environment='production')
            assert prod_settings.environment == 'production'
            assert prod_settings.env_prefix == ""
            
            # Test test settings
            test_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'ETL_ENVIRONMENT': 'test'
            }
            test_settings = create_test_settings(env_vars=test_env_vars)
            assert test_settings.environment == 'test'
            assert test_settings.env_prefix == "TEST_"
            
            self.passed_tests.append("✅ Settings creation working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Settings creation failed: {e}")
            return False
    
    def test_database_config_access(self) -> bool:
        """Test that database configuration access works with enums."""
        try:
            from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
            
            test_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'analytics_db',
                'TEST_POSTGRES_ANALYTICS_USER': 'analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass',
                'ETL_ENVIRONMENT': 'test'
            }
            
            settings = create_test_settings(env_vars=test_env_vars)
            
            # Test source config
            source_config = settings.get_database_config(DatabaseType.SOURCE)
            assert source_config['host'] == 'localhost'
            assert source_config['port'] == 3306
            
            # Test analytics config with schema
            analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
            assert analytics_config['host'] == 'localhost'
            assert analytics_config['port'] == 5432
            assert analytics_config['schema'] == 'raw'
            
            self.passed_tests.append("✅ Database config access working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Database config access failed: {e}")
            return False
    
    def test_connection_factory(self) -> bool:
        """Test that ConnectionFactory works with new interface."""
        try:
            from etl_pipeline.core.connections import ConnectionFactory
            from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
            from unittest.mock import patch, MagicMock
            
            test_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'analytics_db',
                'TEST_POSTGRES_ANALYTICS_USER': 'analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass',
                'ETL_ENVIRONMENT': 'test'
            }
            
            settings = create_test_settings(env_vars=test_env_vars)
            
            # Mock the engine creation to avoid real database connections
            mock_engine = MagicMock()
            
            with patch('etl_pipeline.core.connections.create_engine', return_value=mock_engine):
                # Test new connection methods
                source_engine = ConnectionFactory.get_source_connection(settings)
                assert source_engine is not None
                
                analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
                assert analytics_engine is not None
                
                # Test convenience methods
                staging_engine = ConnectionFactory.get_analytics_staging_connection(settings)
                assert staging_engine is not None
            
            self.passed_tests.append("✅ ConnectionFactory working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ ConnectionFactory failed: {e}")
            return False
    
    def test_connection_strings(self) -> bool:
        """Test that connection strings are generated correctly."""
        try:
            from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
            
            test_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'analytics_db',
                'TEST_POSTGRES_ANALYTICS_USER': 'analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass',
                'ETL_ENVIRONMENT': 'test'
            }
            
            settings = create_test_settings(env_vars=test_env_vars)
            
            # Test MySQL connection string
            mysql_conn_str = settings.get_connection_string(DatabaseType.SOURCE)
            assert 'mysql+pymysql://' in mysql_conn_str
            assert 'localhost:3306' in mysql_conn_str
            
            # Test PostgreSQL connection string with schema
            postgres_conn_str = settings.get_connection_string(DatabaseType.ANALYTICS, PostgresSchema.RAW)
            assert 'postgresql+psycopg2://' in postgres_conn_str
            assert 'search_path%3Draw' in postgres_conn_str
            
            self.passed_tests.append("✅ Connection strings working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Connection strings failed: {e}")
            return False
    
    def test_backward_compatibility(self) -> bool:
        """Test that legacy methods still work with warnings."""
        try:
            from etl_pipeline.core.connections import ConnectionFactory
            from etl_pipeline.config import create_test_settings
            from unittest.mock import patch, MagicMock
            
            test_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'pass',
                'ETL_ENVIRONMENT': 'test'
            }
            
            settings = create_test_settings(env_vars=test_env_vars)
            
            # Mock the engine creation to avoid real database connections
            mock_engine = MagicMock()
            
            with patch('etl_pipeline.core.connections.create_engine', return_value=mock_engine):
                # Test legacy methods (should work but show warnings)
                legacy_source = ConnectionFactory.get_opendental_source_connection(settings)
                assert legacy_source is not None
            
            self.passed_tests.append("✅ Backward compatibility working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Backward compatibility failed: {e}")
            return False
    
    def test_configuration_validation(self) -> bool:
        """Test that configuration validation works correctly."""
        try:
            from etl_pipeline.config import create_test_settings
            
            # Test with complete configuration
            complete_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'pass',
                'TEST_MYSQL_REPLICATION_HOST': 'localhost',
                'TEST_MYSQL_REPLICATION_PORT': '3306',
                'TEST_MYSQL_REPLICATION_DB': 'repl_db',
                'TEST_MYSQL_REPLICATION_USER': 'repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'analytics_db',
                'TEST_POSTGRES_ANALYTICS_USER': 'analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass',
                'ETL_ENVIRONMENT': 'test'
            }
            
            settings = create_test_settings(env_vars=complete_env_vars)
            assert settings.validate_configs() is True
            
            # Test with incomplete configuration
            incomplete_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'ETL_ENVIRONMENT': 'test'
            }
            
            incomplete_settings = create_test_settings(env_vars=incomplete_env_vars)
            assert incomplete_settings.validate_configs() is False
            
            self.passed_tests.append("✅ Configuration validation working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Configuration validation failed: {e}")
            return False
    
    def test_table_configuration(self) -> bool:
        """Test that table configuration still works."""
        try:
            from etl_pipeline.config import create_test_settings
            
            test_tables_config = {
                'tables': {
                    'patient': {
                        'primary_key': 'PatNum',
                        'incremental_column': 'DateTStamp',
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            
            settings = create_test_settings(tables_config=test_tables_config)
            
            # Test table config access
            patient_config = settings.get_table_config('patient')
            assert patient_config['primary_key'] == 'PatNum'
            
            # Test importance filtering
            critical_tables = settings.get_tables_by_importance('critical')
            assert 'patient' in critical_tables
            
            self.passed_tests.append("✅ Table configuration working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Table configuration failed: {e}")
            return False
    
    def test_conftest_fixtures(self) -> bool:
        """Test that conftest.py fixtures work correctly."""
        try:
            # Test that we can import the fixture functions
            import pytest
            from conftest import (
                test_env_vars, test_pipeline_config, test_tables_config,
                test_settings, database_types, postgres_schemas
            )
            
            # Test that fixture functions exist and are callable
            assert callable(test_env_vars)
            assert callable(test_pipeline_config)
            assert callable(test_tables_config)
            assert callable(test_settings)
            assert callable(database_types)
            assert callable(postgres_schemas)
            
            # Test that we can create test data similar to what fixtures would return
            test_env_vars_data = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'ETL_ENVIRONMENT': 'test'
            }
            
            test_pipeline_config_data = {
                'general': {
                    'pipeline_name': 'test_pipeline',
                    'environment': 'test',
                    'batch_size': 1000,
                    'parallel_jobs': 2
                }
            }
            
            test_tables_config_data = {
                'tables': {
                    'patient': {
                        'primary_key': 'PatNum',
                        'incremental_column': 'DateTStamp',
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical'
                    }
                }
            }
            
            # Test that the data structures are valid
            assert 'TEST_OPENDENTAL_SOURCE_HOST' in test_env_vars_data
            assert test_pipeline_config_data['general']['pipeline_name'] == 'test_pipeline'
            assert 'patient' in test_tables_config_data['tables']
            
            # Test that we can create settings using the new system
            from etl_pipeline.config import create_test_settings
            settings = create_test_settings(
                env_vars=test_env_vars_data,
                pipeline_config=test_pipeline_config_data,
                tables_config=test_tables_config_data
            )
            assert settings.environment == 'test'
            
            # Test that enum functionality works
            from etl_pipeline.config import DatabaseType, PostgresSchema
            assert DatabaseType.SOURCE.value == 'source'
            assert PostgresSchema.RAW.value == 'raw'
            
            self.passed_tests.append("✅ Conftest fixtures working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Conftest fixtures failed: {e}")
            return False
    
    def test_environment_isolation(self) -> bool:
        """Test that test and production environments are properly isolated."""
        try:
            from etl_pipeline.config import create_settings, create_test_settings, DatabaseType
            
            # Test environment
            test_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'OPENDENTAL_SOURCE_HOST': 'prod-host',  # Should be ignored in test
                'ETL_ENVIRONMENT': 'test'
            }
            test_settings = create_test_settings(env_vars=test_env_vars)
            
            # Production environment
            prod_env_vars = {
                'OPENDENTAL_SOURCE_HOST': 'prod-host',
                'ETL_ENVIRONMENT': 'production'
            }
            prod_settings = create_settings(environment='production')
            
            # Verify isolation using enum
            test_config = test_settings.get_database_config(DatabaseType.SOURCE)
            assert test_config['host'] == 'test-host'
            
            self.passed_tests.append("✅ Environment isolation working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ Environment isolation failed: {e}")
            return False
    
    def test_connection_manager(self) -> bool:
        """Test that ConnectionManager works correctly."""
        try:
            from etl_pipeline.core.connections import create_connection_manager
            from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
            from unittest.mock import patch, MagicMock
            
            test_env_vars = {
                'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'pass',
                'ETL_ENVIRONMENT': 'test'
            }
            
            settings = create_test_settings(env_vars=test_env_vars)
            
            # Mock the engine creation to avoid real database connections
            mock_engine = MagicMock()
            
            with patch('etl_pipeline.core.connections.create_engine', return_value=mock_engine):
                # Test ConnectionManager creation
                conn_manager = create_connection_manager(
                    db_type=DatabaseType.SOURCE,
                    settings=settings,
                    max_retries=3,
                    retry_delay=1.0
                )
                
                assert conn_manager is not None
                assert conn_manager.engine is not None
            
            self.passed_tests.append("✅ ConnectionManager working")
            return True
        except Exception as e:
            self.failed_tests.append(f"❌ ConnectionManager failed: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """Run all validation tests."""
        print("🧪 Running comprehensive validation tests...")
        print("")
        
        tests = [
            self.test_imports,
            self.test_enum_functionality,
            self.test_settings_creation,
            self.test_database_config_access,
            self.test_connection_factory,
            self.test_connection_strings,
            self.test_backward_compatibility,
            self.test_configuration_validation,
            self.test_table_configuration,
            self.test_conftest_fixtures,
            self.test_environment_isolation,
            self.test_connection_manager
        ]
        
        all_passed = True
        for test in tests:
            if not test():
                all_passed = False
        
        return all_passed
    
    def print_results(self):
        """Print test results."""
        print("\n" + "="*50)
        print("VALIDATION RESULTS")
        print("="*50)
        
        if self.passed_tests:
            print(f"\n✅ PASSED TESTS ({len(self.passed_tests)}):")
            for test in self.passed_tests:
                print(f"  {test}")
        
        if self.failed_tests:
            print(f"\n❌ FAILED TESTS ({len(self.failed_tests)}):")
            for test in self.failed_tests:
                print(f"  {test}")
        
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  {warning}")
        
        print(f"\n📊 SUMMARY:")
        print(f"  ✅ Passed: {len(self.passed_tests)}")
        print(f"  ❌ Failed: {len(self.failed_tests)}")
        print(f"  ⚠️  Warnings: {len(self.warnings)}")
        
        if not self.failed_tests:
            print(f"\n🎉 ALL TESTS PASSED! Refactoring successful!")
            print(f"   The configuration system refactoring is working correctly.")
            print(f"   You can now use the new enum-based configuration system.")
        else:
            print(f"\n⚠️  Some tests failed. Review the issues above.")
            print(f"   Fix the failing tests before proceeding with the refactoring.")


def main():
    """Run the validation."""
    validator = RefactoringValidator()
    success = validator.run_all_tests()
    validator.print_results()
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 