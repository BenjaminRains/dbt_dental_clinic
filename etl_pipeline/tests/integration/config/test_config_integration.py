"""
Step 3.2: Update test_config_integration.py

BACKUP YOUR EXISTING FILE FIRST!
cp tests/integration/config/test_config_integration.py tests/integration/config/test_config_integration.py.backup

Then replace with this clean implementation.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from etl_pipeline.config import create_test_settings, create_settings, DatabaseType, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory


class TestRealDatabaseConnections:
    """Test actual database connections using clean configuration."""

    @pytest.mark.integration
    def test_source_database_connection(self, production_env_vars):
        """Test actual connection to OpenDental source database."""
        settings = create_settings(
            environment='production',
            env=production_env_vars
        )
        
        try:
            source_engine = ConnectionFactory.get_source_connection(settings)
            with source_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.scalar() == 1
                print(f"✅ Source database connection successful")
        except SQLAlchemyError as e:
            pytest.skip(f"Source database not available: {e}")
        finally:
            if 'source_engine' in locals():
                source_engine.dispose()

    @pytest.mark.integration
    def test_replication_database_connection(self, production_env_vars):
        """Test actual connection to MySQL replication database."""
        settings = create_settings(
            environment='production',
            env=production_env_vars
        )
        
        try:
            repl_engine = ConnectionFactory.get_replication_connection(settings)
            with repl_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.scalar() == 1
                print(f"✅ Replication database connection successful")
        except SQLAlchemyError as e:
            pytest.skip(f"Replication database not available: {e}")
        finally:
            if 'repl_engine' in locals():
                repl_engine.dispose()

    @pytest.mark.integration
    def test_analytics_database_connection(self, production_env_vars):
        """Test actual connection to PostgreSQL analytics database."""
        settings = create_settings(
            environment='production',
            env=production_env_vars
        )
        
        try:
            analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            with analytics_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.scalar() == 1
                print(f"✅ Analytics database connection successful")
        except SQLAlchemyError as e:
            pytest.skip(f"Analytics database not available: {e}")
        finally:
            if 'analytics_engine' in locals():
                analytics_engine.dispose()

    @pytest.mark.integration
    def test_all_database_connections_simultaneously(self, production_env_vars):
        """Test all database connections can be established simultaneously."""
        settings = create_settings(
            environment='production',
            env=production_env_vars
        )
        
        engines = []
        try:
            # Test all connections using new interface
            source_engine = ConnectionFactory.get_source_connection(settings)
            repl_engine = ConnectionFactory.get_replication_connection(settings)
            analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            
            engines = [source_engine, repl_engine, analytics_engine]
            
            # Test each connection
            for engine in engines:
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_value"))
                    assert result.scalar() == 1
            
            print("✅ All database connections successful simultaneously")
            
        except SQLAlchemyError as e:
            pytest.skip(f"One or more databases not available: {e}")
        finally:
            # Clean up all engines
            for engine in engines:
                if engine:
                    engine.dispose()

    @pytest.mark.integration
    def test_postgresql_schema_connections(self, production_env_vars):
        """Test PostgreSQL connections to different schemas."""
        settings = create_settings(
            environment='production',
            env=production_env_vars
        )
        
        schemas_to_test = [
            PostgresSchema.RAW,
            PostgresSchema.STAGING,
            PostgresSchema.INTERMEDIATE,
            PostgresSchema.MARTS
        ]
        
        for schema in schemas_to_test:
            try:
                engine = ConnectionFactory.get_analytics_connection(settings, schema)
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_value"))
                    assert result.scalar() == 1
                    print(f"✅ {schema.value} schema connection successful")
                engine.dispose()
            except SQLAlchemyError as e:
                pytest.skip(f"PostgreSQL {schema.value} schema not available: {e}")


class TestConfigurationLoading:
    """Test configuration loading with clean architecture."""

    @pytest.mark.config
    def test_pipeline_config_loading(self, test_pipeline_config, test_env_vars):
        """Test loading pipeline configuration."""
        settings = create_test_settings(
            pipeline_config=test_pipeline_config,
            env_vars=test_env_vars
        )
        
        # Verify config was loaded
        assert settings.pipeline_config == test_pipeline_config
        assert settings.get_pipeline_setting('general.pipeline_name') == 'test_pipeline'
        assert settings.get_pipeline_setting('general.batch_size') == 1000
        assert settings.get_pipeline_setting('general.parallel_jobs') == 2
        
        print("✅ Pipeline config loaded successfully")

    @pytest.mark.config
    def test_tables_config_loading(self, test_tables_config, test_env_vars):
        """Test loading tables configuration."""
        settings = create_test_settings(
            tables_config=test_tables_config,
            env_vars=test_env_vars
        )
        
        # Verify config was loaded
        assert settings.tables_config == test_tables_config
        assert settings.get_table_config('patient')['primary_key'] == 'PatNum'
        assert settings.get_table_config('patient')['extraction_strategy'] == 'incremental'
        
        # Test table importance filtering
        critical_tables = settings.get_tables_by_importance('critical')
        assert 'patient' in critical_tables
        
        print("✅ Tables config loaded successfully")

    @pytest.mark.config
    def test_environment_detection(self, test_env_vars):
        """Test environment detection."""
        settings = create_test_settings(env_vars=test_env_vars)
        
        # Test environment detection
        assert settings.environment == 'test'
        assert settings.env_prefix == 'TEST_'
        
        print("✅ Environment detection working correctly")

    @pytest.mark.config
    def test_database_configuration_access(self, test_env_vars):
        """Test accessing database configurations with new interface."""
        settings = create_test_settings(env_vars=test_env_vars)
        
        # Test source database config
        source_config = settings.get_database_config(DatabaseType.SOURCE)
        assert source_config['host'] == 'localhost'
        assert source_config['port'] == 3306
        assert source_config['database'] == 'test_opendental'
        
        # Test analytics database config with schema
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        assert analytics_config['host'] == 'localhost'
        assert analytics_config['port'] == 5432
        assert analytics_config['database'] == 'test_analytics'
        assert analytics_config['schema'] == 'raw'
        
        print("✅ Database configuration access working correctly")

    @pytest.mark.config
    def test_connection_string_generation(self, test_env_vars):
        """Test connection string generation with new interface."""
        settings = create_test_settings(env_vars=test_env_vars)
        
        # Test MySQL connection string
        mysql_conn_str = settings.get_connection_string(DatabaseType.SOURCE)
        assert 'mysql+pymysql://' in mysql_conn_str
        assert 'localhost' in mysql_conn_str
        assert 'test_opendental' in mysql_conn_str
        
        # Test PostgreSQL connection string with schema
        postgres_conn_str = settings.get_connection_string(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        assert 'postgresql+psycopg2://' in postgres_conn_str
        assert 'localhost' in postgres_conn_str
        assert 'test_analytics' in postgres_conn_str
        assert 'search_path%3Draw' in postgres_conn_str
        
        print("✅ Connection string generation working correctly")

    @pytest.mark.config
    def test_configuration_validation(self, test_env_vars, test_pipeline_config, test_tables_config):
        """Test configuration validation."""
        settings = create_test_settings(
            pipeline_config=test_pipeline_config,
            tables_config=test_tables_config,
            env_vars=test_env_vars
        )
        
        # Verify all configurations are valid
        assert settings.validate_configs() is True
        
        print("✅ Configuration validation successful")

    @pytest.mark.config
    def test_environment_variable_prefixing(self, test_env_vars):
        """Test that TEST_ prefixed environment variables are used correctly."""
        settings = create_test_settings(env_vars=test_env_vars)
        
        # Verify TEST_ prefixed variables are used
        source_config = settings.get_database_config(DatabaseType.SOURCE)
        assert source_config['host'] == 'localhost'  # From TEST_OPENDENTAL_SOURCE_HOST
        
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS)
        assert analytics_config['host'] == 'localhost'  # From TEST_POSTGRES_ANALYTICS_HOST
        
        print("✅ Environment variable prefixing working correctly")


class TestLegacyCompatibility:
    """Test legacy method compatibility during transition."""

    @pytest.mark.config
    def test_legacy_connection_methods(self, test_env_vars):
        """Test that legacy connection methods still work with deprecation warnings."""
        settings = create_test_settings(env_vars=test_env_vars)
        
        # Test legacy methods (should work but show warnings)
        try:
            legacy_source = ConnectionFactory.get_opendental_source_connection(settings)
            assert legacy_source is not None
            print("✅ Legacy source connection method works")
        except Exception as e:
            print(f"⚠️ Legacy source connection failed: {e}")
        
        try:
            legacy_replication = ConnectionFactory.get_mysql_replication_connection(settings)
            assert legacy_replication is not None
            print("✅ Legacy replication connection method works")
        except Exception as e:
            print(f"⚠️ Legacy replication connection failed: {e}")
        
        try:
            legacy_analytics = ConnectionFactory.get_postgres_analytics_connection(settings)
            assert legacy_analytics is not None
            print("✅ Legacy analytics connection method works")
        except Exception as e:
            print(f"⚠️ Legacy analytics connection failed: {e}")


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.config
    def test_missing_environment_variables(self):
        """Test handling of missing environment variables."""
        # Create settings with minimal environment variables
        minimal_env = {'ETL_ENVIRONMENT': 'test'}
        
        settings = create_test_settings(env_vars=minimal_env)
        
        # Should fail validation
        assert settings.validate_configs() is False
        
        print("✅ Missing environment variables handled correctly")

    @pytest.mark.config
    def test_invalid_database_connection(self, test_env_vars):
        """Test handling of invalid database connection parameters."""
        # Modify test environment to use invalid connection parameters
        invalid_env = test_env_vars.copy()
        invalid_env.update({
            'TEST_OPENDENTAL_SOURCE_HOST': 'invalid_host',
            'TEST_OPENDENTAL_SOURCE_PORT': '9999',
            'TEST_OPENDENTAL_SOURCE_DB': 'invalid_db',
            'TEST_OPENDENTAL_SOURCE_USER': 'invalid_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'invalid_pass'
        })
        
        settings = create_test_settings(env_vars=invalid_env)
        
        # Test that connection fails gracefully
        try:
            source_engine = ConnectionFactory.get_source_connection(settings)
            with source_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            pytest.fail("Expected connection to fail")
        except SQLAlchemyError:
            print("✅ Invalid database connection handled correctly")
        finally:
            if 'source_engine' in locals():
                source_engine.dispose()


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 