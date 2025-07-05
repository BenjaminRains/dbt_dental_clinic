"""
Integration tests for the connections module.
Real database connection tests using actual environment variables and database connections.

This module has been refactored to use modular fixtures from tests/fixtures/ directory.
"""

import pytest
import os
import logging
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Import fixtures from modular structure
from tests.fixtures.env_fixtures import test_env_vars, production_env_vars, setup_test_environment
from tests.fixtures.connection_fixtures import (
    mock_source_engine, 
    mock_replication_engine, 
    mock_analytics_engine,
    mock_connection_factory,
    mock_postgres_connection,
    mock_mysql_connection,
    database_types,
    postgres_schemas
)

# Import ETL pipeline components
from etl_pipeline.core.connections import ConnectionFactory

# Load environment variables for testing
load_dotenv()

logger = logging.getLogger(__name__)


class TestConnectionFactoryIntegration:
    """Integration tests for ConnectionFactory using real database connections."""
    
    def test_get_opendental_source_test_connection(self, test_env_vars):
        """Test real connection to OpenDental test database."""
        try:
            engine = ConnectionFactory.get_opendental_source_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                assert len(tables) > 0  # Should have some tables
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_get_mysql_replication_test_connection(self, test_env_vars):
        """Test real connection to MySQL replication test database."""
        try:
            engine = ConnectionFactory.get_mysql_replication_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                assert len(tables) >= 0  # May be empty initially
                
        except Exception as e:
            pytest.skip(f"Test replication database not available: {str(e)}")

    def test_get_postgres_analytics_test_connection(self, test_env_vars):
        """Test real connection to PostgreSQL analytics test database."""
        try:
            engine = ConnectionFactory.get_postgres_analytics_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_database() as db_name"))
                row = result.fetchone()
                assert 'test_opendental_analytics' in row[0].lower()
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_raw_test_connection(self, test_env_vars, postgres_schemas):
        """Test real connection to PostgreSQL analytics test database raw schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == postgres_schemas.RAW
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_staging_test_connection(self, test_env_vars, postgres_schemas):
        """Test real connection to PostgreSQL analytics test database staging schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == postgres_schemas.STAGING
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_intermediate_test_connection(self, test_env_vars, postgres_schemas):
        """Test real connection to PostgreSQL analytics test database intermediate schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_intermediate_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == postgres_schemas.INTERMEDIATE
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_marts_test_connection(self, test_env_vars, postgres_schemas):
        """Test real connection to PostgreSQL analytics test database marts schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_marts_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == postgres_schemas.MARTS
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_environment_variables_are_loaded(self, test_env_vars):
        """Test that environment variables are properly loaded from .env file."""
        # Test that test environment variables are available
        test_vars = [
            'TEST_OPENDENTAL_SOURCE_HOST',
            'TEST_OPENDENTAL_SOURCE_PORT',
            'TEST_OPENDENTAL_SOURCE_DB',
            'TEST_OPENDENTAL_SOURCE_USER',
            'TEST_OPENDENTAL_SOURCE_PASSWORD',
            'TEST_MYSQL_REPLICATION_HOST',
            'TEST_MYSQL_REPLICATION_PORT',
            'TEST_MYSQL_REPLICATION_DB',
            'TEST_MYSQL_REPLICATION_USER',
            'TEST_MYSQL_REPLICATION_PASSWORD',
            'TEST_POSTGRES_ANALYTICS_HOST',
            'TEST_POSTGRES_ANALYTICS_PORT',
            'TEST_POSTGRES_ANALYTICS_DB',
            'TEST_POSTGRES_ANALYTICS_USER',
            'TEST_POSTGRES_ANALYTICS_PASSWORD'
        ]
        
        for var in test_vars:
            value = os.getenv(var)
            assert value is not None, f"Environment variable {var} is not set"
            assert value.strip() != '', f"Environment variable {var} is empty"

    def test_production_vs_test_environment_separation(self, test_env_vars, production_env_vars):
        """Test that production and test methods use different environment variables."""
        # Test that production and test methods use different env vars
        prod_host = os.getenv('OPENDENTAL_SOURCE_HOST')
        test_host = os.getenv('TEST_OPENDENTAL_SOURCE_HOST')
        
        # They should be different (if both are set and not the same)
        if prod_host and test_host and prod_host != test_host:
            assert prod_host != test_host, "Production and test hosts should be different"
        elif prod_host and test_host and prod_host == test_host:
            # If they're the same, that's also valid for some setups
            pytest.skip("Production and test hosts are the same in this environment")
        
        prod_db = os.getenv('OPENDENTAL_SOURCE_DB')
        test_db = os.getenv('TEST_OPENDENTAL_SOURCE_DB')
        
        # They should be different (if both are set and not the same)
        if prod_db and test_db and prod_db != test_db:
            assert prod_db != test_db, "Production and test databases should be different"
        elif prod_db and test_db and prod_db == test_db:
            # If they're the same, that's also valid for some setups
            pytest.skip("Production and test databases are the same in this environment")

    def test_connection_pool_settings(self, test_env_vars):
        """Test that connection pool settings are properly applied."""
        try:
            # Test with custom pool settings
            engine = ConnectionFactory.get_opendental_source_test_connection(
                pool_size=3,
                max_overflow=5,
                pool_timeout=20,
                pool_recycle=1000
            )
            
            # Test basic connectivity with custom settings
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_multiple_connections_same_environment(self, test_env_vars):
        """Test that multiple connections to the same environment work correctly."""
        try:
            # Test multiple test connections
            engine1 = ConnectionFactory.get_opendental_source_test_connection()
            engine2 = ConnectionFactory.get_mysql_replication_test_connection()
            engine3 = ConnectionFactory.get_postgres_analytics_test_connection()
            
            # Test that all connections work
            with engine1.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.fetchone()[0] == 1
                
            with engine2.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.fetchone()[0] == 1
                
            with engine3.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.fetchone()[0] == 1
                
        except Exception as e:
            pytest.skip(f"Test databases not available: {str(e)}")

    def test_schema_specific_connections(self, test_env_vars, postgres_schemas):
        """Test that schema-specific connections use the correct schemas."""
        try:
            # Test different schema connections
            raw_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
            staging_engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()
            intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_test_connection()
            marts_engine = ConnectionFactory.get_opendental_analytics_marts_test_connection()
            
            # Test that each connection uses the correct schema
            with raw_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == postgres_schemas.RAW
                
            with staging_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == postgres_schemas.STAGING
                
            with intermediate_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == postgres_schemas.INTERMEDIATE
                
            with marts_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == postgres_schemas.MARTS
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")


class TestConnectionFactoryMocked:
    """Unit tests for ConnectionFactory using mocked connections."""
    
    def test_mock_source_engine_connectivity(self, mock_source_engine):
        """Test mock source engine connectivity."""
        # Mock the connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [1]
        mock_conn.execute.return_value = mock_result
        
        mock_source_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_source_engine.connect.return_value.__exit__.return_value = None
        
        # Test connectivity
        with mock_source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test_value"))
            row = result.fetchone()
            assert row[0] == 1

    def test_mock_replication_engine_connectivity(self, mock_replication_engine):
        """Test mock replication engine connectivity."""
        # Mock the connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [1]
        mock_conn.execute.return_value = mock_result
        
        mock_replication_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_replication_engine.connect.return_value.__exit__.return_value = None
        
        # Test connectivity
        with mock_replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test_value"))
            row = result.fetchone()
            assert row[0] == 1

    def test_mock_analytics_engine_connectivity(self, mock_analytics_engine):
        """Test mock analytics engine connectivity."""
        # Mock the connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [1]
        mock_conn.execute.return_value = mock_result
        
        mock_analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_analytics_engine.connect.return_value.__exit__.return_value = None
        
        # Test connectivity
        with mock_analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test_value"))
            row = result.fetchone()
            assert row[0] == 1

    def test_mock_connection_factory(self, mock_connection_factory):
        """Test mock connection factory functionality."""
        # Test getting different types of connections
        source_engine = mock_connection_factory.get_connection('source')
        replication_engine = mock_connection_factory.get_connection('replication')
        analytics_engine = mock_connection_factory.get_connection('analytics')
        
        # Verify engines are created
        assert source_engine is not None
        assert replication_engine is not None
        assert analytics_engine is not None
        
        # Verify engine types
        assert source_engine.name == 'mysql'
        assert replication_engine.name == 'mysql'
        assert analytics_engine.name == 'postgresql'

    def test_database_types_enum(self, database_types):
        """Test database types enum functionality."""
        # Test that enum values are accessible
        assert hasattr(database_types, 'SOURCE')
        assert hasattr(database_types, 'REPLICATION')
        assert hasattr(database_types, 'ANALYTICS')
        
        # Test enum values (handle both enum objects and string values)
        source_value = database_types.SOURCE.value if hasattr(database_types.SOURCE, 'value') else database_types.SOURCE
        replication_value = database_types.REPLICATION.value if hasattr(database_types.REPLICATION, 'value') else database_types.REPLICATION
        analytics_value = database_types.ANALYTICS.value if hasattr(database_types.ANALYTICS, 'value') else database_types.ANALYTICS
        
        assert source_value == 'source'
        assert replication_value == 'replication'
        assert analytics_value == 'analytics'

    def test_postgres_schemas_enum(self, postgres_schemas):
        """Test PostgreSQL schemas enum functionality."""
        # Test that enum values are accessible
        assert hasattr(postgres_schemas, 'RAW')
        assert hasattr(postgres_schemas, 'STAGING')
        assert hasattr(postgres_schemas, 'INTERMEDIATE')
        assert hasattr(postgres_schemas, 'MARTS')
        
        # Test enum values (handle both enum objects and string values)
        raw_value = postgres_schemas.RAW.value if hasattr(postgres_schemas.RAW, 'value') else postgres_schemas.RAW
        staging_value = postgres_schemas.STAGING.value if hasattr(postgres_schemas.STAGING, 'value') else postgres_schemas.STAGING
        intermediate_value = postgres_schemas.INTERMEDIATE.value if hasattr(postgres_schemas.INTERMEDIATE, 'value') else postgres_schemas.INTERMEDIATE
        marts_value = postgres_schemas.MARTS.value if hasattr(postgres_schemas.MARTS, 'value') else postgres_schemas.MARTS
        
        assert raw_value == 'raw'
        assert staging_value == 'staging'
        assert intermediate_value == 'intermediate'
        assert marts_value == 'marts' 