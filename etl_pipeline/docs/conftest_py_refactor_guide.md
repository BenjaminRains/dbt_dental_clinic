"""
Step-by-step guide to integrate clean configuration into your existing conftest.py
WITHOUT losing any existing functionality.
"""

# =============================================================================
# STEP 1: ADD IMPORTS TO YOUR EXISTING CONFTEST.PY
# =============================================================================

# Add these imports to the top of your existing conftest.py file
# (Keep all your existing imports and add these)

from etl_pipeline.config import (
    reset_settings, 
    create_settings, 
    create_test_settings, 
    DatabaseType, 
    PostgresSchema
)

# =============================================================================
# STEP 2: ADD THE GLOBAL SETTINGS RESET FIXTURE
# =============================================================================

# Add this fixture to your existing conftest.py
# This ensures test isolation with the new configuration system

@pytest.fixture(autouse=True)
def reset_global_settings():
    """Reset global settings before and after each test."""
    reset_settings()
    yield
    reset_settings()

# =============================================================================
# STEP 3: ADD NEW CLEAN CONFIGURATION FIXTURES
# =============================================================================

# Add these fixtures alongside your existing ones
# These provide the new clean configuration interface

@pytest.fixture
def clean_test_env_vars():
    """Clean test environment variables for new configuration system."""
    return {
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
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_pass',
        'ETL_ENVIRONMENT': 'test'
    }


@pytest.fixture
def clean_test_settings(clean_test_env_vars):
    """Create clean test settings with new configuration system."""
    return create_test_settings(
        pipeline_config={
            'general': {
                'pipeline_name': 'test_pipeline',
                'environment': 'test',
                'batch_size': 1000,
                'parallel_jobs': 2
            }
        },
        tables_config={
            'tables': {
                'patient': {
                    'primary_key': 'PatNum',
                    'incremental_column': 'DateTStamp',
                    'extraction_strategy': 'incremental',
                    'table_importance': 'critical',
                    'batch_size': 100
                }
            }
        },
        env_vars=clean_test_env_vars
    )


@pytest.fixture
def database_types():
    """Provide DatabaseType enum for tests."""
    return DatabaseType


@pytest.fixture
def postgres_schemas():
    """Provide PostgresSchema enum for tests."""
    return PostgresSchema

# =============================================================================
# STEP 4: ENHANCE YOUR EXISTING FIXTURES
# =============================================================================

# Update your existing mock_settings fixture to work with both systems
# Find your existing mock_settings fixture and update it like this:

@pytest.fixture
def mock_settings():
    """Enhanced mock settings that works with both old and new systems."""
    with patch('etl_pipeline.config.settings.Settings') as mock:
        settings_instance = MagicMock()
        
        # Keep all your existing mock configurations
        settings_instance.get_database_config.return_value = {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        # ADD this new method to handle enum-based calls
        def mock_get_database_config(db_type, schema=None):
            if hasattr(db_type, 'value'):  # New enum system
                db_type_str = db_type.value
            else:  # Old string system
                db_type_str = db_type
            
            base_config = {
                'host': 'localhost',
                'port': 3306 if db_type_str in ['source', 'replication'] else 5432,
                'database': f'test_{db_type_str}',
                'user': 'test_user',
                'password': 'test_pass'
            }
            
            if schema and hasattr(schema, 'value'):
                base_config['schema'] = schema.value
            
            return base_config
        
        settings_instance.get_database_config.side_effect = mock_get_database_config
        
        # Keep all your other existing mock configurations
        settings_instance.get_table_config.return_value = {
            'incremental': True,
            'primary_key': 'id',
            'batch_size': 1000,
            'estimated_size_mb': 50,
            'estimated_rows': 10000
        }
        
        # Keep all your existing mock methods
        settings_instance.should_use_incremental.return_value = True
        settings_instance.get_tables_by_importance.return_value = [
            'patient', 'appointment', 'procedurelog'
        ]
        
        mock.return_value = settings_instance
        yield settings_instance

# =============================================================================
# STEP 5: ADD BRIDGE FIXTURES FOR GRADUAL MIGRATION
# =============================================================================

# Add these fixtures to help with gradual migration from old to new system

@pytest.fixture
def connection_factory_bridge():
    """Bridge fixture to create connections using either old or new system."""
    def _create_connection(db_type, use_clean_system=True, settings=None, schema=None):
        from etl_pipeline.core.connections import ConnectionFactory
        
        if use_clean_system and settings:
            # Use new enum-based system
            if db_type == 'source':
                return ConnectionFactory.get_source_connection(settings)
            elif db_type == 'replication':
                return ConnectionFactory.get_replication_connection(settings)
            elif db_type == 'analytics':
                schema_enum = PostgresSchema.RAW if schema is None else schema
                return ConnectionFactory.get_analytics_connection(settings, schema_enum)
        else:
            # Use old system for backward compatibility
            if db_type == 'source':
                return ConnectionFactory.get_opendental_source_connection()
            elif db_type == 'replication':
                return ConnectionFactory.get_mysql_replication_connection()
            elif db_type == 'analytics':
                return ConnectionFactory.get_postgres_analytics_connection()
    
    return _create_connection

# =============================================================================
# STEP 6: KEEP ALL YOUR EXISTING FIXTURES
# =============================================================================

# IMPORTANT: Keep all your existing fixtures:
# - mock_database_engines
# - mock_source_engine
# - mock_replication_engine
# - mock_analytics_engine
# - mock_connection_factory
# - All your transformer fixtures
# - All your PostgresLoader fixtures
# - All your MySQL replicator fixtures
# - All your orchestration fixtures
# - All your metrics fixtures
# - All your configuration test fixtures

# Your existing fixtures will continue to work unchanged!

# =============================================================================
# STEP 7: MIGRATION EXAMPLE
# =============================================================================

# Example of how to update an existing test to use the new system:

# OLD TEST (keep this working):
def test_old_system_example(mock_settings):
    """Example of test using old system - this continues to work."""
    config = mock_settings.get_database_config('source')
    assert config['host'] == 'localhost'

# NEW TEST (add this capability):
def test_new_system_example(clean_test_settings, database_types):
    """Example of test using new system - this is the new capability."""
    config = clean_test_settings.get_database_config(database_types.SOURCE)
    assert config['host'] == 'localhost'

# BRIDGE TEST (for gradual migration):
def test_bridge_example(connection_factory_bridge, clean_test_settings):
    """Example of test using bridge for gradual migration."""
    # Can use either old or new system
    old_conn = connection_factory_bridge('source', use_clean_system=False)
    new_conn = connection_factory_bridge('source', use_clean_system=True, settings=clean_test_settings)
    
    # Both should work
    assert old_conn is not None
    assert new_conn is not None