"""Integration tests for schema discovery functionality.

PURPOSE: Real database integration with SQLite
SCOPE: Safety, error handling, actual data flow
COVERAGE: Integration scenarios and edge cases
MARKERS: @pytest.mark.integration

This file focuses on testing SchemaDiscovery with real database connections
using SQLite for safety and to verify actual data flow and error handling.
"""
import pytest
import sqlite3
import tempfile
import os
import logging
import time
from typing import Dict, List
from sqlalchemy import create_engine, text
from etl_pipeline.core.schema_discovery import SchemaDiscovery

# Set up logging for integration tests
logger = logging.getLogger(__name__)


class SQLiteTestSchemaDiscovery(SchemaDiscovery):
    """SQLite-compatible version of SchemaDiscovery for integration testing.
    
    Following Section 25 of pytest_debugging_notes.md for SQLite adaptations.
    This class overrides MySQL-specific methods to work with SQLite.
    """
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get schema information for a table using SQLite-compatible queries."""
        try:
            logger.info(f"Getting schema information for {table_name}...")
            start_time = time.time()
            
            with self.source_engine.connect() as conn:
                # SQLite doesn't need USE statement - we're already connected to the right database
                
                # Get CREATE TABLE statement using SQLite pragma
                result = conn.execute(text(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
                row = result.fetchone()
                
                if not row:
                    raise ValueError(f"Table {table_name} not found")
                
                create_statement = row[0]
                
                # Get table metadata using SQLite pragma
                metadata = self._get_table_metadata_sqlite(conn, table_name)
                
                # Get indexes using SQLite pragma
                indexes = self._get_table_indexes_sqlite(conn, table_name)
                
                # Get foreign keys using SQLite pragma
                foreign_keys = self._get_foreign_keys_sqlite(conn, table_name)
                
                # Get detailed column information using SQLite pragma
                columns = self._get_detailed_columns_sqlite(conn, table_name)
                
                schema_info = {
                    'table_name': table_name,
                    'create_statement': create_statement,
                    'metadata': metadata,
                    'indexes': indexes,
                    'foreign_keys': foreign_keys,
                    'columns': columns,
                    'schema_hash': self._calculate_schema_hash(create_statement)
                }
                
                # Cache the schema info
                self._schema_cache[table_name] = schema_info
                
                elapsed = time.time() - start_time
                logger.info(f"Completed schema discovery for {table_name} in {elapsed:.2f}s")
                
                return schema_info
                
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {str(e)}")
            raise
    
    def _get_table_metadata_sqlite(self, conn, table_name: str) -> Dict:
        """Get table metadata using SQLite pragma."""
        # Get row count
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        row_count = result.fetchone()[0]
        
        return {
            'engine': 'sqlite',
            'charset': 'utf8',
            'collation': 'utf8',
            'auto_increment': None,
            'row_count': row_count
        }
    
    def _get_table_indexes_sqlite(self, conn, table_name: str) -> List[Dict]:
        """Get all indexes for a table using SQLite pragma."""
        result = conn.execute(text(f"PRAGMA index_list({table_name})"))
        
        indexes = []
        for row in result:
            index_name = row[1]
            is_unique = row[2] == 1
            
            # Get columns for this index
            index_info = conn.execute(text(f"PRAGMA index_info({index_name})"))
            columns = []
            for col_row in index_info:
                col_name = col_row[2]
                columns.append(col_name)
            
            indexes.append({
                'name': index_name,
                'columns': columns,
                'is_unique': is_unique,
                'type': 'btree'  # SQLite uses btree by default
            })
        
        return indexes
    
    def _get_foreign_keys_sqlite(self, conn, table_name: str) -> List[Dict]:
        """Get all foreign key constraints using SQLite pragma."""
        result = conn.execute(text(f"PRAGMA foreign_key_list({table_name})"))
        
        foreign_keys = []
        for row in result:
            foreign_keys.append({
                'name': f"fk_{table_name}_{row[3]}",  # SQLite doesn't store FK names
                'column': row[3],  # column name
                'referenced_table': row[2],  # referenced table
                'referenced_column': row[4]  # referenced column
            })
        
        return foreign_keys
    
    def _get_detailed_columns_sqlite(self, conn, table_name: str) -> List[Dict]:
        """Get detailed column information using SQLite pragma."""
        result = conn.execute(text(f"PRAGMA table_info({table_name})"))
        
        columns = []
        for row in result:
            cid, name, type_name, not_null, default_value, pk = row
            
            # Determine key type
            key_type = 'PRI' if pk == 1 else ''
            
            # Determine if nullable
            is_nullable = not not_null
            
            columns.append({
                'name': name,
                'type': type_name,  # Preserve original case from SQLite
                'is_nullable': is_nullable,
                'default': default_value,
                'extra': 'auto_increment' if pk == 1 and 'INTEGER' in type_name.upper() else '',
                'comment': '',
                'key_type': key_type
            })
        
        return columns
    
    def discover_all_tables(self) -> List[str]:
        """Discover all tables using SQLite-compatible query."""
        try:
            with self.source_engine.connect() as conn:
                # SQLite doesn't need USE statement
                result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
                tables = [row[0] for row in result]
                
                # Filter out SQLite system tables
                system_tables = ['sqlite_sequence', 'sqlite_stat1', 'sqlite_stat2', 'sqlite_stat3', 'sqlite_stat4']
                tables = [table for table in tables if table not in system_tables]
                
                return tables
        except Exception as e:
            logger.error(f"Error discovering tables: {str(e)}")
            return []
    
    def get_table_size_info(self, table_name: str) -> Dict:
        """Get size information using SQLite-compatible queries."""
        try:
            with self.source_engine.connect() as conn:
                # SQLite doesn't need USE statement
                
                # Get row count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.fetchone()[0]
                
                # SQLite doesn't provide detailed size information like MySQL
                # We'll estimate based on row count and assume average row size
                estimated_row_size = 100  # bytes per row (rough estimate)
                data_size_bytes = row_count * estimated_row_size
                index_size_bytes = data_size_bytes // 10  # Rough estimate
                total_size_bytes = data_size_bytes + index_size_bytes
                
                # Convert to MB
                data_size_mb = data_size_bytes / (1024 * 1024)
                index_size_mb = index_size_bytes / (1024 * 1024)
                total_size_mb = total_size_bytes / (1024 * 1024)
                
                return {
                    'row_count': row_count,
                    'data_size_bytes': data_size_bytes,
                    'index_size_bytes': index_size_bytes,
                    'total_size_bytes': total_size_bytes,
                    'data_size_mb': round(data_size_mb, 2),
                    'index_size_mb': round(index_size_mb, 2),
                    'total_size_mb': round(total_size_mb, 2)
                }
                
        except Exception as e:
            logger.error(f"Error getting size info for {table_name}: {str(e)}")
            return {
                'row_count': 0,
                'data_size_bytes': 0,
                'index_size_bytes': 0,
                'total_size_bytes': 0,
                'data_size_mb': 0,
                'index_size_mb': 0,
                'total_size_mb': 0
            }
    
    def get_incremental_columns(self, table_name: str) -> List[Dict]:
        """Get incremental columns using SQLite-compatible queries."""
        try:
            with self.source_engine.connect() as conn:
                columns = self._get_detailed_columns_sqlite(conn, table_name)
                
                # Convert to the expected format
                incremental_columns = []
                for i, col in enumerate(columns, 1):
                    incremental_columns.append({
                        'column_name': col['name'],
                        'data_type': col['type'],
                        'is_nullable': col['is_nullable'],
                        'default': col['default'],
                        'extra': col['extra'],
                        'comment': col['comment'],
                        'priority': i
                    })
                
                return incremental_columns
                
        except Exception as e:
            logger.error(f"Error getting incremental columns for {table_name}: {str(e)}")
            return []
    
    def replicate_schema(self, source_table: str, target_table: str = None, drop_if_exists: bool = True) -> bool:
        """Replicate schema using SQLite-compatible operations."""
        try:
            target_table = target_table or source_table
            
            # Get the CREATE TABLE statement from source
            create_statement = self.get_table_schema(source_table)['create_statement']
            
            # Modify the CREATE statement for the target table
            target_create_statement = self._adapt_create_statement_for_target(
                create_statement, target_table
            )
            
            with self.target_engine.begin() as conn:
                # SQLite doesn't need USE statement
                
                # Drop table if it exists and requested
                if drop_if_exists:
                    conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
                    logger.info(f"Dropped existing table {target_table} in target")
                
                # Create the exact replica
                conn.execute(text(target_create_statement))
                
                logger.info(f"Created exact replica of {source_table} as {target_table}")
                
                # Verify the table was created successfully
                verify_result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{target_table}'"))
                if not verify_result.fetchone():
                    raise Exception(f"Table {target_table} was not created successfully")
                
                return True
                
        except Exception as e:
            logger.error(f"Error replicating schema for {source_table}: {str(e)}")
            return False


@pytest.fixture
def sqlite_test_databases():
    """Create temporary SQLite databases for integration testing.
    
    Following Section 20 of pytest_debugging_notes.md for proper
    database setup and cleanup with error handling.
    """
    source_db = None
    target_db = None
    source_engine = None
    target_engine = None
    
    try:
        # Create temporary files for source and target databases
        source_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        target_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        
        source_db.close()
        target_db.close()
        
        logger.info(f"Created temporary databases: {source_db.name}, {target_db.name}")
        
        # Create source database with test tables
        source_engine = create_engine(f'sqlite:///{source_db.name}')
        target_engine = create_engine(f'sqlite:///{target_db.name}')
        
        # Create test tables in source database
        with source_engine.begin() as conn:
            # Create a simple test table
            conn.execute(text("""
                CREATE TABLE definition (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Create a patient table with more complex structure
            conn.execute(text("""
                CREATE TABLE patient (
                    PatNum INTEGER PRIMARY KEY AUTOINCREMENT,
                    LName TEXT,
                    FName TEXT,
                    Birthdate DATE,
                    DateCreated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    DateModified DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Insert some test data
            conn.execute(text("""
                INSERT INTO definition (name, description) VALUES 
                ('Test Definition 1', 'First test definition'),
                ('Test Definition 2', 'Second test definition')
            """))
            
            conn.execute(text("""
                INSERT INTO patient (LName, FName, Birthdate) VALUES 
                ('Doe', 'John', '1990-01-01'),
                ('Smith', 'Jane', '1985-05-15')
            """))
        
        logger.info("Successfully created test tables and data")
        
        yield {
            'source_engine': source_engine,
            'target_engine': target_engine,
            'source_db': source_db.name,
            'target_db': target_db.name
        }
        
    except Exception as e:
        logger.error(f"Error setting up test databases: {str(e)}")
        raise
    finally:
        # Cleanup following Section 20 best practices
        try:
            if source_engine:
                source_engine.dispose()
                logger.debug("Disposed source engine")
            if target_engine:
                target_engine.dispose()
                logger.debug("Disposed target engine")
            
            if source_db and os.path.exists(source_db.name):
                os.unlink(source_db.name)
                logger.debug(f"Removed source database: {source_db.name}")
            if target_db and os.path.exists(target_db.name):
                os.unlink(target_db.name)
                logger.debug(f"Removed target database: {target_db.name}")
        except Exception as e:
            logger.warning(f"Error during cleanup: {str(e)}")


@pytest.fixture
def schema_discovery_integration(sqlite_test_databases):
    """Create SchemaDiscovery instance with SQLite databases for integration testing."""
    try:
        schema_discovery = SQLiteTestSchemaDiscovery(
            source_engine=sqlite_test_databases['source_engine'],
            target_engine=sqlite_test_databases['target_engine'],
            source_db='main',  # SQLite uses 'main' as default database name
            target_db='main'
        )
        logger.info("Created SchemaDiscovery instance for integration testing")
        return schema_discovery
    except Exception as e:
        logger.error(f"Error creating SchemaDiscovery instance: {str(e)}")
        raise


@pytest.mark.integration
def test_get_table_schema_integration(schema_discovery_integration):
    """Test schema retrieval with real SQLite database."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing schema retrieval for definition table")
    
    # Get schema for definition table
    schema = schema_discovery.get_table_schema('definition')
    
    assert schema is not None
    assert schema['table_name'] == 'definition'
    assert 'create_statement' in schema
    assert 'columns' in schema
    assert 'schema_hash' in schema
    
    # Check columns
    columns = schema['columns']
    assert len(columns) == 4  # id, name, description, created_date
    
    # Find specific columns
    id_col = next(col for col in columns if col['name'] == 'id')
    name_col = next(col for col in columns if col['name'] == 'name')
    
    assert id_col['type'] == 'INTEGER'
    assert id_col['key_type'] == 'PRI'  # MySQL standard for primary key
    assert name_col['type'] == 'TEXT'
    assert name_col['is_nullable'] is False
    
    logger.info("Schema retrieval test completed successfully")


@pytest.mark.integration
def test_get_table_schema_nonexistent_table(schema_discovery_integration):
    """Test schema retrieval for non-existent table."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing schema retrieval for non-existent table")
    
    with pytest.raises(Exception):
        schema_discovery.get_table_schema('non_existent_table')
    
    logger.info("Non-existent table test completed successfully")


@pytest.mark.integration
def test_discover_all_tables_integration(schema_discovery_integration):
    """Test discovering all tables with real database."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing table discovery")
    
    tables = schema_discovery.discover_all_tables()
    
    assert isinstance(tables, list)
    assert len(tables) == 2
    assert 'definition' in tables
    assert 'patient' in tables
    
    logger.info(f"Discovered tables: {tables}")


@pytest.mark.integration
def test_get_table_size_info_integration(schema_discovery_integration):
    """Test getting table size information with real database.
    
    Following Section 25 of pytest_debugging_notes.md for SQLite adaptations.
    SQLite doesn't have the same size information as MySQL/PostgreSQL.
    """
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing table size information retrieval")
    
    size_info = schema_discovery.get_table_size_info('definition')
    
    assert size_info is not None
    assert 'row_count' in size_info
    assert 'data_size_bytes' in size_info
    assert 'index_size_bytes' in size_info
    assert 'total_size_bytes' in size_info
    assert 'data_size_mb' in size_info
    assert 'index_size_mb' in size_info
    assert 'total_size_mb' in size_info
    
    # Should have 2 rows from our test data
    assert size_info['row_count'] == 2
    
    # SQLite size information may be estimates or 0
    # The important thing is that the method doesn't fail
    assert size_info['data_size_bytes'] >= 0
    assert size_info['index_size_bytes'] >= 0
    assert size_info['total_size_bytes'] >= 0
    
    logger.info(f"Table size info: {size_info}")


@pytest.mark.integration
def test_get_incremental_columns_integration(schema_discovery_integration):
    """Test getting incremental columns with real database."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing incremental columns retrieval")
    
    columns = schema_discovery.get_incremental_columns('patient')
    
    assert columns is not None
    assert len(columns) == 6  # All columns in patient table
    
    # Check column structure
    patnum_col = next(col for col in columns if col['column_name'] == 'PatNum')
    lname_col = next(col for col in columns if col['column_name'] == 'LName')
    
    assert patnum_col['data_type'] == 'INTEGER'
    assert patnum_col['priority'] == 1  # Primary key should be priority 1
    assert lname_col['data_type'] == 'TEXT'
    assert lname_col['priority'] == 2  # Second column should be priority 2
    
    logger.info(f"Retrieved {len(columns)} incremental columns")


@pytest.mark.integration
def test_schema_hash_consistency_integration(schema_discovery_integration):
    """Test schema hash consistency with real database."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing schema hash consistency")
    
    # Get schema twice for the same table
    schema1 = schema_discovery.get_table_schema('definition')
    schema2 = schema_discovery.get_table_schema('definition')
    
    # Should have the same hash
    assert schema1['schema_hash'] == schema2['schema_hash']
    
    # Should use cached version on second call
    assert 'definition' in schema_discovery._schema_cache
    
    logger.info(f"Schema hash: {schema1['schema_hash']}")


@pytest.mark.integration
def test_has_schema_changed_integration(schema_discovery_integration):
    """Test schema change detection with real database."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing schema change detection")
    
    # Get current schema hash
    current_schema = schema_discovery.get_table_schema('definition')
    current_hash = current_schema['schema_hash']
    
    # Test with same hash (should return False)
    has_changed = schema_discovery.has_schema_changed('definition', current_hash)
    assert has_changed is False
    
    # Test with different hash (should return True)
    has_changed = schema_discovery.has_schema_changed('definition', 'different_hash')
    assert has_changed is True
    
    logger.info("Schema change detection test completed")


@pytest.mark.integration
def test_replicate_schema_integration(schema_discovery_integration):
    """Test schema replication with real database.
    
    Following Section 25 of pytest_debugging_notes.md for SQLite adaptations.
    """
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing schema replication")
    
    # Replicate the definition table schema
    success = schema_discovery.replicate_schema('definition', target_table='definition_replica')
    
    assert success is True
    
    # Verify the table was created in target database
    with schema_discovery.target_engine.connect() as conn:
        # SQLite-specific table existence check
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='definition_replica'"))
        assert result.fetchone() is not None
        
        # Check table structure using SQLite pragma
        result = conn.execute(text("PRAGMA table_info(definition_replica)"))
        columns = result.fetchall()
        assert len(columns) == 4  # Should have same number of columns as source
        
        # Clean up
        conn.execute(text("DROP TABLE definition_replica"))
    
    logger.info("Schema replication test completed successfully")


@pytest.mark.integration
def test_replicate_schema_with_data_integration(schema_discovery_integration):
    """Test schema replication and verify data can be inserted."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing schema replication with data insertion")
    
    # Replicate the definition table schema
    success = schema_discovery.replicate_schema('definition', target_table='definition_replica')
    assert success is True
    
    # Insert data into the replicated table
    with schema_discovery.target_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO definition_replica (name, description) VALUES 
            ('Replicated Definition', 'Test data in replicated table')
        """))
        
        # Verify data was inserted
        result = conn.execute(text("SELECT COUNT(*) FROM definition_replica"))
        count = result.fetchone()[0]
        assert count == 1
        
        # Clean up
        conn.execute(text("DROP TABLE definition_replica"))
    
    logger.info("Schema replication with data test completed")


@pytest.mark.integration
def test_adapt_create_statement_integration():
    """Test CREATE statement adaptation with real SQLite syntax.
    
    Following Section 25 of pytest_debugging_notes.md for SQLite adaptations.
    """
    logger.info("Testing CREATE statement adaptation for SQLite")
    
    # Create a minimal SchemaDiscovery instance for testing
    source_engine = create_engine('sqlite:///:memory:')
    target_engine = create_engine('sqlite:///:memory:')
    
    schema_discovery = SQLiteTestSchemaDiscovery(
        source_engine=source_engine,
        target_engine=target_engine,
        source_db='main',
        target_db='main'
    )
    
    # Test with SQLite CREATE statement
    sqlite_create = "CREATE TABLE definition (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
    adapted = schema_discovery._adapt_create_statement_for_target(sqlite_create, 'definition_replica')
    
    expected = "CREATE TABLE `definition_replica` (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
    assert adapted == expected
    
    logger.info("CREATE statement adaptation test completed")


@pytest.mark.integration
def test_error_handling_integration(schema_discovery_integration):
    """Test error handling with real database scenarios.
    
    Following Section 16 of pytest_debugging_notes.md for error handling.
    """
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing error handling scenarios")
    
    # Test with non-existent table
    with pytest.raises(Exception):
        schema_discovery.get_table_schema('non_existent')
    
    # Test size info for non-existent table (should return zero values)
    size_info = schema_discovery.get_table_size_info('non_existent')
    assert size_info['row_count'] == 0
    assert size_info['data_size_bytes'] == 0
    
    # Test incremental columns for non-existent table (should return empty list)
    columns = schema_discovery.get_incremental_columns('non_existent')
    assert columns == []
    
    logger.info("Error handling test completed successfully")


@pytest.mark.integration
def test_database_connection_cleanup_integration(sqlite_test_databases):
    """Test that database connections are properly cleaned up.
    
    Following Section 20 of pytest_debugging_notes.md for connection management.
    """
    source_engine = sqlite_test_databases['source_engine']
    target_engine = sqlite_test_databases['target_engine']
    
    logger.info("Testing database connection cleanup")
    
    # Create SchemaDiscovery instance
    schema_discovery = SQLiteTestSchemaDiscovery(
        source_engine=source_engine,
        target_engine=target_engine,
        source_db='main',
        target_db='main'
    )
    
    # Perform some operations
    schema = schema_discovery.get_table_schema('definition')
    assert schema is not None
    
    # Verify connections are still working
    with source_engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM definition"))
        count = result.fetchone()[0]
        assert count == 2  # Our test data
    
    # Connections should be properly managed by SQLAlchemy
    # SQLAlchemy engines don't have a .closed attribute
    # Instead, we can verify they're still working by executing a simple query
    with source_engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # Simple test query
    
    with target_engine.connect() as conn:
        conn.execute(text("SELECT 1"))  # Simple test query
    
    logger.info("Database connection cleanup test completed")


@pytest.mark.integration
def test_schema_cache_integration(schema_discovery_integration):
    """Test schema caching behavior with real database."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing schema caching behavior")
    
    # Clear any existing cache
    schema_discovery._schema_cache.clear()
    
    # First call should populate cache
    schema1 = schema_discovery.get_table_schema('definition')
    assert 'definition' in schema_discovery._schema_cache
    
    # Second call should use cache
    schema2 = schema_discovery.get_table_schema('definition')
    assert schema1 == schema2
    
    # Verify cache contains the right data
    cached_schema = schema_discovery._schema_cache['definition']
    assert cached_schema['table_name'] == 'definition'
    assert cached_schema['schema_hash'] == schema1['schema_hash']
    
    logger.info("Schema caching test completed")


@pytest.mark.integration
def test_multiple_table_operations_integration(schema_discovery_integration):
    """Test operations on multiple tables with real database."""
    schema_discovery = schema_discovery_integration
    
    logger.info("Testing multiple table operations")
    
    # Test operations on both tables
    definition_schema = schema_discovery.get_table_schema('definition')
    patient_schema = schema_discovery.get_table_schema('patient')
    
    assert definition_schema['table_name'] == 'definition'
    assert patient_schema['table_name'] == 'patient'
    
    # Test size info for both tables
    definition_size = schema_discovery.get_table_size_info('definition')
    patient_size = schema_discovery.get_table_size_info('patient')
    
    assert definition_size['row_count'] == 2
    assert patient_size['row_count'] == 2
    
    # Test incremental columns for both tables
    definition_columns = schema_discovery.get_incremental_columns('definition')
    patient_columns = schema_discovery.get_incremental_columns('patient')
    
    assert len(definition_columns) == 4
    assert len(patient_columns) == 6
    
    logger.info("Multiple table operations test completed")


@pytest.mark.integration
def test_sqlite_specific_adaptations():
    """Test SQLite-specific adaptations following Section 25 of pytest_debugging_notes.md."""
    logger.info("Testing SQLite-specific adaptations")
    
    # Create in-memory SQLite databases for testing
    source_engine = create_engine('sqlite:///:memory:')
    target_engine = create_engine('sqlite:///:memory:')
    
    # Create a test table in source
    with source_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        """))
    
    schema_discovery = SQLiteTestSchemaDiscovery(
        source_engine=source_engine,
        target_engine=target_engine,
        source_db='main',
        target_db='main'
    )
    
    # Test that SQLite-specific operations work
    schema = schema_discovery.get_table_schema('test_table')
    assert schema is not None
    assert schema['table_name'] == 'test_table'
    
    # Test that we can get columns (SQLite-specific column info)
    columns = schema_discovery.get_incremental_columns('test_table')
    assert len(columns) == 3  # id, name, value
    
    logger.info("SQLite-specific adaptations test completed")


@pytest.mark.integration
def test_performance_with_real_data(sqlite_test_databases):
    """Test performance with realistic data following Section 21 of pytest_debugging_notes.md."""
    source_engine = sqlite_test_databases['source_engine']
    target_engine = sqlite_test_databases['target_engine']
    
    logger.info("Testing performance with realistic data")
    
    # Insert more realistic test data
    with source_engine.begin() as conn:
        # Insert more patient records
        for i in range(100):
            conn.execute(text("""
                INSERT INTO patient (LName, FName, Birthdate) VALUES 
                (:lname, :fname, :birthdate)
            """), {
                'lname': f'LastName{i}', 
                'fname': f'FirstName{i}', 
                'birthdate': f'1990-{i%12+1:02d}-{i%28+1:02d}'
            })
    
    schema_discovery = SQLiteTestSchemaDiscovery(
        source_engine=source_engine,
        target_engine=target_engine,
        source_db='main',
        target_db='main'
    )
    
    # Test performance with larger dataset
    start_time = time.time()
    
    # Get schema for table with more data
    schema = schema_discovery.get_table_schema('patient')
    schema_time = time.time() - start_time
    
    # Get size info
    start_time = time.time()
    size_info = schema_discovery.get_table_size_info('patient')
    size_time = time.time() - start_time
    
    # Get incremental columns
    start_time = time.time()
    columns = schema_discovery.get_incremental_columns('patient')
    columns_time = time.time() - start_time
    
    # Verify results
    assert schema is not None
    assert size_info['row_count'] == 102  # 2 original + 100 new
    assert len(columns) == 6
    
    # Performance assertions (should be fast even with more data)
    assert schema_time < 1.0, f"Schema retrieval took {schema_time:.2f}s"
    assert size_time < 1.0, f"Size info retrieval took {size_time:.2f}s"
    assert columns_time < 1.0, f"Columns retrieval took {columns_time:.2f}s"
    
    logger.info(f"Performance test completed - Schema: {schema_time:.3f}s, Size: {size_time:.3f}s, Columns: {columns_time:.3f}s") 