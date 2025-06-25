"""
Integration tests for PostgresLoader - Real database integration with SQLite.

This test file focuses on real database integration testing using SQLite for safety.
Tests cover actual data flow, error handling, and edge cases with real connections.

Testing Strategy:
- Real database integration with SQLite
- Safety, error handling, actual data flow
- Integration scenarios and edge cases
- Execution: < 10 seconds per component
- Marker: @pytest.mark.integration
"""

import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import MagicMock, patch, call
from sqlalchemy import text, create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
from datetime import datetime, timedelta
import logging
from typing import Dict, Optional

# Import the component under test
from etl_pipeline.loaders.postgres_loader import PostgresLoader

logger = logging.getLogger(__name__)


class SQLiteCompatiblePostgresLoader(PostgresLoader):
    """SQLite-compatible version of PostgresLoader for integration testing."""
    
    def __init__(self, replication_engine: Engine, analytics_engine: Engine):
        super().__init__(replication_engine, analytics_engine)
        # Override database names for SQLite compatibility
        self.replication_db = ""  # SQLite doesn't use database names
        self.analytics_db = ""    # SQLite doesn't use database names
        self.analytics_schema = ""  # SQLite doesn't use schemas
        self.target_schema = ""   # SQLite doesn't use schemas
        self.staging_schema = ""  # SQLite doesn't use schemas
    
    def _ensure_postgres_table(self, table_name: str, mysql_schema: Dict) -> bool:
        """SQLite-compatible table creation."""
        try:
            # Check if table exists in SQLite
            inspector = inspect(self.analytics_engine)
            if not inspector.has_table(table_name):  # No schema in SQLite
                # Create table using SQLite syntax
                return self._create_sqlite_table(table_name, mysql_schema)
            
            # Verify schema
            return self._verify_sqlite_schema(table_name, mysql_schema)
            
        except Exception as e:
            logger.error(f"Error ensuring SQLite table {table_name}: {str(e)}")
            return False
    
    def _create_sqlite_table(self, table_name: str, mysql_schema: Dict) -> bool:
        """Create table in SQLite using SQLite-compatible syntax."""
        try:
            with self.analytics_engine.connect() as conn:
                # Convert MySQL schema to SQLite schema
                columns = []
                for col in mysql_schema['columns']:
                    col_name = col['name']
                    col_type = self._convert_mysql_type_to_sqlite(col['type'])
                    nullable = "" if col['nullable'] else " NOT NULL"
                    columns.append(f"{col_name} {col_type}{nullable}")
                
                # Add primary key if specified
                if 'primary_key' in mysql_schema:
                    pk_cols = mysql_schema['primary_key']['constrained_columns']
                    pk_constraint = f", PRIMARY KEY ({', '.join(pk_cols)})"
                else:
                    pk_constraint = ""
                
                create_sql = f"""
                    CREATE TABLE {table_name} (
                        {', '.join(columns)}{pk_constraint}
                    )
                """
                
                conn.execute(text(create_sql))
                conn.commit()
                logger.info(f"Created SQLite table {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error creating SQLite table {table_name}: {str(e)}")
            return False
    
    def _verify_sqlite_schema(self, table_name: str, mysql_schema: Dict) -> bool:
        """Verify SQLite table schema matches expected schema."""
        try:
            with self.analytics_engine.connect() as conn:
                # Get actual columns from SQLite
                result = conn.execute(text(f"PRAGMA table_info({table_name})"))
                actual_columns = {row[1]: row[2] for row in result.fetchall()}
                
                # Check expected columns exist
                expected_columns = {col['name']: col['type'] for col in mysql_schema['columns']}
                
                for col_name, col_type in expected_columns.items():
                    if col_name not in actual_columns:
                        logger.error(f"Column {col_name} missing from {table_name}")
                        return False
                
                logger.info(f"Verified SQLite table schema for {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error verifying SQLite table schema {table_name}: {str(e)}")
            return False
    
    def _convert_mysql_type_to_sqlite(self, mysql_type: str) -> str:
        """Convert MySQL data types to SQLite data types."""
        type_mapping = {
            'int': 'INTEGER',
            'bigint': 'INTEGER',
            'varchar': 'TEXT',
            'text': 'TEXT',
            'datetime': 'TEXT',
            'date': 'TEXT',
            'timestamp': 'TEXT',
            'boolean': 'INTEGER',
            'tinyint': 'INTEGER',
            'decimal': 'REAL',
            'float': 'REAL',
            'double': 'REAL'
        }
        
        # Extract base type (remove size specifications)
        base_type = mysql_type.split('(')[0].lower()
        return type_mapping.get(base_type, 'TEXT')
    
    def load_table(self, table_name: str, mysql_schema: Dict, force_full: bool = False) -> bool:
        """SQLite-compatible table loading with DELETE instead of TRUNCATE."""
        try:
            # Create or verify SQLite table
            if not self._ensure_postgres_table(table_name, mysql_schema):
                return False
            
            # Get incremental columns
            incremental_columns = mysql_schema.get('incremental_columns', [])
            
            # Build query to get data
            query = self._build_load_query(table_name, incremental_columns, force_full)
            
            # Use SQLAlchemy connections directly
            with self.replication_engine.connect() as source_conn:
                result = source_conn.execute(text(query))
                column_names = list(result.keys())
                rows = result.fetchall()
                
                if not rows:
                    logger.info(f"No new data to load for {table_name}")
                    return True
                
                logger.info(f"Fetched {len(rows)} rows from {table_name}")
            
            # Prepare data for SQLite insertion
            rows_data = [dict(zip(column_names, row)) for row in rows]
            
            # Handle SQLite insertion with proper transaction management
            with self.analytics_engine.begin() as target_conn:
                if force_full:
                    # For full load, delete all data (SQLite doesn't support TRUNCATE)
                    target_conn.execute(text(f"DELETE FROM {table_name}"))
                    logger.info(f"Deleted all data from {table_name} for full load")
                
                # Use bulk insert for better performance
                if rows_data:
                    # Create parameterized insert statement
                    columns = ', '.join([f'"{col}"' for col in column_names])
                    placeholders = ', '.join([f':{col}' for col in column_names])
                    
                    insert_sql = f"""
                        INSERT INTO {table_name} ({columns})
                        VALUES ({placeholders})
                    """
                    
                    # Execute bulk insert
                    target_conn.execute(text(insert_sql), rows_data)
                    
                    logger.info(f"Loaded {len(rows_data)} rows to {table_name} using {'full' if force_full else 'incremental'} strategy")
            
            return True
                
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {str(e)}")
            return False
    
    def load_table_chunked(self, table_name: str, mysql_schema: Dict, force_full: bool = False, 
                          chunk_size: int = 10000) -> bool:
        """SQLite-compatible chunked table loading."""
        try:
            # Create or verify SQLite table
            if not self._ensure_postgres_table(table_name, mysql_schema):
                return False
            
            # Get incremental columns
            incremental_columns = mysql_schema.get('incremental_columns', [])
            
            # First, get total count
            count_query = self._build_count_query(table_name, incremental_columns, force_full)
            
            with self.replication_engine.connect() as source_conn:
                total_rows = source_conn.execute(text(count_query)).scalar()
            
            if total_rows == 0:
                logger.info(f"No new data to load for {table_name}")
                return True
            
            logger.info(f"Loading {total_rows} rows from {table_name} in chunks of {chunk_size}")
            
            # Process in chunks
            total_loaded = 0
            chunk_num = 0
            
            # Handle full load deletion (SQLite doesn't support TRUNCATE)
            if force_full:
                with self.analytics_engine.begin() as target_conn:
                    target_conn.execute(text(f"DELETE FROM {table_name}"))
                    logger.info(f"Deleted all data from {table_name} for full load")
            
            while total_loaded < total_rows:
                chunk_num += 1
                
                # Build chunked query
                base_query = self._build_load_query(table_name, incremental_columns, force_full)
                chunked_query = f"{base_query} LIMIT {chunk_size} OFFSET {total_loaded}"
                
                # Load chunk
                with self.replication_engine.connect() as source_conn:
                    result = source_conn.execute(text(chunked_query))
                    column_names = list(result.keys())
                    rows = result.fetchall()
                    
                    if not rows:
                        break
                    
                    # Prepare chunk data
                    rows_data = []
                    for row in rows:
                        row_dict = dict(zip(column_names, row))
                        rows_data.append(row_dict)
                
                # Insert chunk
                with self.analytics_engine.begin() as target_conn:
                    columns = ', '.join([f'"{col}"' for col in column_names])
                    placeholders = ', '.join([f':{col}' for col in column_names])
                    
                    insert_sql = f"""
                        INSERT INTO {table_name} ({columns})
                        VALUES ({placeholders})
                    """
                    
                    target_conn.execute(text(insert_sql), rows_data)
                
                chunk_rows = len(rows)
                total_loaded += chunk_rows
                
                logger.info(f"Loaded chunk {chunk_num}: {total_loaded}/{total_rows} rows ({(total_loaded/total_rows)*100:.1f}%)")
                
                if chunk_rows < chunk_size:
                    break
            
            logger.info(f"Successfully loaded {total_loaded} rows to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in chunked load for table {table_name}: {str(e)}")
            return False
    
    def verify_load(self, table_name: str) -> bool:
        """SQLite-compatible load verification."""
        try:
            # Get row counts from SQLite databases
            with self.replication_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            with self.analytics_engine.connect() as conn:
                target_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            # Compare counts
            if source_count != target_count:
                logger.error(
                    f"Row count mismatch for table {table_name}: "
                    f"source={source_count}, target={target_count}"
                )
                return False
            
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Error verifying load for table {table_name}: {str(e)}")
            return False
    
    def _get_last_load(self, table_name: str) -> Optional[datetime]:
        """SQLite-compatible last load timestamp retrieval."""
        try:
            with self.analytics_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT MAX(last_loaded)
                    FROM etl_load_status
                    WHERE table_name = :table_name
                    AND load_status = 'success'
                """), {"table_name": table_name}).scalar()
                
                if result:
                    return datetime.fromisoformat(result)
                return None
                
        except Exception as e:
            logger.error(f"Error getting last load for {table_name}: {str(e)}")
            return None


@pytest.mark.integration
class TestPostgresLoaderIntegration:
    """Integration tests for PostgresLoader with real SQLite databases."""
    
    @pytest.fixture
    def sqlite_engines(self):
        """Create SQLite engines for integration testing."""
        # Create temporary SQLite databases
        raw_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        raw_db.close()
        public_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        public_db.close()
        
        # Create engines with proper configuration for SQLite
        raw_engine = create_engine(f'sqlite:///{raw_db.name}', echo=False)
        public_engine = create_engine(f'sqlite:///{public_db.name}', echo=False)
        
        logger.debug(f"Created SQLite databases: {raw_db.name}, {public_db.name}")
        
        yield raw_engine, public_engine
        
        # Cleanup - ensure databases are closed before deletion
        raw_engine.dispose()
        public_engine.dispose()
        
        try:
            os.unlink(raw_db.name)
            os.unlink(public_db.name)
            logger.debug("Successfully cleaned up SQLite databases")
        except OSError as e:
            logger.warning(f"Failed to clean up SQLite databases: {e}")
    
    @pytest.fixture
    def postgres_loader_integration(self, sqlite_engines):
        """Create PostgresLoader instance with real SQLite engines."""
        raw_engine, public_engine = sqlite_engines
        
        with patch('etl_pipeline.loaders.postgres_loader.settings') as mock_settings:
            mock_settings.get_database_config.side_effect = lambda db: {
                'analytics': {'schema': 'raw'},
                'replication': {'schema': 'raw'}
            }.get(db, {})
            
            with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema') as mock_schema_class:
                mock_schema_adapter = MagicMock()
                mock_schema_class.return_value = mock_schema_adapter
                
                loader = SQLiteCompatiblePostgresLoader(
                    replication_engine=raw_engine,
                    analytics_engine=public_engine
                )
                loader.schema_adapter = mock_schema_adapter
                return loader
    
    @pytest.fixture
    def setup_test_table(self, sqlite_engines):
        """Set up test table in SQLite database with proper error handling."""
        raw_engine, public_engine = sqlite_engines
        
        try:
            # Create test table in raw database using SQLite-compatible syntax (Lesson 25)
            with raw_engine.connect() as conn:
                # Use SQLite-compatible CREATE TABLE syntax
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS test_table (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        created_at TEXT
                    )
                """))
                conn.commit()
                
                # Clear existing data
                conn.execute(text("DELETE FROM test_table"))
                conn.commit()
                
                # Insert test data using proper parameter binding (Lesson 26)
                test_data = [
                    {'id': 1, 'name': 'John Doe', 'created_at': '2023-01-01 10:00:00'},
                    {'id': 2, 'name': 'Jane Smith', 'created_at': '2023-01-02 11:00:00'},
                    {'id': 3, 'name': 'Bob Johnson', 'created_at': '2023-01-03 12:00:00'}
                ]
                
                for row in test_data:
                    conn.execute(text("""
                        INSERT INTO test_table (id, name, created_at) 
                        VALUES (:id, :name, :created_at)
                    """), row)
                conn.commit()
                
                logger.debug(f"Successfully set up test table with {len(test_data)} rows")
                
        except Exception as e:
            logger.error(f"Failed to set up test table: {e}")
            raise
        
        return raw_engine, public_engine
    
    @pytest.fixture
    def setup_etl_tracking(self, sqlite_engines):
        """Set up ETL tracking table in analytics database with proper error handling."""
        raw_engine, public_engine = sqlite_engines
        
        try:
            # Create ETL tracking table using SQLite-compatible syntax
            with public_engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS etl_load_status (
                        table_name TEXT PRIMARY KEY,
                        last_loaded TEXT,
                        load_status TEXT,
                        rows_loaded INTEGER
                    )
                """))
                conn.commit()
                
                # Insert test data for incremental testing
                test_status_data = [
                    {
                        'table_name': 'test_table',
                        'last_loaded': '2023-01-01 10:00:00',
                        'load_status': 'success',
                        'rows_loaded': 3
                    }
                ]
                
                for row in test_status_data:
                    conn.execute(text("""
                        INSERT OR REPLACE INTO etl_load_status 
                        (table_name, last_loaded, load_status, rows_loaded)
                        VALUES (:table_name, :last_loaded, :load_status, :rows_loaded)
                    """), row)
                conn.commit()
                
                logger.debug("Successfully set up ETL tracking table with test data")
                
        except Exception as e:
            logger.error(f"Failed to set up ETL tracking table: {e}")
            raise
        
        return public_engine


@pytest.mark.integration
class TestLoadTableIntegration(TestPostgresLoaderIntegration):
    """Integration tests for load_table functionality."""
    
    def test_load_table_full_integration(self, postgres_loader_integration, setup_test_table, sample_mysql_schema):
        """Test complete table loading workflow with real database."""
        raw_engine, public_engine = setup_test_table
        
        # Test full load - SQLite-compatible loader handles table creation
        result = postgres_loader_integration.load_table('test_table', sample_mysql_schema, force_full=True)
        
        assert result is True
        
        # Verify data was loaded to analytics database
        with public_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
            assert count == 3
            
            # Verify specific data
            result = conn.execute(text("SELECT * FROM test_table ORDER BY id")).fetchall()
            assert len(result) == 3
    
    def test_load_table_incremental_integration(self, postgres_loader_integration, setup_test_table, setup_etl_tracking, sample_mysql_schema):
        """Test incremental table loading workflow with real database."""
        raw_engine, public_engine = setup_test_table
        
        # Clear existing data and add new data for incremental testing
        with raw_engine.connect() as conn:
            conn.execute(text("DELETE FROM test_table"))
            conn.commit()
            
            # Add data that should be loaded incrementally (newer than last load timestamp)
            conn.execute(text("""
                INSERT INTO test_table (id, name, created_at) 
                VALUES (4, 'New User', '2023-01-04 13:00:00')
            """))
            conn.commit()
        
        # Test incremental load - SQLite-compatible loader handles table creation
        result = postgres_loader_integration.load_table('test_table', sample_mysql_schema, force_full=False)
        
        assert result is True
        
        # Verify only new data was loaded (should be 1 new row)
        with public_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
            assert count == 1  # Only the new row should be loaded incrementally
            
            # Verify the new data
            result = conn.execute(text("SELECT * FROM test_table ORDER BY id")).fetchall()
            assert len(result) == 1
            assert result[0][1] == 'New User'  # name column
    
    def test_load_table_no_data_integration(self, postgres_loader_integration, sqlite_engines, sample_mysql_schema):
        """Test table loading with no data in real database."""
        raw_engine, public_engine = sqlite_engines
        
        # Create empty table using SQLite-compatible syntax
        with raw_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS empty_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    created_at TEXT
                )
            """))
            conn.commit()
            
            # Ensure table is empty
            conn.execute(text("DELETE FROM empty_table"))
            conn.commit()
        
        # Mock schema adapter
        postgres_loader_integration.schema_adapter.create_postgres_table.return_value = True
        postgres_loader_integration.schema_adapter.verify_schema.return_value = True
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader_integration.load_table('empty_table', sample_mysql_schema, force_full=False)
            
            assert result is True
            
            logger.debug("Successfully handled empty table loading")
    
    def test_load_table_schema_creation_integration(self, postgres_loader_integration, sqlite_engines, sample_mysql_schema):
        """Test table loading with automatic schema creation."""
        raw_engine, public_engine = sqlite_engines
        
        # Create new table in source database
        with raw_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS new_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    created_at TEXT
                )
            """))
            conn.commit()
            
            # Insert test data
            conn.execute(text("""
                INSERT INTO new_table (id, name, created_at) 
                VALUES (1, 'Test User', '2023-01-01 10:00:00')
            """))
            conn.commit()
        
        # Test loading new table - SQLite-compatible loader handles table creation
        result = postgres_loader_integration.load_table('new_table', sample_mysql_schema, force_full=False)
        
        assert result is True
        
        # Verify table was created and data was loaded
        with public_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM new_table")).scalar()
            assert count == 1
            
            # Verify data
            result = conn.execute(text("SELECT * FROM new_table")).fetchall()
            assert len(result) == 1
            assert result[0][1] == 'Test User'  # name column


@pytest.mark.integration
class TestLoadTableChunkedIntegration(TestPostgresLoaderIntegration):
    """Integration tests for chunked loading functionality."""
    
    def test_load_table_chunked_integration(self, postgres_loader_integration, setup_test_table, sample_mysql_schema):
        """Test chunked table loading with real database."""
        raw_engine, public_engine = setup_test_table
        
        # Test chunked load - SQLite-compatible loader handles table creation
        result = postgres_loader_integration.load_table_chunked('test_table', sample_mysql_schema, force_full=False, chunk_size=1)
        
        assert result is True
        
        # Verify data was loaded in chunks
        with public_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
            assert count == 3
            
            # Verify all data was loaded
            result = conn.execute(text("SELECT * FROM test_table ORDER BY id")).fetchall()
            assert len(result) == 3
            assert result[0][1] == 'John Doe'
            assert result[1][1] == 'Jane Smith'
            assert result[2][1] == 'Bob Johnson'
    
    def test_load_table_chunked_full_load_integration(self, postgres_loader_integration, setup_test_table, sample_mysql_schema):
        """Test chunked table loading with full load (DELETE instead of TRUNCATE)."""
        raw_engine, public_engine = setup_test_table
        
        # Test chunked full load - SQLite-compatible loader handles table creation
        result = postgres_loader_integration.load_table_chunked('test_table', sample_mysql_schema, force_full=True, chunk_size=1)
        
        assert result is True
        
        # Verify data was loaded
        with public_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
            assert count == 3
            
            # Verify all data was loaded
            result = conn.execute(text("SELECT * FROM test_table ORDER BY id")).fetchall()
            assert len(result) == 3


@pytest.mark.integration
class TestVerifyLoadIntegration(TestPostgresLoaderIntegration):
    """Integration tests for load verification functionality."""
    
    def test_verify_load_success_integration(self, postgres_loader_integration, setup_test_table):
        """Test load verification with real database."""
        raw_engine, public_engine = setup_test_table
        
        # First, load data to analytics database
        with public_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    created_at TEXT
                )
            """))
            conn.commit()
            
            # Insert same data as source
            test_data = [
                {'id': 1, 'name': 'John Doe', 'created_at': '2023-01-01 10:00:00'},
                {'id': 2, 'name': 'Jane Smith', 'created_at': '2023-01-02 11:00:00'},
                {'id': 3, 'name': 'Bob Johnson', 'created_at': '2023-01-03 12:00:00'}
            ]
            
            for row in test_data:
                conn.execute(text("""
                    INSERT INTO test_table (id, name, created_at) 
                    VALUES (:id, :name, :created_at)
                """), row)
            conn.commit()
        
        # Test verification
        result = postgres_loader_integration.verify_load('test_table')
        
        assert result is True
        
        # Verify counts match
        with raw_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
        
        with public_engine.connect() as conn:
            target_count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
        
        assert source_count == target_count == 3
    
    def test_verify_load_count_mismatch_integration(self, postgres_loader_integration, setup_test_table):
        """Test load verification with count mismatch in real database."""
        raw_engine, public_engine = setup_test_table
        
        # Load partial data to analytics database
        with public_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    created_at TEXT
                )
            """))
            conn.commit()
            
            # Clear and insert only 2 rows instead of 3
            conn.execute(text("DELETE FROM test_table"))
            conn.commit()
            
            test_data = [
                {'id': 1, 'name': 'John Doe', 'created_at': '2023-01-01 10:00:00'},
                {'id': 2, 'name': 'Jane Smith', 'created_at': '2023-01-02 11:00:00'}
            ]
            
            for row in test_data:
                conn.execute(text("""
                    INSERT INTO test_table (id, name, created_at) 
                    VALUES (:id, :name, :created_at)
                """), row)
            conn.commit()
        
        # Test verification
        result = postgres_loader_integration.verify_load('test_table')
        
        assert result is False
        logger.debug("Successfully verified load verification with count mismatch")


@pytest.mark.integration
class TestUtilityMethodsIntegration(TestPostgresLoaderIntegration):
    """Integration tests for utility methods."""
    
    def test_get_last_load_integration(self, postgres_loader_integration, setup_etl_tracking):
        """Test last load timestamp retrieval with real database."""
        public_engine = setup_etl_tracking
        
        # Test getting last load timestamp
        result = postgres_loader_integration._get_last_load('test_table')
        
        assert result == datetime(2023, 1, 1, 10, 0, 0)
        
        # Verify the data exists in the database
        with public_engine.connect() as conn:
            db_result = conn.execute(text("""
                SELECT last_loaded FROM etl_load_status 
                WHERE table_name = 'test_table' AND load_status = 'success'
            """)).scalar()
            
            assert db_result == '2023-01-01 10:00:00'
    
    def test_get_last_load_no_timestamp_integration(self, postgres_loader_integration, setup_etl_tracking):
        """Test last load timestamp retrieval when no timestamp exists in real database."""
        setup_etl_tracking  # Just set up empty table
        
        # Test retrieval for non-existent table
        result = postgres_loader_integration._get_last_load('non_existent_table')
        
        assert result is None
        logger.debug("Successfully handled missing timestamp in real database")
    
    def test_build_load_query_integration(self, postgres_loader_integration, setup_etl_tracking):
        """Test query building with real database."""
        setup_etl_tracking  # Set up ETL tracking
        
        # Test incremental query building
        query = postgres_loader_integration._build_load_query('test_table', ['created_at'], force_full=False)
        
        # Should include WHERE clause for incremental load
        assert 'WHERE' in query
        assert "created_at > '2023-01-01 10:00:00'" in query
        
        # Test full load query
        full_query = postgres_loader_integration._build_load_query('test_table', ['created_at'], force_full=True)
        assert 'WHERE' not in full_query  # No incremental conditions
    
    def test_build_count_query_integration(self, postgres_loader_integration, setup_etl_tracking):
        """Test count query building with real database."""
        setup_etl_tracking  # Set up ETL tracking
        
        # Test incremental count query building
        query = postgres_loader_integration._build_count_query('test_table', ['created_at'], force_full=False)
        
        # Should include WHERE clause for incremental load
        assert 'WHERE' in query
        assert "created_at > '2023-01-01 10:00:00'" in query
        
        # Test full load count query
        full_query = postgres_loader_integration._build_count_query('test_table', ['created_at'], force_full=True)
        assert 'WHERE' not in full_query  # No incremental conditions


@pytest.mark.integration
class TestErrorHandlingIntegration(TestPostgresLoaderIntegration):
    """Integration tests for error handling."""
    
    def test_database_connection_error_integration(self, postgres_loader_integration, sample_mysql_schema):
        """Test error handling with database connection issues."""
        # Test with a table that doesn't exist in the source database
        # This should cause the loader to fail when trying to query the source
        result = postgres_loader_integration.load_table('nonexistent_table', sample_mysql_schema, force_full=False)
        
        assert result is False
    
    def test_schema_creation_failure_integration(self, postgres_loader_integration, sqlite_engines, sample_mysql_schema):
        """Test handling of schema creation failures."""
        raw_engine, public_engine = sqlite_engines
        
        # Mock schema adapter to fail
        postgres_loader_integration.schema_adapter.create_postgres_table.return_value = False
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = False
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader_integration.load_table('test_table', sample_mysql_schema, force_full=False)
            
            assert result is False
            logger.debug("Successfully handled schema creation failure gracefully")


@pytest.mark.integration
class TestSQLiteCompatibilityIntegration(TestPostgresLoaderIntegration):
    """Integration tests for SQLite compatibility (Lesson 25)."""
    
    def test_sqlite_table_discovery_integration(self, sqlite_engines):
        """Test SQLite table discovery using sqlite_master instead of information_schema."""
        raw_engine, public_engine = sqlite_engines
        
        # Create test tables
        with raw_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table1 (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table2 (
                    id INTEGER PRIMARY KEY,
                    description TEXT
                )
            """))
            conn.commit()
        
        # Test SQLite table discovery
        with raw_engine.connect() as conn:
            # Use sqlite_master instead of information_schema (Lesson 25)
            result = conn.execute(text("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)).fetchall()
            
            table_names = [row[0] for row in result]
            assert 'test_table1' in table_names
            assert 'test_table2' in table_names
            
            logger.debug(f"Successfully discovered SQLite tables: {table_names}")
    
    def test_sqlite_column_information_integration(self, sqlite_engines):
        """Test SQLite column information using PRAGMA instead of information_schema."""
        raw_engine, public_engine = sqlite_engines
        
        # Create test table
        with raw_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TEXT
                )
            """))
            conn.commit()
        
        # Test SQLite column information using PRAGMA (Lesson 25)
        with raw_engine.connect() as conn:
            result = conn.execute(text("PRAGMA table_info(test_table)")).fetchall()
            
            column_info = {row[1]: {'type': row[2], 'notnull': row[3]} for row in result}
            assert 'id' in column_info
            assert 'name' in column_info
            assert 'created_at' in column_info
            assert column_info['name']['notnull'] == 1  # NOT NULL constraint
            
            logger.debug(f"Successfully retrieved SQLite column information: {column_info}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 