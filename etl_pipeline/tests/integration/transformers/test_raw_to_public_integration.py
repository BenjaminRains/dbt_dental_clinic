"""
Integration Tests for RawToPublicTransformer

HYBRID TESTING APPROACH - INTEGRATION TESTS
===========================================
Purpose: Real database integration with SQLite
Scope: Safety, error handling, actual data flow
Coverage: Integration scenarios and edge cases
Execution: < 10 seconds per component
Markers: @pytest.mark.integration

This file focuses on testing RawToPublicTransformer with real database connections
using SQLite for safety and actual data flow validation.
"""

import pytest
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
from datetime import datetime
import tempfile
import os
import logging
from typing import Optional, List, Dict, Any

from etl_pipeline.transformers.raw_to_public import RawToPublicTransformer

logger = logging.getLogger(__name__)

class SQLiteTestTransformer(RawToPublicTransformer):
    """
    SQLite-compatible test transformer that overrides schema-dependent methods.
    
    This class adapts the RawToPublicTransformer for SQLite testing by:
    1. Removing schema prefixes from SQL queries
    2. Using SQLite-compatible table creation
    3. Handling SQLite's lack of schema support
    """
    
    def _read_from_raw(self, table_name: str, is_incremental: bool) -> Optional[pd.DataFrame]:
        """Read data from raw schema (SQLite version without schema prefix)."""
        try:
            query = f"SELECT * FROM {table_name}"
            if is_incremental:
                # Add incremental logic here if needed
                pass
                
            with self.source_engine.connect() as conn:
                return pd.read_sql(query, conn)
                
        except Exception as e:
            logger.error(f"Error reading from raw schema: {str(e)}")
            return None
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table (SQLite version without schema prefix)."""
        try:
            with self.target_engine.connect() as conn:
                return conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).scalar()
        except Exception as e:
            logger.error(f"Error getting row count: {str(e)}")
            return 0
    
    def _write_to_public(self, table_name: str, df: pd.DataFrame, is_incremental: bool) -> bool:
        """Write transformed data to public schema (SQLite version without schema prefix)."""
        try:
            if df.empty:
                logger.warning(f"No data to write to public schema for table: {table_name}")
                return True
                
            # Create table if it doesn't exist
            self._ensure_table_exists(table_name, df)
            
            # Write data
            with self.target_engine.connect() as conn:
                if is_incremental:
                    # Handle incremental updates
                    self._handle_incremental_update(conn, table_name, df)
                else:
                    # Full load
                    df.to_sql(
                        table_name,
                        conn,
                        if_exists='replace',
                        index=False
                    )
                    
            return True
            
        except Exception as e:
            logger.error(f"Error writing to public schema: {str(e)}")
            return False
    
    def _ensure_table_exists(self, table_name: str, df: pd.DataFrame):
        """Ensure the target table exists with correct structure (SQLite version)."""
        try:
            with self.target_engine.connect() as conn:
                # Check if table exists using SQLite pragma
                result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
                table_exists = result.fetchone() is not None
                
                if not table_exists:
                    # Create table with appropriate structure
                    create_table_sql = self._generate_create_table_sql(table_name, df)
                    conn.execute(text(create_table_sql))
                    logger.info(f"Created table {table_name}")
                else:
                    logger.debug(f"Table {table_name} already exists")
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error ensuring table exists: {str(e)}")
            raise
    
    def _generate_create_table_sql(self, table_name: str, df: pd.DataFrame) -> str:
        """Generate CREATE TABLE SQL for SQLite."""
        columns = []
        for col, dtype in df.dtypes.items():
            # Map pandas dtypes to SQLite types
            if 'int' in str(dtype):
                sql_type = 'INTEGER'
            elif 'float' in str(dtype):
                sql_type = 'REAL'
            elif 'datetime' in str(dtype):
                sql_type = 'TEXT'
            else:
                sql_type = 'TEXT'
            
            columns.append(f'"{col}" {sql_type}')
        
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"
    
    def _handle_incremental_update(self, conn, table_name: str, df: pd.DataFrame):
        """Handle incremental updates for SQLite."""
        # For SQLite, we'll use a simple replace strategy
        # In a real implementation, this would be more sophisticated
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    def _update_transformation_status(self, table_name: str, rows_processed: int):
        """Update transformation tracking table (SQLite version with status field)."""
        try:
            with self.target_engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO etl_transform_status 
                        (table_name, transform_type, rows_processed, status, transform_time)
                        VALUES (:table_name, 'raw_to_public', :rows_processed, :status, :transform_time)
                    """),
                    {
                        'table_name': table_name,
                        'rows_processed': rows_processed,
                        'status': 'success',
                        'transform_time': datetime.now()
                    }
                )
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating transformation status: {str(e)}")
            # Don't raise - this is non-critical
    
    def get_last_transformed(self, table_name: str) -> Optional[datetime]:
        """Get the last transformation timestamp for a table (SQLite version)."""
        try:
            with self.target_engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT MAX(transform_time) 
                        FROM etl_transform_status 
                        WHERE table_name = :table_name 
                        AND transform_type = 'raw_to_public'
                    """),
                    {'table_name': table_name}
                ).scalar()
                # Convert string to datetime if needed
                if result and isinstance(result, str):
                    return datetime.fromisoformat(result.replace('Z', '+00:00'))
                return result
        except Exception as e:
            logger.error(f"Error getting last transform time: {str(e)}")
            return None
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get the columns for a table (SQLite version)."""
        try:
            with self.target_engine.connect() as conn:
                result = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
                columns = []
                for row in result:
                    columns.append({
                        'name': row[1],
                        'type': row[2],
                        'nullable': not row[3],
                        'default': row[4],
                        'primary_key': bool(row[5])
                    })
                return columns
        except Exception as e:
            logger.error(f"Error getting columns: {str(e)}")
            return []
    
    def get_table_primary_key(self, table_name: str) -> Optional[List[str]]:
        """Get the primary key columns for a table (SQLite version)."""
        try:
            columns = self.get_table_columns(table_name)
            pk_columns = [col['name'] for col in columns if col['primary_key']]
            return pk_columns if pk_columns else None
        except Exception as e:
            logger.error(f"Error getting primary key: {str(e)}")
            return None
    
    def get_table_size(self, table_name: str) -> int:
        """Get the size of a table in bytes (SQLite version)."""
        try:
            with self.target_engine.connect() as conn:
                # SQLite doesn't have pg_total_relation_size, so we'll estimate
                # based on row count and average row size
                row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                # Estimate 100 bytes per row as a rough approximation
                return row_count * 100 if row_count else 0
        except Exception as e:
            logger.error(f"Error getting table size: {str(e)}")
            return 0
    
    def verify_transform(self, table_name: str) -> bool:
        """Verify that the transformation was successful (SQLite version)."""
        try:
            # Get row count from source (raw) table
            with self.source_engine.connect() as conn:
                source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            # Get row count from target (public) table
            with self.target_engine.connect() as conn:
                target_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            
            return source_count == target_count
        except Exception as e:
            logger.error(f"Error verifying transform: {str(e)}")
            return False

@pytest.fixture
def sqlite_engines():
    """Create SQLite engines for integration testing."""
    # Create temporary SQLite databases
    raw_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    raw_db.close()
    public_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    public_db.close()
    
    # Create engines
    raw_engine = create_engine(f'sqlite:///{raw_db.name}')
    public_engine = create_engine(f'sqlite:///{public_db.name}')
    
    yield raw_engine, public_engine
    
    # Cleanup - close connections first
    raw_engine.dispose()
    public_engine.dispose()
    
    # Wait a moment for connections to close
    import time
    time.sleep(0.1)
    
    # Now try to delete files
    try:
        os.unlink(raw_db.name)
    except (OSError, PermissionError):
        pass  # File might still be in use, but that's okay for tests
    try:
        os.unlink(public_db.name)
    except (OSError, PermissionError):
        pass  # File might still be in use, but that's okay for tests

@pytest.fixture
def transformer(sqlite_engines):
    """Create a real SQLiteTestTransformer with SQLite engines."""
    raw_engine, public_engine = sqlite_engines
    return SQLiteTestTransformer(raw_engine, public_engine)

@pytest.fixture
def setup_test_data(sqlite_engines):
    """Set up test data in the raw schema."""
    raw_engine, public_engine = sqlite_engines
    
    # Create raw schema table (SQLite doesn't support schemas, so we use table names directly)
    with raw_engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS patient (
                "PatNum" INTEGER PRIMARY KEY,
                "LName" TEXT,
                "FName" TEXT,
                "BirthDate" TEXT,
                "Gender" TEXT,
                "SSN" TEXT,
                "Address" TEXT,
                "City" TEXT,
                "State" TEXT,
                "Zip" TEXT
            )
        """))
        
        # Insert test data using individual inserts with proper parameter binding
        test_data = [
            {'patnum': 1, 'lname': 'Doe', 'fname': 'John', 'birthdate': '1990-01-01', 'gender': 'M', 'ssn': '123-45-6789', 'address': '123 Main St', 'city': 'Anytown', 'state': 'CA', 'zip': '12345'},
            {'patnum': 2, 'lname': 'Smith', 'fname': 'Jane', 'birthdate': '1985-05-15', 'gender': 'F', 'ssn': '987-65-4321', 'address': '456 Oak Ave', 'city': 'Somewhere', 'state': 'NY', 'zip': '67890'},
            {'patnum': 3, 'lname': 'Johnson', 'fname': 'Bob', 'birthdate': '1978-12-25', 'gender': 'M', 'ssn': '555-12-3456', 'address': '789 Pine Rd', 'city': 'Elsewhere', 'state': 'TX', 'zip': '11111'},
            {'patnum': 4, 'lname': 'Brown', 'fname': 'Alice', 'birthdate': '1992-03-10', 'gender': 'F', 'ssn': '111-22-3333', 'address': '321 Elm St', 'city': 'Nowhere', 'state': 'FL', 'zip': '22222'},
            {'patnum': 5, 'lname': 'Wilson', 'fname': 'Charlie', 'birthdate': '1988-07-20', 'gender': 'M', 'ssn': '444-55-6666', 'address': '654 Maple Dr', 'city': 'Everywhere', 'state': 'WA', 'zip': '33333'}
        ]
        
        # Insert each row individually
        for row in test_data:
            conn.execute(text("""
                INSERT INTO patient 
                ("PatNum", "LName", "FName", "BirthDate", "Gender", "SSN", "Address", "City", "State", "Zip")
                VALUES (:patnum, :lname, :fname, :birthdate, :gender, :ssn, :address, :city, :state, :zip)
            """), row)
        
        conn.commit()

@pytest.fixture
def setup_etl_tracking(transformer):
    """Set up ETL tracking table in public schema."""
    with transformer.target_engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_transform_status (
                table_name TEXT,
                transform_type TEXT,
                rows_processed INTEGER,
                status TEXT,
                transform_time TIMESTAMP
            )
        """))
        conn.commit()

@pytest.mark.integration
def test_transformer_initialization_integration(transformer, sqlite_engines):
    """Test transformer initialization with real SQLite engines."""
    raw_engine, public_engine = sqlite_engines
    
    assert transformer.source_engine == raw_engine
    assert transformer.target_engine == public_engine
    assert transformer.source_schema == 'raw'
    assert transformer.target_schema == 'public'
    assert transformer.inspector is not None

@pytest.mark.integration
def test_read_from_raw_integration(transformer, setup_test_data):
    """Test reading from raw schema with real database."""
    result = transformer._read_from_raw('patient', False)
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5
    assert list(result.columns) == [
        'PatNum', 'LName', 'FName', 'BirthDate', 'Gender',
        'SSN', 'Address', 'City', 'State', 'Zip'
    ]
    
    # Check data values
    assert result.iloc[0]['PatNum'] == 1
    assert result.iloc[0]['LName'] == 'Doe'
    assert result.iloc[0]['FName'] == 'John'

@pytest.mark.integration
def test_apply_transformations_integration(transformer, setup_test_data):
    """Test data transformations with real data."""
    raw_data = transformer._read_from_raw('patient', False)
    result = transformer._apply_transformations(raw_data, 'patient')
    
    # Check column name transformation
    expected_columns = [
        'patnum', 'lname', 'fname', 'birthdate', 'gender',
        'ssn', 'address', 'city', 'state', 'zip'
    ]
    assert list(result.columns) == expected_columns
    
    # Check data preservation
    assert result.iloc[0]['patnum'] == 1
    assert result.iloc[0]['lname'] == 'Doe'
    assert result.iloc[0]['fname'] == 'John'
    assert result.iloc[0]['gender'] == 'M'
    
    # Check all rows preserved
    assert len(result) == 5

@pytest.mark.integration
def test_write_to_public_integration(transformer, setup_test_data):
    """Test writing to public schema with real database."""
    raw_data = transformer._read_from_raw('patient', False)
    transformed_data = transformer._apply_transformations(raw_data, 'patient')
    
    result = transformer._write_to_public('patient', transformed_data, False)
    
    assert result is True
    
    # Verify data was written
    with transformer.target_engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
        assert result == 5
        
        # Check data integrity
        data = conn.execute(text("SELECT * FROM patient ORDER BY patnum")).fetchall()
        assert len(data) == 5
        assert data[0][0] == 1  # patnum
        assert data[0][1] == 'Doe'  # lname
        assert data[0][2] == 'John'  # fname

@pytest.mark.integration
def test_transform_table_full_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test complete table transformation with real database."""
    result = transformer.transform_table('patient')
    
    assert result is True
    
    # Verify data was transformed and written
    with transformer.target_engine.connect() as conn:
        # Check public table
        count = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
        assert count == 5
        
        # Check ETL tracking
        tracking = conn.execute(text("""
            SELECT rows_processed, status 
            FROM etl_transform_status 
            WHERE table_name = 'patient' AND transform_type = 'raw_to_public'
        """)).fetchone()
        assert tracking is not None
        assert tracking[0] == 5  # rows_processed
        assert tracking[1] == 'success'  # status

@pytest.mark.integration
def test_transform_table_incremental_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test incremental table transformation with real database."""
    # First transformation
    result1 = transformer.transform_table('patient', is_incremental=True)
    assert result1 is True
    
    # Add more data to raw
    with transformer.source_engine.connect() as conn:
        # Use proper parameter binding for single insert
        conn.execute(text("""
            INSERT INTO patient 
            ("PatNum", "LName", "FName", "BirthDate", "Gender", "SSN", "Address", "City", "State", "Zip")
            VALUES (:patnum, :lname, :fname, :birthdate, :gender, :ssn, :address, :city, :state, :zip)
        """), {
            'patnum': 6,
            'lname': 'Davis',
            'fname': 'Emma',
            'birthdate': '1995-09-15',
            'gender': 'F',
            'ssn': '777-88-9999',
            'address': '999 Oak St',
            'city': 'Newtown',
            'state': 'OR',
            'zip': '44444'
        })
        conn.commit()
    
    # Second transformation (incremental)
    result2 = transformer.transform_table('patient', is_incremental=True)
    assert result2 is True
    
    # Verify incremental behavior
    with transformer.target_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
        assert count == 6  # Should have 6 rows (5 + 1 new)

@pytest.mark.integration
def test_get_last_transformed_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test getting last transformation timestamp with real database."""
    # No transformation yet
    result = transformer.get_last_transformed('patient')
    assert result is None
    
    # Perform transformation
    transformer.transform_table('patient')
    
    # Check last transformed
    result = transformer.get_last_transformed('patient')
    assert result is not None
    assert isinstance(result, datetime)

@pytest.mark.integration
def test_update_transform_status_integration(transformer, setup_etl_tracking):
    """Test updating transformation status with real database."""
    transformer.update_transform_status('patient', 100, 'success')
    
    with transformer.target_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT rows_processed, status 
            FROM etl_transform_status 
            WHERE table_name = 'patient' AND transform_type = 'raw_to_public'
        """)).fetchone()
        
        assert result is not None
        assert result[0] == 100  # rows_processed
        assert result[1] == 'success'  # status

@pytest.mark.integration
def test_verify_transform_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test transformation verification with real database."""
    # Before transformation
    result = transformer.verify_transform('patient')
    assert result is False  # No data in public schema yet
    
    # After transformation
    transformer.transform_table('patient')
    result = transformer.verify_transform('patient')
    assert result is True  # Should match

@pytest.mark.integration
def test_get_table_row_count_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test getting table row count with real database."""
    # Before transformation
    count = transformer.get_table_row_count('patient')
    assert count == 0  # No data in public schema yet
    
    # After transformation
    transformer.transform_table('patient')
    count = transformer.get_table_row_count('patient')
    assert count == 5

@pytest.mark.integration
def test_get_table_schema_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test getting table schema with real database."""
    # Transform table first
    transformer.transform_table('patient')
    
    schema = transformer.get_table_schema('patient')
    
    assert isinstance(schema, dict)
    assert 'columns' in schema
    assert 'primary_key' in schema
    assert 'foreign_keys' in schema
    assert 'indexes' in schema
    assert 'constraints' in schema
    
    # Check columns
    columns = schema['columns']
    assert len(columns) > 0
    column_names = [col['name'] for col in columns]
    assert 'patnum' in column_names
    assert 'lname' in column_names
    assert 'fname' in column_names

@pytest.mark.integration
def test_has_schema_changed_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test schema change detection with real database."""
    # Transform table first
    transformer.transform_table('patient')
    
    # Get current schema hash
    schema = transformer.get_table_schema('patient')
    schema_hash = str(hash(str(schema)))
    
    # Test no change
    result = transformer.has_schema_changed('patient', schema_hash)
    assert result is False
    
    # Test with different hash
    result = transformer.has_schema_changed('patient', 'different_hash')
    assert result is True

@pytest.mark.integration
def test_get_table_metadata_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test getting table metadata with real database."""
    # Transform table first
    transformer.transform_table('patient')
    
    metadata = transformer.get_table_metadata('patient')
    
    assert isinstance(metadata, dict)
    assert 'row_count' in metadata
    assert 'size' in metadata
    assert 'last_transformed' in metadata
    assert 'schema' in metadata
    
    assert metadata['row_count'] == 5
    assert metadata['size'] > 0
    assert metadata['last_transformed'] is not None
    assert isinstance(metadata['schema'], dict)

@pytest.mark.integration
def test_error_handling_integration(transformer):
    """Test error handling with real database."""
    # Test reading from non-existent table
    result = transformer._read_from_raw('non_existent_table', False)
    assert result is None
    
    # Test writing empty dataframe
    empty_df = pd.DataFrame()
    result = transformer._write_to_public('test_table', empty_df, False)
    assert result is True  # Should handle empty data gracefully

@pytest.mark.integration
def test_data_integrity_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test data integrity during transformation with real database."""
    # Perform transformation
    transformer.transform_table('patient')
    
    # Verify data integrity
    with transformer.source_engine.connect() as raw_conn, \
         transformer.target_engine.connect() as public_conn:
        
        # Get raw data
        raw_data = raw_conn.execute(text("SELECT * FROM patient ORDER BY \"PatNum\"")).fetchall()
        
        # Get public data
        public_data = public_conn.execute(text("SELECT * FROM patient ORDER BY patnum")).fetchall()
        
        # Check row count
        assert len(raw_data) == len(public_data)
        
        # Check data values (accounting for column name transformation)
        for i, (raw_row, public_row) in enumerate(zip(raw_data, public_data)):
            assert raw_row[0] == public_row[0]  # PatNum/patnum
            assert raw_row[1] == public_row[1]  # LName/lname
            assert raw_row[2] == public_row[2]  # FName/fname

@pytest.mark.integration
def test_performance_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test performance with real database."""
    import time
    
    # Measure transformation time
    start_time = time.time()
    result = transformer.transform_table('patient')
    end_time = time.time()
    
    assert result is True
    assert (end_time - start_time) < 5.0  # Should complete within 5 seconds

@pytest.mark.integration
def test_idempotency_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test idempotency of transformation with real database."""
    # First transformation
    result1 = transformer.transform_table('patient')
    assert result1 is True
    
    # Second transformation (should be idempotent)
    result2 = transformer.transform_table('patient')
    assert result2 is True
    
    # Verify same result
    with transformer.target_engine.connect() as conn:
        count1 = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
        
    # Third transformation
    result3 = transformer.transform_table('patient')
    assert result3 is True
    
    with transformer.target_engine.connect() as conn:
        count2 = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
    
    # Should have same count (idempotent)
    assert count1 == count2

@pytest.mark.integration
def test_large_dataset_integration(transformer, setup_etl_tracking):
    """Test with larger dataset using real database."""
    # Create larger dataset
    with transformer.source_engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS large_patient (
                "PatNum" INTEGER PRIMARY KEY,
                "LName" TEXT,
                "FName" TEXT,
                "BirthDate" TEXT,
                "Gender" TEXT
            )
        """))
        
        # Insert 100 rows using proper parameter binding
        for i in range(1, 101):
            conn.execute(text("""
                INSERT INTO large_patient 
                ("PatNum", "LName", "FName", "BirthDate", "Gender")
                VALUES (:patnum, :lname, :fname, :birthdate, :gender)
            """), {
                'patnum': i,
                'lname': f'LastName{i}',
                'fname': f'FirstName{i}',
                'birthdate': '1990-01-01',
                'gender': 'M' if i % 2 == 0 else 'F'
            })
        
        conn.commit()
    
    # Transform large dataset
    result = transformer.transform_table('large_patient')
    assert result is True
    
    # Verify all data
    with transformer.target_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM large_patient")).scalar()
        assert count == 100

@pytest.mark.integration
def test_schema_evolution_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test handling of schema evolution with real database."""
    # Initial transformation
    transformer.transform_table('patient')
    
    # Add new column to raw schema
    with transformer.source_engine.connect() as conn:
        conn.execute(text("ALTER TABLE patient ADD COLUMN \"Phone\" TEXT"))
        conn.execute(text("UPDATE patient SET \"Phone\" = '555-1234' WHERE \"PatNum\" = 1"))
        conn.commit()
    
    # Transform again (should handle schema change)
    result = transformer.transform_table('patient')
    assert result is True
    
    # Verify new column exists in public schema
    with transformer.target_engine.connect() as conn:
        columns = conn.execute(text("PRAGMA table_info(patient)")).fetchall()
        column_names = [col[1] for col in columns]
        assert 'phone' in column_names  # Should be lowercase

@pytest.mark.integration
def test_cleanup_integration(transformer, setup_test_data, setup_etl_tracking):
    """Test cleanup and resource management."""
    # Perform transformation
    transformer.transform_table('patient')
    
    # Verify connections are properly closed
    # This is implicit in the context manager usage, but we can verify
    # that the transformer still works after multiple operations
    
    # Multiple operations should work
    for i in range(3):
        result = transformer.get_table_row_count('patient')
        assert result == 5
        assert transformer.get_last_transformed('patient') is not None 