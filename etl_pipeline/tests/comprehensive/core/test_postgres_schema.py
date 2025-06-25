import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import text
from sqlalchemy.engine import Engine
from etl_pipeline.core.postgres_schema import PostgresSchema
import re

# Create mock inspectors at module level
mysql_inspector = Mock()
postgres_inspector = Mock()

@pytest.fixture
def mock_engines():
    """Create mock SQLAlchemy engines."""
    mysql_engine = Mock(spec=Engine)
    postgres_engine = Mock(spec=Engine)
    return mysql_engine, postgres_engine

@pytest.fixture
def postgres_schema(mock_engines):
    """Create PostgresSchema instance with mock engines."""
    mysql_engine, postgres_engine = mock_engines
    
    # Patch inspect to return appropriate inspector
    with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect:
        def get_inspector(engine):
            if engine is mysql_engine:
                return mysql_inspector
            return postgres_inspector
        mock_inspect.side_effect = get_inspector
        
        schema = PostgresSchema(
            mysql_engine=mysql_engine,
            postgres_engine=postgres_engine,
            mysql_db='test_mysql',
            postgres_db='test_postgres',
            postgres_schema='raw'
        )
        return schema

def test_convert_mysql_type_standard(postgres_schema):
    """Test standard MySQL to PostgreSQL type conversion."""
    # Test basic type conversions
    assert postgres_schema._convert_mysql_type_standard('int') == 'integer'
    assert postgres_schema._convert_mysql_type_standard('varchar(255)') == 'character varying(255)'
    assert postgres_schema._convert_mysql_type_standard('decimal(10,2)') == 'numeric(10,2)'
    assert postgres_schema._convert_mysql_type_standard('datetime') == 'timestamp'
    assert postgres_schema._convert_mysql_type_standard('json') == 'jsonb'
    
    # Test unknown type falls back to text
    assert postgres_schema._convert_mysql_type_standard('unknown_type') == 'text'

def test_analyze_column_data_boolean(postgres_schema):
    """Test column data analysis for boolean detection."""
    # Mock the MySQL connection and query result
    mock_result = Mock()
    mock_result.scalar.return_value = 0  # No non-boolean values found
    mock_conn = Mock()
    mock_conn.execute.return_value = mock_result
    
    # Create a proper context manager mock using MagicMock
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    postgres_schema.mysql_engine.connect.return_value = mock_context
    
    # Test TINYINT column that should be boolean
    result = postgres_schema._analyze_column_data('test_table', 'is_active', 'TINYINT')
    assert result == 'boolean'
    
    # Verify the query was executed
    mock_conn.execute.assert_called_once()
    query = mock_conn.execute.call_args[0][0]
    assert hasattr(query, 'text')  # Check if it's a SQLAlchemy text object
    assert 'WHERE is_active IS NOT NULL' in str(query)

def test_analyze_column_data_non_boolean(postgres_schema):
    """Test column data analysis for non-boolean TINYINT."""
    # Mock the MySQL connection and query result
    mock_result = Mock()
    mock_result.scalar.return_value = 1  # Found non-boolean values
    mock_conn = Mock()
    mock_conn.execute.return_value = mock_result
    
    # Create a proper context manager mock using MagicMock
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    postgres_schema.mysql_engine.connect.return_value = mock_context
    
    # Test TINYINT column that should be smallint
    result = postgres_schema._analyze_column_data('test_table', 'status', 'TINYINT')
    assert result == 'smallint'

def test_adapt_schema(postgres_schema):
    """Test schema adaptation from MySQL to PostgreSQL."""
    # Test with a more complex CREATE TABLE statement
    mysql_schema = {
        'create_statement': """
        CREATE TABLE `test_table` (
            `id` int NOT NULL AUTO_INCREMENT,
            `name` varchar(255) NOT NULL,
            `is_active` tinyint(1) NOT NULL,
            `created_at` datetime NOT NULL,
            `updated_at` timestamp NULL DEFAULT NULL,
            `description` text,
            `price` decimal(10,2) NOT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_name` (`name`),
            KEY `idx_created_at` (`created_at`)
        )
        """
    }
    
    # Mock _convert_mysql_type instead of the individual methods
    with patch.object(postgres_schema, '_convert_mysql_type') as mock_convert:
        # Set up the mock to return appropriate types
        def convert_side_effect(mysql_type, table_name=None, column_name=None):
            type_map = {
                'id': 'integer',
                'name': 'character varying(255)',
                'is_active': 'boolean',
                'created_at': 'timestamp',
                'updated_at': 'timestamp',
                'description': 'text',
                'price': 'numeric(10,2)'
            }
            return type_map.get(column_name, 'text')
        mock_convert.side_effect = convert_side_effect
        
        # Call the method being tested
        result = postgres_schema.adapt_schema('test_table', mysql_schema)
        
        # Verify the result
        assert 'CREATE TABLE raw.test_table' in result
        
        # Verify column definitions
        expected_columns = [
            '"id" integer',
            '"name" character varying(255)',
            '"is_active" boolean',
            '"created_at" timestamp',
            '"updated_at" timestamp',
            '"description" text',
            '"price" numeric(10,2)',
            'PRIMARY KEY ("id")'
        ]
        
        for expected in expected_columns:
            assert expected in result, f"Expected column definition not found: {expected}"
        
        # Verify mock was called
        assert mock_convert.call_count > 0, "_convert_mysql_type was not called"
        
        # Verify mock calls for specific columns
        convert_calls = [call[0] for call in mock_convert.call_args_list]
        
        # Verify specific type conversions
        assert any('int' in str(call) for call in convert_calls), "int type was not converted"
        assert any('varchar' in str(call) for call in convert_calls), "varchar type was not converted"
        assert any('tinyint' in str(call) for call in convert_calls), "tinyint type was not converted"

def test_create_postgres_table(postgres_schema):
    """Test PostgreSQL table creation."""
    mysql_schema = {
        'create_statement': """
        CREATE TABLE `test_table` (
            `id` int NOT NULL,
            `name` varchar(255) NOT NULL,
            PRIMARY KEY (`id`)
        )
        """
    }
    
    # Mock the schema adaptation
    with patch.object(postgres_schema, 'adapt_schema', return_value='CREATE TABLE raw.test_table (id integer, name varchar(255))'):
        # Mock the PostgreSQL connection
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        postgres_schema.postgres_engine.begin.return_value = mock_context
        
        # Test table creation
        result = postgres_schema.create_postgres_table('test_table', mysql_schema)
        assert result is True
        
        # Verify the CREATE TABLE was executed
        mock_conn.execute.assert_called()

def test_verify_schema(postgres_schema):
    """Test schema verification."""
    mysql_schema = {
        'create_statement': """
        CREATE TABLE `test_table` (
            `id` int NOT NULL,
            `name` varchar(255) NOT NULL,
            PRIMARY KEY (`id`)
        )
        """
    }
    
    # Mock the PostgreSQL inspector to return proper column info
    mock_columns = [
        {'name': 'id', 'type': 'integer', 'nullable': False},
        {'name': 'name', 'type': 'varchar(255)', 'nullable': False}
    ]
    postgres_schema.postgres_inspector.get_columns.return_value = mock_columns
    
    # Mock the type conversion
    with patch.object(postgres_schema, '_convert_mysql_type') as mock_convert:
        # Set up the mock to return appropriate types
        def convert_side_effect(mysql_type, table_name=None, column_name=None):
            # Extract base type from MySQL type string
            # First remove any NOT NULL or other modifiers
            base_type = mysql_type.split('NOT NULL')[0].strip()
            # Then extract the base type without parameters
            base_type = base_type.split('(')[0].strip().lower()
            print(f"Converting MySQL type: {mysql_type}")
            print(f"Extracted base type: {base_type}")
            
            type_map = {
                'int': 'integer',
                'varchar': 'varchar',
                'tinyint': 'boolean',
                'datetime': 'timestamp',
                'text': 'text',
                'decimal': 'numeric'
            }
            
            # Handle types with parameters
            if '(' in mysql_type:
                param_match = re.search(r'\(([^)]+)\)', mysql_type)
                if param_match:
                    params = param_match.group(1)
                    mapped_type = type_map.get(base_type)
                    print(f"Found mapped type: {mapped_type}")
                    if mapped_type:
                        return f"{mapped_type}({params})"
                    print(f"Type {base_type} not found in map, falling back to text")
                    return f"text({params})"
            
            mapped_type = type_map.get(base_type)
            print(f"Found mapped type: {mapped_type}")
            if mapped_type:
                return mapped_type
            print(f"Type {base_type} not found in map, falling back to text")
            return 'text'
            
        mock_convert.side_effect = convert_side_effect
        
        # Test schema verification
        result = postgres_schema.verify_schema('test_table', mysql_schema)
        assert result is True
        
        # Verify the inspector was called
        postgres_schema.postgres_inspector.get_columns.assert_called_once_with('test_table', schema='raw')
        
        # Verify type conversion was called for each column
        assert mock_convert.call_count == 2
        convert_calls = [call[0] for call in mock_convert.call_args_list]
        assert any('int' in str(call) for call in convert_calls), "int type was not converted"
        assert any('varchar' in str(call) for call in convert_calls), "varchar type was not converted"
        
        # Test with mismatched column count
        postgres_schema.postgres_inspector.get_columns.return_value = mock_columns[:1]  # Only return id column
        result = postgres_schema.verify_schema('test_table', mysql_schema)
        assert result is False
        
        # Test with mismatched column name
        mock_columns[1]['name'] = 'wrong_name'
        postgres_schema.postgres_inspector.get_columns.return_value = mock_columns
        result = postgres_schema.verify_schema('test_table', mysql_schema)
        assert result is False
        
        # Test with mismatched column type
        mock_columns[1]['name'] = 'name'  # Reset name
        mock_columns[1]['type'] = 'wrong_type'
        postgres_schema.postgres_inspector.get_columns.return_value = mock_columns
        result = postgres_schema.verify_schema('test_table', mysql_schema)
        assert result is False 