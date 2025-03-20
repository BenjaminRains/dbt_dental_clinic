# MariaDB to PostgreSQL ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for migrating data from MariaDB to PostgreSQL, specifically designed for analytics use cases.

## Overview

This script provides a robust solution for transferring data from a MariaDB source database to a PostgreSQL target database, with special considerations for analytics workflows. It handles schema conversion, data type mapping, and maintains a tracking system for synchronization status.

## Features

- **Flexible Data Type Mapping**: Intelligent conversion between MariaDB and PostgreSQL data types
- **Chunked Processing**: Handles large tables efficiently through chunked data transfer
- **Index Management**: Preserves and optimizes indexes during migration
- **Progress Tracking**: Maintains sync status and progress for each table
- **Error Handling**: Comprehensive error catching and logging
- **Analytics-First Design**: Relaxed constraints in raw layer for data exploration

## Prerequisites

- Python 3.7+
- Required Python packages:
  - pandas
  - sqlalchemy
  - pymysql
  - psycopg2
  - python-dotenv

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# MariaDB Configuration
MARIADB_ROOT_USER=your_user
MARIADB_ROOT_PASSWORD=your_password
MARIADB_ROOT_HOST=localhost
MARIADB_ROOT_PORT=3306
DBT_MYSQL_DATABASE=your_database

# PostgreSQL Configuration
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_database

# ETL Configuration (optional)
ETL_CHUNK_SIZE=100000
ETL_SUB_CHUNK_SIZE=10000
ETL_MAX_RETRIES=3
ETL_TOLERANCE=0.001
ETL_SMALL_TABLE_THRESHOLD=10000
```

## Usage

### Basic Usage

```bash
python mariadb_postgre_pipe.py
```

### Sync Specific Tables

```bash
python mariadb_postgre_pipe.py --tables table1 table2 table3
```

## Data Quality Considerations

- Primary keys are preserved and enforced
- Other constraints are relaxed in the raw layer
- Data validation should be handled in transformation layer (e.g., dbt)
- Row count validation ensures data completeness
- Null values are allowed for analytics flexibility

## Logging

The script maintains detailed logs in `etl.log`, including:
- Connection status
- Table creation details
- Data transfer progress
- Error messages
- Data quality check results

## Best Practices

1. **Testing**: Always test with a subset of data first
2. **Monitoring**: Watch the logs for any warnings or errors
3. **Validation**: Use dbt tests for data quality checks
4. **Backup**: Ensure source data is backed up before migration
5. **Performance**: Adjust chunk sizes based on your system resources

## Error Handling

The script includes comprehensive error handling for:
- Connection failures
- Schema mismatches
- Data type conversion issues
- Primary key violations
- Resource constraints

## Contributing

[Add contribution guidelines if this is an open-source project]

## License

[Add license information]

def map_type(mysql_type: str, column_name: str = '') -> str:
    """Map MySQL types to PostgreSQL types with more precise type mapping."""
    type_lower = str(mysql_type).lower()
    
    # Basic integer mappings
    if type_lower == 'tinyint(1)':
        return 'BOOLEAN'
    elif type_lower.startswith('tinyint'):
        return 'SMALLINT'
    elif type_lower.startswith('int'):
        return 'INTEGER'
    elif type_lower.startswith('bigint'):
        return 'BIGINT'
    
    # String types - preserve lengths where possible
    elif type_lower.startswith('varchar'):
        try:
            length = type_lower.split('(')[1].split(')')[0]
            return f'VARCHAR({length})'
        except:
            return 'VARCHAR(255)'  # Safe fallback
    
    # Handle special types as TEXT
    elif type_lower.startswith('set(') or type_lower.startswith('enum('):
        return 'TEXT'  # Convert to TEXT for maximum flexibility

def convert_data_types(df: pd.DataFrame, table_name: str = None) -> pd.DataFrame:
    """Convert DataFrame types to be compatible with PostgreSQL, but be lenient with NULLs."""
    
    # Handle TIME type with NULL allowance
    if target_type == 'time without time zone':
        df[col] = df[col].apply(lambda x: None if pd.isna(x) or x == '' else x)
    
    # Boolean type - allow NULLs
    elif target_type == 'boolean':
        bool_values = df[col].map(lambda x: bool(x) if pd.notnull(x) else None)
        df[col] = pd.Series(bool_values, dtype="boolean")
    
    # Integer types - use 'coerce' to convert invalid to NULL
    elif target_type in ('smallint', 'integer', 'bigint'):
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Dates - allow NULLs for invalid dates
    elif target_type in ('timestamp', 'date'):
        df[col] = pd.to_datetime(df[col], errors='coerce')