# MariaDB to PostgreSQL ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for migrating data from MariaDB to PostgreSQL, specifically designed for analytics use cases in healthcare and dental clinic environments.

## Overview

This script provides a robust solution for transferring data from a MariaDB source database to a PostgreSQL target database, with special considerations for analytics workflows. It handles schema conversion, data type mapping, and maintains a tracking system for synchronization status.

## Features

- **Flexible Data Type Mapping**: Intelligent conversion between MariaDB and PostgreSQL data types
- **Chunked Processing**: Handles large tables efficiently through chunked data transfer
- **Index Management**: Preserves and optimizes indexes during migration
- **Progress Tracking**: Maintains sync status and progress for each table
- **Error Handling**: Comprehensive error catching and logging
- **Analytics-First Design**: Relaxed constraints in raw layer for data exploration
- **Special Date Handling**: Properly converts MySQL zero dates (`0000-00-00`) to NULL values in PostgreSQL

## Prerequisites

- Python 3.7+
- Required Python packages:
  - pandas
  - sqlalchemy
  - pymysql
  - psycopg2-binary
  - python-dotenv

```bash
pip install pandas sqlalchemy pymysql psycopg2-binary python-dotenv
```

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

## Common Issues and Solutions

### Date/Time Issues

PostgreSQL is strict about date/time values, unlike MySQL which allows invalid dates like `0000-00-00`. This pipeline handles these differences by:

1. Converting invalid dates to NULL values
2. Special handling for common date columns in healthcare tables
3. Robust error handling for date conversion failures

Example tables requiring special handling:
- histappointment
- insverify
- insverifyhist
- patplan

### Boolean Type Handling

The script automatically maps MySQL `tinyint(1)` to PostgreSQL `BOOLEAN` and provides special handling for columns with boolean-like names (is_*, has_*, etc.).

### Character Set and Encoding

By default, the pipeline assumes UTF-8 encoding. If you encounter character set issues, you may need to adjust encoding parameters in the database connection strings.

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

### Troubleshooting Common Log Messages

- `DatetimeFieldOverflow`: Indicates invalid date values in source data
- `ProgrammingError: column X does not exist`: Schema mismatch between source and target
- `OperationalError: connection failed`: Database connection issues

## Performance Tuning

For large datasets, you can adjust the following parameters in your `.env` file:

- `ETL_CHUNK_SIZE`: Number of rows to process in memory at once
- `ETL_SUB_CHUNK_SIZE`: Number of rows per batch insert
- `ETL_SMALL_TABLE_THRESHOLD`: Tables smaller than this will use exact row count matching

## Best Practices

1. **Testing**: Always test with a subset of data first
2. **Monitoring**: Watch the logs for any warnings or errors
3. **Validation**: Use dbt tests for data quality checks
4. **Backup**: Ensure source data is backed up before migration
5. **Performance**: Adjust chunk sizes based on your system resources
6. **Date Columns**: Pay special attention to date columns when troubleshooting

## Advanced Usage

### Customizing Type Mapping

You can extend the `map_type()` function to handle specific columns or data types unique to your database:

```python
def map_type(mysql_type: str, column_name: str = '') -> str:
    # Custom mapping for specific column names
    if column_name == 'special_column':
        return 'JSONB'  # Use PostgreSQL JSON type
        
    # Default mappings
    type_lower = str(mysql_type).lower()
    # ... rest of function
```

### Handling Large Binary Objects

For tables with BLOB/BINARY columns, consider:
1. Using `ETL_CHUNK_SIZE=10000` (smaller chunks)
2. Setting `method='multi'` in the `to_sql` call
3. Potentially skipping these columns or tables if they're not needed for analytics

## Error Handling

The script includes comprehensive error handling for:
- Connection failures
- Schema mismatches
- Data type conversion issues
- Primary key violations
- Resource constraints

## License

[Add license information]

## Contributing

[Add contribution guidelines if this is an open-source project]