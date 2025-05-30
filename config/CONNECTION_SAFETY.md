# Connection Safety Guidelines for ETL Process

## Overview

This document outlines the connection architecture and safety guidelines for ETL processes that 
connect to the client's live operational database (OpenDental) and transfer data to the
analytics PostgreSQL database.

## Connection Architecture

```
[Client Live Server] → [Read-Only Connection] → [ETL Process] → [Write Connection] → [PostgreSQL Analytics DB]
```

The ETL process must follow a strict unidirectional data flow:
1. **Read-only** access to the source database (client's operational server)
2. Process/transform data locally
3. **Write-only** to the target analytics database (PostgreSQL)

## Safety Measures

### 1. Read-Only Source Access

- **NEVER** use root or admin credentials for ETL operations
- **ALWAYS** use a dedicated read-only user for accessing the client's operational server
- Enforce read-only at multiple levels:
  - Database user permissions
  - Connection string parameters
  - Query validation in application code

### 2. Connection Configuration

Update your `.env` file (based on `.env.template`) with the following structure:

```
# Source Database (READ-ONLY)
SOURCE_DB_HOST=client-server.example.com  # LIVE operation server
SOURCE_DB_PORT=3306
SOURCE_DB_NAME=opendental
SOURCE_DB_READONLY_USER=readonly_user
SOURCE_DB_READONLY_PASSWORD=readonly_password

# Target Database (WRITE)
TARGET_DB_HOST=localhost
TARGET_DB_PORT=5432
TARGET_DB_NAME=opendental_analytics
TARGET_DB_USER=postgres
TARGET_DB_PASSWORD=your_postgres_password

# Environment Configuration
ENVIRONMENT=development  # or production
```

### 3. Setting Up Read-Only User

Create a dedicated read-only user on the source database with these credentials:

```sql
CREATE USER 'readonly_user'@'%' IDENTIFIED BY 'readonly_password';
GRANT SELECT ON opendental.* TO 'readonly_user'@'%';
FLUSH PRIVILEGES;
```

### 4. Implementation Guidelines

When updating the ETL script, follow these guidelines:

1. **Connection Factory Pattern**:
   ```python
   def get_source_connection(readonly=True):
       """Create source connection with mandatory readonly flag"""
       if not readonly:
           raise ValueError("Write access to source database is not allowed")
       
       return create_engine(
           f"mysql+pymysql://{os.getenv('SOURCE_DB_READONLY_USER')}:{os.getenv('SOURCE_DB_READONLY_PASSWORD')}@"
           f"{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/"
           f"{os.getenv('SOURCE_DB_NAME')}"
       )
   
   def get_target_connection():
       """Create target PostgreSQL connection"""
       return create_engine(
           f"postgresql+psycopg2://{os.getenv('TARGET_DB_USER')}:{os.getenv('TARGET_DB_PASSWORD')}@"
           f"{os.getenv('TARGET_DB_HOST')}:{os.getenv('TARGET_DB_PORT')}/"
           f"{os.getenv('TARGET_DB_NAME')}"
       )
   ```

2. **Query Validation**:
   ```python
   def execute_source_query(query, params=None):
       """Execute read-only queries against source database"""
       if not query.strip().upper().startswith("SELECT"):
           raise ValueError("Only SELECT queries are allowed against source database")
       
       source_conn = get_source_connection(readonly=True)
       with source_conn.connect() as conn:
           return conn.execute(text(query), params)
   ```

3. **Two-Phase ETL Process**:
   ```python
   def sync_table_safely(table_name: str) -> bool:
       """Two-phase ETL with validation between phases"""
       try:
           # Phase 1: Extract to temporary in-memory data
           temp_extracted_data = extract_to_staging(table_name)
           
           # Validate data quality before loading
           if not validate_staging_data(temp_extracted_data, table_name):
               logger.error(f"Data validation failed for {table_name}")
               return False
               
           # Phase 2: Load from temporary data to target only after validation
           return load_from_staging(temp_extracted_data, table_name)
       except Exception as e:
           logger.error(f"Error in two-phase sync for {table_name}: {str(e)}")
           return False
   ```

4. **Transaction Handling**:
   ```python
   def load_from_staging(staging_data, target_table):
       """Atomic transaction for loading validated data"""
       target_conn = get_target_connection()
       try:
           with target_conn.begin() as transaction:
               # Load data in a single transaction
               staging_data.to_sql(
                   name=target_table,
                   con=transaction.connection,
                   if_exists='replace',
                   index=False
               )
               return True
       except Exception as e:
           logger.error(f"Transaction failed: {str(e)}")
           return False
   ```

## Environment-Specific Safety

For production environments, add additional safeguards:

```python
def is_production_environment():
    """Detect if we're running against production"""
    return os.getenv('ENVIRONMENT') == 'production'

def require_confirmation(operation, target):
    """Require explicit confirmation for destructive operations in production"""
    if is_production_environment():
        confirmation = input(f"⚠️ WARNING: About to {operation} {target} in PRODUCTION. Type 'yes' to confirm: ")
        return confirmation.lower() == 'yes'
    return True  # Auto-confirm in non-production
```

## Testing Connections

Before running the ETL process, always test connections separately:

```python
def test_source_connection():
    """Test read-only connection to source database"""
    try:
        conn = get_source_connection(readonly=True)
        with conn.connect() as c:
            result = c.execute(text("SELECT 1"))
            logger.info("Source connection successful (read-only)")
        return True
    except Exception as e:
        logger.error(f"Source connection failed: {str(e)}")
        return False

def test_target_connection():
    """Test write connection to target database"""
    try:
        conn = get_target_connection()
        with conn.connect() as c:
            result = c.execute(text("SELECT 1"))
            logger.info("Target connection successful")
        return True
    except Exception as e:
        logger.error(f"Target connection failed: {str(e)}")
        return False
```

## Best Practices Checklist

- [ ] Use dedicated read-only user for source database
- [ ] Clearly separate source and target connection configurations
- [ ] Implement connection factory pattern with role enforcement
- [ ] Validate queries to ensure read-only operations on source
- [ ] Use transactions for atomic write operations
- [ ] Include error handling with proper logging
- [ ] Test connections before starting ETL processes
- [ ] Add extra confirmation steps for production environments
- [ ] Never store credentials in code - always use environment variables

Remember: The operational database is mission-critical for the dental clinic. Any disruption could
affect patient care. Always prioritize safety and data integrity over convenience or performance.