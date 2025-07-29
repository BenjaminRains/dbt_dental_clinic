# ETL Tracking Management and Refactoring Plan

## **Overview**

This document outlines the ETL tracking infrastructure, current problems, and comprehensive
 refactoring plan to integrate proper tracking into both `SimpleMySQLReplicator` and `PostgresLoader`,
 with enhanced support for **primary incremental column** functionality.

## **Primary Incremental Column Integration**

### **1. Primary Incremental Column Overview**

The **primary incremental column** is the **bridge** between tracking tables and incremental logic:

- **Purpose**: Determines the most reliable column for incremental processing
- **Configuration**: Set in `tables.yml` via `primary_incremental_column` field
- **Fallback**: When `primary_incremental_column` is `'none'` or `null`, uses multi-column logic
- **Integration**: Works with tracking tables to store the last processed value

### **2. Primary Column Value Tracking**

**Enhanced tracking tables** now support storing both timestamps and primary column values:

#### **Copy Phase Tracking (`etl_copy_status` - MySQL Replication Database)**
```sql
CREATE TABLE etl_copy_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL UNIQUE,
    last_copied TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
    last_primary_value VARCHAR(255) NULL,  -- ← NEW: Stores actual primary column value
    primary_column_name VARCHAR(255) NULL, -- ← NEW: Tracks which column was used
    rows_copied INT DEFAULT 0,
    copy_status VARCHAR(50) DEFAULT 'pending',
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

#### **Load Phase Tracking (`raw.etl_load_status` - PostgreSQL Analytics Database)**
```sql
CREATE TABLE raw.etl_load_status (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL UNIQUE,
    last_loaded TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
    last_primary_value VARCHAR(255) NULL,  -- ← NEW: Stores actual primary column value
    primary_column_name VARCHAR(255) NULL, -- ← NEW: Tracks which column was used
    rows_loaded INTEGER DEFAULT 0,
    load_status VARCHAR(50) DEFAULT 'pending',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **Transform Phase Tracking (`raw.etl_transform_status` - PostgreSQL Analytics Database)**
```sql
CREATE TABLE raw.etl_transform_status (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL UNIQUE,
    last_transformed TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
    last_primary_value VARCHAR(255) NULL,  -- ← NEW: Stores actual primary column value
    primary_column_name VARCHAR(255) NULL, -- ← NEW: Tracks which column was used
    rows_transformed INTEGER DEFAULT 0,
    transformation_status VARCHAR(50) DEFAULT 'pending',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### **3. Primary Column Integration Points**

```
MySQL OpenDental (Source)
    ↓
SimpleMySQLReplicator (Copy Phase)
    ↓ [Uses primary_incremental_column for copy logic]
    ↓ [Updates etl_copy_status.last_primary_value]
opendental_replication (MySQL Replication)
    ↓
PostgresLoader (Load Phase)
    ↓ [Uses primary_incremental_column for load logic]
    ↓ [Updates raw.etl_load_status.last_primary_value]
opendental_analytics.raw (PostgreSQL Raw)
    ↓
dbt Models (Transform Phase)
    ↓ [Uses primary_incremental_column for transform logic]
    ↓ [Updates raw.etl_transform_status.last_primary_value]
opendental_analytics.staging/intermediate/marts
```

## **Current Tracking Infrastructure**

### **1. Tracking Table Architecture**

The project uses a **three-tier tracking system** with separate tables for different ETL phases:

### **2. Infrastructure Components**

#### **ETL Pipeline Infrastructure (Primary Approach)**
- **SimpleMySQLReplicator** - Creates and manages `etl_copy_status` in MySQL replication database
- **PostgresLoader** - Creates and manages `raw.etl_load_status` and `raw.etl_transform_status` in PostgreSQL analytics database
- **dbt Models** - References existing tracking tables for transform phase updates

#### **dbt Infrastructure (Reference Only)**
- **`models/staging/raw/etl_tracking.yml`** - dbt model definitions and tests
- **`models/staging/raw/etl_load_status.sql`** - dbt model for load status
- **`models/staging/raw/etl_transform_status.sql`** - dbt model for transform status
- **`models/staging/raw/_sources.yml`** - dbt source definitions

## **Current Problems**

### **1. Primary Incremental Column Integration Issues**

#### **Missing Primary Column Value Tracking**
```python
# CURRENT PROBLEM - Only timestamps are tracked
def _get_last_load_time(self, table_name: str) -> Optional[datetime]:
    # Only returns timestamp, not the actual primary column value
    result = conn.execute(text(f"""
        SELECT last_loaded  # ← MISSING: last_primary_value
        FROM {self.analytics_schema}.etl_load_status
        WHERE table_name = :table_name
    """)).scalar()
```

#### **Incomplete Fallback Logic**
```python
# CURRENT PROBLEM - No proper fallback when primary_incremental_column is 'none'
def _get_primary_incremental_column(self, config: Dict) -> Optional[str]:
    primary_column = config.get('primary_incremental_column')
    if primary_column and primary_column != 'none':
        return primary_column
    # ← MISSING: Proper fallback to multi-column logic
    return None
```

#### **Tracking Table Schema Gaps**
- Current tracking tables only store `last_copied`/`last_loaded` timestamps
- **Missing**: Storage of the actual primary column value that was last processed
- **Problem**: If primary column is not a timestamp, tracking becomes unreliable

### **2. PostgresLoader Integration Issues**

#### **Wrong Column Names**
```python
# CURRENT (WRONG) - PostgresLoader uses incorrect column names
def _get_last_load(self, table_name: str) -> Optional[datetime]:
    result = conn.execute(text(f"""
        SELECT MAX(last_loaded)  # ← WRONG: should be last_loaded (this is correct now)
        FROM {self.analytics_schema}.etl_load_status
        WHERE table_name = :table_name
        AND load_status = 'success'  # ← WRONG: should be load_status (this is correct now)
    """), {"table_name": table_name}).scalar()
```

#### **Missing Tracking Record Management**
- No creation of initial tracking records
- No updates after successful loads
- No UPSERT logic for duplicate handling

#### **Flawed Incremental Logic**
- Uses AND logic instead of OR logic
- No data quality validation
- Missing 34% of incremental updates

### **3. SimpleMySQLReplicator Integration Issues**

#### **No Tracking Integration**
- No updates to `etl_copy_status` after successful copies
- No tracking record creation for new tables
- No incremental logic using tracking data

### **4. Database State Issues**

#### **Missing Tracking Records**
```sql
-- Current state: Only 1 table has tracking record
SELECT table_name, last_loaded, rows_loaded, load_status 
FROM raw.etl_load_status;

-- Result: Only 'securitylog' has a record
-- Need: Records for all tables with incremental_columns
```

#### **Schema Inconsistencies**
- Column naming confusion (`last_extracted` vs `last_loaded` vs `last_copied`)
- Status field naming confusion (`extraction_status` vs `load_status` vs `copy_status`)
- **CRITICAL**: Both components updating same table with different meanings

## **Refactoring Plan**

### **Phase 1: Enhanced Tracking Table Creation**

#### **1.1 Add to SimpleMySQLReplicator**

**File:** `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py`

**Add this method:**

```python
def _ensure_tracking_tables_exist(self):
    """Ensure MySQL tracking tables exist in replication database with primary column support."""
    try:
        with self.target_engine.connect() as conn:
            # Create enhanced etl_copy_status table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS etl_copy_status (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_copied TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                last_primary_value VARCHAR(255) NULL,
                primary_column_name VARCHAR(255) NULL,
                rows_copied INT DEFAULT 0,
                copy_status VARCHAR(50) DEFAULT 'pending',
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_table_sql))
            
            # Create indexes
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_etl_copy_status_table_name 
            ON etl_copy_status(table_name);
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_etl_copy_status_last_copied 
            ON etl_copy_status(last_copied);
            """))
            
            conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_etl_copy_status_primary_value 
            ON etl_copy_status(last_primary_value);
            """))
            
            conn.commit()
            logger.info("MySQL tracking tables created/verified successfully with primary column support")
            return True
            
    except Exception as e:
        logger.error(f"Error creating MySQL tracking tables: {str(e)}")
        return False
```

**Modify `__init__()` method:**

```python
def __init__(self, settings: Optional[Settings] = None, tables_config_path: Optional[str] = None):
    # ... existing initialization ...
    
    # Ensure tracking tables exist
    self._ensure_tracking_tables_exist()
```

#### **1.2 Add to PostgresLoader**

**File:** `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

**Add this method:**

```python
def _ensure_tracking_tables_exist(self):
    """Ensure PostgreSQL tracking tables exist in analytics database with primary column support."""
    try:
        with self.analytics_engine.connect() as conn:
            # Create enhanced etl_load_status table if it doesn't exist
            create_load_status_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.analytics_schema}.etl_load_status (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_loaded TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                last_primary_value VARCHAR(255) NULL,
                primary_column_name VARCHAR(255) NULL,
                rows_loaded INTEGER DEFAULT 0,
                load_status VARCHAR(50) DEFAULT 'pending',
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_load_status_sql))
            
            # Create enhanced etl_transform_status table if it doesn't exist
            create_transform_status_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.analytics_schema}.etl_transform_status (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_transformed TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                last_primary_value VARCHAR(255) NULL,
                primary_column_name VARCHAR(255) NULL,
                rows_transformed INTEGER DEFAULT 0,
                transformation_status VARCHAR(50) DEFAULT 'pending',
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_transform_status_sql))
            
            # Create indexes
            conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_etl_load_status_table_name 
            ON {self.analytics_schema}.etl_load_status(table_name);
            """))
            
            conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_etl_load_status_last_loaded 
            ON {self.analytics_schema}.etl_load_status(last_loaded);
            """))
            
            conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_etl_load_status_primary_value 
            ON {self.analytics_schema}.etl_load_status(last_primary_value);
            """))
            
            conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_etl_transform_status_table_name 
            ON {self.analytics_schema}.etl_transform_status(table_name);
            """))
            
            conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_etl_transform_status_last_transformed 
            ON {self.analytics_schema}.etl_transform_status(last_transformed);
            """))
            
            conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_etl_transform_status_primary_value 
            ON {self.analytics_schema}.etl_transform_status(last_primary_value);
            """))
            
            conn.commit()
            logger.info("PostgreSQL tracking tables created/verified successfully with primary column support")
            return True
            
    except Exception as e:
        logger.error(f"Error creating PostgreSQL tracking tables: {str(e)}")
        return False
```

**Modify `__init__()` method:**

```python
def __init__(self, tables_config_path: Optional[str] = None, use_test_environment: bool = False, settings: Optional[Settings] = None):
    # ... existing initialization ...
    
    # Ensure tracking tables exist
    self._ensure_tracking_tables_exist()
```

### **Phase 2: Primary Column Value Tracking**

#### **2.1 Add to PostgresLoader**

**File:** `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

**Add these methods:**

```python
def _ensure_tracking_record_exists(self, table_name: str) -> bool:
    """Ensure a tracking record exists for the table with primary column support."""
    try:
        with self.analytics_engine.connect() as conn:
            # Check if record exists
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {self.analytics_schema}.etl_load_status 
                WHERE table_name = :table_name
            """), {"table_name": table_name}).scalar()
            
            if result == 0:
                # Create initial tracking record
                conn.execute(text(f"""
                    INSERT INTO {self.analytics_schema}.etl_load_status (
                        table_name, last_loaded, last_primary_value, primary_column_name,
                        rows_loaded, load_status, _loaded_at, _created_at, _updated_at
                    ) VALUES (
                        :table_name, '2024-01-01 00:00:00', NULL, NULL,
                        0, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """), {"table_name": table_name})
                conn.commit()
                logger.info(f"Created initial tracking record for {table_name}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error ensuring tracking record for {table_name}: {str(e)}")
        return False

def _update_load_status_with_primary_value(self, table_name: str, rows_loaded: int, 
                                         primary_column: Optional[str] = None,
                                         last_primary_value: Optional[str] = None,
                                         load_status: str = 'success') -> bool:
    """Create or update load tracking record with primary column value."""
    try:
        with self.analytics_engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {self.analytics_schema}.etl_load_status (
                    table_name, last_loaded, last_primary_value, primary_column_name,
                    rows_loaded, load_status, _loaded_at, _created_at, _updated_at
                ) VALUES (
                    :table_name, CURRENT_TIMESTAMP, :last_primary_value, :primary_column_name,
                    :rows_loaded, :load_status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (table_name) DO UPDATE SET
                    last_loaded = CURRENT_TIMESTAMP,
                    last_primary_value = :last_primary_value,
                    primary_column_name = :primary_column_name,
                    rows_loaded = :rows_loaded,
                    load_status = :load_status,
                    _updated_at = CURRENT_TIMESTAMP
            """), {
                "table_name": table_name,
                "last_primary_value": last_primary_value,
                "primary_column_name": primary_column,
                "rows_loaded": rows_loaded,
                "load_status": load_status
            })
            conn.commit()
            logger.info(f"Updated load status for {table_name}: {rows_loaded} rows, {load_status}, primary_value={last_primary_value}")
            return True
            
    except Exception as e:
        logger.error(f"Error updating load status for {table_name}: {str(e)}")
        return False

def _get_last_primary_value(self, table_name: str) -> Optional[str]:
    """Get the last primary column value for incremental loading."""
    try:
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT last_primary_value, primary_column_name
                FROM {self.analytics_schema}.etl_load_status
                WHERE table_name = :table_name
                AND load_status = 'success'
                ORDER BY last_loaded DESC
                LIMIT 1
            """), {"table_name": table_name}).fetchone()
            
            if result:
                last_primary_value, primary_column_name = result
                logger.debug(f"Retrieved last primary value for {table_name}: {last_primary_value} (column: {primary_column_name})")
                return last_primary_value
            return None
            
    except Exception as e:
        logger.error(f"Error getting last primary value for {table_name}: {str(e)}")
        return None
```

**Modify `load_table()` method:**

```python
def load_table(self, table_name: str, force_full: bool = False) -> bool:
    try:
        # Ensure tracking record exists before loading
        if not self._ensure_tracking_record_exists(table_name):
            logger.error(f"Failed to ensure tracking record for {table_name}")
            return False
        
        # Get table configuration
        table_config = self.get_table_config(table_name)
        if not table_config:
            logger.error(f"No configuration found for table: {table_name}")
            return False
        
        # Get primary incremental column
        primary_column = self._get_primary_incremental_column(table_config)
        incremental_columns = table_config.get('incremental_columns', [])
        
        # ... existing loading logic ...
        
        # After successful load, update tracking record with primary column value
        if rows_data:
            # Get the maximum value of the primary column from loaded data
            last_primary_value = None
            if primary_column and primary_column != 'none':
                # Extract the maximum value of the primary column from loaded data
                primary_values = [row.get(primary_column) for row in rows_data if row.get(primary_column) is not None]
                if primary_values:
                    last_primary_value = str(max(primary_values))
            
            self._update_load_status_with_primary_value(
                table_name, len(rows_data), primary_column, last_primary_value
            )
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading table {table_name}: {str(e)}")
        # Update tracking record with failure status
        self._update_load_status_with_primary_value(table_name, 0, None, None, 'failed')
        return False
```

#### **2.2 Add to SimpleMySQLReplicator**

**File:** `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py`

**Add these methods:**

```python
def _update_copy_status_with_primary_value(self, table_name: str, rows_copied: int, 
                                         primary_column: Optional[str] = None,
                                         last_primary_value: Optional[str] = None,
                                         copy_status: str = 'success') -> bool:
    """Update copy tracking with primary column value."""
    try:
        with self.target_engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_copy_status (
                    table_name, last_copied, last_primary_value, primary_column_name,
                    rows_copied, copy_status, _created_at, _updated_at
                ) VALUES (
                    :table_name, CURRENT_TIMESTAMP, :last_primary_value, :primary_column_name,
                    :rows_copied, :copy_status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON DUPLICATE KEY UPDATE
                    last_copied = CURRENT_TIMESTAMP,
                    last_primary_value = :last_primary_value,
                    primary_column_name = :primary_column_name,
                    rows_copied = :rows_copied,
                    copy_status = :copy_status,
                    _updated_at = CURRENT_TIMESTAMP
            """), {
                "table_name": table_name,
                "last_primary_value": last_primary_value,
                "primary_column_name": primary_column,
                "rows_copied": rows_copied,
                "copy_status": copy_status
            })
            conn.commit()
            logger.info(f"Updated copy status for {table_name}: {rows_copied} rows, {copy_status}, primary_value={last_primary_value}")
            return True
            
    except Exception as e:
        logger.error(f"Error updating copy status for {table_name}: {str(e)}")
        return False

def _get_last_copy_primary_value(self, table_name: str) -> Optional[str]:
    """Get last copy primary column value for incremental loading."""
    try:
        with self.target_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT last_primary_value, primary_column_name
                FROM etl_copy_status
                WHERE table_name = :table_name
                AND copy_status = 'success'
                ORDER BY last_copied DESC
                LIMIT 1
            """), {"table_name": table_name}).fetchone()
            
            if result:
                last_primary_value, primary_column_name = result
                logger.debug(f"Retrieved last copy primary value for {table_name}: {last_primary_value} (column: {primary_column_name})")
                return last_primary_value
            return None
            
    except Exception as e:
        logger.error(f"Error getting last copy primary value for {table_name}: {str(e)}")
        return None
```

**Modify `copy_table()` method:**

```python
def copy_table(self, table_name: str, force_full: bool = False) -> bool:
    try:
        # Get table configuration
        table_config = self.table_configs.get(table_name, {})
        primary_column = self._get_primary_incremental_column(table_config)
        
        # ... existing copy logic ...
        
        if success:
            # Get the maximum value of the primary column from copied data
            last_primary_value = None
            if primary_column and primary_column != 'none':
                # This would need to be implemented based on how data is copied
                # For now, we'll set it to None and update it in the tracking
                pass
            
            # Update copy tracking with primary column value
            self._update_copy_status_with_primary_value(
                table_name, rows_copied, primary_column, last_primary_value
            )
        
        return success
        
    except Exception as e:
        logger.error(f"Error copying table {table_name}: {str(e)}")
        # Update tracking with failure status
        self._update_copy_status_with_primary_value(table_name, 0, None, None, 'failed')
        return False
```

### **Phase 3: Enhanced Incremental Logic with Primary Column Support**

#### **3.1 Fix PostgresLoader Incremental Logic**

**File:** `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

**Add these methods:**

```python
def _build_enhanced_load_query(self, table_name: str, incremental_columns: List[str], 
                              primary_column: Optional[str] = None,
                              force_full: bool = False, use_or_logic: bool = True) -> str:
    """Build query with enhanced incremental logic supporting primary column."""
    
    # Data quality validation
    valid_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
    
    if force_full or not valid_columns:
        return f"SELECT * FROM {table_name}"
    
    # Use primary column logic if available
    if primary_column and primary_column != 'none':
        last_primary_value = self._get_last_primary_value(table_name)
        if last_primary_value:
            # Use primary column for incremental logic
            return f"SELECT * FROM {table_name} WHERE {primary_column} > '{last_primary_value}'"
        else:
            # No primary value found, use timestamp fallback
            last_load = self._get_last_load_time(table_name)
            if last_load:
                return f"SELECT * FROM {table_name} WHERE {primary_column} > '{last_load}'"
            else:
                return f"SELECT * FROM {table_name}"
    else:
        # Use multi-column logic with OR/AND
        last_load = self._get_last_load_time(table_name)
        if not last_load:
            return f"SELECT * FROM {table_name}"
        
        # Build conditions with OR logic (configurable)
        conditions = []
        for col in valid_columns:
            conditions.append(f"{col} > '{last_load}'")
        
        if use_or_logic:
            where_clause = " OR ".join(conditions)
        else:
            where_clause = " AND ".join(conditions)
        
        return f"SELECT * FROM {table_name} WHERE {where_clause}"

def _get_primary_incremental_column(self, config: Dict) -> Optional[str]:
    """Get the primary incremental column from configuration with fallback logic."""
    primary_column = config.get('primary_incremental_column')
    
    # Check if primary column is valid (not null, 'none', or empty)
    if primary_column and primary_column != 'none' and primary_column.strip():
        return primary_column
    
    # Fallback: if no primary column specified, return None to use multi-column logic
    return None

def _log_incremental_strategy(self, table_name: str, primary_column: Optional[str], incremental_columns: List[str]):
    """Log which incremental strategy is being used."""
    if primary_column and primary_column != 'none':
        logger.info(f"Table {table_name}: Using primary incremental column '{primary_column}' for optimized incremental loading")
    else:
        logger.info(f"Table {table_name}: Using multi-column incremental logic with columns: {incremental_columns}")
```

**Replace `_build_load_query()` method:**

```python
def _build_load_query(self, table_name: str, incremental_columns: List[str], force_full: bool = False) -> str:
    """Build query with enhanced incremental logic supporting primary column."""
    # Get primary column from configuration
    table_config = self.get_table_config(table_name)
    primary_column = self._get_primary_incremental_column(table_config) if table_config else None
    
    # Log the strategy being used
    self._log_incremental_strategy(table_name, primary_column, incremental_columns)
    
    return self._build_enhanced_load_query(table_name, incremental_columns, primary_column, force_full, use_or_logic=True)
```

### **Phase 4: Create Missing Tracking Records with Primary Column Support**

#### **4.1 One-Time SQL Fix**

**Execute this SQL to create missing tracking records with primary column support:**

```sql
-- Create missing tracking records for all tables with incremental_columns
-- Note: This SQL should be generated dynamically from tables.yml configuration
-- Use the Python script create_missing_tracking_records.py instead of hardcoded values

-- Example of dynamic SQL generation approach:
-- INSERT INTO raw.etl_load_status (
--     table_name, last_loaded, last_primary_value, primary_column_name,
--     rows_loaded, load_status, _loaded_at, _created_at, _updated_at
-- )
-- SELECT 
--     table_name,
--     '2024-01-01 00:00:00'::timestamp as last_loaded,
--     NULL as last_primary_value,
--     NULL as primary_column_name,
--     0 as rows_loaded,
--     'pending' as load_status,
--     CURRENT_TIMESTAMP as _loaded_at,
--     CURRENT_TIMESTAMP as _created_at,
--     CURRENT_TIMESTAMP as _updated_at
-- FROM (
--     -- This would be generated from tables.yml configuration
--     SELECT DISTINCT table_name FROM (
--         -- Generated dynamically from tables.yml
--         -- Only tables with incremental_columns defined
--     ) AS t(table_name)
-- ) AS source_tables
-- WHERE NOT EXISTS (
--     SELECT 1 FROM raw.etl_load_status 
--     WHERE etl_load_status.table_name = source_tables.table_name
-- );
```

### **Phase 5: Testing and Validation**

#### **5.1 Unit Tests**

**File:** `etl_pipeline/tests/unit/loaders/test_postgres_loader_tracking.py`

```python
import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from etl_pipeline.loaders.postgres_loader import PostgresLoader

class TestPostgresLoaderTracking:
    
    def test_ensure_tracking_record_exists_creates_new_record(self):
        """Test that _ensure_tracking_record_exists creates new record when none exists."""
        # Arrange
        loader = PostgresLoader()
        
        # Act
        result = loader._ensure_tracking_record_exists('test_table')
        
        # Assert
        assert result is True
        
    def test_update_load_status_with_primary_value_upserts_correctly(self):
        """Test that _update_load_status_with_primary_value creates/updates records correctly."""
        # Arrange
        loader = PostgresLoader()
        
        # Act
        result = loader._update_load_status_with_primary_value(
            'test_table', 1000, 'DateTStamp', '2024-01-01 00:00:00'
        )
        
        # Assert
        assert result is True
        
    def test_get_primary_incremental_column_with_valid_column(self):
        """Test that _get_primary_incremental_column returns valid column."""
        # Arrange
        loader = PostgresLoader()
        config = {'primary_incremental_column': 'DateTStamp'}
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result == 'DateTStamp'
        
    def test_get_primary_incremental_column_with_none_value(self):
        """Test that _get_primary_incremental_column handles 'none' value."""
        # Arrange
        loader = PostgresLoader()
        config = {'primary_incremental_column': 'none'}
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result is None
        
    def test_build_enhanced_load_query_uses_primary_column(self):
        """Test that enhanced load query uses primary column when available."""
        # Arrange
        loader = PostgresLoader()
        incremental_columns = ['created_date', 'updated_date']
        primary_column = 'DateTStamp'
        
        # Act
        query = loader._build_enhanced_load_query('test_table', incremental_columns, primary_column, force_full=False)
        
        # Assert
        assert 'DateTStamp >' in query
        assert ' OR ' not in query  # Should not use OR logic with primary column
```

#### **5.2 Integration Tests**

**File:** `etl_pipeline/tests/integration/loaders/test_postgres_loader_tracking_integration.py`

```python
import pytest
from datetime import datetime
from etl_pipeline.loaders.postgres_loader import PostgresLoader

class TestPostgresLoaderTrackingIntegration:
    
    @pytest.mark.integration
    def test_primary_column_value_tracking(self):
        """Test full primary column value tracking lifecycle."""
        # Arrange
        loader = PostgresLoader()
        
        # Act - Create tracking record
        created = loader._ensure_tracking_record_exists('test_table')
        
        # Act - Update tracking record with primary column value
        updated = loader._update_load_status_with_primary_value(
            'test_table', 1000, 'DateTStamp', '2024-01-01 00:00:00'
        )
        
        # Act - Retrieve primary column value
        last_primary_value = loader._get_last_primary_value('test_table')
        
        # Assert
        assert created is True
        assert updated is True
        assert last_primary_value == '2024-01-01 00:00:00'
        
    @pytest.mark.integration
    def test_enhanced_incremental_logic_with_primary_column(self):
        """Test enhanced incremental logic with primary column support."""
        # Arrange
        loader = PostgresLoader()
        
        # Act - Build query with primary column
        query = loader._build_enhanced_load_query(
            'test_table', ['created_date', 'updated_date'], 'DateTStamp', force_full=False
        )
        
        # Assert
        assert 'DateTStamp >' in query
        assert 'test_table' in query
```

## **Implementation Timeline**

### **Week 1: Enhanced Foundation**
- [ ] Add enhanced table creation methods to SimpleMySQLReplicator and PostgresLoader
- [ ] Add primary column value tracking methods
- [ ] Add unit tests for primary column functionality
- [ ] Remove legacy DDL approach and dbt macro

### **Week 2: SimpleMySQLReplicator Primary Column Integration**
- [ ] Add copy status tracking with primary column values to SimpleMySQLReplicator
- [ ] Add last copy primary value retrieval
- [ ] Modify copy methods to update tracking with primary column values
- [ ] Add unit tests for SimpleMySQLReplicator primary column tracking

### **Week 3: Enhanced Incremental Logic**
- [ ] Implement enhanced incremental query building with primary column support
- [ ] Add data quality validation for primary columns
- [ ] Add UPSERT logic for duplicate key handling
- [ ] Add integration tests for primary column incremental logic

### **Week 4: Database Fixes and Validation**
- [ ] Execute one-time SQL to create missing tracking records with primary column support
- [ ] Validate tracking table state with primary column values
- [ ] Test incremental loading with real data using primary columns
- [ ] Performance testing and optimization

## **Success Metrics**

### **1. Primary Column Tracking Coverage**
- **Target**: 100% of tables with `primary_incremental_column` have primary column value tracking
- **Current**: 0% (not implemented)
- **Measurement**: SQL query to count tables with primary column values in tracking

### **2. Incremental Logic Accuracy with Primary Columns**
- **Target**: 0% missing incremental updates when using primary columns
- **Current**: Unknown (not measured)
- **Measurement**: Compare row counts between full and incremental loads using primary columns

### **3. Fallback Logic Accuracy**
- **Target**: 100% successful fallback when `primary_incremental_column` is `'none'`
- **Current**: Unknown (not measured)
- **Measurement**: Test incremental loading for tables with `primary_incremental_column: 'none'`

### **4. Performance Impact**
- **Target**: < 5% performance degradation
- **Current**: Unknown
- **Measurement**: Compare load times before/after primary column changes

## **Conclusion**

This enhanced refactoring plan addresses the fundamental issues with ETL tracking integration and **primary incremental column** functionality:

1. **Enhanced ETL Pipeline Table Creation** - Both SimpleMySQLReplicator and PostgresLoader create tracking tables with primary column value support
2. **Adds proper primary column value tracking** to both SimpleMySQLReplicator (copy phase) and PostgresLoader (load phase)
3. **Implements enhanced incremental logic** with primary column support and fallback to multi-column logic
4. **Implements UPSERT logic** to handle duplicate key violations
5. **Creates missing tracking records** for all tables with incremental columns and primary column support

The phased approach ensures minimal risk while delivering maximum benefit, with proper testing at each stage to validate the changes and ensure primary incremental column functionality works correctly with tracking tables. 