# ETL Tracking Management and Refactoring Plan

## **Overview**

This document outlines the ETL tracking infrastructure, current problems, and comprehensive
 refactoring plan to integrate proper tracking into both `SimpleMySQLReplicator` and `PostgresLoader`.

## **Current Tracking Infrastructure**

### **1. Tracking Table Architecture**

The project uses a **three-tier tracking system** with separate tables for different ETL phases:

#### **Copy Phase Tracking (`etl_copy_status` - MySQL Replication Database)**
```sql
CREATE TABLE etl_copy_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL UNIQUE,
    last_copied TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
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
    rows_transformed INTEGER DEFAULT 0,
    transformation_status VARCHAR(50) DEFAULT 'pending',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

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


### **3. Pipeline Integration Points**

```
MySQL OpenDental (Source)
    ↓
SimpleMySQLReplicator (Copy Phase)
    ↓ [Creates etl_copy_status table]
    ↓ [Updates etl_copy_status.last_copied]
opendental_replication (MySQL Replication)
    ↓
PostgresLoader (Load Phase)
    ↓ [Creates raw.etl_load_status and raw.etl_transform_status tables]
    ↓ [Updates raw.etl_load_status.last_loaded]
opendental_analytics.raw (PostgreSQL Raw)
    ↓
dbt Models (Transform Phase)
    ↓ [Updates raw.etl_transform_status.last_transformed]
opendental_analytics.staging/intermediate/marts
```

## **Current Problems**

### **1. PostgresLoader Integration Issues**

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

### **2. SimpleMySQLReplicator Integration Issues**

#### **No Tracking Integration**
- No updates to `etl_copy_status` after successful copies
- No tracking record creation for new tables
- No incremental logic using tracking data

### **3. Database State Issues**

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

### **Phase 1: ETL Pipeline Table Creation**

#### **1.1 Add to SimpleMySQLReplicator**

**File:** `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py`

**Add this method:**

```python
def _ensure_tracking_tables_exist(self):
    """Ensure MySQL tracking tables exist in replication database."""
    try:
        with self.target_engine.connect() as conn:
            # Create etl_copy_status table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS etl_copy_status (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_copied TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
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
            
            conn.commit()
            logger.info("MySQL tracking tables created/verified successfully")
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
    """Ensure PostgreSQL tracking tables exist in analytics database."""
    try:
        with self.analytics_engine.connect() as conn:
            # Create etl_load_status table if it doesn't exist
            create_load_status_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.analytics_schema}.etl_load_status (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_loaded TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
                rows_loaded INTEGER DEFAULT 0,
                load_status VARCHAR(50) DEFAULT 'pending',
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            conn.execute(text(create_load_status_sql))
            
            # Create etl_transform_status table if it doesn't exist
            create_transform_status_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.analytics_schema}.etl_transform_status (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255) NOT NULL UNIQUE,
                last_transformed TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
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
            CREATE INDEX IF NOT EXISTS idx_etl_transform_status_table_name 
            ON {self.analytics_schema}.etl_transform_status(table_name);
            """))
            
            conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_etl_transform_status_last_transformed 
            ON {self.analytics_schema}.etl_transform_status(last_transformed);
            """))
            
            conn.commit()
            logger.info("PostgreSQL tracking tables created/verified successfully")
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

### **Phase 2: Add Tracking Record Management**

#### **2.1 Add to PostgresLoader**

**File:** `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

**Add these methods:**

```python
def _ensure_tracking_record_exists(self, table_name: str) -> bool:
    """Ensure a tracking record exists for the table."""
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
                        table_name, last_loaded, rows_loaded, load_status,
                        _loaded_at, _created_at, _updated_at
                    ) VALUES (
                        :table_name, '2024-01-01 00:00:00', 0, 'pending',
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """), {"table_name": table_name})
                conn.commit()
                logger.info(f"Created initial tracking record for {table_name}")
            
            return True
            
    except Exception as e:
        logger.error(f"Error ensuring tracking record for {table_name}: {str(e)}")
        return False

def _update_load_status(self, table_name: str, rows_loaded: int, 
                       load_status: str = 'success') -> bool:
    """Create or update load tracking record after successful PostgreSQL load."""
    try:
        with self.analytics_engine.connect() as conn:
            conn.execute(text(f"""
                INSERT INTO {self.analytics_schema}.etl_load_status (
                    table_name, last_loaded, rows_loaded, load_status,
                    _loaded_at, _created_at, _updated_at
                ) VALUES (
                    :table_name, CURRENT_TIMESTAMP, :rows_loaded, :load_status,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (table_name) DO UPDATE SET
                    last_loaded = CURRENT_TIMESTAMP,
                    rows_loaded = :rows_loaded,
                    load_status = :load_status,
                    _updated_at = CURRENT_TIMESTAMP
            """), {
                "table_name": table_name,
                "rows_loaded": rows_loaded,
                "load_status": load_status
            })
            conn.commit()
            logger.info(f"Updated load status for {table_name}: {rows_loaded} rows, {load_status}")
            return True
            
    except Exception as e:
        logger.error(f"Error updating load status for {table_name}: {str(e)}")
        return False
```

**Modify `load_table()` method:**

```python
def load_table(self, table_name: str, force_full: bool = False) -> bool:
    try:
        # Ensure tracking record exists before loading
        if not self._ensure_tracking_record_exists(table_name):
            logger.error(f"Failed to ensure tracking record for {table_name}")
            return False
        
        # ... existing loading logic ...
        
        # After successful load, update tracking record
        if rows_data:
            self._update_load_status(table_name, len(rows_data))
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading table {table_name}: {str(e)}")
        # Update tracking record with failure status
        self._update_load_status(table_name, 0, 'failed')
        return False
```

#### **2.2 Add to SimpleMySQLReplicator**

**File:** `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py`

**Add these methods:**

```python
def _update_copy_status(self, table_name: str, rows_copied: int, 
                       copy_status: str = 'success') -> bool:
    """Update copy tracking after successful MySQL replication."""
    try:
        # Note: This updates the replication database's tracking table
        # The PostgresLoader will later update the analytics database's tracking table
        with self.target_engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_copy_status (
                    table_name, last_copied, rows_copied, copy_status,
                    _created_at, _updated_at
                ) VALUES (
                    :table_name, CURRENT_TIMESTAMP, :rows_copied, :copy_status,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON DUPLICATE KEY UPDATE
                    last_copied = CURRENT_TIMESTAMP,
                    rows_copied = :rows_copied,
                    copy_status = :copy_status,
                    _updated_at = CURRENT_TIMESTAMP
            """), {
                "table_name": table_name,
                "rows_copied": rows_copied,
                "copy_status": copy_status
            })
            conn.commit()
            logger.info(f"Updated copy status for {table_name}: {rows_copied} rows, {copy_status}")
            return True
            
    except Exception as e:
        logger.error(f"Error updating copy status for {table_name}: {str(e)}")
        return False

def _get_last_copy_time(self, table_name: str) -> Optional[datetime]:
    """Get last copy time for incremental loading."""
    try:
        with self.target_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT last_copied
                FROM etl_copy_status
                WHERE table_name = :table_name
                AND copy_status = 'success'
                ORDER BY last_copied DESC
                LIMIT 1
            """), {"table_name": table_name}).scalar()
            
            return result
    except Exception as e:
        logger.error(f"Error getting last copy time for {table_name}: {str(e)}")
        return None
```

**Modify `copy_table()` method:**

```python
def copy_table(self, table_name: str, force_full: bool = False) -> bool:
    try:
        # ... existing copy logic ...
        
        if success:
            # Update copy tracking
            self._update_copy_status(table_name, rows_copied)
        
        return success
        
    except Exception as e:
        logger.error(f"Error copying table {table_name}: {str(e)}")
        # Update tracking with failure status
        self._update_copy_status(table_name, 0, 'failed')
        return False
```

### **Phase 3: Improve Incremental Logic**

#### **3.1 Fix PostgresLoader Incremental Logic**

**File:** `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

**Add these methods:**

```python
def _build_improved_load_query(self, table_name: str, incremental_columns: List[str], 
                              force_full: bool = False, use_or_logic: bool = True) -> str:
    """Build query with improved incremental logic."""
    
    # Data quality validation
    valid_columns = self._filter_valid_incremental_columns(table_name, incremental_columns)
    
    if force_full or not valid_columns:
        return f"SELECT * FROM {table_name}"
    
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

def _filter_valid_incremental_columns(self, table_name: str, columns: List[str]) -> List[str]:
    """Filter out columns with data quality issues."""
    try:
        with self.replication_engine.connect() as conn:
            valid_columns = []
            
            for col in columns:
                # Check for data quality issues
                result = conn.execute(text(f"""
                    SELECT MIN({col}), MAX({col}), COUNT(*)
                    FROM {table_name}
                    WHERE {col} IS NOT NULL
                """)).fetchone()
                
                if result:
                    min_val, max_val, count = result
                    
                    # Skip columns with obvious data quality issues
                    if min_val and max_val:
                        if isinstance(min_val, datetime) and isinstance(max_val, datetime):
                            # Date validation
                            if min_val.year < 1900 or max_val.year > 2100:
                                logger.warning(f"Skipping {col} due to date range issues: {min_val} to {max_val}")
                                continue
                    
                    valid_columns.append(col)
            
            return valid_columns
            
    except Exception as e:
        logger.error(f"Error filtering incremental columns for {table_name}: {str(e)}")
        return columns  # Return all columns if filtering fails
```

**Replace `_build_load_query()` method:**

```python
def _build_load_query(self, table_name: str, incremental_columns: List[str], force_full: bool = False) -> str:
    """Build query with improved incremental logic."""
    return self._build_improved_load_query(table_name, incremental_columns, force_full, use_or_logic=True)
```

#### **3.2 Add UPSERT Logic for Duplicate Key Handling**

**Add to PostgresLoader:**

```python
def _build_upsert_sql(self, table_name: str, columns: List[str], primary_keys: List[str]) -> str:
    """Build UPSERT SQL with dynamic primary key handling."""
    
    # Build column list
    column_list = ", ".join([f'"{col}"' for col in columns])
    
    # Build value placeholders
    value_placeholders = ", ".join([f":{col}" for col in columns])
    
    # Build conflict resolution
    conflict_columns = ", ".join([f'"{pk}"' for pk in primary_keys])
    update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in primary_keys])
    
    sql = f"""
        INSERT INTO {self.analytics_schema}.{table_name} ({column_list})
        VALUES ({value_placeholders})
        ON CONFLICT ({conflict_columns}) DO UPDATE SET
            {update_clause}
    """
    
    return sql

def _get_table_primary_keys(self, table_name: str) -> List[str]:
    """Get primary key columns for a table."""
    try:
        with self.analytics_engine.connect() as conn:
            # Get primary key information
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = :schema
                AND table_name = :table_name
                AND constraint_name = (
                    SELECT constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = :schema
                    AND table_name = :table_name
                    AND constraint_type = 'PRIMARY KEY'
                )
                ORDER BY ordinal_position
            """), {
                "schema": self.analytics_schema,
                "table_name": table_name
            })
            
            primary_keys = [row[0] for row in result.fetchall()]
            return primary_keys
            
    except Exception as e:
        logger.error(f"Error getting primary keys for {table_name}: {str(e)}")
        return []
```

### **Phase 4: Create Missing Tracking Records**

#### **4.1 One-Time SQL Fix**

**Execute this SQL to create missing tracking records:**

```sql
-- Create missing tracking records for all tables with incremental_columns
INSERT INTO raw.etl_load_status (
    table_name, last_loaded, rows_loaded, load_status,
    _loaded_at, _created_at, _updated_at
)
SELECT 
    table_name,
    '2024-01-01 00:00:00'::timestamp as last_loaded,
    0 as rows_loaded,
    'pending' as load_status,
    CURRENT_TIMESTAMP as _loaded_at,
    CURRENT_TIMESTAMP as _created_at,
    CURRENT_TIMESTAMP as _updated_at
FROM (
    -- Get all tables that have incremental_columns in tables.yml
    SELECT DISTINCT table_name
    FROM (
        VALUES 
            ('adjustment'),
            ('allergy'),
            ('appointment'),
            ('claim'),
            ('claimproc'),
            ('commlog'),
            ('document'),
            ('employee'),
            ('fee'),
            ('feesched'),
            ('insplan'),
            ('inssub'),
            ('labcase'),
            ('medication'),
            ('patient'),
            ('patientnote'),
            ('patplan'),
            ('payment'),
            ('payplan'),
            ('paysplit'),
            ('perioexam'),
            ('periomeasure'),
            ('pharmacy'),
            ('pharmclinic'),
            ('procedurecode'),
            ('procedurelog'),
            ('procgroupitem'),
            ('procmultivisit'),
            ('procnote'),
            ('proctp'),
            ('program'),
            ('programproperty'),
            ('provider'),
            ('recall'),
            ('recalltrigger'),
            ('recalltype'),
            ('refattach'),
            ('referral'),
            ('rxdef'),
            ('rxnorm'),
            ('rxpat'),
            ('schedule'),
            ('scheduleop'),
            ('securitylog'),
            ('securityloghash'),
            ('sheet'),
            ('sheetdef'),
            ('sheetfield'),
            ('sheetfielddef'),
            ('statement'),
            ('statementprod'),
            ('task'),
            ('taskhist'),
            ('tasklist'),
            ('tasknote'),
            ('tasksubscription'),
            ('taskunread'),
            ('timeadjust'),
            ('toothinitial'),
            ('treatplan'),
            ('treatplanattach'),
            ('treatplanparam'),
            ('usergroup'),
            ('usergroupattach'),
            ('userod'),
            ('userodapptview'),
            ('userodpref'),
            ('zipcode')
    ) AS t(table_name)
) AS source_tables
WHERE NOT EXISTS (
    SELECT 1 FROM raw.etl_load_status 
    WHERE etl_load_status.table_name = source_tables.table_name
);
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
        
    def test_update_load_status_upserts_correctly(self):
        """Test that _update_load_status creates/updates records correctly."""
        # Arrange
        loader = PostgresLoader()
        
        # Act
        result = loader._update_load_status('test_table', 1000)
        
        # Assert
        assert result is True
        
    def test_build_improved_load_query_uses_or_logic(self):
        """Test that improved load query uses OR logic by default."""
        # Arrange
        loader = PostgresLoader()
        incremental_columns = ['created_date', 'updated_date']
        
        # Act
        query = loader._build_improved_load_query('test_table', incremental_columns, force_full=False)
        
        # Assert
        assert ' OR ' in query
        assert 'created_date >' in query
        assert 'updated_date >' in query
```

#### **5.2 Integration Tests**

**File:** `etl_pipeline/tests/integration/loaders/test_postgres_loader_tracking_integration.py`

```python
import pytest
from datetime import datetime
from etl_pipeline.loaders.postgres_loader import PostgresLoader

class TestPostgresLoaderTrackingIntegration:
    
    @pytest.mark.integration
    def test_tracking_record_creation_and_update(self):
        """Test full tracking record lifecycle."""
        # Arrange
        loader = PostgresLoader()
        
        # Act - Create tracking record
        created = loader._ensure_tracking_record_exists('test_table')
        
        # Act - Update tracking record
        updated = loader._update_load_status('test_table', 1000)
        
        # Assert
        assert created is True
        assert updated is True
        
        # Verify record exists in database
        last_load = loader._get_last_load_time('test_table')
        assert last_load is not None
```

## **Implementation Timeline**

### **Week 1: Foundation**
- [ ] Add table creation methods to SimpleMySQLReplicator and PostgresLoader
- [ ] Remove legacy DDL approach and dbt macro
- [ ] Add tracking record management methods to PostgresLoader
- [ ] Add unit tests for tracking methods

### **Week 2: SimpleMySQLReplicator Integration**
- [ ] Add copy status tracking to SimpleMySQLReplicator
- [ ] Add last copy time retrieval
- [ ] Modify copy methods to update tracking
- [ ] Add unit tests for SimpleMySQLReplicator tracking

### **Week 3: Improved Incremental Logic**
- [ ] Implement improved incremental query building
- [ ] Add data quality validation
- [ ] Add UPSERT logic for duplicate key handling
- [ ] Add integration tests

### **Week 4: Database Fixes and Validation**
- [ ] Execute one-time SQL to create missing tracking records
- [ ] Validate tracking table state
- [ ] Test incremental loading with real data
- [ ] Performance testing and optimization

## **Success Metrics**

### **1. Tracking Coverage**
- **Target**: 100% of tables with `incremental_columns` have tracking records
- **Current**: 1 table (2% coverage)
- **Measurement**: SQL query to count missing records

### **2. Incremental Logic Accuracy**
- **Target**: 0% missing incremental updates
- **Current**: 34% missing updates due to AND logic
- **Measurement**: Compare row counts between full and incremental loads

### **3. Duplicate Key Resolution**
- **Target**: 0 duplicate key violations
- **Current**: Unknown (not measured)
- **Measurement**: Monitor error logs for duplicate key errors

### **4. Performance Impact**
- **Target**: < 5% performance degradation
- **Current**: Unknown
- **Measurement**: Compare load times before/after changes

## **Conclusion**

This refactoring plan addresses the fundamental issues with ETL tracking integration:

1. **ETL Pipeline Table Creation** - Both SimpleMySQLReplicator and PostgresLoader create their own tracking tables
2. **Adds proper tracking record management** to both SimpleMySQLReplicator (copy phase) and PostgresLoader (load phase)
3. **Improves incremental logic** with OR logic and data quality validation
4. **Implements UPSERT logic** to handle duplicate key violations
5. **Creates missing tracking records** for all tables with incremental columns

The phased approach ensures minimal risk while delivering maximum benefit, with proper testing at each stage to validate the changes. 