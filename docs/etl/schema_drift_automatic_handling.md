# Schema Drift Automatic Handling - Refactor Plan

## Executive Summary

**Problem**: When source database schemas change, the ETL pipeline attempts to recreate PostgreSQL
 analytics tables but fails to do so atomically, leaving tables in inconsistent states
  (wrong schema, missing data, corrupted tracking metadata).

**Solution**: Implement a tiered schema evolution system that automatically handles compatible
 changes (ALTER TABLE), gracefully handles breaking changes (DROP CASCADE + full reload), and
  ensures tracking metadata stays synchronized with table state.

**Impact**: Zero manual intervention for 80% of schema changes, automatic recovery for 20% of
 breaking changes, elimination of data inconsistency issues.

---

## Problem Statement

### Current Behavior (Broken)

When schema changes are detected:

1. **Detection**: `postgres_loader.py` compares PostgreSQL column count to MySQL column count
2. **Attempted Fix**: Logs warning "recreating table with correct schema"
3. **Actual Result**: 
   - `DROP TABLE` fails without CASCADE (foreign keys exist)
   - Or succeeds but tracking metadata not updated
   - Table ends up empty or with partial data
   - `last_primary_value` still references old high-water mark
   - Incremental queries return 0 rows (looking beyond actual data)
   - System requires manual intervention

### Real-World Examples

**Example 1: `sheetfield`**
- Column `UserSigned` removed from source database
- PostgreSQL table had 28 columns, MySQL has 29
- Recreation attempted, failed
- Error: `(1054, "Unknown column 'UserSigned' in 'field list'")`
- Required manual `DROP TABLE` to fix

**Example 2: `histappointment`**
- Schema changed after analyzer ran
- Recreation left table with 3,711 rows instead of 622,126
- Tracking shows `last_primary_value = 622126` (wrong)
- Incremental query looks for rows > 622,126 (finds 0)
- System correctly falls back to full load (recovery worked, but inefficient)

### Impact

- **Data Inconsistency**: Tables have wrong schema, missing rows
- **Failed Loads**: ETL pipeline fails on schema mismatches
- **Manual Intervention**: Requires DBA to manually drop tables
- **Lost Time**: Full reloads required for large tables (hours)
- **Production Risk**: Schema changes break production ETL runs

---

## Solution: Tiered Schema Evolution System

### Architecture Overview

```
Schema Change Detected
    ↓
Classify Change Type
    ↓
┌──────────────────────────────────────┐
│ Tier 1: Compatible Changes          │
│ (Automatic, Zero Downtime)           │
│                                      │
│ • Add new columns (ALTER TABLE)      │
│ • Widen column types (INT→BIGINT)    │
│ • Add indexes                        │
│                                      │
│ Action: ALTER TABLE, continue load   │
└──────────────────────────────────────┘
    ↓
┌──────────────────────────────────────┐
│ Tier 2: Breaking Changes             │
│ (Automatic with Notification)        │
│                                      │
│ • Remove columns                     │
│ • Narrow column types                │
│ • Rename columns                     │
│ • Change primary keys                │
│                                      │
│ Action: DROP CASCADE + full reload   │
│ Notification: Alert sent             │
└──────────────────────────────────────┘
    ↓
┌──────────────────────────────────────┐
│ Critical: Reset Tracking Metadata    │
│                                      │
│ • Clear last_primary_value → NULL   │
│ • Clear _loaded_at → NULL            │
│ • Set load_status → 'pending'       │
│                                      │
│ Result: Next run does full load      │
└──────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Schema Comparison Engine

**New Component**: `SchemaComparator` class in `etl_pipeline/core/schema_comparator.py`

**Responsibilities**:
1. Compare PostgreSQL table schema to MySQL source schema
2. Identify specific column differences:
   - Added columns
   - Removed columns
   - Type changes
   - Nullability changes
   - Primary key changes
3. Classify changes as compatible, breaking, or catastrophic
4. Generate appropriate migration SQL

**Key Methods**:
```python
class SchemaComparator:
    def compare_schemas(self, pg_schema: Dict, mysql_schema: Dict) -> SchemaChanges
    def classify_change(self, change: ColumnChange) -> ChangeType
    def generate_alter_statements(self, changes: List[ColumnChange]) -> List[str]
    def is_compatible_change(self, change: ColumnChange) -> bool
```

**Output**:
```python
SchemaChanges(
    added_columns=['UserNum', 'IsActive'],
    removed_columns=['UserSigned'],
    type_changes=[('PatNum', 'int', 'bigint')],
    classification='BREAKING',  # or 'COMPATIBLE', 'CATASTROPHIC'
    migration_sql=['ALTER TABLE...']
)
```

---

### Phase 2: Automated Migration Logic

**Enhanced Component**: `PostgresSchema` class in `etl_pipeline/core/postgres_schema.py`

#### Tier 1: Compatible Changes (Automatic)

**When**: Schema changes don't break existing queries or data

**Actions**:
1. Generate ALTER TABLE statements:
   ```sql
   ALTER TABLE raw.sheetfield 
   ADD COLUMN UserNum BIGINT DEFAULT NULL;
   ```
2. Execute in transaction
3. Log change to schema changelog
4. Continue with incremental load (no full reload needed)

**Compatible Changes**:
- Adding nullable columns
- Adding columns with defaults
- Widening numeric types (INT → BIGINT)
- Widening string types (VARCHAR(50) → VARCHAR(100))
- Adding indexes (non-unique)
- Making columns more permissive (NOT NULL → NULL)

**Implementation**:
```python
def handle_compatible_changes(self, table_name: str, changes: SchemaChanges):
    """Apply compatible schema changes without data reload."""
    with self.analytics_engine.begin() as trans:
        for sql in changes.migration_sql:
            trans.execute(text(sql))
            logger.info(f"Applied schema change: {sql}")
        
        # Log to changelog
        self._log_schema_change(table_name, changes, 'COMPATIBLE_AUTO')
        
    # Continue with incremental load (no tracking reset needed)
    return True
```

#### Tier 2: Breaking Changes (Automatic + Notification)

**When**: Schema changes break existing queries or lose data

**Actions**:
1. Send notification (Slack, email, log)
2. Execute DROP CASCADE:
   ```sql
   DROP TABLE IF EXISTS raw.sheetfield CASCADE;
   ```
3. Recreate table with new schema
4. **CRITICAL**: Reset tracking metadata:
   ```sql
   DELETE FROM raw.etl_load_status WHERE table_name = 'sheetfield';
   ```
5. Trigger full reload from replication database

**Breaking Changes**:
- Removing columns
- Narrowing types (BIGINT → INT)
- Narrowing strings (VARCHAR(100) → VARCHAR(50))
- Changing nullability (NULL → NOT NULL)
- Changing primary keys
- Renaming columns (looks like drop + add)

**Implementation**:
```python
def handle_breaking_changes(self, table_name: str, changes: SchemaChanges):
    """Handle breaking schema changes with table recreation."""
    # Send notification
    self._send_schema_change_notification(table_name, changes, 'BREAKING')
    
    with self.analytics_engine.begin() as trans:
        # Drop table with CASCADE
        trans.execute(text(f"DROP TABLE IF EXISTS {self.schema}.{table_name} CASCADE"))
        logger.warning(f"Dropped table {table_name} due to breaking schema changes")
        
        # Recreate with new schema
        self.ensure_table_exists(table_name, force_recreate=True)
        
        # CRITICAL: Reset tracking metadata
        trans.execute(text(f"""
            DELETE FROM {self.schema}.etl_load_status 
            WHERE table_name = :table_name
        """), {"table_name": table_name})
        logger.info(f"Reset tracking metadata for {table_name} - will trigger full reload")
        
        # Log to changelog
        self._log_schema_change(table_name, changes, 'BREAKING_AUTO')
    
    return True
```

---

### Phase 3: Tracking Metadata Management

**New Component**: `TrackingMetadataManager` in `etl_pipeline/core/tracking_metadata.py`

**Critical Functions**:

#### Reset Tracking on Table Recreation
```python
def reset_tracking_for_table(self, table_name: str, reason: str):
    """
    Reset tracking metadata to trigger full reload.
    
    Called when:
    - Table is dropped and recreated
    - Schema incompatibility detected
    - Manual reset requested
    """
    with self.analytics_engine.begin() as trans:
        # Clear all tracking fields
        trans.execute(text(f"""
            INSERT INTO {self.schema}.etl_load_status (
                table_name,
                last_primary_value,
                primary_column_name,
                rows_loaded,
                load_status,
                _loaded_at
            ) VALUES (
                :table_name,
                NULL,  -- Force full reload
                :primary_column,
                0,
                'pending',
                NULL   -- Clear timestamp
            )
            ON CONFLICT (table_name) DO UPDATE SET
                last_primary_value = NULL,
                rows_loaded = 0,
                load_status = 'pending',
                _loaded_at = NULL
        """), {
            "table_name": table_name,
            "primary_column": self._get_primary_column(table_name)
        })
        
        logger.warning(f"Reset tracking for {table_name}: {reason}")
```

#### Verify Tracking Consistency
```python
def verify_tracking_consistency(self, table_name: str) -> bool:
    """
    Verify that tracking metadata matches actual table state.
    
    Checks:
    - Row count in PostgreSQL vs tracking.rows_loaded
    - MAX(primary_key) vs tracking.last_primary_value
    - Schema hash consistency
    """
    analytics_count = self._get_table_row_count(table_name)
    tracking = self._get_tracking_record(table_name)
    
    if abs(analytics_count - tracking['rows_loaded']) > 1000:
        logger.error(
            f"Tracking inconsistency for {table_name}: "
            f"actual rows={analytics_count}, tracked={tracking['rows_loaded']}"
        )
        return False
    
    return True
```

---

### Phase 4: Schema Changelog and Audit Trail

**New Table**: `raw.etl_schema_changes`

```sql
CREATE TABLE raw.etl_schema_changes (
    change_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    change_type VARCHAR(50) NOT NULL,  -- 'COMPATIBLE_AUTO', 'BREAKING_AUTO', 'MANUAL'
    change_classification VARCHAR(50),  -- 'TIER1', 'TIER2', 'MANUAL'
    
    -- What changed
    added_columns TEXT[],
    removed_columns TEXT[],
    modified_columns JSONB,
    
    -- Actions taken
    migration_sql TEXT[],
    recreation_required BOOLEAN,
    full_reload_triggered BOOLEAN,
    
    -- Metadata
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    applied_at TIMESTAMP,
    applied_by VARCHAR(100),  -- 'ETL_PIPELINE', 'MANUAL', 'SCHEMA_ANALYZER'
    
    -- Schema hashes
    old_schema_hash VARCHAR(64),
    new_schema_hash VARCHAR(64),
    
    -- Notifications
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_recipients TEXT[],
    
    -- Audit
    status VARCHAR(50),  -- 'PENDING', 'APPLIED', 'FAILED', 'ROLLED_BACK'
    error_message TEXT,
    
    CONSTRAINT unique_table_change UNIQUE (table_name, detected_at)
);

CREATE INDEX idx_schema_changes_table ON raw.etl_schema_changes(table_name);
CREATE INDEX idx_schema_changes_detected ON raw.etl_schema_changes(detected_at);
CREATE INDEX idx_schema_changes_status ON raw.etl_schema_changes(status);
```

**Usage**:
```python
def log_schema_change(self, table_name: str, changes: SchemaChanges, classification: str):
    """Log schema change to audit trail."""
    with self.analytics_engine.begin() as trans:
        trans.execute(text("""
            INSERT INTO raw.etl_schema_changes (
                table_name, change_type, change_classification,
                added_columns, removed_columns, modified_columns,
                migration_sql, recreation_required, full_reload_triggered,
                old_schema_hash, new_schema_hash,
                status, applied_by
            ) VALUES (
                :table_name, :change_type, :classification,
                :added_columns, :removed_columns, :modified_columns,
                :migration_sql, :recreation_required, :full_reload_triggered,
                :old_hash, :new_hash,
                'APPLIED', 'ETL_PIPELINE'
            )
        """), {
            "table_name": table_name,
            "change_type": changes.change_type,
            "classification": classification,
            # ... other fields
        })
```

---

### Phase 5: Notification System

**New Component**: `SchemaChangeNotifier` in `etl_pipeline/notifications/schema_notifier.py`

**Notification Channels**:
1. **Logging**: Always log to ETL logs
2. **Email**: For breaking changes (optional)
3. **Slack**: For breaking changes (optional)
4. **Database**: Record in `etl_schema_changes` table

**Notification Content**:
```
Subject: [ETL WARNING] Breaking Schema Change: sheetfield

Table: sheetfield
Classification: BREAKING (Tier 2)
Action Taken: DROP CASCADE + Full Reload

Changes Detected:
  ❌ Removed Columns:
     - UserSigned (bigint)
  
  ✅ Added Columns:
     - None

Impact:
  • Table dropped and recreated with new schema
  • All data will be reloaded from replication database
  • Estimated reload time: 6.9 minutes
  • Downstream dbt models may be affected

Status: AUTOMATIC RECOVERY IN PROGRESS

View Details:
  Log: logs/etl_pipeline/etl_pipeline_run_20251011_205600.log
  Changelog: SELECT * FROM raw.etl_schema_changes WHERE table_name = 'sheetfield'

---
Sent by ETL Pipeline Automated Schema Manager
```

---

## Implementation Details

### Component Integration

#### 1. Enhanced `PostgresLoader.__init__()`
```python
def __init__(self, ...):
    # Existing initialization
    self.analytics_engine = ...
    self.replication_engine = ...
    
    # NEW: Schema management components
    self.schema_comparator = SchemaComparator()
    self.tracking_manager = TrackingMetadataManager(self.analytics_engine)
    self.schema_notifier = SchemaChangeNotifier()
```

#### 2. Modified `PostgresLoader.load_table()`
```python
def load_table(self, table_name: str, data: List[Dict]) -> bool:
    # 1. Ensure table exists (may detect schema changes)
    schema_changes = self.schema_adapter.ensure_table_exists(
        table_name, 
        check_schema_drift=True  # NEW parameter
    )
    
    # 2. Handle schema changes if detected
    if schema_changes:
        if schema_changes.classification == 'COMPATIBLE':
            self._handle_tier1_changes(table_name, schema_changes)
        elif schema_changes.classification == 'BREAKING':
            self._handle_tier2_changes(table_name, schema_changes)
            # Table was recreated, tracking reset, trigger full reload
            return self._trigger_full_reload(table_name)
    
    # 3. Continue with normal load
    return self._perform_load(table_name, data)
```

#### 3. Enhanced `PostgresSchema.ensure_table_exists()`
```python
def ensure_table_exists(self, table_name: str, check_schema_drift: bool = True):
    """
    Ensure PostgreSQL table exists with correct schema.
    
    Now includes schema drift detection and automatic handling.
    """
    # Check if table exists
    table_exists = self._table_exists(table_name)
    
    if not table_exists:
        # Create new table
        self._create_table(table_name)
        return None  # No schema changes, new table
    
    # NEW: Check for schema drift
    if check_schema_drift:
        pg_schema = self._get_postgres_schema(table_name)
        mysql_schema = self._get_mysql_schema(table_name)
        
        schema_changes = self.schema_comparator.compare_schemas(
            pg_schema, mysql_schema
        )
        
        if schema_changes.has_changes:
            logger.warning(f"Schema drift detected for {table_name}: {schema_changes}")
            return schema_changes
    
    return None  # No changes
```

---

## Testing Strategy

### Unit Tests

#### Test Schema Comparison
```python
def test_schema_comparator_detects_added_column():
    old_schema = {'columns': ['id', 'name']}
    new_schema = {'columns': ['id', 'name', 'email']}
    
    changes = comparator.compare_schemas(old_schema, new_schema)
    
    assert changes.added_columns == ['email']
    assert changes.classification == 'COMPATIBLE'

def test_schema_comparator_detects_removed_column():
    old_schema = {'columns': ['id', 'name', 'email']}
    new_schema = {'columns': ['id', 'name']}
    
    changes = comparator.compare_schemas(old_schema, new_schema)
    
    assert changes.removed_columns == ['email']
    assert changes.classification == 'BREAKING'
```

#### Test Tracking Reset
```python
def test_tracking_reset_clears_metadata():
    # Setup: Table with tracking data
    tracking_manager.update_tracking('test_table', 
                                    last_primary_value=1000,
                                    rows_loaded=1000)
    
    # Reset tracking
    tracking_manager.reset_tracking_for_table('test_table', 'schema_change')
    
    # Verify: All fields cleared
    tracking = tracking_manager.get_tracking('test_table')
    assert tracking['last_primary_value'] is None
    assert tracking['rows_loaded'] == 0
    assert tracking['load_status'] == 'pending'
```

### Integration Tests

#### Test Tier 1: Compatible Changes
```python
def test_tier1_add_column_auto_migration():
    # Setup: Create table with old schema
    create_table('test_table', columns=['id', 'name'])
    
    # Simulate: Source schema adds column
    update_source_schema('test_table', add_column='email')
    
    # Execute: Run ETL
    result = postgres_loader.load_table('test_table', data)
    
    # Verify: Column added without full reload
    assert table_has_column('test_table', 'email')
    assert result.full_reload_triggered == False
    assert schema_changelog_contains('test_table', 'COMPATIBLE_AUTO')
```

#### Test Tier 2: Breaking Changes
```python
def test_tier2_remove_column_auto_recreation():
    # Setup: Create table with old schema
    create_table('test_table', columns=['id', 'name', 'old_column'])
    insert_data('test_table', rows=1000)
    
    # Simulate: Source schema removes column
    update_source_schema('test_table', remove_column='old_column')
    
    # Execute: Run ETL
    result = postgres_loader.load_table('test_table', data)
    
    # Verify: Table recreated, tracking reset, full reload triggered
    assert not table_has_column('test_table', 'old_column')
    assert get_tracking('test_table')['last_primary_value'] is None
    assert result.full_reload_triggered == True
    assert schema_changelog_contains('test_table', 'BREAKING_AUTO')
    assert notification_sent('test_table')
```

### End-to-End Tests

```python
def test_e2e_schema_evolution_workflow():
    """
    Full workflow test:
    1. Initial load with schema A
    2. Schema changes to B (add column)
    3. Incremental load continues (Tier 1)
    4. Schema changes to C (remove column)  
    5. Table recreated, full reload (Tier 2)
    6. Verify data consistency
    """
    # Initial load
    load_table('patient', schema_version='A')
    assert get_row_count('patient') == 1000
    
    # Add column (Tier 1)
    change_schema('patient', add_column='email')
    load_table('patient', incremental=True)
    assert table_has_column('patient', 'email')
    assert get_row_count('patient') == 1000  # No reload
    
    # Remove column (Tier 2)
    change_schema('patient', remove_column='old_field')
    load_table('patient', incremental=True)
    assert not table_has_column('patient', 'old_field')
    assert get_row_count('patient') == 1000  # Full reloaded
    assert get_tracking('patient')['last_primary_value'] == 1000
```

---

## Rollout Plan

### Phase 1: Foundation (Week 1-2)
- ✅ Create `SchemaComparator` class
- ✅ Create `TrackingMetadataManager` class
- ✅ Add `etl_schema_changes` table
- ✅ Unit tests for comparison logic
- ✅ Integration tests for tracking reset

### Phase 2: Tier 1 Implementation (Week 3)
- ✅ Implement ALTER TABLE logic for compatible changes
- ✅ Add schema drift detection to `ensure_table_exists()`
- ✅ Test with non-critical tables in test environment
- ✅ Validate no data loss or corruption

### Phase 3: Tier 2 Implementation (Week 4)
- ✅ Implement DROP CASCADE logic for breaking changes
- ✅ Add automatic tracking reset
- ✅ Implement notification system
- ✅ Test with controlled schema changes

### Phase 4: Production Rollout (Week 5-6)
- ✅ Deploy to production with monitoring
- ✅ Test with real schema changes
- ✅ Document manual override procedures
- ✅ Train team on new system

### Phase 5: Monitoring & Optimization (Week 7+)
- ✅ Monitor schema change frequency
- ✅ Optimize ALTER TABLE performance
- ✅ Add more sophisticated change classification
- ✅ Consider blue-green deployment for large tables

---

## Configuration

### Environment-Specific Behavior

```yaml
# .env_test
SCHEMA_EVOLUTION_TIER1_ENABLED=true   # Auto ALTER TABLE
SCHEMA_EVOLUTION_TIER2_ENABLED=true   # Auto DROP CASCADE
SCHEMA_EVOLUTION_NOTIFICATIONS=false  # No notifications in test
SCHEMA_EVOLUTION_REQUIRE_APPROVAL=false  # Full automation

# .env_production  
SCHEMA_EVOLUTION_TIER1_ENABLED=true   # Auto ALTER TABLE
SCHEMA_EVOLUTION_TIER2_ENABLED=true   # Auto DROP CASCADE  
SCHEMA_EVOLUTION_NOTIFICATIONS=true   # Send notifications
SCHEMA_EVOLUTION_REQUIRE_APPROVAL=false  # Auto-handle but notify
```

### Per-Table Configuration

```yaml
# etl_pipeline/config/tables.yml
tables:
  patient:
    schema_evolution:
      tier1_allowed: true   # Allow automatic ALTER TABLE
      tier2_allowed: true   # Allow automatic DROP CASCADE
      notification_level: high  # Always notify for this critical table
      
  audit_log:
    schema_evolution:
      tier1_allowed: true
      tier2_allowed: false  # Never auto-drop audit tables
      require_manual_approval: true
```

---

## Success Criteria

### Immediate (Post-Implementation)
- ✅ Zero manual table drops required for schema changes
- ✅ Tracking metadata always consistent with table state
- ✅ No data loss from schema evolution
- ✅ Full audit trail of all schema changes

### Short-term (1 Month)
- ✅ 80%+ of schema changes handled automatically (Tier 1)
- ✅ 100% of breaking changes recover automatically (Tier 2)
- ✅ Average schema change handling time < 1 minute
- ✅ Zero production ETL failures from schema drift

### Long-term (3 Months)
- ✅ Complete schema change history and analytics
- ✅ Predictive alerts for problematic schema patterns
- ✅ Documentation of all OpenDental schema evolution patterns
- ✅ Team confidence in automatic schema handling

---

## Risks and Mitigations

### Risk 1: Incorrect Classification
**Risk**: Compatible change classified as breaking, causing unnecessary full reload

**Mitigation**:
- Conservative classification (err on side of caution)
- Dry-run mode to preview actions
- Manual override capability
- Extensive test coverage

### Risk 2: Data Loss on DROP CASCADE
**Risk**: Breaking change triggers DROP CASCADE, loses data permanently

**Mitigation**:
- Always DROP + immediate reload (atomic in code flow)
- Replication database preserves all source data
- Backup tracking shows pre-drop state
- Notification sent before drop
- Manual approval mode for critical tables

### Risk 3: Notification Fatigue
**Risk**: Too many notifications for minor changes

**Mitigation**:
- Only notify for breaking changes (Tier 2)
- Compatible changes logged only (Tier 1)
- Configurable notification levels per table
- Daily summary instead of per-change

### Risk 4: ALTER TABLE Performance
**Risk**: ALTER TABLE on large table causes locks/downtime

**Mitigation**:
- Run ALTER TABLE during maintenance windows
- Monitor lock duration
- Fallback to DROP + reload if ALTER takes too long
- Consider blue-green for very large tables

---

## Future Enhancements

### Phase 6: Blue-Green Schema Migrations
- For tables > 100M rows
- Zero-downtime schema changes
- Parallel load to new table
- Atomic switch when complete

### Phase 7: Schema Preview and Approval
- Dry-run mode shows planned actions
- Web UI for approving pending changes
- Integration with change management system
- Scheduled migrations during maintenance windows

### Phase 8: Intelligent Schema Learning
- ML model learns schema change patterns
- Predicts likely future changes
- Proactive notifications to stakeholders
- Automated schema documentation generation

### Phase 9: Cross-Database Schema Sync
- Keep test and production schemas in sync
- Propagate approved changes across environments
- Version-controlled schema state
- Automated testing of schema migrations

---

## Appendix

### Related Documents
- [ETL Pipeline Architecture](./etl_architecture.md)
- [PostgreSQL Schema Management](./postgres_schema_management.md)
- [Tracking Metadata Design](./tracking_metadata_design.md)

### Schema Change Decision Tree

```
Schema Change Detected
    ↓
Is it adding columns only?
    YES → Tier 1: ALTER TABLE ADD COLUMN
    NO ↓
    
Is it removing columns?
    YES → Tier 2: DROP CASCADE + Full Reload
    NO ↓
    
Is it changing column types?
    ↓
    Widening types? (INT→BIGINT)
        YES → Tier 1: ALTER TABLE ALTER COLUMN
        NO → Tier 2: DROP CASCADE + Full Reload
    
Is it changing primary key?
    YES → Tier 2: DROP CASCADE + Full Reload
    NO ↓
    
Unknown change type
    → Default: Tier 2 (safe fallback)
```

### Error Recovery Procedures

**If Tier 1 ALTER TABLE Fails:**
1. Rollback transaction
2. Log failure to changelog
3. Escalate to Tier 2 (DROP CASCADE)
4. Send notification
5. Proceed with recreation

**If Tier 2 DROP CASCADE Fails:**
1. Manual intervention required
2. Notify DBA immediately
3. Check for dependencies
4. Manual DROP with careful CASCADE
5. Reset tracking manually
6. Resume ETL

**If Tracking Reset Fails:**
1. ETL will detect inconsistency
2. Automatic full reload will fix
3. May cause one wasted incremental run
4. Self-healing on next run

---

## Approval and Sign-off

**Document Version**: 1.0  
**Date**: 2025-10-11  
**Status**: Pending Implementation

**Prepared by**: ETL Architecture Team  
**Reviewed by**: TBD  
**Approved by**: TBD

---

*This document outlines the complete solution for automatic schema drift handling in the ETL pipeline. Implementation should follow the phased rollout plan with comprehensive testing at each stage.*

