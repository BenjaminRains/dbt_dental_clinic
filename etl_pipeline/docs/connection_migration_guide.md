# Connection Name Migration Guide

This document lists all the places in the ETL pipeline where the old connection names (`source`, `staging`, `target`) are used and need to be updated to the new specific names.

## ✅ **Already Updated**

### 1. **Settings Class** (`etl_pipeline/config/settings.py`)
- ✅ Added `CONNECTION_MAPPINGS` for backward compatibility
- ✅ Updated `get_database_config()` to handle new names
- ✅ Updated `get_connection_string()` to handle new names
- ✅ Added schema-specific configurations for PostgreSQL

### 2. **Pipeline Configuration** (`etl_pipeline/config/pipeline.yml`)
- ✅ Updated connection names to be specific:
  - `source` → `opendental_source`
  - `staging` → `opendental_replication`
  - `target` → `opendental_analytics_public`
- ✅ Added schema-specific connections for PostgreSQL analytics database

### 3. **ConnectionFactory** (`etl_pipeline/core/connections.py`)
- ✅ Added new methods for specific schema connections:
  - `get_opendental_analytics_raw_connection()`
  - `get_opendental_analytics_public_connection()`
  - `get_opendental_analytics_staging_connection()`
  - `get_opendental_analytics_intermediate_connection()`
  - `get_opendental_analytics_marts_connection()`
- ✅ Kept existing methods for backward compatibility

### 4. **TableProcessor** (`etl_pipeline/orchestration/table_processor.py`)
- ✅ Updated to use new connection names:
  - `self.settings.get_database_config('opendental_source')`
  - `self.settings.get_database_config('opendental_replication')`
  - `self.settings.get_database_config('opendental_analytics_raw')`

## 🔄 **Still Using Old Names (Need Updates)**

### 1. **Test Files** (Can keep old names for testing)
- `tests/unit/config/test_settings.py` - Uses `'source'` in tests
- `tests/unit/orchestration/test_table_processor*.py` - Uses old connection names
- `tests/conftest.py` - Uses old connection names in mocks

### 2. **Scripts** (Should be updated)
- `scripts/generate_table_config.py` - Uses `get_opendental_source_connection()`
- `scripts/analyze_schema_discovery.py` - Uses `get_opendental_source_connection()`

### 3. **DLT Pipeline** (Separate project)
- `dlt_pipeline/sources/opendental.py` - Uses `get_database_config('source')`
- `dlt_pipeline/tests/dlt_test_connections.py` - Uses old connection methods

### 4. **Airflow DAGs** (Should be updated)
- `airflow/dags/nightly_incremental_DAG.py` - Uses `get_target_connection()`

## 📋 **Migration Checklist**

### **High Priority (Production Code)**
- [ ] Update `scripts/generate_table_config.py`
- [ ] Update `scripts/analyze_schema_discovery.py`
- [ ] Update `airflow/dags/nightly_incremental_DAG.py`

### **Medium Priority (DLT Pipeline)**
- [ ] Update `dlt_pipeline/sources/opendental.py`
- [ ] Update `dlt_pipeline/tests/dlt_test_connections.py`

### **Low Priority (Tests - Can Keep Old Names)**
- [ ] Consider updating test files to use new names for consistency
- [ ] Update test mocks to use new connection names

## 🔧 **How to Update**

### **For Settings Calls:**
```python
# OLD
config = settings.get_database_config('source')
conn_str = settings.get_connection_string('target')

# NEW
config = settings.get_database_config('opendental_source')
conn_str = settings.get_connection_string('opendental_analytics_public')
```

### **For ConnectionFactory Calls:**
```python
# OLD
engine = ConnectionFactory.get_postgres_analytics_connection()

# NEW (for specific schemas)
engine = ConnectionFactory.get_opendental_analytics_public_connection()
engine = ConnectionFactory.get_opendental_analytics_raw_connection()
engine = ConnectionFactory.get_opendental_analytics_staging_connection()
```

### **For Pipeline Configuration:**
```yaml
# OLD
connections:
  source:
    type: "mysql"
  target:
    type: "postgresql"

# NEW
connections:
  opendental_source:
    type: "mysql"
  opendental_analytics_public:
    type: "postgresql"
    schema: "public"
```

## ✅ **Backward Compatibility**

The old connection names will continue to work because:
1. **Settings class** has `CONNECTION_MAPPINGS` that maps old names to new ones
2. **ConnectionFactory** keeps the old methods alongside new ones
3. **Pipeline configuration** can use either old or new names

## 🎯 **Recommended Approach**

1. **Update production code first** (scripts, DAGs)
2. **Test thoroughly** with new names
3. **Update DLT pipeline** if needed
4. **Consider updating tests** for consistency
5. **Document the changes** in team communications

## 📝 **Benefits of Migration**

- **Clarity**: Connection names clearly indicate which database/schema
- **Maintainability**: Easier to understand and debug
- **Scalability**: Ready for multiple schemas and databases
- **Documentation**: Self-documenting code
- **Future-proof**: Supports complex multi-schema architectures

## 🗄️ **Database Structure**

**Important**: All PostgreSQL connections point to the **same database** (`opendental_analytics`) but use different **schemas**:

```
PostgreSQL Database: opendental_analytics
    ├── Schema: raw
    ├── Schema: public_staging
    ├── Schema: public_intermediate
    ├── Schema: public_marts
    └── Schema: public
```

This is different from having separate databases - it's one database with multiple schemas for organization. 