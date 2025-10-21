# Schema Analyzer Downstream Dependencies

**Document Purpose**: Identify all files that depend on `analyze_opendental_schema.py` to ensure safe refactoring.

**Script Location**: `etl_pipeline/scripts/analyze_opendental_schema.py`  
**Date**: 2025-01-17  
**Related Doc**: `docs/refactor_schema_analyzer_methods.md`

---

## Summary

✅ **Good News**: The schema analyzer is a **standalone script** with minimal direct dependencies!

- **Direct Imports**: Only test files
- **Production Usage**: CLI calls it as a subprocess (loose coupling)
- **Data Consumer**: SimpleMySQLReplicator reads `tables.yml` (not the Python code)
- **Total Downstream Files**: 3 production files + 24 test files

---

## Production Code Dependencies

### 1. CLI Command (Subprocess Execution)

**File**: `etl_pipeline/etl_pipeline/cli/commands.py`  
**Lines**: 468-600 (`update_schema` command)  
**Usage**: Executes the script as a subprocess

```python
def update_schema(backup: bool, force: bool, output_dir: str, log_level: str):
    """Update ETL schema configuration by running analyze_opendental_schema.py."""
    
    # Get script path
    script_path = Path(__file__).parent.parent.parent / "scripts" / "analyze_opendental_schema.py"
    
    # Execute script (no direct import!)
    # Uses subprocess or direct execution
```

**Dependency Type**: ⚠️ **Loose Coupling** (subprocess)  
**Refactoring Impact**: ✅ **NONE** - Script can be refactored freely  
**Why Safe**: CLI doesn't import methods, just executes the script

---

### 2. SimpleMySQLReplicator (Configuration Consumer)

**File**: `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py`  
**Lines**: Various (configuration reading)  
**Usage**: Reads `tables.yml` output file, NOT the Python code

```python
def _load_configuration(self) -> Dict:
    """Load table configuration from tables.yml."""
    with open(self.tables_config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    tables = config.get('tables', {})
    return tables

# Used in various methods
def calculate_adaptive_batch_size(self, table_name: str, config: Dict) -> int:
    config_batch_size = config.get('batch_size')  # From tables.yml
    incremental_columns = config.get('incremental_columns', [])  # From tables.yml
    # ...

def get_extraction_strategy(self, table_name: str) -> str:
    strategy = config.get('extraction_strategy', 'full_table')  # From tables.yml
    # ...
```

**Dependency Type**: 📄 **Data Dependency** (reads YAML file)  
**Refactoring Impact**: ⚠️ **YAML FORMAT ONLY**  
**What to Preserve**: The structure of `tables.yml` output

#### Critical Fields Used by SimpleMySQLReplicator

```yaml
tables:
  patient:
    # CRITICAL - Used directly by replicator
    extraction_strategy: 'incremental'           # REQUIRED
    batch_size: 50000                           # REQUIRED
    incremental_columns: ['DateTStamp']         # REQUIRED
    primary_incremental_column: 'DateTStamp'    # REQUIRED
    
    # METADATA - Not used by replicator (safe to change)
    estimated_rows: 150000
    estimated_size_mb: 45.2
    performance_category: 'medium'
    processing_priority: 'high'
    time_gap_threshold_days: 30
    # ... other metadata
```

**Backward Compatibility Requirements**:
- ✅ Keep these fields: `extraction_strategy`, `batch_size`, `incremental_columns`, `primary_incremental_column`
- ✅ Can modify: All other metadata fields (safe to rename/add/remove)

---

### 3. PriorityProcessor (Indirect Consumer)

**File**: `etl_pipeline/etl_pipeline/orchestration/priority_processor.py`  
**Lines**: Various  
**Usage**: Processes tables based on configuration

```python
# PriorityProcessor uses TableProcessor which uses SimpleMySQLReplicator
# So it indirectly depends on tables.yml format
```

**Dependency Type**: 📄 **Indirect Data Dependency**  
**Refactoring Impact**: ✅ **NONE** - Uses same `tables.yml`

---

## Test Files (24 Total)

### Unit Tests (12 Files)

All import `OpenDentalSchemaAnalyzer` class directly:

```
etl_pipeline/tests/unit/scripts/analyze_opendental_schema/
├── test_batch_processing_unit.py
├── test_configuration_generation_unit.py
├── test_dbt_integration_unit.py
├── test_error_handling_unit.py
├── test_extraction_strategy_unit.py
├── test_importance_determination_unit.py
├── test_incremental_strategy_unit.py
├── test_initialization_unit.py
├── test_progress_monitoring_unit.py
├── test_schema_analysis_unit.py
├── test_size_analysis_unit.py
└── test_table_discovery_unit.py
```

**Import Pattern**:
```python
from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
```

**Refactoring Impact**: ⚠️ **HIGH** - Tests call methods directly  
**Action Required**: Update tests after refactoring

---

### Integration Tests (12 Files)

All import `OpenDentalSchemaAnalyzer` class:

```
etl_pipeline/tests/integration/scripts/analyze_opendental_schema/
├── test_batch_processing_integration.py
├── test_configuration_generation_integration.py
├── test_data_quality_integration.py
├── test_dbt_integration_integration.py
├── test_extraction_strategy_integration.py
├── test_incremental_strategy_integration.py
├── test_initialization_integration.py
├── test_performance_analysis_integration.py
├── test_reporting_integration.py
├── test_schema_analysis_integration.py
├── test_size_analysis_integration.py
└── test_table_discovery_integration.py
```

**Import Pattern**:
```python
from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
```

**Refactoring Impact**: ⚠️ **HIGH** - Tests call methods directly  
**Action Required**: Update tests after refactoring

---

## tables.yml Contract (Critical!)

This is the **only production interface** between the analyzer and the ETL pipeline.

### Required Fields (DO NOT REMOVE)

```yaml
# Top-level structure
metadata:
  generated_at: <ISO timestamp>
  analyzer_version: <string>
  source_database: <string>
  total_tables: <int>
  schema_hash: <string>

tables:
  <table_name>:
    # CRITICAL FIELDS (used by SimpleMySQLReplicator)
    extraction_strategy: 'full_table' | 'incremental' | 'incremental_chunked'
    batch_size: <int>
    incremental_columns: [<string>, ...]
    primary_incremental_column: <string>
    
    # METADATA FIELDS (not used by replicator, safe to change)
    estimated_rows: <int>
    estimated_size_mb: <float>
    performance_category: <string>
    processing_priority: <string>
    time_gap_threshold_days: <int>
    estimated_processing_time_minutes: <float>
    memory_requirements_mb: <int>
    monitoring: <dict>
    schema_hash: <int>
    last_analyzed: <ISO timestamp>
```

### Safe to Modify

These fields are NOT used by production code:
- ✅ `estimated_rows` - Can rename/remove
- ✅ `estimated_size_mb` - Can rename/remove
- ✅ `performance_category` - Can rename/remove
- ✅ `processing_priority` - Can rename/remove
- ✅ `time_gap_threshold_days` - Can rename/remove
- ✅ `estimated_processing_time_minutes` - Can rename/remove
- ✅ `memory_requirements_mb` - Can rename/remove
- ✅ `monitoring` - Can change structure
- ✅ `schema_hash` - Can change
- ✅ `last_analyzed` - Can change

### MUST Preserve

These fields ARE used by SimpleMySQLReplicator:
- ❌ `extraction_strategy` - **REQUIRED**, must be one of: `full_table`, `incremental`, `incremental_chunked`
- ❌ `batch_size` - **REQUIRED**, integer > 0
- ❌ `incremental_columns` - **REQUIRED**, list of strings (can be empty)
- ❌ `primary_incremental_column` - **REQUIRED**, string or null

---

## Refactoring Safety Checklist

### ✅ Safe to Refactor (No Breaking Changes)

These changes won't affect production code:

- ✅ **Method Consolidation**: Merge `get_estimated_row_count()` and `get_table_size_info()`
- ✅ **Method Renaming**: Rename any method (tests will need updates)
- ✅ **Method Deletion**: Remove unused methods
- ✅ **Add New Methods**: Add helpers, utilities, etc.
- ✅ **Change Internal Logic**: As long as `tables.yml` format stays the same
- ✅ **Performance Optimization**: Database query consolidation, caching, etc.
- ✅ **Documentation**: Improve docstrings, add comments
- ✅ **Code Organization**: Reorder methods, add sections

### ⚠️ Requires Caution (Breaking Changes)

These changes need verification:

- ⚠️ **tables.yml Format**: Changing structure of critical fields
- ⚠️ **Script Entry Point**: Changing `main()` signature
- ⚠️ **Exit Codes**: CLI expects certain exit codes
- ⚠️ **Output Files**: CLI expects specific output files

### ❌ Not Safe (Will Break Production)

These changes WILL break production:

- ❌ **Remove `tables.yml` Output**: ETL pipeline depends on it
- ❌ **Change Critical Field Names**: `extraction_strategy`, `batch_size`, etc.
- ❌ **Change Field Types**: E.g., `batch_size` from int to string
- ❌ **Remove Script File**: CLI calls it by path

---

## Testing Requirements After Refactoring

### Phase 1: Unit Tests

```bash
# Run all analyzer unit tests
pytest etl_pipeline/tests/unit/scripts/analyze_opendental_schema/ -v

# Expected: Some tests will fail (need updates)
# Action: Update tests to match refactored methods
```

### Phase 2: Integration Tests

```bash
# Run all analyzer integration tests
pytest etl_pipeline/tests/integration/scripts/analyze_opendental_schema/ -v

# Expected: Some tests will fail (need updates)
# Action: Update tests to match refactored methods
```

### Phase 3: End-to-End Test

```bash
# 1. Run schema analyzer
python etl_pipeline/scripts/analyze_opendental_schema.py

# 2. Verify output
ls -la etl_pipeline/config/tables.yml

# 3. Verify format
python -c "import yaml; yaml.safe_load(open('etl_pipeline/config/tables.yml'))"

# 4. Test ETL pipeline can read config
etl run --tables patient --dry-run

# Expected: No errors, pipeline can parse tables.yml
```

### Phase 4: Replicator Integration Test

```bash
# Test that SimpleMySQLReplicator can read the new tables.yml
pytest etl_pipeline/tests/unit/core/simple_mysql_replicator/test_configuration_unit.py -v

# Expected: All tests pass (config format preserved)
```

---

## Method Call Analysis

### Methods Called by Tests (Need Updates)

Based on test files, these methods are called directly:

```python
# Discovery Methods
analyzer.discover_all_tables()
analyzer.discover_dbt_models()

# Schema Analysis
analyzer.get_table_schema(table_name)
analyzer.get_estimated_row_count(table_name)  # ⚠️ REFACTORING
analyzer.get_table_size_info(table_name)      # ⚠️ REFACTORING

# Incremental Loading
analyzer.find_incremental_columns(table_name, schema_info)
analyzer.validate_incremental_column_data_quality(table_name, column)
analyzer.select_primary_incremental_column(table_name, columns, schema)
analyzer.determine_incremental_strategy(table_name, schema, columns)
analyzer.determine_extraction_strategy(table_name, schema, size)  # ⚠️ REFACTORING

# Performance
analyzer.analyze_table_performance_characteristics(table_name, schema, size)  # ⚠️ RENAMING

# SCD Detection
analyzer.compare_with_previous_schema(config, previous_path)

# Batch Processing
analyzer.get_batch_schema_info(table_names)
analyzer.get_batch_size_info(table_names)

# Configuration
analyzer.generate_complete_configuration(output_dir)
analyzer.analyze_complete_schema(output_dir)
```

### Methods NOT Called by Tests (Safe to Delete)

```python
# These are only used internally, safe to refactor/delete
analyzer._generate_schema_hash()
analyzer._calculate_processing_priority()
analyzer._estimate_processing_time()
analyzer._estimate_memory_requirements()
analyzer._validate_extraction_strategy()
analyzer._generate_detailed_analysis_report()
analyzer._generate_performance_summary()
analyzer._generate_schema_changelog()
analyzer._get_batch_size_range()
```

---

## Refactoring Action Plan

### Step 1: Implement Phase 1 (Database Query Consolidation)

**Files to Change**:
- ✅ `etl_pipeline/scripts/analyze_opendental_schema.py`

**Tests to Update**:
- ⚠️ `test_size_analysis_unit.py` - May need mock updates
- ⚠️ `test_size_analysis_integration.py` - Verify behavior unchanged

**Verification**:
```bash
# 1. Run analyzer
python etl_pipeline/scripts/analyze_opendental_schema.py

# 2. Compare tables.yml (should be identical)
diff etl_pipeline/config/tables.yml.backup etl_pipeline/config/tables.yml

# 3. Test ETL can read it
etl validate-config
```

---

### Step 2: Implement Phase 2 (Method Cleanup)

**Files to Change**:
- ✅ `etl_pipeline/scripts/analyze_opendental_schema.py`

**Tests to Update**:
- ⚠️ `test_extraction_strategy_unit.py` - Update method calls
- ⚠️ `test_extraction_strategy_integration.py` - Update method calls

**Verification**:
```bash
pytest etl_pipeline/tests/unit/scripts/analyze_opendental_schema/test_extraction_strategy_unit.py -v
```

---

### Step 3: Implement Phase 3 (Naming Improvements)

**Files to Change**:
- ✅ `etl_pipeline/scripts/analyze_opendental_schema.py`

**Tests to Update**:
- ⚠️ `test_performance_analysis_integration.py` - Update method name
- ⚠️ Any test calling `analyze_table_performance_characteristics()`

**Verification**:
```bash
grep -r "analyze_table_performance_characteristics" etl_pipeline/tests/
# Update all occurrences
```

---

### Step 4: Update All Tests

**Action**:
```bash
# Run all tests, fix failures one by one
pytest etl_pipeline/tests/unit/scripts/analyze_opendental_schema/ -v --tb=short
pytest etl_pipeline/tests/integration/scripts/analyze_opendental_schema/ -v --tb=short
```

---

### Step 5: Final Validation

**Checklist**:
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] `tables.yml` format unchanged (critical fields)
- [ ] CLI command works: `etl update-schema`
- [ ] SimpleMySQLReplicator can read config: `etl run --tables patient --dry-run`
- [ ] Performance improved (measure query count)
- [ ] Documentation updated

---

## Rollback Procedure

If something breaks:

```bash
# 1. Revert code changes
git checkout main -- etl_pipeline/scripts/analyze_opendental_schema.py

# 2. Restore previous tables.yml if needed
cp logs/schema_analysis/backups/tables.yml.backup.<timestamp> etl_pipeline/config/tables.yml

# 3. Verify ETL works
etl run --tables patient --dry-run

# 4. Report issue
git diff main HEAD -- etl_pipeline/scripts/analyze_opendental_schema.py
```

---

## Contact Points for Issues

### If tables.yml Format Changes Break Things

**Affected**: `etl_pipeline/etl_pipeline/core/simple_mysql_replicator.py`  
**Methods**: `_load_configuration()`, `calculate_adaptive_batch_size()`, `get_extraction_strategy()`  
**Fix**: Ensure critical fields are preserved in `tables.yml`

### If CLI Command Fails

**Affected**: `etl_pipeline/etl_pipeline/cli/commands.py`  
**Method**: `update_schema()`  
**Fix**: Verify script path and subprocess execution

### If Tests Fail

**Affected**: All test files in `etl_pipeline/tests/.../analyze_opendental_schema/`  
**Fix**: Update test method calls to match refactored names

---

## Conclusion

**Refactoring is SAFE** because:

1. ✅ **Loose Coupling**: Production code doesn't import analyzer methods
2. ✅ **Data Interface**: Only `tables.yml` format matters
3. ✅ **Isolated Tests**: Test failures are isolated, won't affect production
4. ✅ **Backward Compatible**: Refactoring preserves `tables.yml` structure

**Key Takeaway**: As long as you preserve the critical fields in `tables.yml`, you can refactor the Python code freely!

---

**Last Updated**: 2025-01-17  
**Related Docs**:
- `docs/refactor_schema_analyzer_methods.md` - Refactoring plan
- `etl_pipeline/docs/DATA_FLOW_DIAGRAM.md` - Overall architecture

