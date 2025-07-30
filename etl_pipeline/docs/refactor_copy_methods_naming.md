# SimpleMySQLReplicator Comprehensive Refactor Guide

## Overview

This document provides a complete step-by-step guide to refactor the `SimpleMySQLReplicator` class
 and related components. The refactor addresses naming confusion, improves code organization, and
  creates clear separation of concerns between extraction strategies and copy methods.

## Current Issues & Solutions

### Problem Summary
- **Naming Confusion**: `get_copy_strategy()` returns size-based methods ('small', 'medium', 'large') while `get_extraction_strategy()` returns config-based strategies
- **Strategy Mismatch**: Code expects `'chunked_incremental'` but schema analyzer generates `'small_table'`
- **Mixed Responsibilities**: Copy methods (how to copy) and extraction strategies (what to copy) are conflated

### Solution Overview
- **Clear Separation**: Copy methods determine *how* to copy based on size/performance; extraction strategies determine *what* to copy based on business logic
- **Consistent Naming**: Align strategy names between schema analyzer and replicator
- **Clean Architecture**: Remove confusion and create intuitive method names

## Refactor Plan

### Phase 1: Update Schema Analyzer (Priority: Critical)
### Phase 2: Update SimpleMySQLReplicator (Priority: Critical)  
### Phase 3: Update Downstream Components (Priority: Medium)
### Phase 4: Testing & Validation (Priority: High)
### Phase 5: Documentation Updates (Priority: Medium)

---

## Phase 1: Update Schema Analyzer

The schema analyzer generates `tables.yml`, so we must update it first to produce clean strategy names.

### Step 1.1: Update `determine_extraction_strategy()` Method

**File**: `etl_pipeline/scripts/analyze_opendental_schema.py`

**Current Code** (around line 400):
```python
def determine_extraction_strategy(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
    """Determine optimal extraction strategy for a table based on size and available incremental columns."""
    estimated_row_count = size_info.get('estimated_row_count', 0)
    
    # Find available incremental columns
    incremental_columns = self.find_incremental_columns(table_name, schema_info)
    has_incremental_columns = len(incremental_columns) > 0
    
    # If no incremental columns, use full table
    if not has_incremental_columns:
        return 'full_table'
    
    # Chunked incremental for very large tables (> 1M rows)
    if estimated_row_count > 1_000_000:
        return 'chunked_incremental'  # ❌ PROBLEM: Inconsistent name
    
    # Incremental for medium to large tables (> 10k rows)
    if estimated_row_count > 10_000:
        return 'incremental'
    
    # For small tables (< 10k rows), use small_table strategy
    return 'small_table'  # ❌ PROBLEM: Confusing name
```

**Updated Code**:
```python
def determine_extraction_strategy(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
    """Determine optimal extraction strategy for a table based on size and available incremental columns."""
    estimated_row_count = size_info.get('estimated_row_count', 0)
    
    # Find available incremental columns
    incremental_columns = self.find_incremental_columns(table_name, schema_info)
    has_incremental_columns = len(incremental_columns) > 0
    
    # If no incremental columns, use full table
    if not has_incremental_columns:
        return 'full_table'
    
    # Chunked incremental for very large tables (> 1M rows)
    if estimated_row_count > 1_000_000:
        return 'incremental_chunked'  # ✅ FIXED: Consistent naming
    
    # Incremental for medium to large tables (> 10k rows)
    if estimated_row_count > 10_000:
        return 'incremental'
    
    # For small tables with incremental columns, use incremental strategy
    return 'incremental'  # ✅ FIXED: Clear business logic
```

### Step 1.2: Add Strategy Validation

**Add this new method** to `OpenDentalSchemaAnalyzer` class:

```python
def _validate_extraction_strategy(self, strategy: str) -> bool:
    """Validate that the extraction strategy is supported by SimpleMySQLReplicator."""
    # These must match the strategies in SimpleMySQLReplicator
    valid_strategies = ['full_table', 'incremental', 'incremental_chunked']
    
    if strategy not in valid_strategies:
        logger.warning(f"Invalid extraction strategy generated: {strategy}. Using 'full_table' as fallback.")
        return False
    return True
```

**Update the existing method** to use validation:

```python
def determine_extraction_strategy(self, table_name: str, schema_info: Dict, size_info: Dict) -> str:
    """Determine optimal extraction strategy with validation."""
    estimated_row_count = size_info.get('estimated_row_count', 0)
    
    # Find available incremental columns
    incremental_columns = self.find_incremental_columns(table_name, schema_info)
    has_incremental_columns = len(incremental_columns) > 0
    
    # Determine strategy
    if not has_incremental_columns:
        strategy = 'full_table'
    elif estimated_row_count > 1_000_000:
        strategy = 'incremental_chunked'
    elif estimated_row_count > 10_000:
        strategy = 'incremental'
    else:
        strategy = 'incremental'
    
    # Validate the strategy before returning
    if not self._validate_extraction_strategy(strategy):
        strategy = 'full_table'  # Safe fallback
        logger.warning(f"Using fallback strategy 'full_table' for {table_name}")
    
    return strategy
```

### Step 1.3: Add Strategy Validation in Configuration Generation

**Update** `generate_complete_configuration()` method around line 520:

**Find this section**:
```python
# Determine table characteristics
importance = self.determine_table_importance(table_name, schema_info, size_info)
extraction_strategy = self.determine_extraction_strategy(table_name, schema_info, size_info)
```

**Add validation after**:
```python
# Determine table characteristics
importance = self.determine_table_importance(table_name, schema_info, size_info)
extraction_strategy = self.determine_extraction_strategy(table_name, schema_info, size_info)

# Validate extraction strategy
if not self._validate_extraction_strategy(extraction_strategy):
    extraction_strategy = 'full_table'
    logger.warning(f"Using fallback strategy 'full_table' for {table_name}")
```

### Step 1.4: Test Schema Analyzer Changes

**Run the schema analyzer** to generate new `tables.yml`:

```bash
# Navigate to project root
cd /path/to/your/etl_pipeline_project

# Run schema analyzer
python etl_pipeline/scripts/analyze_opendental_schema.py
```

**Verify the output** in `etl_pipeline/config/tables.yml`:
- Should contain `incremental_chunked` instead of `chunked_incremental`
- Should contain `incremental` instead of `small_table` for small tables
- Should have no invalid strategy names

---

## Phase 2: Update SimpleMySQLReplicator

Now we update the replicator to handle the new strategy names and create clear separation of concerns.

### Step 2.1: Rename Core Methods

**File**: `etl_pipeline/mysql/simple_mysql_replicator.py`

**Find and rename**:
```python
# OLD METHOD NAME
def get_copy_strategy(self, table_name: str) -> str:
    """
    Determine copy strategy based on table size.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Copy strategy: 'small', 'medium', or 'large'
    """
```

**RENAME TO**:
```python
def get_copy_method(self, table_name: str) -> str:
    """
    Determine copy method based on table size.
    
    This determines HOW to copy data based on performance characteristics.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Copy method: 'small', 'medium', or 'large'
    """
```

### Step 2.2: Update Strategy Handling in `copy_table()`

**Find the `copy_table()` method** (around line 200):

**Current Code**:
```python
# Get copy strategy based on table size
copy_strategy = self.get_copy_strategy(table_name)

# Log strategy information  
logger.info(f"Using {copy_strategy} copy strategy for {table_name}")

# Determine extraction strategy
extraction_strategy = self.get_extraction_strategy(table_name)
if force_full:
    extraction_strategy = 'full_table'

logger.info(f"Using {extraction_strategy} extraction strategy for {table_name}")

# Copy data based on extraction strategy
if extraction_strategy == 'full_table':
    success, rows_copied = self._copy_full_table(table_name, config)
elif extraction_strategy == 'incremental':
    success, rows_copied = self._copy_incremental_table(table_name, config)
elif extraction_strategy == 'chunked_incremental':  # ❌ OLD NAME
    success, rows_copied = self._copy_chunked_incremental_table(table_name, config)
else:
    logger.error(f"Unknown extraction strategy: {extraction_strategy}")
    return False
```

**Updated Code**:
```python
# Get extraction strategy (WHAT to copy - business logic)
extraction_strategy = self.get_extraction_strategy(table_name)
if force_full:
    extraction_strategy = 'full_table'

# Get copy method (HOW to copy - performance optimization)
copy_method = self.get_copy_method(table_name)  # ✅ RENAMED METHOD

# Log clear separation of concerns
logger.info(f"Using '{extraction_strategy}' extraction strategy with '{copy_method}' copy method for {table_name}")

# Copy data based on extraction strategy
if extraction_strategy == 'full_table':
    success, rows_copied = self._copy_full_table(table_name, config)
elif extraction_strategy == 'incremental':
    success, rows_copied = self._copy_incremental_table(table_name, config)
elif extraction_strategy == 'incremental_chunked':  # ✅ NEW NAME
    success, rows_copied = self._copy_chunked_incremental_table(table_name, config)
else:
    logger.error(f"Unknown extraction strategy: {extraction_strategy}")
    return False
```

### Step 2.3: Update Method References

**Find all references** to `get_copy_strategy()` and update them:

**In `_copy_full_table()` method** (around line 350):
```python
# OLD
copy_strategy = self.get_copy_strategy(table_name)
logger.info(f"Using {copy_strategy} copy strategy for {table_name} with batch size: {optimized_batch_size}")

# NEW
copy_method = self.get_copy_method(table_name)
logger.info(f"Using {copy_method} copy method for {table_name} with batch size: {optimized_batch_size}")
```

**Update the logic in `_copy_full_table()`**:
```python
# Copy all data based on copy method
if copy_method == 'small':
    success, rows_copied = self._copy_small_table(table_name)
elif copy_method == 'medium':
    success, rows_copied = self._copy_medium_table(table_name, optimized_batch_size)
else:  # large
    success, rows_copied = self._copy_large_table(table_name, optimized_batch_size)
```

### Step 2.4: Add Strategy Validation

**Add validation method** to `SimpleMySQLReplicator` class:

```python
def _validate_extraction_strategy(self, strategy: str) -> bool:
    """Validate extraction strategy against supported strategies."""
    valid_strategies = ['full_table', 'incremental', 'incremental_chunked']
    return strategy in valid_strategies
```

**Update `get_extraction_strategy()`** to include validation:

```python
def get_extraction_strategy(self, table_name: str) -> str:
    """
    Get extraction strategy from table configuration with validation.
    
    This determines WHAT to copy based on business logic and configuration.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Extraction strategy: 'full_table', 'incremental', or 'incremental_chunked'
    """
    config = self.table_configs.get(table_name, {})
    strategy = config.get('extraction_strategy', 'full_table')
    
    if not self._validate_extraction_strategy(strategy):
        logger.warning(f"Invalid strategy '{strategy}' for {table_name}, using 'full_table'")
        return 'full_table'
    
    return strategy
```

### Step 2.5: Update Documentation and Comments

**Update class docstring** at the top of `SimpleMySQLReplicator`:

```python
"""
Simple MySQL replicator that copies data using the new Settings-centric architecture.

Copy Methods (HOW to copy - performance-based):
- SMALL (< 1MB): Direct INSERT ... SELECT
- MEDIUM (1-100MB): Chunked INSERT with LIMIT/OFFSET
- LARGE (> 100MB): Chunked INSERT with WHERE conditions

Extraction Strategies (WHAT to copy - business logic):
- full_table: Drop and recreate entire table
- incremental: Only copy new/changed data using incremental column
- incremental_chunked: Smaller batches for very large incremental tables
"""
```

---

## Phase 3: Update Downstream Components

Minor updates to handle new strategy names in downstream components.

### Step 3.1: Update PostgresLoader (Optional)

**File**: `etl_pipeline/core/postgres_loader.py`

PostgresLoader primarily uses configuration directly from `tables.yml` and doesn't call `SimpleMySQLReplicator` methods. However, if there's any logic that checks extraction strategies, update it:

**Search for** any references to old strategy names:
```bash
# Search for old strategy names in postgres_loader.py
grep -n "chunked_incremental\|small_table" etl_pipeline/core/postgres_loader.py
```

**If found**, update conditional logic:
```python
# OLD (if it exists)
if extraction_strategy == 'chunked_incremental':
    # Handle chunked incremental logic

if extraction_strategy == 'small_table':
    # Handle small table logic

# NEW
if extraction_strategy == 'incremental_chunked':
    # Handle chunked incremental logic

if extraction_strategy == 'incremental':
    # Handle incremental logic (was 'small_table')
```

**Note**: Based on code review, PostgresLoader doesn't appear to use extraction strategies directly - it focuses on incremental columns and load strategies. This step may not be needed, but it's good to verify.

### Step 3.2: Verify PostgresSchema Compatibility

**File**: `etl_pipeline/core/postgres_schema.py`

**No changes needed** - PostgresSchema works independently and doesn't interact with SimpleMySQLReplicator methods.

**Verification only**:
```python
# Test that PostgresSchema still works with new tables.yml
schema_adapter = PostgresSchema()
mysql_schema = schema_adapter.get_table_schema_from_mysql('patient')
pg_create = schema_adapter.adapt_schema('patient', mysql_schema)
print(f"Schema conversion still works: {len(pg_create) > 0}")
```

### Step 3.3: Test Configuration Compatibility

**Create test script** `test_downstream_compatibility.py`:
```python
#!/usr/bin/env python3
"""
Test that downstream components work with refactored configuration.
"""

from etl_pipeline.core.postgres_loader import PostgresLoader
from etl_pipeline.core.postgres_schema import PostgresSchema

def test_downstream_compatibility():
    """Test that downstream components handle new strategy names."""
    
    print("Testing downstream component compatibility...")
    
    # Test PostgresLoader configuration reading
    try:
        loader = PostgresLoader()
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        for table_name in test_tables:
            config = loader.get_table_config(table_name)
            extraction_strategy = config.get('extraction_strategy', 'unknown')
            incremental_columns = config.get('incremental_columns', [])
            
            print(f"✅ {table_name}: strategy={extraction_strategy}, columns={len(incremental_columns)}")
            
            # Verify new strategy names are handled
            if extraction_strategy in ['incremental_chunked', 'incremental', 'full_table']:
                print(f"   ✅ Valid strategy: {extraction_strategy}")
            elif extraction_strategy in ['chunked_incremental', 'small_table']:
                print(f"   ❌ Old strategy found: {extraction_strategy} - needs regeneration")
                return False
            else:
                print(f"   ⚠️  Unknown strategy: {extraction_strategy}")
        
        print("✅ PostgresLoader configuration compatibility verified")
        
    except Exception as e:
        print(f"❌ PostgresLoader compatibility test failed: {e}")
        return False
    
    # Test PostgresSchema independence
    try:
        schema_adapter = PostgresSchema()
        print("✅ PostgresSchema initialization successful")
        
        # Test schema conversion (doesn't depend on SimpleMySQLReplicator)
        test_table = 'carrier'  # Usually a small table
        mysql_schema = schema_adapter.get_table_schema_from_mysql(test_table)
        
        if mysql_schema and 'table_name' in mysql_schema:
            print(f"✅ PostgresSchema schema extraction works: {test_table}")
        else:
            print(f"⚠️  PostgresSchema schema extraction issue: {test_table}")
        
    except Exception as e:
        print(f"❌ PostgresSchema compatibility test failed: {e}")
        return False
    
    print("\n" + "="*50)
    print("✅ All downstream components compatible!")
    print("="*50)
    return True

if __name__ == "__main__":
    success = test_downstream_compatibility()
    exit(0 if success else 1)
```

**Run the test**:
```bash
python test_downstream_compatibility.py
```

---

## Phase 4: Testing & Validation

Comprehensive testing to ensure the refactor works correctly.

### Step 4.1: Unit Test Updates

**File**: Update test files (if they exist)

**Find and update test method calls**:
```python
# OLD TEST
def test_copy_strategy_selection(self):
    strategy = self.replicator.get_copy_strategy('patient')
    self.assertIn(strategy, ['small', 'medium', 'large'])

# NEW TEST
def test_copy_method_selection(self):
    copy_method = self.replicator.get_copy_method('patient')
    self.assertIn(copy_method, ['small', 'medium', 'large'])

def test_extraction_strategy_validation(self):
    """Test that invalid strategies are handled gracefully."""
    # Mock invalid strategy in config
    self.replicator.table_configs['test_table'] = {
        'extraction_strategy': 'invalid_strategy'
    }
    strategy = self.replicator.get_extraction_strategy('test_table')
    self.assertEqual(strategy, 'full_table')  # Should fallback
```

### Step 4.2: Integration Testing

**Create test script** `test_refactor.py`:

```python
#!/usr/bin/env python3
"""
Test script to validate SimpleMySQLReplicator refactor
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from etl_pipeline.mysql.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.config import get_settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_refactor():
    """Test the refactored SimpleMySQLReplicator."""
    
    print("=" * 50)
    print("Testing SimpleMySQLReplicator Refactor")
    print("=" * 50)
    
    try:
        # Initialize replicator
        replicator = SimpleMySQLReplicator()
        print("✅ SimpleMySQLReplicator initialized successfully")
        
        # Test method renaming
        test_tables = ['patient', 'appointment', 'procedurelog']
        
        for table_name in test_tables:
            # Test copy method (renamed from copy strategy)
            copy_method = replicator.get_copy_method(table_name)
            print(f"✅ {table_name}: copy_method = {copy_method}")
            
            # Test extraction strategy
            extraction_strategy = replicator.get_extraction_strategy(table_name)
            print(f"✅ {table_name}: extraction_strategy = {extraction_strategy}")
            
            # Validate strategy
            is_valid = replicator._validate_extraction_strategy(extraction_strategy)
            print(f"✅ {table_name}: strategy_valid = {is_valid}")
            
            print()
        
        print("=" * 50)
        print("All tests passed! Refactor successful.")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_refactor()
    sys.exit(0 if success else 1)
```

**Run the test**:
```bash
python test_refactor.py
```

### Step 4.3: End-to-End Testing

**Test with actual table copying**:

```python
#!/usr/bin/env python3
"""
End-to-end test of the refactored replicator
"""

from etl_pipeline.mysql.simple_mysql_replicator import SimpleMySQLReplicator

def test_e2e():
    replicator = SimpleMySQLReplicator()
    
    # Test copying a small table
    test_table = 'carrier'  # Usually a small reference table
    
    print(f"Testing copy of {test_table}...")
    
    # Get strategies
    copy_method = replicator.get_copy_method(test_table)
    extraction_strategy = replicator.get_extraction_strategy(test_table)
    
    print(f"Copy method: {copy_method}")
    print(f"Extraction strategy: {extraction_strategy}")
    
    # Attempt copy
    success = replicator.copy_table(test_table)
    
    if success:
        print("✅ End-to-end test successful!")
    else:
        print("❌ End-to-end test failed!")
    
    return success

if __name__ == "__main__":
    test_e2e()
```

### Step 4.4: Configuration Validation

**Validate generated tables.yml**:

```python
#!/usr/bin/env python3
"""
Validate the generated tables.yml configuration
"""

import yaml
import os

def validate_tables_config():
    config_path = 'etl_pipeline/config/tables.yml'
    
    if not os.path.exists(config_path):
        print("❌ tables.yml not found. Run schema analyzer first.")
        return False
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    tables = config.get('tables', {})
    
    # Valid strategies after refactor
    valid_strategies = ['full_table', 'incremental', 'incremental_chunked']
    
    invalid_tables = []
    strategy_counts = {strategy: 0 for strategy in valid_strategies}
    
    for table_name, table_config in tables.items():
        strategy = table_config.get('extraction_strategy', 'unknown')
        
        if strategy in valid_strategies:
            strategy_counts[strategy] += 1
        else:
            invalid_tables.append((table_name, strategy))
    
    print("Tables Configuration Validation")
    print("=" * 40)
    print(f"Total tables: {len(tables)}")
    print(f"Strategy distribution:")
    for strategy, count in strategy_counts.items():
        print(f"  {strategy}: {count}")
    
    if invalid_tables:
        print(f"\n❌ Invalid strategies found:")
        for table_name, strategy in invalid_tables[:10]:  # Show first 10
            print(f"  {table_name}: {strategy}")
        if len(invalid_tables) > 10:
            print(f"  ... and {len(invalid_tables) - 10} more")
        return False
    else:
        print("\n✅ All strategies are valid!")
        return True

if __name__ == "__main__":
    validate_tables_config()
```

---

## Phase 5: Documentation Updates

Update all related documentation to reflect the refactor.

### Step 5.1: Update README Files

**Update project README** to reflect new terminology:

```markdown
## ETL Pipeline Architecture

### SimpleMySQLReplicator

The replicator uses two complementary systems:

#### Copy Methods (Performance-Based)
Determine **HOW** to copy data based on table size:
- `small`: Direct INSERT...SELECT (< 1MB)
- `medium`: Chunked with LIMIT/OFFSET (1-100MB)  
- `large`: Progress-tracked chunks (> 100MB)

#### Extraction Strategies (Business Logic)
Determine **WHAT** to copy based on configuration:
- `full_table`: Complete table replacement
- `incremental`: Copy only new/changed records
- `incremental_chunked`: Small batches for large incremental tables
```

### Step 5.2: Update Code Comments

**Add comprehensive docstrings** to key methods:

```python
def copy_table(self, table_name: str, force_full: bool = False) -> bool:
    """
    Copy a single table from source to target with clear separation of concerns.
    
    This method demonstrates the refactored architecture:
    1. Extraction Strategy (WHAT to copy): Determined by business logic and configuration
    2. Copy Method (HOW to copy): Determined by table size and performance characteristics
    
    Args:
        table_name: Name of the table to copy
        force_full: Force full copy (ignore incremental configuration)
        
    Returns:
        True if successful, False otherwise
        
    Example:
        >>> replicator = SimpleMySQLReplicator()
        >>> success = replicator.copy_table('patient')
        >>> # Logs: "Using 'incremental' extraction strategy with 'medium' copy method for patient"
    """
```

### Step 5.3: Create Migration Guide

**Create** `MIGRATION_GUIDE.md`:

```markdown
# Migration Guide: SimpleMySQLReplicator Refactor

## Overview
This guide helps migrate from the old naming conventions to the new refactored architecture.

## Method Name Changes

| Old Method | New Method | Purpose |
|------------|------------|---------|
| `get_copy_strategy()` | `get_copy_method()` | Returns size-based copy method |
| N/A | `get_extraction_strategy()` | Returns config-based extraction strategy |

## Strategy Name Changes

| Old Strategy | New Strategy | Meaning |
|--------------|--------------|---------|
| `small_table` | `incremental` | Copy incrementally regardless of size |
| `chunked_incremental` | `incremental_chunked` | Chunked incremental for large tables |

## Code Migration Examples

### Before Refactor
```python
copy_strategy = replicator.get_copy_strategy('patient')  # Confusing name
# Returns: 'small', 'medium', 'large' (size-based)

extraction_strategy = replicator.get_extraction_strategy('patient') 
# Returns: 'small_table', 'chunked_incremental' (inconsistent names)
```

### After Refactor  
```python
copy_method = replicator.get_copy_method('patient')  # Clear purpose
# Returns: 'small', 'medium', 'large' (HOW to copy - performance)

extraction_strategy = replicator.get_extraction_strategy('patient')
# Returns: 'incremental_chunked' (WHAT to copy - business logic)
```
```

---

## Validation Checklist

Use this checklist to ensure the refactor is complete and working:

### ✅ Schema Analyzer Updates
- [ ] `determine_extraction_strategy()` generates correct strategy names
- [ ] `_validate_extraction_strategy()` method added
- [ ] Validation integrated into configuration generation
- [ ] New `tables.yml` generated with correct strategies

### ✅ SimpleMySQLReplicator Updates  
- [ ] `get_copy_strategy()` renamed to `get_copy_method()`
- [ ] `copy_table()` method updated to handle new strategy names
- [ ] `incremental_chunked` strategy handling added
- [ ] Strategy validation added
- [ ] All method references updated

### ✅ Testing Complete
- [ ] Unit tests updated/passing
- [ ] Integration tests passing  
- [ ] End-to-end test successful
- [ ] Configuration validation passing
- [ ] No invalid strategies in generated config

### ✅ Documentation Updated
- [ ] README files updated
- [ ] Code comments and docstrings updated
- [ ] Migration guide created
- [ ] Architecture documentation reflects changes

## Rollback Plan

If issues arise, rollback steps:

1. **Revert schema analyzer**: Git checkout previous version of `analyze_opendental_schema.py`
2. **Regenerate config**: Run schema analyzer to restore old `tables.yml`
3. **Revert replicator**: Git checkout previous version of `simple_mysql_replicator.py`
4. **Test**: Ensure system works with old configuration

## Post-Refactor Benefits

After completing this refactor:

1. **Clear Separation**: Copy methods and extraction strategies have distinct purposes
2. **Intuitive Names**: Method names clearly indicate their function
3. **Consistent Configuration**: Schema analyzer and replicator use same strategy names  
4. **Better Maintainability**: Future developers can easily understand the code
5. **Validation**: Built-in validation prevents configuration mismatches
6. **Extensibility**: Easy to add new strategies or copy methods

## Troubleshooting

### Issue: "Unknown extraction strategy" errors
**Cause**: Old strategy names in `tables.yml`
**Solution**: Regenerate `tables.yml` with updated schema analyzer

### Issue: Method not found errors  
**Cause**: References to old `get_copy_strategy()` method
**Solution**: Find and replace all references with `get_copy_method()`

### Issue: Tests failing
**Cause**: Test assertions using old method names
**Solution**: Update test files to use new method names

---

## Conclusion

This refactor creates a cleaner, more maintainable architecture for the SimpleMySQLReplicator. The clear separation between copy methods (performance) and extraction strategies (business logic) will make the codebase much easier to understand and extend.

The changes are breaking changes, but since you're still in development, this is the perfect time to implement them for long-term benefits.