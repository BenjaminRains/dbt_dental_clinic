# Macro Implementation Checklist

## Overview

This checklist works in conjunction with:
- **`naming_conventions_models_data_type.md`** - Defines the standardized naming conventions and data type transformations
- **`naming_conventions_implementation_strategy.md`** - Outlines the phased migration plan for implementing these standards

Together, these documents provide a comprehensive framework for standardizing staging models across the dbt_dental_clinic project.

---

## Implementation Standards Per Model

### ✅ Macro Implementation Checklist (per model):

#### **ID Columns:**
- [ ] Replace manual ID transformations with `{{ transform_id_columns([...]) }}`
  - **Note**: Use dictionary structure with `source` and `target` keys:
    ```sql
    {{ transform_id_columns([
        {'source': '"RefAttachNum"', 'target': 'ref_attach_id'},
        {'source': '"PatNum"', 'target': 'patient_id'}
    ]) }}
    ```
  - Always include quotes around source column names (e.g., `'"PatNum"'`)
- [ ] Standardize primary key naming: `*_id` pattern
- [ ] Group related ID transformations together

#### **Boolean Columns:**
- [ ] Replace manual case statements with `{{ convert_opendental_boolean('"ColumnName"') }}`
- [ ] Verify all 0/1 integer fields are converted
- [ ] Check for `COALESCE` patterns that can be simplified

#### **Date Columns:**
- [ ] Replace manual null date handling with `{{ clean_opendental_date('"ColumnName"') }}`
- [ ] Handle `'0001-01-01'` and `'1900-01-01'` patterns
- [ ] Convert timestamp fields appropriately

#### **Metadata Columns:**
- [ ] Replace manual metadata with `{{ standardize_metadata_columns() }}`
  - **Usage**: Always include this macro. Pass source columns as parameters, or `none` if not available:
    ```sql
    -- When all metadata columns exist:
    {{ standardize_metadata_columns(
        created_at_column='"DateTStamp"',
        updated_at_column='"DateTStamp"'
    ) }}
    
    -- When some columns don't exist:
    {{ standardize_metadata_columns(
        created_at_column='"DateTEntry"',
        updated_at_column='"DateTEntry"',
        created_by_column=none
    ) }}
    ```
  - **Important**: Always include this macro even if source table lacks metadata columns
- [ ] Ensure consistent `_loaded_at`, `_created_at`, `_updated_at`, `_created_by_user_id`
- [ ] Verify source column mapping is correct

#### **CTE Structure:**
- [ ] Convert CamelCase CTEs to snake_case: `SourceData` → `source_data` 
- [ ] Use descriptive CTE names: `staged` → `renamed_columns`
- [ ] Follow consistent CTE order: source → transformations → final

#### **Code Quality:**
- [ ] Remove redundant COALESCE statements 
- [ ] Consolidate similar transformation patterns
- [ ] Add comments for complex business logic
- [ ] Verify column ordering (IDs, business, metadata)


## Quality Gates

### After Each Phase:
- [ ] All models compile successfully 
- [ ] dbt tests pass for updated models
- [ ] Data validation shows no discrepancies
- [ ] Team review of changes completed
- [ ] Documentation updated

### Final Validation:
- [ ] 100% staging models use standardized macros
- [ ] No manual boolean/date/ID transformations remain
- [ ] All CTEs follow snake_case convention
- [ ] Metadata columns standardized across all models
- [ ] Performance benchmarks maintained or improved