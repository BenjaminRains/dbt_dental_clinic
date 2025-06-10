# Macro Implementation Checklist

## Implementation Standards Per Model

### ✅ Macro Implementation Checklist (per model):

#### **ID Columns:**
- [ ] Replace manual ID transformations with `{{ transform_id_columns([...]) }}`
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