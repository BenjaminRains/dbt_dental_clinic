# Macro Implementation Checklist

## Phase 1A: High-Priority Models (10-15 models)

### Core Business Entities
- [ ] `stg_opendental__patient.sql` - ⭐ **PRIORITY 1** (already partially compliant)
- [ ] `stg_opendental__appointment.sql` - ⭐ **PRIORITY 1** 
- [ ] `stg_opendental__procedurelog.sql` - ⭐ **PRIORITY 1**
- [ ] `stg_opendental__clinic.sql` - ⭐ **PRIORITY 1**

### High-Dependency Models  
- [x] `stg_opendental__provider.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__payment.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__paysplit.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)

### Complex Logic Models
- [x] `stg_opendental__histappointment.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__insverifyhist.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__procedurecode.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)

## Phase 1B: Medium-Priority Models (20-30 models)

### Reference Data
- [x] `stg_opendental__referral.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__recall.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__adjustment.sql` - ✅ **COMPLETED** (fixed source references, uses macros)

### Pattern Groups (implement as batches)
#### Sheet Models
- [x] `stg_opendental__sheet.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__sheetfield.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__sheetfielddef.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__sheetdef.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)

#### Task Models  
- [x] `stg_opendental__task.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__taskhist.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__tasklist.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__tasknote.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__tasksubscription.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__taskunread.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)

#### User Models
- [x] `stg_opendental__userod.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__userodpref.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__userodapptview.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__usergroup.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__usergroupattach.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)

## Phase 1C: Low-Priority Models (Batch process remaining ~100+ models)

### Simple Lookups
- [x] `stg_opendental__zipcode.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__recalltype.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__rxnorm.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [x] `stg_opendental__pharmacy.sql` - ✅ **COMPLETED** (uses all macros, snake_case CTEs)
- [ ] All remaining simple models

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

---

