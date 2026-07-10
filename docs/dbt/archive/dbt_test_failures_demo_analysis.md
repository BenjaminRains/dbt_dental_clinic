# dbt Test Failures Analysis - Demo Database

**Date:** 2025-01-XX  
**Target:** demo  
**Total Tests:** 1256  
**Results:** PASS=1150 WARN=48 ERROR=58 SKIP=0

## Summary by Category

### Category 1: Accepted Values Tests (Data Quality Issues)
**Count:** 3 failures

1. **accepted_values_fact_appointment_appointment_type** - 5 rows
   - Invalid appointment_type values (not in allowed list)
   - **Status:** Previously fixed but still failing - needs investigation

2. **accepted_values_mart_appointment_summary_clinic_id__0** - 5 rows
   - clinic_id = 0 (should be NULL instead of 0)

3. **accepted_values_mart_new_patient_clinic_id__0** - 5 rows
   - clinic_id = 0 (should be NULL instead of 0)

4. **accepted_values_mart_provider_performance_provider_status** - Database Error
   - Type mismatch: test expects integer but column has text values ('Active', 'Inactive', etc.)
   - **Error:** `invalid input syntax for type integer: "Active"`

---

### Category 2: Database Syntax Errors (Test Definition Issues)
**Count:** 12 errors

These are errors in the test definitions themselves, not data issues:

1. **dbt_utils_expression_is_true** (multiple tests) - Syntax errors
   - Duplicate column names in WHERE clauses
   - Examples:
     - `appointment_length_minutes appointment_length_minutes is null` (missing operator)
     - `treatment_time_minutes treatment_time_minutes is null` (missing operator)
     - `is_insurance_payment is_insurance_payment = ...` (missing operator)
     - `splits_match_payment splits_match_payment is true` (missing operator)
     - `provider_id provider_id is null` (missing operator)
     - `actual_fee actual_fee >= 0` (missing operator)
     - `recovery_priority_score (recovery_priority_score > ...` (function call syntax error)
     - `opportunity_subtype (opportunity_type = ...` (function call syntax error)

2. **check_link_types_array_valid** - Type mismatch
   - Column is `text` but test uses `array_length()` function
   - **Error:** `function array_length(text, integer) does not exist`

3. **check_linked_patient_ids_array_valid** - Type mismatch
   - Column is `text` but test uses `array_length()` function
   - **Error:** `function array_length(text, integer) does not exist`

---

### Category 3: Missing Columns (Schema Mismatch)
**Count:** 13 errors

These tests reference columns that don't exist in the models:

1. **fact_appointment._created_at** - Column doesn't exist
2. **fact_communication._loaded_at** - Column doesn't exist
3. **mart_appointment_summary.provider_first_name** - Column doesn't exist
4. **mart_appointment_summary.provider_preferred_name** - Column doesn't exist
5. **mart_appointment_summary.provider_last_name** - Column doesn't exist
6. **mart_hygiene_retention.hygienist_name** - Column doesn't exist
7. **mart_hygiene_retention.provider_name** - Column doesn't exist
8. **mart_new_patient.primary_provider_name** - Column doesn't exist
9. **mart_patient_retention.primary_provider_name** - Column doesn't exist
10. **mart_production_summary._created_at** - Column doesn't exist
11. **mart_production_summary.provider_first_name** - Column doesn't exist
12. **mart_production_summary.provider_preferred_name** - Column doesn't exist
13. **mart_production_summary.provider_last_name** - Column doesn't exist
14. **mart_provider_performance._created_at** - Column doesn't exist
15. **mart_provider_performance._loaded_at** - Column doesn't exist
16. **mart_provider_performance._updated_at** - Column doesn't exist
17. **mart_provider_performance.provider_first_name** - Column doesn't exist
18. **mart_provider_performance.provider_last_name** - Column doesn't exist
19. **mart_provider_performance.provider_preferred_name** - Column doesn't exist

---

### Category 4: Not Null Tests (Data Completeness Issues)
**Count:** 9 failures

1. **not_null_dim_procedure_base_units** - 1727 rows with NULL
2. **not_null_dim_procedure_description** - 11 rows with NULL
3. **not_null_dim_procedure_treatment_area** - 1727 rows with NULL
4. **not_null_dim_procedure_treatment_area_desc** - 1727 rows with NULL
5. **not_null_fact_communication_communication_mode** - 20168 rows with NULL
6. **not_null_fact_payment__created_at** - 27554 rows with NULL
7. **not_null_mart_production_summary_provider_status_description** - 14182 rows with NULL
8. **not_null_mart_provider_performance_provider_status** - 14182 rows with NULL
9. **not_null_mart_provider_performance_specialty_description** - 14182 rows with NULL

---

### Category 5: Range/Validation Tests (Business Logic Issues)
**Count:** 6 failures

1. **check_billing_cycle_day_valid** - 10000 rows failing
   - billing_cycle_day validation issue

2. **dbt_expectations_expect_column_values_to_be_between_fact_payment_payment_type_id** - 27554 rows
   - payment_type_id outside expected range (5-0)

3. **dbt_expectations_expect_column_values_to_be_between_mart_appointment_summary_lost_appointment_rate** - 3039 rows
   - lost_appointment_rate > 100% (invalid)

4. **dbt_expectations_expect_column_values_to_be_in_set_mart_patient_retention_clinic_id** - 7106 rows
   - clinic_id not in expected set

5. **dbt_expectations_expect_table_row_count_to_be_between** (4 tests)
   - Row counts outside expected ranges:
     - mart_appointment_summary: Expected 1-10000, actual outside range
     - mart_hygiene_retention: Expected 100-100000, actual outside range
     - mart_production_summary: Expected 1-10000, actual outside range
     - mart_provider_performance: Expected 1-10000, actual outside range

6. **dbt_utils_expression_is_true_dim_clinic_clinic_id_IS_NULL** - 5 rows
   - clinic_id should be NULL but isn't

7. **dbt_utils_expression_is_true_fact_appointment_case_when_is_no_show** - 4124 rows
   - Logic validation failing

---

## Priority Classification

### 🔴 Critical (Blocking/Data Quality)
- **Category 1:** Accepted values tests (3-4 issues)
  - These indicate data doesn't match expected business rules
  - **Action:** Fix data generator or transformation logic

### 🟡 High (Test Definition Issues)
- **Category 2:** Syntax errors in tests (12 issues)
  - Tests are broken, not data
  - **Action:** Fix test definitions in YAML files

- **Category 3:** Missing columns (13 issues)
  - Tests reference columns that don't exist
  - **Action:** Either add columns to models or remove/update tests

### 🟢 Medium (Data Completeness)
- **Category 4:** Not null tests (9 issues)
  - Synthetic data may legitimately have NULLs
  - **Action:** Decide if NULLs are acceptable or fix data generation

### 🔵 Low (Business Logic Validation)
- **Category 5:** Range/validation tests (6-7 issues)
  - Some may be expected for synthetic data
  - **Action:** Review if failures are acceptable or adjust test thresholds

---

## Recommended Fix Order

1. **Fix test syntax errors** (Category 2) - Quick wins, tests are broken
2. **Fix missing column tests** (Category 3) - Either add columns or update tests
3. **Investigate accepted_values failures** (Category 1) - Data quality issues
4. **Review not_null failures** (Category 4) - Decide if NULLs are acceptable
5. **Review range/validation failures** (Category 5) - May be expected for demo data

---

## Notes

- Many failures may be expected for synthetic/demo data
- Some tests may need to be disabled or adjusted for demo environment
- Missing columns suggest models may have changed but tests weren't updated
- Syntax errors in tests need immediate fixing (broken test definitions)
