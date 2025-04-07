# Insurance Verification System Investigation

## Overview
We're investigating the relationship between insurance subscriber records (`inssub`) and their verification records in both `insverify` and `insverifyhist` tables. Currently, we have 791 failing records in our relationship tests, and we need to understand the business rules to properly configure these tests.

## Current Test Structure

### Test 1: Current Verifications
```yaml
- dbt_utils.relationships_where:
    column_name: inssub_id
    to: ref('stg_opendental__insverify')
    field: foreign_key_id
    from_condition: >
      inssub_id is not null 
      and effective_date is not null 
      and effective_date >= '2020-01-01'
      and (termination_date is null or termination_date >= current_date)
    to_condition: "verify_type = 1"
```

### Test 2: Historical Verifications
```yaml
- dbt_utils.relationships_where:
    column_name: inssub_id
    to: ref('stg_opendental__insverifyhist')
    field: foreign_key_id
    from_condition: >
      inssub_id is not null 
      and effective_date is not null 
      and effective_date >= '2020-01-01'
      and effective_date <= current_date
      and (termination_date is null or termination_date >= current_date)
      and (effective_date <= current_date - interval '30 days' or last_modified_at <= current_date - interval '7 days')
    to_condition: "verify_type = 1"
```

## Data Structure

### Insurance Verification Tables
1. `insverify`: Current verification records
2. `insverifyhist`: Historical verification records

### Key Fields
- `foreign_key_id`: References different entities based on verify_type
  - `verify_type = 1`: References `inssub.inssub_id` (95% match rate)
  - `verify_type = 2`: References another entity (62% match rate with inssub)
- `verify_type`: Indicates the type of verification (1 or 2)
- `last_verified_date`: When the insurance was last verified
- `last_assigned_date`: When the verification task was last assigned

## Questions for Stakeholders

### 1. Verification Requirements
- Should every active insurance subscriber record have a verification in `insverify`?
- Should every active insurance subscriber record have a verification in `insverifyhist`?
- What is the expected verification frequency for insurance records?

### 2. Time-based Rules
- Why do we only check records from 2020 onwards?
- What is the significance of the 30-day/7-day condition in the history test?
- How should we handle historical records vs. current records?

### 3. Verification Types
- What is the business meaning of `verify_type = 1` vs `verify_type = 2`?
- Should we be checking both types in our relationship tests?
- Why do we have different match rates for different verify types?

### 4. Data Quality Expectations
- The documentation mentions "95% match rate" - is this an acceptable threshold?
- Should we adjust our tests to account for this expected mismatch?
- Are there known business cases where verification records might be missing?

## Next Steps
1. Review failing records to identify patterns
2. Gather stakeholder input on the above questions
3. Adjust test conditions based on business rules
4. Consider implementing separate tests for different verification scenarios

## Example Queries for Investigation
```sql
-- Count of failing records by insurance plan
SELECT 
    i.insurance_plan_id,
    COUNT(*) as failing_count
FROM stg_opendental__inssub i
LEFT JOIN stg_opendental__insverifyhist v 
    ON i.inssub_id = v.foreign_key_id 
    AND v.verify_type = 1
WHERE i.inssub_id is not null 
    AND i.effective_date is not null 
    AND i.effective_date >= '2020-01-01'
    AND i.effective_date <= current_date
    AND (i.termination_date is null or i.termination_date >= current_date)
    AND (i.effective_date <= current_date - interval '30 days' or i.last_modified_at <= current_date - interval '7 days')
    AND v.foreign_key_id IS NULL
GROUP BY i.insurance_plan_id
ORDER BY failing_count DESC;

-- Sample of failing records with key dates
SELECT 
    i.inssub_id,
    i.insurance_plan_id,
    i.effective_date,
    i.termination_date,
    i.last_modified_at,
    i.subscriber_external_id
FROM stg_opendental__inssub i
LEFT JOIN stg_opendental__insverifyhist v 
    ON i.inssub_id = v.foreign_key_id 
    AND v.verify_type = 1
WHERE i.inssub_id is not null 
    AND i.effective_date is not null 
    AND i.effective_date >= '2020-01-01'
    AND i.effective_date <= current_date
    AND (i.termination_date is null or i.termination_date >= current_date)
    AND (i.effective_date <= current_date - interval '30 days' or i.last_modified_at <= current_date - interval '7 days')
    AND v.foreign_key_id IS NULL
LIMIT 10;
``` 