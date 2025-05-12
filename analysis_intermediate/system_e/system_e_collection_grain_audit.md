# System E Collection Models - Grain and Deduplication Audit

## Overview

This document summarizes the findings and recommendations from the grain and deduplication audit of 
the six intermediate models in the System E Collection module. The audit evaluated the grain 
definition, primary key usage, and potential duplicate data issues.

## Models Analyzed

1. `int_billing_statements`
2. `int_statement_metrics`
3. `int_collection_campaigns`
4. `int_collection_tasks` 
5. `int_collection_communication`
6. `int_collection_metrics`

## Key Findings and Recommendations

### 1. int_billing_statements

**Findings:**
- The model uses the generated `billing_statement_id` as the primary key instead of the source `statement_id`
- The grain is at the individual statement level (1 row per statement)
- Uses `ROW_NUMBER()` to create a synthetic key
- Configured as incremental with `unique_key='billing_statement_id'`

**Recommendations:**
- Replace `ROW_NUMBER()` with a more stable key generation method to avoid inconsistencies during incremental loads
- Consider using the source `statement_id` as the unique key or generate a stable hash key
- Add a test to verify that `statement_id` is not duplicated (1:1 mapping from source)

### 2. int_statement_metrics

**Findings:**
- Uses a compound key as unique_key: `['snapshot_date', 'metric_type']`
- The grain is at the metric type and snapshot date level, further partitioned by delivery_method or campaign_id
- Configured as incremental
- Uses `ROW_NUMBER()` to create a synthetic key `statement_metric_id`

**Recommendations:**
- Modify the unique_key to include all grain columns: `['snapshot_date', 'metric_type', 'delivery_method', 'campaign_id']`
- Replace `ROW_NUMBER()` with a stable hash key for better incremental stability
- Add tests to verify grain uniqueness across all dimensions

### 3. int_collection_campaigns

**Findings:**
- Uses `campaign_id` as the unique key
- The grain is at the campaign level (1 row per campaign)
- Configured as a table (not incremental)
- Hard-coded campaign definitions with joins to dynamic account data

**Recommendations:**
- No grain issues as `campaign_id` is a strong natural key
- Add a test to ensure `campaign_name` is unique for better business usability
- For future development, separate static campaign definitions from dynamic metrics for better maintainability

### 4. int_collection_communication

**Findings:**
- Uses `collection_communication_id` as the unique key
- The grain is at the communication event level (1 row per communication)
- Uses `ROW_NUMBER()` to generate `collection_communication_id`
- Linkage to `commlog_id` from source data

**Recommendations:**
- Add a test to verify that `commlog_id` is not duplicated within the model
- Add foreign key tests to verify integrity with tasks and campaigns
- Replace `ROW_NUMBER()` with a stable hash key for better incremental reliability

### 5. int_collection_tasks

**Findings:**
- Uses `collection_task_id` as the unique key
- The grain is at the task level (1 row per task)
- Uses `ROW_NUMBER()` to generate `collection_task_id`
- Links back to source `task_id`

**Recommendations:**
- Add a test to verify there are no duplicate `task_id` values within the model
- Implement more robust text extraction patterns or reference tables for complex text parsing
- Replace `ROW_NUMBER()` with a stable hash key
- Add validation tests for text-extracted values like promised amounts and dates

### 6. int_collection_metrics

**Findings:**
- Uses a compound key as unique_key: `['snapshot_date', 'campaign_id', 'user_id', 'metric_level']`
- The grain is at the metric level, partitioned by snapshot date, campaign, user, and metric level
- Configured as incremental
- Uses `ROW_NUMBER()` to generate `metric_id`

**Recommendations:**
- The unique key correctly includes all dimensions of the grain
- Add tests to verify no duplicates appear when using the full compound key
- Add boundaries/checks for derived metrics (collection_rate, etc.) to validate they're in reasonable ranges (0-1.0)
- For performance improvements, consider materializing the underlying CTEs in large production databases

## General Recommendations

1. **Replace ROW_NUMBER() with Hash Keys**: For all models using ROW_NUMBER() to generate IDs, replace with:
   ```sql
   MD5(CAST(<<grain columns>> AS VARCHAR)) AS stable_surrogate_key
   ```

2. **Add Cross-Model Validation Tests**: Create tests that validate relationships between models, such as:
   - Task counts in int_collection_metrics match actual counts in int_collection_tasks
   - Statement metrics align with billing_statements counts
   - Communications link correctly to tasks

3. **Document Grain Explicitly**: In model YML files, explicitly document the grain for each model

4. **Consider Adding Slowly Changing Dimension Logic**: For campaign and task data that changes over time, implement Type 2 SCD patterns to track changes

5. **Add Data Quality Tests**: Implement tests that validate derived metrics like collection_rate are within expected ranges (0-1.0)

6. **Standardize Key Naming**: Standardize to either `id` or `_id` suffix consistently across all models

## Validation SQL

Validation SQL queries have been provided in the file `analysis/system_e_collection_grain_validation.sql`. These queries should be run in DBeaver to validate the grain, check for duplicates, and verify cross-model relationships.

## Next Steps

1. Implement hash-based key generation for models using ROW_NUMBER()
2. Update unique_key definitions in models to include all grain columns
3. Add dbt tests based on the validation queries
4. Document grain clearly in each model's YAML file
5. Run the validation SQL regularly as part of the CI/CD workflow