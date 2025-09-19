# Mart Model Standardization Strategy

## Overview

This document defines the SQL model file standardization patterns and technical implementation for mart models. It works in conjunction with:
- **`mart_yml_standardization_strategy.md`** - Provides detailed documentation template patterns and standards
- **`mart_template_to_model_mapping_guide.md`** - Maps each model to appropriate documentation templates and implementation phases
- **`mart_roadmap_checklist.md`** - Provides the detailed implementation roadmap and quality gates

Together, these documents provide a comprehensive framework for standardizing both SQL files and documentation across all mart models in the dbt_dental_clinic project.

---

## Strategy Overview

After analyzing your mart models (dimension tables, fact tables, and summary marts), I've identified the need for **three separate but complementary standardization strategies**:

1. **SQL Model File Standardization** - Technical patterns and code structure
2. **Documentation File Standardization** - Business context and testing patterns
3. **Model Type-Specific Patterns** - Dimension, Fact, and Summary mart variations

## Part 1: SQL Model File Standardization

### Current State Analysis

**Strengths Observed:**
- ✅ Good use of dimensional modeling principles
- ✅ Consistent primary key patterns across model types
- ✅ Comprehensive business logic in summary marts
- ✅ Proper foreign key relationships
- ✅ Some existing documentation structure

**Areas Needing Standardization:**
- ❌ Inconsistent config block patterns across model types
- ❌ Variable CTE organization and complexity
- ❌ Different approaches to business logic documentation
- ❌ Inconsistent patterns for handling complex aggregations
- ❌ Variable performance optimization strategies
- ❌ Mixed approaches to metadata handling

### 1.1 Configuration Block Standards by Model Type

#### **Dimension Tables (`dim_*`) - Standard Pattern:**
```sql
{{
    config(
        materialized='table',
        schema='marts',
        unique_key='[entity]_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['[entity]_id'], 'unique': true},
            {'columns': ['_updated_at']},
            {'columns': ['patient_id']}  -- Include if applicable
        ]
    )
}}
```

#### **Fact Tables (`fact_*`) - Standard Pattern:**
```sql
{{
    config(
        materialized='table',
        schema='marts',
        unique_key='[entity]_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['[entity]_id'], 'unique': true},
            {'columns': ['patient_id']},     -- Always include if applicable
            {'columns': ['provider_id']},    -- Always include if applicable
            {'columns': ['[date_field]']},   -- Date-based indexing
            {'columns': ['_updated_at']}
        ]
    )
}}
```

#### **Summary Marts (`mart_*`) - Standard Pattern:**
```sql
{{
    config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'dimension1_id', 'dimension2_id'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['date_id']},
            {'columns': ['patient_id']},     -- If applicable
            {'columns': ['provider_id']},    -- If applicable
            {'columns': ['clinic_id']}       -- If applicable
        ]
    )
}}
```

**Schema Specification Requirements:**
- Always explicitly specify `schema='marts'` (per naming conventions)
- Use `on_schema_change='fail'` for data integrity (per naming conventions)
- Use composite unique keys for summary marts with multiple dimensions
- Include performance-optimized indexes based on query patterns

### 1.2 CTE Organization Standards by Model Type

#### **Dimension Tables Pattern (3-5 CTEs):**
```sql
-- 1. Source data retrieval
with source_[entity] as (
    select * from {{ ref('stg_opendental__[entity]') }}
),

-- 2. Lookup/reference data (if needed)
[entity]_lookup as (
    select [key_fields], [lookup_fields]
    from {{ ref('stg_opendental__[lookup_table]') }}
),

-- 3. Business logic enhancement
[entity]_enhanced as (
    select
        -- Primary identification
        [primary_key],
        -- Business attributes
        [business_attributes],
        -- Calculated fields
        [calculated_fields],
        -- Metadata
        {{ standardize_mart_metadata() }}
    from source_[entity]
    left join [entity]_lookup using ([key_field])
),

-- 4. Final validation and filtering
final as (
    select * from [entity]_enhanced
    where [data_quality_filters]
)

select * from final
```

#### **Fact Tables Pattern (4-6 CTEs):**
```sql
-- 1. Source data retrieval
with source_[entity] as (
    select * from {{ ref('stg_opendental__[entity]') }}
),

-- 2. Related dimension lookups
[entity]_dimensions as (
    select [dimension_fields]
    from {{ ref('dim_[dimension]') }}
),

-- 3. Business logic and calculations
[entity]_calculated as (
    select
        -- Primary key
        [primary_key],
        -- Foreign keys
        [foreign_keys],
        -- Measures and calculations
        [measures],
        -- Flags and indicators
        [flags],
        -- Metadata
        {{ standardize_mart_metadata() }}
    from source_[entity]
    left join [entity]_dimensions using ([key])
),

-- 4. Final validation
final as (
    select * from [entity]_calculated
    where [data_quality_filters]
)

select * from final
```

#### **Summary Marts Pattern (6+ CTEs):**
```sql
-- 1. Base fact data
with [entity]_base as (
    select * from {{ ref('fact_[entity]') }}
),

-- 2. Dimension data
[entity]_dimensions as (
    select [dimension_fields]
    from {{ ref('dim_[dimension]') }}
),

-- 3. Date dimension
date_dimension as (
    select * from {{ ref('dim_date') }}
),

-- 4. Aggregations and calculations
[entity]_aggregated as (
    select
        [grouping_dimensions],
        [aggregate_measures],
        [calculated_metrics]
    from [entity]_base
    group by [grouping_dimensions]
),

-- 5. Business logic enhancement
[entity]_enhanced as (
    select
        *,
        case [business_logic] end as [category_field]
    from [entity]_aggregated
),

-- 6. Final integration
final as (
    select
        -- Date and dimensions
        [dimension_fields],
        -- Metrics and measures
        [metrics],
        -- Metadata
        {{ standardize_mart_metadata() }}
    from [entity]_enhanced
    left join [entity]_dimensions using ([key])
    left join date_dimension using ([date_key])
)

select * from final
```

### 1.3 Business Logic Patterns (Aligned with Naming Conventions)

**Foreign Key Reference Pattern:**
```sql
-- Always use proper foreign key references to dimension tables
dp.patient_id,
dp.primary_provider_id,
dp.clinic_id,
di.insurance_plan_id,
dpr.provider_id
```

**Measure Calculation Pattern:**
```sql
-- Financial measures with proper null handling
coalesce(sum(fc.billed_amount), 0) as total_billed,
coalesce(sum(fc.paid_amount), 0) as total_paid,
coalesce(sum(fc.write_off_amount), 0) as total_write_offs,
round(
    coalesce(sum(fc.paid_amount), 0) / 
    nullif(coalesce(sum(fc.billed_amount), 0), 0) * 100, 2
) as collection_rate
```

**Flag Creation Pattern:**
```sql
-- Business logic flags with clear naming
case 
    when total_balance = 0 then 'No Balance'
    when balance_over_90_days > 0 then 'High Risk'
    when balance_61_90_days > 0 then 'Medium Risk'
    when balance_31_60_days > 0 then 'Low Risk'
    else 'Current'
end as aging_risk_category,

case 
    when collection_rate >= 95 then 'Excellent'
    when collection_rate >= 90 then 'Good'
    when collection_rate >= 85 then 'Fair'
    else 'Poor'
end as collection_performance_tier
```

**Percentage Calculation Pattern:**
```sql
-- Safe percentage calculations with null handling
round(
    coalesce(balance_0_30_days, 0)::numeric / 
    nullif(coalesce(total_balance, 0), 0) * 100, 2
) as pct_current,

round(
    coalesce(balance_over_90_days, 0)::numeric / 
    nullif(coalesce(total_balance, 0), 0) * 100, 2
) as pct_over_90
```

### 1.4 Metadata Fields Pattern (Standardized Approach)

**Mart Metadata Macro Usage:**
```sql
-- Use the standardized macro for all mart models
{{ standardize_mart_metadata() }}

-- This macro provides:
-- _loaded_at: ETL pipeline timestamp (preserved from staging)
-- _created_at: Original creation timestamp (preserved from staging)
-- _updated_at: Last update timestamp (preserved from staging)
-- _transformed_at: dbt mart processing timestamp (current_timestamp)
-- _mart_refreshed_at: Mart-specific refresh timestamp (current_timestamp)

-- For manual implementation (if macro not available):
-- _loaded_at,                              -- From ETL pipeline (preserved from staging)
-- _created_at,                             -- Original creation timestamp (preserved from staging)
-- _updated_at,                             -- Last update (preserved from staging)
-- current_timestamp as _transformed_at,    -- dbt processing time
-- current_timestamp as _mart_refreshed_at  -- Mart refresh time
```

**Metadata Macro Benefits:**
- **Consistency**: All mart models use the same metadata pattern
- **Data Lineage**: Preserves source metadata from staging/intermediate models
- **Processing Tracking**: Adds mart-specific transformation timestamps
- **Maintainability**: Centralized metadata logic reduces code duplication

### 1.5 Header Documentation Pattern

**Required Header for All Mart Models:**
```sql
{{
    config(
        [config_settings]
    )
}}

/*
    [Model Type] model for [entity_name]
    Part of System [X]: [System_Name]
    
    This model:
    1. [Primary purpose]
    2. [Secondary purpose]
    3. [Additional purposes]
    
    Business Logic Features:
    - [Feature 1]: [Description]
    - [Feature 2]: [Description]
    
    Key Metrics:
    - [Metric 1]: [Description]
    - [Metric 2]: [Description]
    
    Data Quality Notes:
    - [Known issue 1]: [Description and handling]
    - [Known issue 2]: [Description and handling]
    
    Performance Considerations:
    - [Performance note 1]
    - [Performance note 2]
    
    Dependencies:
    - [Upstream model 1]: [Purpose]
    - [Upstream model 2]: [Purpose]
*/
```

### 1.6 Model Type-Specific Patterns

#### **Dimension Table Specific Patterns:**

**Patient Dimension Enhancement:**
```sql
-- Patient-specific business logic
case 
    when age < 18 then 'Minor'
    when age between 18 and 64 then 'Adult'
    when age >= 65 then 'Senior'
    else 'Unknown'
end as age_category,

case 
    when estimated_balance = 0 then 'No Balance'
    when estimated_balance > 0 then 'Outstanding Balance'
    else 'Credit Balance'
end as balance_status
```

**Provider Dimension Enhancement:**
```sql
-- Provider-specific business logic
case 
    when specialty = 'General' then 'General Practice'
    when specialty in ('Orthodontics', 'Oral Surgery') then 'Specialist'
    else 'Other'
end as provider_category,

case 
    when is_active and is_primary then 'Active Primary'
    when is_active then 'Active'
    else 'Inactive'
end as provider_status
```

#### **Fact Table Specific Patterns:**

**Appointment Fact Enhancement:**
```sql
-- Appointment-specific calculations
case 
    when is_completed then 'Completed'
    when is_no_show then 'No Show'
    when is_cancelled then 'Cancelled'
    else 'Scheduled'
end as appointment_status,

case 
    when appointment_length_minutes <= 30 then 'Short'
    when appointment_length_minutes <= 60 then 'Standard'
    when appointment_length_minutes <= 120 then 'Long'
    else 'Extended'
end as appointment_duration_category
```

**Payment Fact Enhancement:**
```sql
-- Payment-specific calculations
case 
    when payment_amount > 0 then 'Payment'
    when payment_amount < 0 then 'Refund'
    else 'Zero'
end as payment_type,

case 
    when payment_date <= service_date then 'Pre-payment'
    when payment_date <= service_date + interval '30 days' then 'Timely'
    when payment_date <= service_date + interval '90 days' then 'Late'
    else 'Very Late'
end as payment_timing_category
```

#### **Summary Mart Specific Patterns:**

**AR Summary Mart Calculations:**
```sql
-- AR-specific business logic
case 
    when total_balance = 0 then 'No Balance'
    when balance_over_90_days > total_balance * 0.5 then 'High Risk'
    when balance_over_90_days > 0 then 'Medium Risk'
    else 'Low Risk'
end as aging_risk_category,

-- Collection performance metrics
round(
    coalesce(total_paid, 0) / 
    nullif(coalesce(total_billed, 0), 0) * 100, 2
) as collection_rate,

-- Payment timing metrics
case 
    when avg_payment_days <= 30 then 'Excellent'
    when avg_payment_days <= 60 then 'Good'
    when avg_payment_days <= 90 then 'Fair'
    else 'Poor'
end as payment_performance_tier
```

**Provider Performance Mart Calculations:**
```sql
-- Provider performance metrics
round(
    coalesce(total_collections, 0) / 
    nullif(coalesce(total_production, 0), 0) * 100, 2
) as collection_rate,

round(
    coalesce(completed_appointments, 0) / 
    nullif(coalesce(total_appointments, 0), 0) * 100, 2
) as completion_rate,

-- Performance categorization
case 
    when collection_rate >= 95 and completion_rate >= 90 then 'Top Performer'
    when collection_rate >= 90 and completion_rate >= 85 then 'High Performer'
    when collection_rate >= 85 and completion_rate >= 80 then 'Average Performer'
    else 'Needs Improvement'
end as performance_tier
```

### 1.7 Performance Optimization Patterns

**Index Strategy by Model Type:**
```sql
-- Dimension tables: Focus on lookup performance
indexes=[
    {'columns': ['[entity]_id'], 'unique': true},
    {'columns': ['_updated_at']},
    {'columns': ['patient_id']}  -- If applicable
]

-- Fact tables: Focus on query performance
indexes=[
    {'columns': ['[entity]_id'], 'unique': true},
    {'columns': ['patient_id']},
    {'columns': ['provider_id']},
    {'columns': ['[date_field]']},
    {'columns': ['_updated_at']}
]

-- Summary marts: Focus on analytical queries
indexes=[
    {'columns': ['date_id']},
    {'columns': ['patient_id']},     -- If applicable
    {'columns': ['provider_id']},    -- If applicable
    {'columns': ['clinic_id']}       -- If applicable
]
```

**Query Optimization Patterns:**
```sql
-- Use proper joins for performance
left join dim_patient dp using (patient_id)
left join dim_provider dpr using (provider_id)
left join dim_date dd using (date_id)

-- Use efficient aggregations
sum(case when condition then amount else 0 end) as conditional_sum,
count(case when condition then 1 end) as conditional_count

-- Use window functions for complex calculations
row_number() over (partition by patient_id order by appointment_date) as appointment_sequence
```

## Part 2: Documentation File Standardization

### Current Documentation Analysis

**Best Practices Observed:**
- ✅ Some comprehensive business context in existing yml files
- ✅ Good use of technical specifications
- ✅ Some detailed dependency documentation

**Areas Needing Standardization:**
- ❌ Inconsistent meta section usage
- ❌ Variable test pattern application
- ❌ Different approaches to business rule documentation
- ❌ Inconsistent relationship test patterns
- ❌ Missing performance and usage notes

### 2.1 Model Header Documentation Standard

**Mandatory Structure:**
```yaml
version: 2

models:
  - name: [model_name]
    description: >
      [Primary business purpose - 1-2 sentences]
      
      ## Business Context
      [Detailed business context including]:
      - Business purpose and use cases
      - Key stakeholders and consumers
      - Business rules and logic
      - Performance expectations
      
      ## Technical Specifications
      - Grain: [One row per what]
      - Source: [Primary source model]
      - Refresh: [Frequency]
      - Dependencies: [List of upstream models]
      
      ## Business Logic
      ### [Logic Category 1]
      - [Rule 1]: [Description]
      - [Rule 2]: [Description]
      
      ### [Logic Category 2]
      - [Rule 1]: [Description]
      
      ## Data Quality Notes
      - [Known issue 1]: [Description and handling]
      - [Known issue 2]: [Description and handling]
      
      ## Performance Considerations
      - [Performance note 1]
      - [Performance note 2]
      
      ## Usage Notes
      - [Usage consideration 1]
      - [Usage consideration 2]
    
    config:
      materialized: [table]
      schema: marts
      unique_key: [primary_key]
```

### 2.2 Column Documentation Standards

**Primary Key Pattern:**
```yaml
- name: [entity]_id
  description: >
    Primary key - [Business description and purpose]
    
    Business Rules:
    - [Rule 1]
    - [Rule 2]
  tests:
    - unique
    - not_null
    - positive_values
```

**Foreign Key Pattern:**
```yaml
- name: [related_entity]_id
  description: >
    Foreign key to [related entity] - [Business relationship description]
    
    Business Rules:
    - [Relationship rule 1]
    - [Relationship rule 2]
    
    Data Quality Notes:
    - [Known issue with relationship if any]
  tests:
    - relationships:
        to: ref('[target_model]')
        field: [target_field]
        severity: [error|warn]
        where: "[condition_if_applicable]"
```

**Measure Pattern:**
```yaml
- name: [measure_name]
  description: >
    [Business description of the measure]
    
    Calculation Logic:
    - [Step 1 of calculation]
    - [Step 2 of calculation]
    
    Business Rules:
    - [Business rule 1]
    - [Business rule 2]
    
    Data Quality Notes:
    - [Known limitation or issue]
  tests:
    - not_null:
        where: "[condition_when_required]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: [min]
        max_value: [max]
        severity: [error|warn]
```

**Flag Field Pattern:**
```yaml
- name: [flag_name]
  description: >
    Flag indicating [business condition and purpose]
    
    Logic:
    - true when: [condition for true]
    - false when: [condition for false]
    
    Business Impact:
    - [Why this flag matters for business]
    - [How it's used in downstream processes]
  tests:
    - not_null
    - accepted_values:
        values: [list_of_valid_values]
```

### 2.3 Test Standardization Patterns

**Model-Level Test Categories:**

#### Data Volume Tests:
```yaml
tests:
  - dbt_expectations.expect_table_row_count_to_be_between:
      min_value: [expected_min]
      max_value: [expected_max]
      config:
        severity: error
        description: "[Business context for expected volume]"
```

#### Business Rule Tests:
```yaml
  - dbt_expectations.expression_is_true:
      expression: "[business_rule_logic]"
      config:
        severity: error
        description: "[Business rule description and importance]"
```

#### Data Quality Tests:
```yaml
  - dbt_expectations.expression_is_true:
      expression: "[data_quality_rule]"
      config:
        severity: warn
        description: "[Data quality expectation and impact]"
```

#### Referential Integrity Tests:
```yaml
  - dbt_utils.expression_is_true:
      expression: "[referential_integrity_rule]"
      config:
        severity: error
        description: "[Relationship requirement and business impact]"
```

### 2.4 Meta Section Standardization

**Required Meta Section:**
```yaml
meta:
  owner: "[business_team_name]"
  contains_pii: [true|false]
  business_process: "[Process Name]"
  refresh_frequency: "[frequency]"
  business_impact: "[High|Medium|Low]"
  mart_type: "[dimension|fact|summary]"
  grain_description: "[One row per what]"
  primary_consumers: ["[consumer1]", "[consumer2]"]
  data_quality_requirements:
    - "[Requirement 1]"
    - "[Requirement 2]"
  performance_requirements:
    - "[Performance expectation 1]"
    - "[Performance expectation 2]"
```

## Implementation Strategy

### Phase 1: Foundation Models (Weeks 1-2)

**Week 1: Core Dimension Tables**
- [ ] `dim_patient` - Foundation for all patient-related models
- [ ] `dim_provider` - Foundation for provider-related models
- [ ] `dim_insurance` - Foundation for insurance-related models

**Week 2: Core Fact Tables**
- [ ] `fact_appointment` - Core scheduling and clinical data
- [ ] `fact_payment` - Core financial transaction data

### Phase 2: Complex Models (Weeks 3-4)

**Week 3: Complex Fact Tables**
- [ ] `fact_claim` - Complex insurance and billing logic
- [ ] `fact_communication` - Communication tracking and metrics

**Week 4: High-Volume Summary Marts**
- [ ] `mart_ar_summary` - AR aging and collection analysis
- [ ] `mart_provider_performance` - Provider performance metrics

### Phase 3: Supporting Models (Weeks 5-6)

**Week 5: Remaining Summary Marts**
- [ ] `mart_new_patient` - New patient acquisition analytics
- [ ] `mart_hygiene_retention` - Hygiene patient retention
- [ ] `mart_patient_retention` - General patient retention

**Week 6: Final Models and Validation**
- [ ] `mart_production_summary` - Production and scheduling metrics
- [ ] `mart_revenue_lost` - Revenue analysis and optimization
- [ ] Cross-model consistency validation

## Quality Assurance Framework

### SQL File Quality Gates
- [ ] All config blocks follow standard pattern for model type
- [ ] CTEs organized according to complexity guidelines
- [ ] Business logic follows established patterns
- [ ] Performance optimizations implemented where needed
- [ ] Header documentation complete
- [ ] Metadata macro usage consistent

### Documentation Quality Gates
- [ ] All models have comprehensive business descriptions
- [ ] Column documentation includes business context
- [ ] Tests have business-relevant descriptions
- [ ] Meta sections complete and accurate
- [ ] Known issues properly documented
- [ ] Performance considerations documented

## Success Metrics

### Technical Metrics:
- **Compilation**: 100% models compile successfully
- **Performance**: Query times within acceptable limits
- **Consistency**: All models follow established patterns
- **Test Coverage**: Appropriate business rule validation

### Business Metrics:
- **Documentation Quality**: Stakeholders can understand model purpose
- **Usability**: Analysts can use models effectively
- **Maintainability**: Changes can be implemented efficiently
- **Data Quality**: Business rules are properly validated

This comprehensive strategy ensures both technical excellence and business value while maintaining the sophisticated business logic you've already developed in your mart models.
