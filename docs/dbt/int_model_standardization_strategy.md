# Intermediate Model Standardization Strategy

## Overview

This document defines the SQL model file standardization patterns and technical implementation for intermediate models. It works in conjunction with:
- **`int_yml_standardization_strategy.md`** - Provides detailed documentation template patterns and standards
- **`int_template_to_model_mapping_guide.md`** - Maps each model to appropriate documentation templates and implementation phases
- **`int_roadmap_checklist.md`** - Provides the detailed implementation roadmap and quality gates

Together, these documents provide a comprehensive framework for standardizing both SQL files and documentation across all intermediate models in the dbt_dental_clinic project.

---

## Strategy Overview

After analyzing your intermediate models (`int_adjustments`, `int_insurance_coverage`, `int_payment_split`, `int_ar_analysis`, `int_appointment_details`), I've identified the need for **two separate but complementary standardization strategies**:

1. **SQL Model File Standardization** - Technical patterns and code structure
2. **Documentation File Standardization** - Business context and testing patterns

## Part 1: SQL Model File Standardization

### Current State Analysis

**Strengths Observed:**
- ✅ Good use of CTEs with snake_case naming
- ✅ Complex business logic implementation (`int_adjustments`, `int_payment_split`)
- ✅ Proper metadata field preservation
- ✅ Incremental materialization where appropriate

**Areas Needing Standardization:**
- ❌ Inconsistent config block patterns
- ❌ Variable CTE organization and complexity
- ❌ Different approaches to business logic documentation
- ❌ Inconsistent patterns for handling complex joins
- ❌ Variable performance optimization strategies

### 1.1 Configuration Block Standards

**Mandatory Config Pattern (Aligned with Naming Conventions):**
```sql
{{
    config(
        materialized='[table|incremental]',
        schema='intermediate',
        unique_key='[primary_key]_id',
        on_schema_change='fail',
        incremental_strategy='merge',
        indexes=[
            {'columns': ['[primary_key]_id'], 'unique': true},
            {'columns': ['patient_id']},  -- Always include if applicable
            {'columns': ['_updated_at']}   -- Metadata-based indexing
        ]
    )
}}
```

**Schema Specification Requirements:**
- Always explicitly specify `schema='intermediate'` (per naming conventions)
- Use `on_schema_change='fail'` for data integrity (per naming conventions)
- Use `incremental_strategy='merge'` as default for incremental models

**Materialization Decision Matrix:**
- **Use `table`** for:
  - Complex aggregations (`int_ar_analysis`)
  - Multiple source joins (`int_insurance_coverage`)
  - Heavy business logic processing (`int_adjustments`)
- **Use `incremental`** for:
  - Large transaction tables (`int_payment_split`)
  - Time-series data (`int_appointment_details`)
  - Models with clear date-based filtering

### 1.2 CTE Organization Standards

**Mandatory CTE Structure by Complexity:**

#### Simple Models (3-5 CTEs):
```sql
-- 1. Source data retrieval
with source_[entity] as (
    select * from {{ ref('stg_opendental__[entity]') }}
),

-- 2. Lookup/reference data
[entity]_lookup as (
    select [key_fields], [lookup_fields]
    from {{ ref('stg_opendental__[lookup_table]') }}
),

-- 3. Business logic transformation
[entity]_enhanced as (
    select
        -- Primary identification
        [primary_key],
        -- Business transformations
        [business_logic],
        -- Metadata
        [metadata_fields]
    from source_[entity]
    left join [entity]_lookup using ([key_field])
)

select * from [entity]_enhanced
```

#### Complex Models (6+ CTEs):
```sql
-- 1. Source CTEs (multiple sources)
with source_[primary_entity] as (
    select * from {{ ref('stg_opendental__[primary]') }}
),

source_[secondary_entity] as (
    select * from {{ ref('stg_opendental__[secondary]') }}
),

-- 2. Lookup/Definition CTEs
[entity]_definitions as (
    select definition_id, item_name, category_id
    from {{ ref('stg_opendental__definition') }}
    where category_id in ([relevant_categories])
),

-- 3. Calculation/Aggregation CTEs
[entity]_summary as (
    select
        [grouping_columns],
        [aggregate_calculations]
    from source_[entity]
    group by [grouping_columns]
),

-- 4. Business Logic CTEs (can be multiple)
[entity]_categorization as (
    select
        *,
        case [business_logic] end as [category_field]
    from source_[entity]
),

[entity]_validation as (
    select
        *,
        case [validation_logic] end as [validation_field]
    from [entity]_categorization
),

-- 5. Integration CTE (joins everything together)
[entity]_integrated as (
    select
        -- Core fields
        [primary_fields],
        -- Enhanced fields
        [calculated_fields],
        -- Validation fields
        [validation_fields],
        -- Metadata
        [metadata_fields]
    from [entity]_validation v
    left join [entity]_summary s using ([key])
    left join [entity]_definitions d using ([definition_key])
),

-- 6. Final filtering/validation
final as (
    select * from [entity]_integrated
    where [data_quality_filters]
)

select * from final
```

### 1.3 Business Logic Patterns (Aligned with Naming Conventions)

**Source Column Reference Pattern:**
```sql
-- Always quote OpenDental source columns (CamelCase)
"PatNum" as patient_id,
"ClaimNum" as claim_id,
"DateService" as service_date,
"IsHidden" as is_hidden
```

**ID Column Transformation Pattern (CRITICAL RULE):**
```sql
-- All OpenDental columns ending in "Num" MUST be transformed to snake_case with "_id" suffix
"PatNum" as patient_id,           -- Standard transformation
"ClaimNum" as claim_id,           -- Remove 'Num', add 'id'
"ProcNum" as procedure_id,        -- Consistent across all models
"CanadianNetworkNum" as canadian_network_id,  -- NOT canadian_network_num
"FeatureNum" as feature_id,       -- NOT feature_num
"CategoryNum" as category_id      -- NOT category_num
```

**Boolean Conversion Pattern (Use Established Macro):**
```sql
-- Use the standardized macro for all boolean conversions
{{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
{{ convert_opendental_boolean('"IsActive"') }} as is_active,
{{ convert_opendental_boolean('"IsCompleted"') }} as is_completed
```

**Metadata Fields Pattern (Standardized Approach):**
```sql
-- Use the single metadata approach from naming conventions
_extracted_at,                              -- From ETL pipeline
"DateEntry" as _created_at,                 -- Original creation timestamp  
coalesce("DateTStamp", "DateEntry") as _updated_at,  -- Last update
current_timestamp as _transformed_at        -- dbt processing time
```

### 1.4 Incremental Model Patterns (Aligned with Naming Conventions)

**Standard Incremental Configuration:**
```sql
{{ config(
    materialized='incremental',
    unique_key='[entity]_id',
    on_schema_change='fail',
    incremental_strategy='merge',
    schema='intermediate'
) }}
```

**Incremental Filtering Pattern (Using Metadata Strategy):**
```sql
{% if is_incremental() %}
    WHERE _updated_at > (SELECT MAX(_updated_at) FROM {{ this }})
{% endif %}
```

**Alternative Pattern for Date-Based Filtering:**
```sql
{% if is_incremental() %}
    WHERE [date_field] >= (
        SELECT MAX([date_field]) + INTERVAL '1 day' FROM {{ this }}
    )
{% endif %}
```

### 1.5 Header Documentation Pattern

**Required Header for All Models:**
```sql
{{
    config(
        [config_settings]
    )
}}

/*
    Intermediate model for [entity_name]
    Part of System [X]: [System_Name]
    
    This model:
    1. [Primary purpose]
    2. [Secondary purpose]
    3. [Additional purposes]
    
    Business Logic Features:
    - [Feature 1]: [Description]
    - [Feature 2]: [Description]
    
    Data Quality Notes:
    - [Known issue 1]: [Description and handling]
    - [Known issue 2]: [Description and handling]
    
    Performance Considerations:
    - [Performance note 1]
    - [Performance note 2]
*/
```

## Part 2: Documentation File Standardization

### Current Documentation Analysis

**Best Practices Observed:**
- ✅ Comprehensive business context (`int_adjustments.yml`)
- ✅ Detailed test coverage (`int_payment_split.yml`)
- ✅ Good known issues documentation (`int_ar_analysis.yml`)

**Areas Needing Standardization:**
- ❌ Inconsistent meta section usage
- ❌ Variable test pattern application
- ❌ Different approaches to business rule documentation
- ❌ Inconsistent relationship test patterns

### 2.1 Model Header Documentation Standard

**Mandatory Structure:**
```yaml
version: 2

models:
  - name: int_[entity_name]
    description: >
      [Primary business purpose - 1-2 sentences]
      
      [Detailed business context including]:
      - Business purpose and use cases
      - Data sources and relationships
      - Key business rules implemented
      - Data quality considerations
      - Integration with other systems
      
      Key Features:
      - [Feature 1]: [Business impact]
      - [Feature 2]: [Business impact]
      - [Feature 3]: [Business impact]
      
      Data Sources:
      - [source_model_1]: [Purpose and data provided]
      - [source_model_2]: [Purpose and data provided]
      
      Business Logic Features:
      - [Logic 1]: [Implementation and rules]
      - [Logic 2]: [Implementation and rules]
      
      Data Quality Notes:
      - [Known issue 1]: [Description, impact, and handling]
      - [Known issue 2]: [Description, impact, and handling]
      
      Performance Notes:
      - [Performance consideration 1]
      - [Performance consideration 2]
      
    config:
      materialized: [table|incremental]
      schema: intermediate
      unique_key: [primary_key]_id
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
    - relationships:
        to: ref('[source_model]')
        field: [source_field]
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
        config:
          severity: [error|warn]
          description: "[Business context for test]"
```

**Business Logic Field Pattern:**
```yaml
- name: [calculated_field]
  description: >
    [Business purpose and calculation description]
    
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
    - accepted_values:
        values: [list_of_valid_values]
        where: "[condition_if_applicable]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: [min]
        max_value: [max]
        severity: [error|warn]
        config:
          description: "[Business context for range]"
```

**Flag Field Pattern:**
```yaml
- name: is_[condition]
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
        values: [true, false]
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
  system_integration: "[System identifier from your framework]"
  data_quality_requirements:
    - "[Requirement 1]"
    - "[Requirement 2]"
    - "[Requirement 3]"
  performance_requirements:
    - "[Performance expectation 1]"
    - "[Performance expectation 2]"
```

## Implementation Strategy

### Phase 1: SQL File Standardization (Weeks 1-3)

**Week 1: Configuration and CTE Structure**
- [ ] Standardize all config blocks
- [ ] Implement CTE organization patterns
- [ ] Add proper header documentation

**Week 2: Business Logic Patterns**
- [ ] Standardize categorization logic
- [ ] Implement validation patterns
- [ ] Standardize flag creation

**Week 3: Performance and Complex Models**
- [ ] Optimize complex models with materialized CTEs
- [ ] Implement deduplication patterns
- [ ] Add performance considerations

### Phase 2: Documentation Standardization (Weeks 4-6)

**Week 4: Model Headers and Descriptions**
- [ ] Standardize model-level descriptions
- [ ] Implement consistent meta sections
- [ ] Add business context documentation

**Week 5: Column Documentation**
- [ ] Standardize column description patterns
- [ ] Document business rules consistently
- [ ] Add data quality notes

**Week 6: Test Standardization**
- [ ] Implement consistent test patterns
- [ ] Add business rule validation tests
- [ ] Standardize test descriptions

## Quality Assurance Framework

### SQL File Quality Gates
- [ ] All config blocks follow standard pattern
- [ ] CTEs organized according to complexity guidelines
- [ ] Business logic follows established patterns
- [ ] Performance optimizations implemented where needed
- [ ] Header documentation complete

### Documentation Quality Gates
- [ ] All models have comprehensive business descriptions
- [ ] Column documentation includes business context
- [ ] Tests have business-relevant descriptions
- [ ] Meta sections complete and accurate
- [ ] Known issues properly documented

## Additional Patterns Identified

### 1.6 Unique Key Strategy Standards

**Problem Observed:** Inconsistent unique key generation approaches across models

**Standardized Approach:**
```sql
-- For simple models with natural primary keys
unique_key='[entity]_id'

-- For complex grain models (metrics, aggregations)
unique_key=['date', 'dimension1', 'dimension2']

-- For models with composite business keys
{{ dbt_utils.generate_surrogate_key(['field1', 'field2']) }} AS unique_id
```

**Surrogate Key Generation Pattern:**
```sql
-- Deterministic numeric ID for consistency
ABS(MOD(
    ('x' || SUBSTR(MD5(
        CAST(field1 AS VARCHAR) || '|' ||
        CAST(field2 AS VARCHAR) || '|' ||
        COALESCE(CAST(field3 AS VARCHAR), 'NULL')
    ), 1, 16))::bit(64)::bigint,
    9223372036854775807  -- Max bigint to avoid overflow
)) AS [entity]_id
```

### 1.7 Incremental Model Patterns

**From Communication Models:**
```sql
{% if is_incremental() %}
    WHERE [date_field] > (SELECT MAX([date_field]) FROM {{ this }})
{% endif %}
```

**From Complex Aggregation Models:**
```sql
{% if is_incremental() %}
    WHERE [date_field] >= (
        SELECT MAX([date_field]) + INTERVAL '1 day' FROM {{ this }}
    )
{% endif %}
```

### 1.8 System Integration Patterns

**System Identification Header Pattern:**
```sql
/*
    Intermediate model for [entity_name]
    Part of System [X]: [System_Name]
    
    Systems Integration:
    - System A: Fee Processing & Verification
    - System B: Insurance & Claims Processing  
    - System C: Payment Allocation & Reconciliation
    - System D: AR Analysis
    - System E: Collections
    - System F: Communications
    - System G: Scheduling
*/
```

## Model-Specific Implementation Priority

### Phase 1: Foundation Models (Weeks 1-2):
1. **int_patient_profile** - Foundation for all patient-related models
2. **int_provider_profile** - Foundation for provider-related models
3. **int_ar_analysis** - Foundation for financial reporting

### Phase 2: High-Volume Transaction Models (Weeks 3-4):
1. **int_payment_split** - High volume, needs performance optimization
2. **int_patient_communications_base** - High volume incremental model
3. **int_appointment_details** - Incremental patterns and calculations

### Phase 3: Complex Business Logic Models (Weeks 5-6):
1. **int_adjustments** - Complex business logic to standardize
2. **int_insurance_coverage** - Complex joins and data quality handling
3. **int_communication_metrics** - Complex aggregation patterns

### Phase 4: System-Specific Models (Weeks 7-8):
1. **int_billing_statements** (System E: Collections)
2. **int_collection_metrics** (System E: Collections)
3. **int_opendental_system_logs** (System monitoring)

### Phase 5: Supporting Models (Weeks 9-10):
1. Supporting models and less complex intermediate models
2. System integration validation
3. Cross-model consistency verification

## Success Metrics

### Technical Metrics:
- **Compilation**: 100% models compile successfully
- **Performance**: Query times within acceptable limits
- **Consistency**: All models follow established patterns

### Business Metrics:
- **Documentation Quality**: Stakeholders can understand model purpose
- **Test Coverage**: Business rules validated appropriately
- **Maintainability**: Changes can be implemented efficiently

This dual strategy ensures both technical excellence and business value while maintaining the sophisticated business logic you've already developed.