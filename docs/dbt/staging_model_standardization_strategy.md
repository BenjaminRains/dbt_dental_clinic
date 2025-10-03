# Staging Model Standardization Strategy

## Overview

This document provides comprehensive strategic context, patterns, and implementation framework for standardizing OpenDental staging models in the dbt dental clinic project. It establishes consistent architecture, transformation patterns, and quality standards across all staging models to ensure reliable data foundation for intermediate and mart layers.

The staging layer serves as the critical transformation bridge between raw OpenDental data and business-ready analytics models, implementing consistent data cleaning, type conversion, and standardization while preserving data lineage and quality.

---

## Current State Analysis

### Existing Staging Model Architecture

Based on analysis of existing staging models, the following consistent patterns have been identified:

#### **Materialization Patterns**
- **Table Materialization**: Core entity models (patient, provider) for performance
- **Incremental Materialization**: High-volume transactional models (appointment, procedurelog, claim, payment)
- **View Materialization**: Reference/lookup models and simple transformations

#### **Transformation Patterns**
- **Consistent CTE Structure**: `source_data` → `renamed_columns` → final select
- **Macro-Based Transformations**: Standardized use of transformation macros
- **Metadata Standardization**: Consistent metadata column patterns
- **Data Quality Handling**: Safe type conversions and null handling

#### **Naming Conventions**
- **Model Naming**: `stg_opendental__[table_name]`
- **Column Naming**: snake_case with business-meaningful names
- **ID Transformations**: `[entity]_id` pattern for all primary/foreign keys

---

## Staging Model Architecture Framework

### 1. Standardized Model Structure

```sql
{{ config(
    materialized='[table|incremental|view]',
    unique_key='[primary_key]'  -- for incremental models
) }}

with source_data as (
    select * from {{ source('opendental', '[table_name]') }}
    [-- Date filtering for performance]
    [-- Incremental logic if applicable]
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"[SourceColumn]"', 'target': '[entity]_id'},
            -- Additional ID transformations
        ]) }},
        
        -- Business field transformations
        [-- Field transformations with business context]
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"[BooleanColumn]"') }} as [business_name],
        
        -- Date fields using macro
        {{ clean_opendental_date('"[DateColumn]"') }} as [business_name],
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"[CreatedColumn]"',
            updated_at_column='"[UpdatedColumn]"',
            created_by_column='"[UserColumn]"'
        ) }}

    from source_data
)

select * from renamed_columns
```

### 2. Materialization Strategy

#### **Table Materialization**
**Use for**: Core entity models, reference data, frequently queried models
```sql
{{ config(
    materialized='table'
) }}
```
**Examples**: `stg_opendental__patient`, `stg_opendental__provider`, `stg_opendental__procedurecode`

#### **Incremental Materialization**
**Use for**: High-volume transactional models with clear update patterns
```sql
{{ config(
    materialized='incremental',
    unique_key='[primary_key]'
) }}

-- In source_data CTE:
{% if is_incremental() %}
    and {{ clean_opendental_date('"[UpdateColumn]"') }} > (select max(_loaded_at) from {{ this }})
{% endif %}
```
**Examples**: `stg_opendental__appointment`, `stg_opendental__procedurelog`, `stg_opendental__claim`

#### **View Materialization**
**Use for**: Simple transformations, reference lookups, rarely changing data
```sql
{{ config(
    materialized='view'
) }}
```
**Examples**: `stg_opendental__clinic`, `stg_opendental__appointmenttype`

### 3. Transformation Macro Standards

#### **ID Column Transformations**
```sql
-- Primary and Foreign Key transformations using macro
{{ transform_id_columns([
    {'source': '"PatNum"', 'target': 'patient_id'},
    {'source': '"ProvNum"', 'target': 'provider_id'},
    {'source': '"ClinicNum"', 'target': 'clinic_id'},
    {'source': 'NULLIF("OptionalId", 0)', 'target': 'optional_entity_id'}
]) }},
```

**Best Practices**:
- Always use `transform_id_columns` macro for ID fields
- Handle optional relationships with `NULLIF(column, 0)`
- Use business-meaningful target names (`patient_id`, not `pat_num`)

#### **Boolean Field Transformations**
```sql
-- Boolean fields using macro
{{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
{{ convert_opendental_boolean('"IsProsthesis"') }} as is_prosthesis,
```

**Best Practices**:
- Always use `convert_opendental_boolean` macro
- Use descriptive boolean names (`is_hidden`, not `hidden`)
- Document business meaning of boolean flags

#### **Date Field Transformations**
```sql
-- Date fields using macro
{{ clean_opendental_date('"DateEntry"') }} as entry_date,
{{ clean_opendental_date('"DateTimeDeceased"') }} as deceased_datetime,
```

**Best Practices**:
- Always use `clean_opendental_date` macro for date fields
- Use descriptive date names with business context
- Handle invalid OpenDental date patterns consistently

#### **String Field Transformations**
```sql
-- String fields with cleaning
nullif(trim("CheckNum"), '') as check_number,
nullif(trim("ExternalId"), '') as external_id,
```

**Best Practices**:
- Trim whitespace and convert empty strings to null
- Use descriptive field names
- Handle special characters and encoding issues

### 4. Metadata Standardization

#### **Standard Metadata Columns**
All staging models must include standardized metadata using the `standardize_metadata_columns` macro:

```sql
{{ standardize_metadata_columns(
    created_at_column='"[CreatedColumn]"',
    updated_at_column='"[UpdatedColumn]"',
    created_by_column='"[UserColumn]"'
) }}
```

**Metadata Column Purposes**:
- `_loaded_at`: ETL extraction timestamp (pipeline monitoring)
- `_transformed_at`: dbt model build timestamp (model-specific tracking)
- `_created_at`: Business creation timestamp (primary focus)
- `_updated_at`: Business update timestamp (primary focus)
- `_created_by`: Business user ID (secondary focus)

#### **Metadata Best Practices**:
- Always include business timestamps when available
- Use consistent column mapping across related models
- Document metadata column sources in YAML documentation
- Preserve data lineage from source to staging

---

## Data Quality and Transformation Standards

### 1. Type Conversion Standards

#### **Integer ID Fields**
```sql
-- Safe integer conversion with pattern matching
CASE 
    WHEN "PatNum"::text ~ '^[0-9]+$' THEN "PatNum"::text::integer
    ELSE NULL
END as patient_id
```

#### **Boolean Fields**
```sql
-- Consistent boolean conversion
CASE 
    WHEN "IsHidden"::text = '1' THEN true
    WHEN "IsHidden"::text = '0' THEN false
    ELSE false  -- Default unknown values to false
END as is_hidden
```

#### **Date Fields**
```sql
-- Handle OpenDental invalid date patterns
CASE 
    WHEN "DateEntry" = '0001-01-01 00:00:00.000'::timestamp
        OR "DateEntry" = '1900-01-01 00:00:00.000'::timestamp
    THEN null
    ELSE "DateEntry"
END as entry_date
```

### 2. Data Filtering Standards

#### **Performance Filtering**
```sql
-- Date-based filtering for performance
where "ProcDate" >= '2023-01-01'
    or "ProcNum" in (
        select "ProcNum" 
        from {{ source('opendental', 'related_table') }}
        where "DateEntry" >= '2023-01-01'
    )
```

#### **Incremental Filtering**
```sql
-- Incremental update logic
{% if is_incremental() %}
    and {{ clean_opendental_date('"DateTStamp"') }} > (select max(_loaded_at) from {{ this }})
{% endif %}
```

### 3. Business Logic Standards

#### **Status Field Handling**
```sql
-- Preserve status as integer for business logic
"AptStatus"::smallint as appointment_status,
"Confirmed"::bigint as confirmation_status,
```

#### **Financial Field Handling**
```sql
-- Consistent financial field formatting
"PayAmt"::double precision as payment_amount,
coalesce("MerchantFee", 0.0)::double precision as merchant_fee,
```

---

## Naming Convention Standards

### 1. Model Naming
- **Pattern**: `stg_opendental__[table_name]`
- **Examples**: `stg_opendental__patient`, `stg_opendental__appointment`
- **Consistency**: Always use `stg_opendental__` prefix

### 2. Column Naming
- **Pattern**: `snake_case` with business context
- **ID Fields**: `[entity]_id` (e.g., `patient_id`, `provider_id`)
- **Boolean Fields**: `is_[description]` (e.g., `is_hidden`, `is_prosthesis`)
- **Date Fields**: `[description]_date` or `[description]_datetime`
- **Status Fields**: `[description]_status` (e.g., `appointment_status`)

### 3. Field Transformation Examples

#### **Before (OpenDental Source)**
```sql
"PatNum", "ProvNum", "IsHidden", "DateEntry", "SecDateTEdit"
```

#### **After (Staging Model)**
```sql
patient_id, provider_id, is_hidden, entry_date, _updated_at
```

---

## Documentation Standards

### 1. Model Documentation Structure
```yaml
version: 2

models:
  - name: stg_opendental__[table_name]
    description: >
      Staging model for [business entity] from OpenDental.
      
      [Business context and purpose]
      [Record count and scope]
      [Key relationships]
      [Important business rules]
    
    config:
      tags: ['opendental', 'staging', '[entity]', '[business_area]']
    
    meta:
      record_count: "[current count]"
      data_scope: "[date range or scope description]"
      
      known_issues:
        - description: "[issue description]"
          severity: "[warn|error]"
          business_impact: "[impact description]"
      
      business_rules:
        - rule: "[business rule description]"
          impact: "[business impact]"
    
    columns:
      # Comprehensive column documentation
```

### 2. Column Documentation Standards
```yaml
columns:
  # Primary Keys
  - name: [entity]_id
    description: "Primary key - [business description] (maps to [SourceColumn] in OpenDental)"
    tests:
      - unique
      - not_null
      - positive_values
  
  # Foreign Keys
  - name: [related_entity]_id
    description: "Foreign key to [related entity] - [business relationship description]"
    tests:
      - relationships:
          to: ref('stg_opendental__[related_table]')
          field: [related_entity]_id
          severity: [warn|error]
  
  # Business Columns
  - name: [column_name]
    description: >
      [Business description including]:
      - Purpose and usage
      - Value ranges or meanings
      - Business rules
      - Known data quality issues if applicable
  
  # Metadata Columns
  - name: _loaded_at
    description: "Timestamp when the data was loaded into the data warehouse by the ETL pipeline"
    tests:
      - not_null
```

---

## Implementation Phases

### **Phase 1: Foundation Models (Week 1)**
- [ ] **Core Entity Models**
  - `stg_opendental__patient` (table materialization)
  - `stg_opendental__provider` (incremental materialization)
  - `stg_opendental__clinic` (view materialization)
- [ ] **Reference Models**
  - `stg_opendental__procedurecode` (table materialization)
  - `stg_opendental__appointmenttype` (view materialization)

### **Phase 2: Financial Core (Week 2)**
- [ ] **Fee Processing Models**
  - `stg_opendental__fee` (incremental materialization)
  - `stg_opendental__feesched` (table materialization)
- [ ] **Payment Models**
  - `stg_opendental__payment` (incremental materialization)
  - `stg_opendental__paysplit` (incremental materialization)

### **Phase 3: Clinical Operations (Week 3)**
- [ ] **Appointment Models**
  - `stg_opendental__appointment` (incremental materialization)
  - `stg_opendental__schedule` (incremental materialization)
- [ ] **Procedure Models**
  - `stg_opendental__procedurelog` (incremental materialization)
  - `stg_opendental__treatplan` (incremental materialization)

### **Phase 4: Insurance and Claims (Week 4)**
- [ ] **Insurance Models**
  - `stg_opendental__insplan` (table materialization)
  - `stg_opendental__inssub` (incremental materialization)
- [ ] **Claims Models**
  - `stg_opendental__claim` (incremental materialization)
  - `stg_opendental__claimproc` (incremental materialization)

### **Phase 5: Supporting Systems (Week 5)**
- [ ] **Communication Models**
  - `stg_opendental__commlog` (incremental materialization)
  - `stg_opendental__task` (incremental materialization)
- [ ] **Reference Models**
  - `stg_opendental__definition` (table materialization)
  - `stg_opendental__preference` (table materialization)

### **Phase 6: Quality Assurance (Week 6)**
- [ ] **Cross-Model Validation**
  - Foreign key relationship testing
  - Data consistency validation
  - Performance optimization review
- [ ] **Documentation Review**
  - YAML documentation completeness
  - Business context accuracy
  - Test coverage validation

---

## Quality Gates and Success Criteria

### **Per-Model Quality Gates**
- [ ] **Architecture Compliance**: Follows standardized model structure
- [ ] **Macro Usage**: Uses appropriate transformation macros consistently
- [ ] **Naming Conventions**: Follows established naming patterns
- [ ] **Metadata Standards**: Includes standardized metadata columns
- [ ] **Documentation**: Comprehensive YAML documentation with business context
- [ ] **Test Coverage**: Appropriate tests for data quality validation

### **System-Level Quality Gates**
- [ ] **Consistency**: All staging models follow the same patterns
- [ ] **Performance**: Appropriate materialization strategies for each model type
- [ ] **Data Quality**: Consistent handling of data quality issues
- [ ] **Lineage**: Clear data lineage from source through staging
- [ ] **Business Context**: Documentation supports business understanding

### **Integration Quality Gates**
- [ ] **Intermediate Model Support**: Staging models support intermediate model development
- [ ] **Mart Model Support**: Staging models provide clean foundation for mart models
- [ ] **ETL Pipeline Integration**: Models work seamlessly with ETL pipeline
- [ ] **Business User Accessibility**: Non-technical stakeholders can understand model purposes

---

## Benefits and Expected Outcomes

### **Immediate Benefits**
1. **Consistent Foundation**: Standardized staging models provide reliable data foundation
2. **Improved Development Speed**: Consistent patterns accelerate model development
3. **Enhanced Data Quality**: Standardized transformations ensure consistent data quality
4. **Reduced Maintenance**: Consistent patterns reduce maintenance overhead

### **Long-term Benefits**
1. **Scalable Architecture**: Standardized patterns support project growth
2. **Team Efficiency**: Consistent patterns improve team productivity
3. **Data Reliability**: Standardized transformations ensure data consistency
4. **Business Confidence**: Reliable staging layer builds stakeholder trust

### **Integration Benefits**
1. **Seamless Intermediate Development**: Clean staging models accelerate intermediate model development
2. **Consistent Mart Foundation**: Standardized staging supports consistent mart models
3. **Pipeline Reliability**: Consistent patterns improve ETL pipeline reliability
4. **Quality Assurance**: Standardized testing ensures data quality across all models

---

## Maintenance and Evolution Strategy

### **Regular Maintenance**
- **Quarterly Reviews**: Model performance and business context updates
- **Change Documentation**: Update models when OpenDental schema changes
- **Test Evolution**: Update tests as business rules evolve
- **Stakeholder Feedback**: Regular review with business users and data consumers

### **Change Management Process**
1. **Impact Assessment**: Evaluate downstream impact of staging model changes
2. **Intermediate Model Alignment**: Ensure changes maintain consistency with intermediate models
3. **Business Review**: Validate business context changes with stakeholders
4. **Version Control**: Track all model changes with implementation changes

### **Evolution Guidelines**
- **Backward Compatibility**: Maintain backward compatibility when possible
- **Gradual Migration**: Implement changes gradually to minimize disruption
- **Documentation Updates**: Update documentation with all model changes
- **Testing**: Comprehensive testing before deploying changes

This standardization strategy creates a solid, consistent foundation for staging models that directly supports intermediate and mart model development while maintaining data quality and business context throughout the analytics pipeline.
