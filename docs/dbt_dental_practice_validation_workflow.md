# OpenDental DBT Validation Workflow

## Overview

This document outlines our end-to-end workflow for developing and validating DBT staging models for 
OpenDental data. The process moves through three distinct phases: Analysis, Testing, and 
Implementation.

## Project Structure

```
dbt_dental_practice/
├── analysis/
│   └── <table_name>/                    # One directory per source table
│       ├── <table_name>_ddl.sql         # Source table DDL (ground truth)
│       ├── <table_name>_analysis.sql    # Exploratory analysis queries
│       ├── analysis_output.csv          # Analysis query results
│       └── *.ipynb                      # Analysis notebooks
├── tests/
│   └── staging/
│       └── <table_name>_validation_rules.sql  # Validation rules
├── models/
│   └── staging/
│       └── opendental/
│           └── stg_opendental__<table_name>.sql  # Final staging model
```

## Phase 1: Analysis

### 1.1 DDL as Ground Truth
- Each table analysis begins with its DDL file (`<table_name>_ddl.sql`)
- DDL files serve as the source of truth for:
  - Column names and data types
  - Primary/foreign key relationships
  - Constraints and default values
  - Table-level documentation

### 1.2 Exploratory Analysis
The analysis phase follows an iterative process:

1. **Initial Query Development**
   - Create `<table_name>_analysis.sql` based on DDL structure
   - Work with stakeholders to identify key metrics and patterns
   - Focus on:
     - Data distributions
     - Value ranges
     - Relationship validations
     - Business rule discovery

2. **Iterative Refinement**
   - Export analysis results to CSV
   - Further explore in notebooks
   - Update analysis queries based on findings
   - Repeat until patterns are well understood

3. **Documentation**
   - Record discovered patterns
   - Document business rules
   - Note data quality issues
   - Track stakeholder decisions

Example Analysis Query Structure:
```sql
-- Basic data profiling
with DataProfile as (
    select 
        count(*) as total_records,
        count(distinct column_name) as unique_values,
        min(date_column) as earliest_date,
        max(date_column) as latest_date
    from source_table
),

-- Business pattern analysis
PatternAnalysis as (
    select 
        category_column,
        count(*) as category_count,
        avg(amount_column) as avg_amount
    from source_table
    group by category_column
)

select * from DataProfile;
select * from PatternAnalysis;
```

## Phase 2: Validation Rules Development

### 2.1 Converting Analysis to Tests
- Create `<table_name>_validation_rules.sql` in tests/staging/
- Transform discovered patterns into explicit validation rules
- Focus on:
  - Data integrity rules
  - Business logic validation
  - Edge case handling

Example Validation Rules:
```sql
with ValidationChecks as (
    select 
        -- Basic data quality
        count(case when primary_key is null then 1 end) as null_key_count,
        
        -- Business rules
        count(case when amount < 0 and type != 'REFUND' then 1 end) as invalid_negative_amounts,
        
        -- Relationship validation
        count(case when foreign_key not in (select id from reference_table) then 1 end) as orphaned_records
    from source_table
)
```

## Phase 3: Staging Model Implementation

### 3.1 Model Development
- Create `stg_opendental__<table_name>.sql`
- Implement standard transformations:
  - Column naming standardization
  - Data type conversions
  - Business rule implementation
  - Quality checks

Example Staging Model:
```sql
with source as (
    select * from {{ source('opendental', 'table_name') }}
),

renamed as (
    select
        -- Primary keys
        id as record_id,
        
        -- Standard transformations
        case 
            when amount = 0 then null 
            else amount 
        end as cleaned_amount,
        
        -- Add metadata
        current_timestamp() as _loaded_at
    from source
)

select * from renamed
```

### 3.2 Testing and Documentation
- Add model tests based on validation rules
- Document transformations and business logic
- Validate against original analysis findings
- Get stakeholder sign-off

## Best Practices

1. **Version Control**
   - Commit DDL files first
   - Track analysis query evolution
   - Document major findings in commits

2. **Documentation**
   - Link analysis findings to validation rules
   - Document stakeholder decisions
   - Maintain clear transformation logic

3. **Testing**
   - Start with DDL-based structural tests
   - Add business rule validations
   - Include data quality checks

4. **Collaboration**
   - Regular stakeholder reviews
   - Clear documentation of decisions
   - Traceable evolution of analysis