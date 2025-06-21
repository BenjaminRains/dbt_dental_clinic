# PostgreSQL-Based OpenDental DBT Intermediate Model Development Workflow

## Overview

This document outlines our workflow for developing intermediate models in our DBT project. 
Intermediate models transform our stable staging layer into business-focused entities that align 
with core business processes. This workflow ensures consistent development practices and 
maintains our established naming conventions.

## Model Selection: int_procedure_complete

For our initial intermediate model development, we've selected `int_procedure_complete` because:
1. It builds on stable staging models
2. Represents core business data (procedures)
3. Involves multiple related staging models
4. Has clear business rules for transformation

## Development Workflow Phases

### Phase 1: Business Logic Documentation

1. **Source Model Analysis**
   - Document required staging models
   - Map relationships between models
   - Identify key business rules
   - Create ERD for involved tables

2. **Business Rules Documentation**
   ```sql
   -- Example business rules for procedures
   -- Rule 1: Complete procedures must have:
   --   - Valid procedure code
   --   - Non-null fee amount
   --   - Valid provider ID
   --   - Valid date of service
   
   -- Rule 2: Status mapping:
   --   'C'  -> 'Complete'
   --   'EC' -> 'Existing Current'
   --   'EO' -> 'Existing Other'
   --   'R'  -> 'Referred'
   --   'TP' -> 'Treatment Planned'
   ```

### Phase 2: Model Development

1. **Initial Structure**
   ```sql:models/intermediate/opendental/int_procedure_complete.sql
   WITH Source AS (  -- CamelCase for CTEs per convention
       SELECT * FROM {{ ref('stg_opendental__procedure') }}
   ),

   ProcedureCodes AS (
       SELECT * FROM {{ ref('stg_opendental__procedurecode') }}
   ),

   Providers AS (
       SELECT * FROM {{ ref('stg_opendental__provider') }}
   )
   ```

2. **Incremental Logic Planning**
   ```sql
   {{ config(
       materialized='incremental',
       unique_key='procedure_id',
       schema='intermediate'
   ) }}
   ```

3. **Business Logic Implementation**
   - Follow naming conventions for derived fields
   - Document complex transformations
   - Include inline comments for clarity

### Phase 3: Testing Strategy

1. **Data Integrity Tests**
   ```yaml
   models:
     - name: int_procedure_complete
       columns:
         - name: procedure_id
           tests:
             - unique
             - not_null
         - name: procedure_status
           tests:
             - accepted_values:
                 values: ['Complete', 'Existing Current', 'Existing Other', 
                         'Referred', 'Treatment Planned']
   ```

2. **Business Logic Tests**
   - Create custom tests for business rules
   - Validate calculated fields
   - Check relationship integrity

### Phase 4: Documentation

1. **Model Documentation**
   ```yaml
   models:
     - name: int_procedure_complete
       description: >
         Comprehensive procedure information combining procedure details,
         codes, providers, and related clinical information.
       columns:
         - name: procedure_id
           description: Unique identifier for the procedure
         - name: procedure_status
           description: >
             Standardized status indicating the current state of the procedure.
             Valid values are...
   ```

2. **Lineage Documentation**
   - Document upstream dependencies
   - Note impact on downstream models
   - Include business context

## Naming Convention Compliance

Following our [SQL Naming Conventions](sql_naming_conventions.md):

1. **CTE Names**: Use CamelCase
   ```sql
   WITH Source AS (
       SELECT ...
   ),
   
   ProcedureDetails AS (
       SELECT ...
   )
   ```

2. **Field Names**:
   - Raw database columns: CamelCase ("ProcNum", "ProcStatus")
   - Derived fields: snake_case (total_fee, provider_name)

3. **File Naming**:
   - Model files: snake_case (`int_procedure_complete.sql`)
   - Test files: snake_case (`test_procedure_status_valid.sql`)

## Implementation Process

1. **Development Branch**
   ```bash
   git checkout -b feature/int-procedure-complete
   ```

2. **Iterative Development**
   - Create initial model structure
   - Add business logic incrementally
   - Write tests as features are added
   - Document throughout development

3. **Testing Cycle**
   ```bash
   # Run model-specific tests
   dbt test --select int_procedure_complete
   
   # Test model and dependencies
   dbt test --select +int_procedure_complete
   ```

4. **Code Review Process**
   - Verify naming conventions
   - Check test coverage
   - Review documentation
   - Validate business logic

## Validation and Quality Checks

1. **Data Quality Monitoring**
   - Record counts vs. source
   - Null value analysis
   - Distribution checks
   - Relationship validation

2. **Performance Optimization**
   - Query execution plan review
   - Incremental logic validation
   - Index usage analysis

## Next Steps

After successful implementation of `int_procedure_complete`:
1. Review and document lessons learned
2. Adjust workflow as needed
3. Select next intermediate model
4. Apply refined workflow to next model

## Maintenance

1. **Regular Reviews**
   - Monthly performance check
   - Quarterly business rule validation
   - Semi-annual full refresh test

2. **Documentation Updates**
   - Keep business rules current
   - Update impact analysis
   - Maintain test coverage

This workflow document should be treated as living documentation, updated based on learnings from each intermediate model implementation.