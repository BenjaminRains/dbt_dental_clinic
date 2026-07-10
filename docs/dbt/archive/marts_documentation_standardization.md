# Marts Documentation Standardization Strategy

## Overview

This document focuses exclusively on standardizing the `.yml` documentation files for marts models. It works in conjunction with:
- **`staging_documentation_standardization_strategy.md`** - Defines staging model documentation patterns and standards
- **`int_yml_standardization_strategy.md`** - Defines intermediate model documentation patterns and business context
- **`marts_model_standardization_strategy.md`** - Defines SQL model file standardization patterns and technical implementation

Together, these documents provide a comprehensive framework for standardizing both SQL files and documentation across all marts models in the dbt_dental_clinic project.

---

## Documentation-Specific Strategy

This strategy focuses exclusively on standardizing the `.yml` documentation files for marts models, building on the excellent business documentation patterns established in staging and intermediate models.

## Current Documentation Analysis

After reviewing multiple marts model documentation files, I've identified significant inconsistencies:

### Quality Tiers Observed:
1. **Excellent**: `_mart_production_summary.yml`, `_fact_claim.yml` - Comprehensive descriptions, business context, data quality notes
2. **Good**: `_dim_clinic.yml` - Detailed but could be improved with more business context
3. **Basic**: `_dim_patient.yml` - Has some quality notes but inconsistent structure
4. **Minimal**: `_facts.yml` - Basic documentation only

### Excellence Patterns Observed:

**From `_mart_production_summary.yml`:**
- ✅ Comprehensive business context with system identification
- ✅ Detailed aggregation logic and performance categorization
- ✅ Extensive data quality findings with specific examples
- ✅ Clear business rule documentation and usage guidelines

**From `_fact_claim.yml`:**
- ✅ Detailed business logic explanations with system integration
- ✅ Comprehensive test coverage with business rationale
- ✅ Good handling of dimensional relationships
- ✅ Clear validation rules and business impact descriptions

### Standardization Needs:

- ❌ Inconsistent model header structure across marts
- ❌ Variable test coverage patterns
- ❌ Different approaches to business rule documentation
- ❌ Inconsistent meta section usage
- ❌ Variable relationship test patterns
- ❌ Missing system integration context
- ❌ Limited data quality notes and known issues

## Documentation Template Framework

### Template 1: Dimension Models

Use for models like `dim_patient`, `dim_provider`, `dim_procedure`

```yaml
version: 2

models:
  - name: dim_[entity_name]
    description: >
      [Primary business purpose in 1-2 sentences]
      
      This dimension table serves as [role in business process] and provides [key business value].
      Part of [System X]: [System Name] workflow.
      
      Key Features:
      - [Business feature 1]: [Impact and benefit]
      - [Business feature 2]: [Impact and benefit]
      - [Business feature 3]: [Impact and benefit]
      - [Data integration]: [Sources and relationships]
      - [Business categorization]: [Classification and grouping logic]
      
      Data Sources:
      - [source_model_1]: [Data provided and purpose]
      - [source_model_2]: [Data provided and purpose]
      - [source_model_3]: [Data provided and purpose]
      
      Business Logic Features:
      - [Complex logic 1]: [Implementation details and business rules]
      - [Complex logic 2]: [Implementation details and business rules]
      - [Categorization logic]: [Categories and determination rules]
      - [Validation logic]: [Rules and business impact]
      
      Dimensional Relationships:
      - [Relationship 1]: [Business purpose and cardinality]
      - [Relationship 2]: [Business purpose and cardinality]
      - [Hierarchy logic]: [Parent-child relationships and business rules]
      
      Data Quality Notes:
      - [Known issue 1]: [Description, impact, examples, and mitigation]
      - [Known issue 2]: [Description, impact, examples, and mitigation]
      - [Performance consideration]: [Issue and optimization approach]
      
      Business Rules:
      - [Rule 1]: [Implementation and enforcement]
      - [Rule 2]: [Implementation and enforcement]
      - [Validation rule]: [Criteria and business rationale]
    
    config:
      materialized: table
      schema: marts
      unique_key: [entity]_id
      on_schema_change: fail
      indexes:
        - columns: ['[entity]_id']
          unique: true
      tags: ['dimension', 'daily']
    
    columns:
      [Column documentation following dimension model patterns]
    
    tests:
      [Comprehensive dimension validation tests]
    
    meta:
      owner: "[business_team_name]"
      contains_pii: [true|false]
      contains_phi: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "High"
      mart_type: "dimension"
      grain_description: "One row per [entity]"
      primary_consumers: ["[Team1]", "[Team2]"]
      system_integration: "[System X]: [System Name]"
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
        - "[Validation requirement 1]"
        - "[Validation requirement 2]"
      performance_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
```

### Template 2: Fact Models

Use for models like `fact_appointment`, `fact_claim`, `fct_procedure`

```yaml
version: 2

models:
  - name: fact_[entity_name]
    description: >
      [Primary business purpose in 1-2 sentences]
      
      This fact table serves as [role in business process] and provides [key business value].
      Part of [System X]: [System Name] workflow.
      
      Key Features:
      - [Business feature 1]: [Impact and benefit]
      - [Business feature 2]: [Impact and benefit]
      - [Business feature 3]: [Impact and benefit]
      - [Financial calculation]: [Methodology and business rules]
      - [Data integration]: [Sources and relationships]
      
      Data Sources:
      - [source_model_1]: [Data provided and purpose]
      - [source_model_2]: [Data provided and purpose]
      - [source_model_3]: [Data provided and purpose]
      
      Business Logic Features:
      - [Complex logic 1]: [Implementation details and business rules]
      - [Complex logic 2]: [Implementation details and business rules]
      - [Categorization logic]: [Categories and determination rules]
      - [Validation logic]: [Rules and business impact]
      
      Financial Calculations:
      - [Calculation 1]: [Formula and business purpose]
      - [Calculation 2]: [Formula and business purpose]
      - [Amount classifications]: [Thresholds and categories]
      
      Data Quality Notes:
      - [Known issue 1]: [Description, impact, examples, and mitigation]
      - [Known issue 2]: [Description, impact, examples, and mitigation]
      - [Performance consideration]: [Issue and optimization approach]
      
      Business Rules:
      - [Rule 1]: [Implementation and enforcement]
      - [Rule 2]: [Implementation and enforcement]
      - [Validation rule]: [Criteria and business rationale]
    
    config:
      materialized: table
      schema: marts
      unique_key: [composite_key_components]
      on_schema_change: fail
      indexes:
        - columns: ['[key_columns]']
          unique: true
      tags: ['fact', 'daily']
    
    columns:
      [Column documentation following fact model patterns]
    
    tests:
      [Comprehensive fact validation tests]
    
    meta:
      owner: "[business_team_name]"
      contains_pii: [true|false]
      contains_phi: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "High"
      mart_type: "fact"
      grain_description: "One row per [entity] (composite key: [key_components])"
      primary_consumers: ["[Team1]", "[Team2]"]
      system_integration: "[System X]: [System Name]"
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
        - "[Validation requirement 1]"
        - "[Validation requirement 2]"
      performance_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
```

### Template 3: Summary Marts

Use for models like `mart_production_summary`, `mart_appointment_summary`

```yaml
version: 2

models:
  - name: mart_[entity_name]_summary
    description: >
      [Primary business purpose in 1-2 sentences]
      
      This summary mart serves as [role in business process] and provides [key business value].
      Part of [System X]: [System Name] workflow.
      
      Grain: [Detailed description of record granularity and unique combinations]
      
      Key Features:
      - [Aggregation feature 1]: [Business metrics and KPI calculation]
      - [Performance feature 2]: [Performance tracking and trend analysis]
      - [Efficiency feature 3]: [Operational efficiency measurement]
      - [Quality feature 4]: [Data quality and validation metrics]
      
      Data Sources:
      - [base_source_1]: [Transactional data and events]
      - [dimension_source_2]: [Dimensional data and context]
      - [reference_source_3]: [Reference data and lookup values]
      
      Aggregation Logic Features:
      - [Metric calculation 1]: [Formula and business rules]
      - [Rate calculation 2]: [Methodology and normalization]
      - [Trend analysis 3]: [Time-based calculations and patterns]
      
      Business Metrics:
      - [KPI 1]: [Definition and business significance]
      - [Performance indicator 2]: [Calculation and operational value]
      - [Efficiency metric 3]: [Formula and optimization target]
      
      Data Quality Notes:
      - [Aggregation issue 1]: [Impact on metrics accuracy and handling]
      - [Performance issue 2]: [Impact on calculation speed and optimization]
      - [Data completeness issue 3]: [Impact on metric reliability and resolution]
      
      Business Rules:
      - [Aggregation rule 1]: [Calculation methodology and validation]
      - [Performance rule 2]: [Metric thresholds and business standards]
      - [Quality rule 3]: [Data validation and integrity requirements]
    
    config:
      materialized: table
      schema: marts
      unique_key: [composite_key_fields]
      on_schema_change: fail
      indexes:
        - columns: ['[key_columns]']
          unique: true
      tags: ['summary', 'daily']
    
    columns:
      [Column documentation following summary mart patterns]
    
    tests:
      [Summary mart validation and business rule tests]
    
    meta:
      owner: "[business_team_name]"
      contains_pii: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "High"
      mart_type: "summary"
      grain_description: "One row per [granularity] + [grouping dimensions]"
      primary_consumers: ["[Team1]", "[Team2]"]
      system_integration: "[System X]: [System Name]"
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
        - "[Validation requirement 1]"
        - "[Validation requirement 2]"
      performance_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
```

## Advanced Column Documentation Patterns

### Pattern 1: Primary Key Documentation (Aligned with Naming Conventions)

```yaml
- name: [entity]_id
  description: >
    Primary key - [Business entity description] (maps from "[SourceColumn]Num" in OpenDental)
    
    Source Transformation:
    - OpenDental source: "[SourceColumn]Num" (CamelCase)
    - Transformed to: [entity]_id (snake_case with _id suffix)
    - Transformation rule: All columns ending in "Num" become "_id" fields
    
    Business Context:
    - [Uniqueness requirement and business rule]
    - [Usage in downstream processes]
  tests:
    - unique
    - not_null
    - positive_values
    - relationships:
        to: ref('[source_model]')
        field: [source_field]
        config:
          severity: error
          description: "[Business importance of relationship integrity]"
```

### Pattern 2: Foreign Key with Source Mapping

```yaml
- name: [related_entity]_id
  description: >
    Foreign key to [related entity] (maps from "[SourceColumn]Num" in OpenDental)
    
    Source Transformation:
    - OpenDental source: "[SourceColumn]Num" (CamelCase as stored)
    - Transformed to: [related_entity]_id (snake_case per naming conventions)
    
    Business Relationship:
    - [Nature of relationship and cardinality]
    - [Business rules governing the relationship]
    - [Impact on data integrity and business processes]
    
    Data Quality Considerations:
    - [Historical data handling if applicable]
    - [Null value business meaning if applicable]
    - [Referential integrity requirements]
  tests:
    - relationships:
        to: ref('[target_model]')
        field: [target_field]
        severity: [error|warn]
        where: "[business_condition_if_applicable]"
        config:
          severity: [error|warn]
          description: >
            [Business context for relationship requirement and impact of violations]
```

### Pattern 3: Metrics/KPI Fields

```yaml
- name: [metric_field]
  description: >
    [Business KPI and performance measurement description]
    
    Calculation Method:
    - Numerator: [What is being measured]
    - Denominator: [What it's being measured against]
    - Formula: [Mathematical formula or business logic]
    
    Business Significance:
    - Target Range: [Acceptable performance range]
    - Benchmark: [Industry or internal benchmark]
    - Decision Impact: [How this metric drives business decisions]
    
    Data Quality:
    - Accuracy: [Reliability and precision considerations]
    - Timeliness: [Data freshness requirements]
    - Completeness: [Required data coverage]
  tests:
    - not_null:
        where: "[condition_when_metric_should_exist]"
        config:
          description: "[Business requirement for metric calculation]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: [business_minimum]
        max_value: [business_maximum]
        severity: [error|warn]
        config:
          description: >
            [Business rationale for metric range and performance standards]
    - dbt_utils.expression_is_true:
        expression: "[metric_business_rule]"
        config:
          severity: [error|warn]
          description: "[Business logic validation for metric integrity]"
```

### Pattern 4: Financial/Amount Fields

```yaml
- name: [amount_field]
  description: >
    [Financial purpose and business significance]
    
    Financial Context:
    - Currency: [Currency and precision requirements]
    - Calculation: [How amount is determined]
    - Business Rules: [Financial business rules and constraints]
    
    Accounting Impact:
    - [Impact on financial reporting]
    - [Reconciliation requirements]
    - [Audit trail considerations]
    
    Common Values:
    - [Typical range or common amounts if applicable]
    - [Special values and their meaning (e.g., zero, negative)]
    - [Outlier patterns and business explanation]
  tests:
    - not_null:
        where: "[financial_requirement_condition]"
        config:
          description: "[Financial business rule requiring value]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: [financial_minimum]
        max_value: [financial_maximum]
        severity: [error|warn]
        config:
          description: >
            [Financial business rationale for range and audit significance]
    - dbt_utils.expression_is_true:
        expression: "[financial_business_rule]"
        config:
          severity: error
          description: "[Financial integrity requirement]"
```

### Pattern 5: Status/Category Fields

```yaml
- name: [status_category_field]
  description: >
    [Business status or category and its operational significance]
    
    Valid Values:
    - '[value_1]': [Business meaning and operational impact]
    - '[value_2]': [Business meaning and operational impact]
    - '[value_3]': [Business meaning and operational impact]
    
    Business Rules:
    - [Status transition rules if applicable]
    - [Category assignment criteria]
    - [Operational workflow implications]
    
    Operational Impact:
    - [How status affects business processes]
    - [Workflow decisions based on status]
    - [Reporting and analytics usage]
  tests:
    - not_null:
        config:
          description: "[Business requirement for status assignment]"
    - accepted_values:
        values: ['[value_1]', '[value_2]', '[value_3]']
        config:
          description: "[Business validation of acceptable status values]"
```

### Pattern 6: Metadata Fields (Standardized Approach)

```yaml
- name: _loaded_at
  description: >
    ETL pipeline loading timestamp - when the record was loaded into the data warehouse
    
    Source: ETL pipeline metadata (added during loading process)
    Purpose: Data lineage tracking and pipeline monitoring
    Usage: ETL debugging and data freshness validation
  tests:
    - not_null
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: "'2020-01-01 00:00:00'::timestamp"
        max_value: "current_timestamp"

- name: _created_at  
  description: >
    Original creation timestamp from OpenDental source system
    
    Source Transformation:
    - OpenDental source: "DateEntry" (CamelCase as stored)
    - Represents: When the record was originally created in OpenDental
    - Usage: Business timeline analysis and record lifecycle tracking
  tests:
    - not_null:
        where: "[condition_when_creation_date_should_exist]"

- name: _updated_at
  description: >
    Last update timestamp from OpenDental source system
    
    Source Transformation:
    - OpenDental source: COALESCE("DateTStamp", "DateEntry") 
    - Logic: Uses DateTStamp if available, falls back to DateEntry
    - Purpose: Change tracking and incremental loading
  tests:
    - not_null

- name: _transformed_at
  description: >
    dbt model processing timestamp - when this mart model was last run
    
    Source: current_timestamp at dbt model execution
    Purpose: Model execution tracking and debugging
    Usage: Understanding data processing timeline
  tests:
    - not_null
```

## Enhanced Test Documentation Patterns

### Business Rule Validation:
```yaml
- dbt_expectations.expression_is_true:
    expression: "[business_rule_logic]"
    config:
      severity: error
      description: >
        Business Rule: [Rule name and description]
        
        Requirement: [What the business requires]
        Impact: [What happens if rule is violated]
        Enforcement: [How this test enforces the rule]
```

### Financial Integrity Tests:
```yaml
- dbt_expectations.expression_is_true:
    expression: "[financial_integrity_rule]"
    config:
      severity: error
      description: >
        Financial Integrity: [Rule description]
        
        Accounting Requirement: [Financial/accounting requirement]
        Audit Significance: [Why this matters for auditing]
        Business Impact: [Impact on financial reporting]
```

### Performance Monitoring Tests:
```yaml
- dbt_expectations.expect_table_row_count_to_be_between:
    min_value: [performance_minimum]
    max_value: [performance_maximum]
    config:
      severity: warn
      description: >
        Performance Monitor: [Performance aspect being monitored]
        
        Baseline: [Normal operating range]
        Alert Threshold: [When to investigate performance]
        Action Required: [What to do if performance degrades]
```

## Implementation Patterns by Model Type

### Dimension Models Implementation:
```yaml
# Pattern for dimension models
meta:
  owner: "clinical_operations_team"
  contains_pii: true
  contains_phi: true
  business_process: "Patient/Provider/Procedure Management"
  refresh_frequency: "daily"
  business_impact: "High"
  mart_type: "dimension"
  system_integration: "System Foundation: Core Dimensions"
  data_quality_requirements:
    - "All dimension records must be unique"
    - "Primary keys must be non-null and positive"
    - "Foreign key relationships must be valid"
    - "Status fields must have valid values"
    - "Business categorization must be accurate"
```

### Fact Models Implementation:
```yaml
# Pattern for fact models
meta:
  owner: "operations_team"
  contains_pii: true
  contains_phi: true
  business_process: "Transaction and Event Tracking"
  refresh_frequency: "daily"
  business_impact: "High"
  mart_type: "fact"
  system_integration: "System X: [Business Process]"
  data_quality_requirements:
    - "All fact records must have valid dimensional relationships"
    - "Financial amounts must be non-negative where applicable"
    - "Date fields must be valid and within business ranges"
    - "Status transitions must follow business rules"
    - "Composite keys must be unique"
```

### Summary Marts Implementation:
```yaml
# Pattern for summary/aggregated marts
meta:
  owner: "analytics_team"
  contains_pii: false
  business_process: "Business Intelligence and Reporting"
  refresh_frequency: "daily"
  business_impact: "High"
  mart_type: "summary"
  system_integration: "System Analytics: Performance Tracking"
  data_quality_requirements:
    - "Aggregated metrics must be mathematically correct"
    - "Performance indicators must be within expected ranges"
    - "Time-based aggregations must be consistent"
    - "KPI calculations must follow business definitions"
    - "Summary records must be complete for reporting periods"
```

## Implementation Priority

### Phase 1: Critical Models (High Priority)
1. `_dim_patient.yml` - Core patient dimension
2. `_dim_provider.yml` - Core provider dimension
3. `_dim_procedure.yml` - Core procedure dimension
4. `_facts.yml` - Core fact table documentation
5. `_mart_appointment_summary.yml` - Key operational mart

### Phase 2: Important Models (Medium Priority)
1. `_dim_clinic.yml` - Clinic dimension
2. `_dim_insurance.yml` - Insurance dimension
3. `_dim_fee_schedule.yml` - Fee schedule dimension
4. `_mart_production_summary.yml` - Production analytics
5. `_mart_provider_performance.yml` - Provider analytics

### Phase 3: Supporting Models (Lower Priority)
1. `_dim_date.yml` - Date dimension
2. `_mart_hygiene_retention.yml` - Hygiene analytics
3. `_mart_new_patient.yml` - New patient analytics
4. `_mart_patient_retention.yml` - Patient retention
5. `_mart_revenue_lost.yml` - Revenue analytics

## Documentation Templates

### Dimension Model Template
```yaml
version: 2

models:
  - name: dim_[entity]
    description: >
      Dimension table containing standardized [entity] information. This table serves as the central
      reference for all [entity]-related data, including [key attributes], [business context],
      and [operational details] for each [entity] in the system.
      
      ## Business Context
      The [entity] dimension is a critical component of our dimensional model, enabling:
      - [Business capability 1]
      - [Business capability 2]
      - [Business capability 3]
      
      ## Technical Specifications
      - Grain: One row per [entity]
      - Source: [source models]
      - Refresh: [frequency]
      - Dependencies: [list of dependencies]
      
      ## Business Logic
      ### [Entity] Processing
      - [Business rule 1]
      - [Business rule 2]
      - [Business rule 3]
      
      ## Data Quality Notes
      - [Data quality consideration 1]
      - [Data quality consideration 2]
      - [Data quality consideration 3]
      
      ## Usage Guidelines
      - [Usage guideline 1]
      - [Usage guideline 2]
      - [Usage guideline 3]
    
    meta:
      owner: "[Business Team]"
      contains_pii: [true|false]
      contains_phi: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "[High|Medium|Low]"
      mart_type: "dimension"
      grain_description: "One row per [entity]"
      primary_consumers: ["[Team1]", "[Team2]"]
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
      performance_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
    
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [primary_key_components]
    
    columns:
      - name: [entity]_id
        description: >
          Primary key - Unique identifier for each [entity]
          
          Source Transformation:
          - OpenDental source: "[SourceField]" (CamelCase)
          - Transformed to: [entity]_id (snake_case with _id suffix)
          
          Business Context:
          - Must be unique across all [entities]
          - Used as foreign key in multiple downstream models
          - Critical for [entity] identification and record linking
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('[source_model]')
              field: [entity]_id
```

### Fact Model Template
```yaml
version: 2

models:
  - name: fact_[entity]
    description: >
      Fact table containing [entity] transactions and metrics.
      This model serves as the foundation for [entity]-level analysis and reporting,
      providing detailed information about each [entity] and its associated
      [related entities], [statuses], and [documentation].
      
      ## Business Context
      The [entity] fact table is a critical component of our dimensional model, enabling:
      - [Business capability 1]
      - [Business capability 2]
      - [Business capability 3]
      
      ## Technical Specifications
      - Grain: One row per [entity] (composite key: [key_components])
      - Source: [source models]
      - Refresh: [frequency]
      - Dependencies: [list of dependencies]
      
      ## Business Logic
      ### [Entity] Processing
      - [Business rule 1]
      - [Business rule 2]
      - [Business rule 3]
      
      ## Data Quality Notes
      - [Data quality consideration 1]
      - [Data quality consideration 2]
      - [Data quality consideration 3]
      
      ## Usage Guidelines
      - [Usage guideline 1]
      - [Usage guideline 2]
      - [Usage guideline 3]
    
    meta:
      owner: "[Business Team]"
      contains_pii: [true|false]
      contains_phi: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "[High|Medium|Low]"
      mart_type: "fact"
      grain_description: "One row per [entity]"
      primary_consumers: ["[Team1]", "[Team2]"]
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
      performance_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
    
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [composite_key_components]
    
    columns:
      - name: [entity]_id
        description: >
          Primary key for the [entity]
          
          Business Context:
          - Uniquely identifies each [entity] record
          - Used for [entity]-level analysis and reporting
          - Critical for [entity] identification and record linking
        tests:
          - not_null
```

### Summary Mart Template
```yaml
version: 2

models:
  - name: mart_[entity]_summary
    description: >
      Summary mart for comprehensive [entity] analytics and performance tracking.
      This mart provides aggregated metrics and KPIs to support [business purpose],
      [operational reporting], and [performance analysis] at [granularity] level
      by [grouping dimensions].
      
      ## Business Context
      The [entity] summary mart is a critical component of our analytical framework, enabling:
      - [Business capability 1]
      - [Business capability 2]
      - [Business capability 3]
      
      ## Technical Specifications
      - Grain: One row per [granularity] + [grouping dimensions]
      - Source: [source models]
      - Refresh: [frequency]
      - Dependencies: [list of dependencies]
      
      ## Business Logic
      ### Aggregation Rules
      - [Aggregation rule 1]
      - [Aggregation rule 2]
      - [Aggregation rule 3]
      
      ## Data Quality Notes
      - [Data quality consideration 1]
      - [Data quality consideration 2]
      - [Data quality consideration 3]
      
      ## Performance Considerations
      - [Performance consideration 1]
      - [Performance consideration 2]
      - [Performance consideration 3]
      
      ## Usage Notes
      - [Usage note 1]
      - [Usage note 2]
      - [Usage note 3]
    
    meta:
      owner: "[Business Team]"
      contains_pii: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "[High|Medium|Low]"
      mart_type: "summary"
      grain_description: "One row per [granularity] + [grouping dimensions]"
      primary_consumers: ["[Team1]", "[Team2]"]
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
      performance_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
    
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: [min]
          max_value: [max]
          config:
            severity: error
            description: "Expected [entity] summary records should be reasonable for operational scale"
    
    columns:
      - name: [key_column]
        description: >
          [Key column description]
          
          Business Rules:
          - [Business rule 1]
          - [Business rule 2]
        tests:
          - relationships:
              to: ref('[dimension_model]')
              field: [key_field]
              severity: error
          - not_null
```

## Implementation Checklist per Model

### Documentation Completeness Checklist:

#### Model Header:
- [ ] **Business Purpose**: Clear 1-2 sentence description
- [ ] **System Integration**: Part of System X identification
- [ ] **Key Features**: 3-5 major business features listed
- [ ] **Data Sources**: All source models documented with purpose
- [ ] **Business Logic**: Complex logic explained with business context
- [ ] **Data Quality Notes**: Known issues documented with impact
- [ ] **Business Rules**: Rules documented with enforcement approach

#### Column Documentation:
- [ ] **Primary Keys**: Business context and uniqueness requirements
- [ ] **Foreign Keys**: Relationship purpose and business impact
- [ ] **Calculated Fields**: Calculation logic and business purpose
- [ ] **Financial Fields**: Accounting context and business rules
- [ ] **Status Fields**: Valid values with business meaning
- [ ] **Flag Fields**: Business logic and operational impact

#### Test Coverage:
- [ ] **Primary Key Tests**: Uniqueness and referential integrity
- [ ] **Business Rule Tests**: Core business logic validation
- [ ] **Financial Tests**: Financial integrity and ranges
- [ ] **Data Quality Tests**: Quality monitoring and alerting
- [ ] **Relationship Tests**: Cross-model integrity with business context

#### Meta Section:
- [ ] **Ownership**: Business and technical owners identified
- [ ] **Business Process**: Process name and integration point
- [ ] **Impact Level**: Business impact assessment
- [ ] **Quality Requirements**: Specific quality standards
- [ ] **Refresh Requirements**: Update frequency and dependencies

## Quality Assurance Framework

### Documentation Quality Gates:
1. **Business Context**: Every model has clear business purpose
2. **Completeness**: All patterns implemented according to model type
3. **Consistency**: Similar models follow same documentation approach
4. **Accuracy**: Documentation matches actual model behavior
5. **Usefulness**: Stakeholders can understand and use documentation

### Review Process:
1. **Technical Review**: Implementation accuracy and consistency
2. **Business Review**: Business logic accuracy and completeness
3. **Stakeholder Validation**: Documentation usefulness and clarity
4. **Quality Assurance**: Pattern compliance and standard adherence

## Success Metrics

### Documentation Quality
- 100% of marts models have comprehensive descriptions
- 100% of marts models have complete metadata
- 100% of marts models have appropriate test coverage
- 100% of marts models have business context

### Consistency
- All marts models follow standardized structure
- All marts models use consistent metadata fields
- All marts models have consistent test patterns
- All marts models have consistent column documentation

### Business Value
- Improved model discoverability and understanding
- Enhanced data quality through comprehensive testing
- Better business context for model usage
- Consistent documentation patterns across all layers

## Conclusion

This documentation strategy ensures that your sophisticated business logic in marts models is properly documented while maintaining consistency across all marts models. By following the established patterns from staging and intermediate models, we create a comprehensive framework that supports:

- **Business Understanding**: Clear documentation of business purpose and context
- **Technical Implementation**: Detailed patterns for different model types
- **Quality Assurance**: Comprehensive testing and validation approaches
- **Maintainability**: Consistent structure that can evolve with business needs

The standardization effort should be prioritized based on business impact and implemented systematically to ensure consistency and completeness across all marts models, creating a unified documentation approach across the entire data platform.
