# Mart YML Documentation Standardization Strategy

## Current State Analysis

After reviewing multiple mart model documentation files, I've identified significant inconsistencies:

### Quality Tiers Observed:
1. **Excellent**: `_fact_claim.yml` - Comprehensive descriptions, business context, technical specifications
2. **Good**: `_dim_insurance.yml` - Detailed but could be improved with more business context
3. **Basic**: `_marts.yml` - Has some structure but lacks comprehensive documentation
4. **Minimal**: Some models lack dedicated yml files entirely

### Key Inconsistencies:
- **Description Quality**: Ranges from single line to comprehensive business context
- **Test Coverage**: Inconsistent application of data validation tests
- **Metadata Documentation**: Varies between models
- **Business Context**: Some models lack usage notes and known issues
- **Technical Specifications**: Missing grain descriptions and dependencies
- **Performance Notes**: Rarely documented despite being critical for mart models

## Standardization Goals

1. **Consistent Structure**: All mart yml files follow the same template by model type
2. **Comprehensive Documentation**: Include business context, usage notes, and technical specifications
3. **Standardized Testing**: Apply consistent test patterns across all model types
4. **Clear Relationships**: Document all foreign key relationships with appropriate severity levels
5. **Metadata Standards**: Consistent metadata column documentation
6. **Performance Transparency**: Document performance considerations and optimization strategies
7. **Business Impact**: Clear documentation of business value and stakeholder usage

## Template Structure by Model Type

### Dimension Table Documentation Template

```yaml
version: 2

models:
  - name: dim_[entity_name]
    description: >
      Dimension table containing comprehensive [entity] information for analytical reporting.
      This model serves as the primary reference for [entity]-related analytics and
      reporting, providing standardized attributes and business logic for consistent
      analysis across all fact tables and summary marts.
      
      ## Business Context
      The [entity] dimension is a critical component of our dimensional model, enabling:
      - [Entity] analysis and reporting
      - [Specific business use case 1]
      - [Specific business use case 2]
      - [Specific business use case 3]
      - Historical tracking and trend analysis
      - Cross-system integration and data consistency
      
      ## Technical Specifications
      - Grain: One row per [entity] (one row per [entity]_id)
      - Source: stg_opendental__[entity] (primary source)
      - Refresh: Daily
      - Dependencies: 
        * stg_opendental__[entity] (core [entity] data)
        * stg_opendental__[related_table] (related attributes)
      
      ## Business Logic
      ### [Entity] Status Management
      - [Status rule 1]: [Description and business impact]
      - [Status rule 2]: [Description and business impact]
      
      ### [Entity] Categorization
      - [Category rule 1]: [Description and business impact]
      - [Category rule 2]: [Description and business impact]
      
      ### Data Quality Handling
      - [Data quality rule 1]: [Description and handling]
      - [Data quality rule 2]: [Description and handling]
      
      ## Data Quality Notes
      - [Known issue 1]: [Description, impact, and handling]
      - [Known issue 2]: [Description, impact, and handling]
      
      ## Performance Considerations
      - [Performance consideration 1]
      - [Performance consideration 2]
      
      ## Usage Notes
      - [Usage consideration 1]
      - [Usage consideration 2]
      - [Best practice 1]
      - [Best practice 2]
    
    meta:
      owner: "[business_team_name]"
      contains_pii: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "daily"
      business_impact: "[High|Medium|Low]"
      mart_type: "dimension"
      grain_description: "One row per [entity]"
      primary_consumers: ["[consumer1]", "[consumer2]", "[consumer3]"]
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
      performance_requirements:
        - "[Performance expectation 1]"
        - "[Performance expectation 2]"
    
    tests:
      # Model-level tests
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: [expected_min]
          max_value: [expected_max]
          config:
            severity: error
            description: "[Business context for expected volume]"
      
      - dbt_expectations.expression_is_true:
          expression: "[business_rule_logic]"
          config:
            severity: error
            description: "[Business rule description and importance]"
    
    columns:
      # Primary Key
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
      
      # Business Attributes
      - name: [attribute_name]
        description: >
          [Business description of the attribute]
          
          Business Rules:
          - [Rule 1]
          - [Rule 2]
          
          Data Quality Notes:
          - [Known limitation or issue]
        tests:
          - not_null:
              where: "[condition_when_required]"
          - accepted_values:
              values: [list_of_valid_values]
              where: "[condition_if_applicable]"
      
      # Status/Enum Columns
      - name: [status_column]
        description: >
          [Status description with all possible values]:
          [value1] = [meaning]
          [value2] = [meaning]
          [value3] = [meaning]
          
          Business Rules:
          - [Status rule 1]
          - [Status rule 2]
        tests:
          - not_null
          - accepted_values:
              values: [list_of_values]
      
      # Boolean Columns
      - name: [boolean_column]
        description: >
          Flag indicating [business condition and purpose]
          
          Logic:
          - true when: [condition for true]
          - false when: [condition for false]
          
          Business Impact:
          - [Why this flag matters for business]
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      
      # Required Metadata Columns (ALL models must have these)
      - name: _loaded_at
        description: "Timestamp when the data was loaded into the data warehouse by the ETL pipeline (using current_timestamp)"
        tests:
          - not_null
      
      - name: _created_at
        description: "Timestamp when the record was created in the source system (OpenDental). [Source column mapping]. May be null for [specific conditions]."
        tests:
          - not_null:
              where: "[conditions where not null expected]"
      
      - name: _updated_at
        description: "Timestamp when the record was last updated in the source system (OpenDental). [Source column mapping]."
        tests:
          - not_null
      
      - name: _transformed_at
        description: "Timestamp when the record was processed by the dbt mart model (using current_timestamp)"
        tests:
          - not_null
      
      - name: _mart_refreshed_at
        description: "Timestamp when the mart model was last refreshed (using current_timestamp)"
        tests:
          - not_null
```

### Fact Table Documentation Template

```yaml
version: 2

models:
  - name: fact_[entity_name]
    description: >
      Fact table containing individual [entity] transactions and events.
      This model serves as the foundation for [entity]-level analysis and reporting,
      providing detailed information about each [entity] and its associated
      measures, dimensions, and business context.
      
      ## Business Context
      The [entity] fact table is a critical component of our dimensional model, enabling:
      - Individual [entity] transaction analysis
      - [Specific business use case 1]
      - [Specific business use case 2]
      - [Specific business use case 3]
      - Performance metrics and KPIs
      - Trend analysis and forecasting
      
      ## Technical Specifications
      - Grain: One row per [entity] (one row per [entity]_id)
      - Source: stg_opendental__[entity] (primary source)
      - Refresh: Daily
      - Dependencies: 
        * stg_opendental__[entity] (core [entity] data)
        * dim_[related_entity] (dimension lookups)
      
      ## Business Logic
      ### [Entity] Processing
      - [Processing rule 1]: [Description and business impact]
      - [Processing rule 2]: [Description and business impact]
      
      ### Measure Calculations
      - [Calculation rule 1]: [Description and business impact]
      - [Calculation rule 2]: [Description and business impact]
      
      ### Status and Flag Logic
      - [Status rule 1]: [Description and business impact]
      - [Status rule 2]: [Description and business impact]
      
      ## Data Quality Notes
      - [Known issue 1]: [Description, impact, and handling]
      - [Known issue 2]: [Description, impact, and handling]
      
      ## Performance Considerations
      - [Performance consideration 1]
      - [Performance consideration 2]
      
      ## Usage Notes
      - [Usage consideration 1]
      - [Usage consideration 2]
      - [Best practice 1]
      - [Best practice 2]
    
    meta:
      owner: "[business_team_name]"
      contains_pii: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "daily"
      business_impact: "[High|Medium|Low]"
      mart_type: "fact"
      grain_description: "One row per [entity]"
      primary_consumers: ["[consumer1]", "[consumer2]", "[consumer3]"]
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
      performance_requirements:
        - "[Performance expectation 1]"
        - "[Performance expectation 2]"
    
    tests:
      # Model-level tests
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: [expected_min]
          max_value: [expected_max]
          config:
            severity: error
            description: "[Business context for expected volume]"
      
      - dbt_expectations.expression_is_true:
          expression: "[business_rule_logic]"
          config:
            severity: error
            description: "[Business rule description and importance]"
    
    columns:
      # Primary Key
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
      
      # Foreign Keys
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
              to: ref('dim_[related_entity]')
              field: [related_entity]_id
              severity: [error|warn]
              where: "[condition_if_applicable]"
      
      # Measures
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
      
      # Flags
      - name: [flag_name]
        description: >
          Flag indicating [business condition and purpose]
          
          Logic:
          - true when: [condition for true]
          - false when: [condition for false]
          
          Business Impact:
          - [Why this flag matters for business]
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      
      # Required Metadata Columns
      - name: _loaded_at
        description: "Timestamp when the data was loaded into the data warehouse by the ETL pipeline (using current_timestamp)"
        tests:
          - not_null
      
      - name: _created_at
        description: "Timestamp when the record was created in the source system (OpenDental). [Source column mapping]. May be null for [specific conditions]."
        tests:
          - not_null:
              where: "[conditions where not null expected]"
      
      - name: _updated_at
        description: "Timestamp when the record was last updated in the source system (OpenDental). [Source column mapping]."
        tests:
          - not_null
      
      - name: _transformed_at
        description: "Timestamp when the record was processed by the dbt mart model (using current_timestamp)"
        tests:
          - not_null
      
      - name: _mart_refreshed_at
        description: "Timestamp when the mart model was last refreshed (using current_timestamp)"
        tests:
          - not_null
```

### Summary Mart Documentation Template

```yaml
version: 2

models:
  - name: mart_[entity_name]
    description: >
      Summary mart for comprehensive [entity] analytics and performance tracking.
      This mart provides aggregated metrics and KPIs to support [business purpose],
      enabling stakeholders to analyze trends, performance, and business outcomes
      at various levels of granularity.
      
      ## Business Context
      The [entity] summary mart is a critical component of our analytical framework, enabling:
      - [Specific business use case 1]
      - [Specific business use case 2]
      - [Specific business use case 3]
      - Performance monitoring and alerting
      - Executive reporting and dashboards
      - Operational decision making
      
      ## Technical Specifications
      - Grain: One row per [grain description] (e.g., one row per date + provider + clinic)
      - Source: fact_[entity] (primary source)
      - Refresh: Daily
      - Dependencies: 
        * fact_[entity] (core transaction data)
        * dim_[related_entity] (dimension lookups)
        * dim_date (date dimension)
      
      ## Business Logic
      ### Aggregation Rules
      - [Aggregation rule 1]: [Description and business impact]
      - [Aggregation rule 2]: [Description and business impact]
      
      ### Metric Calculations
      - [Calculation rule 1]: [Description and business impact]
      - [Calculation rule 2]: [Description and business impact]
      
      ### Performance Categorization
      - [Categorization rule 1]: [Description and business impact]
      - [Categorization rule 2]: [Description and business impact]
      
      ## Data Quality Notes
      - [Known issue 1]: [Description, impact, and handling]
      - [Known issue 2]: [Description, impact, and handling]
      
      ## Performance Considerations
      - [Performance consideration 1]
      - [Performance consideration 2]
      
      ## Usage Notes
      - [Usage consideration 1]
      - [Usage consideration 2]
      - [Best practice 1]
      - [Best practice 2]
    
    meta:
      owner: "[business_team_name]"
      contains_pii: [true|false]
      business_process: "[Process Name]"
      refresh_frequency: "daily"
      business_impact: "[High|Medium|Low]"
      mart_type: "summary"
      grain_description: "[One row per what]"
      primary_consumers: ["[consumer1]", "[consumer2]", "[consumer3]"]
      data_quality_requirements:
        - "[Requirement 1]"
        - "[Requirement 2]"
      performance_requirements:
        - "[Performance expectation 1]"
        - "[Performance expectation 2]"
    
    tests:
      # Model-level tests
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: [expected_min]
          max_value: [expected_max]
          config:
            severity: error
            description: "[Business context for expected volume]"
      
      - dbt_expectations.expression_is_true:
          expression: "[business_rule_logic]"
          config:
            severity: error
            description: "[Business rule description and importance]"
    
    columns:
      # Composite Key Components
      - name: date_id
        description: >
          Foreign key to dim_date - Date dimension for temporal analysis
          
          Business Rules:
          - [Date rule 1]
          - [Date rule 2]
        tests:
          - relationships:
              to: ref('dim_date')
              field: date_id
              severity: error
          - not_null
      
      - name: [dimension1]_id
        description: >
          Foreign key to dim_[dimension1] - [Business relationship description]
          
          Business Rules:
          - [Relationship rule 1]
          - [Relationship rule 2]
        tests:
          - relationships:
              to: ref('dim_[dimension1]')
              field: [dimension1]_id
              severity: error
          - not_null
      
      # Metrics and Measures
      - name: [metric_name]
        description: >
          [Business description of the metric]
          
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
      
      # Performance Categories
      - name: [category_field]
        description: >
          [Business description of the category]
          
          Categories:
          - [category1]: [description]
          - [category2]: [description]
          - [category3]: [description]
          
          Business Rules:
          - [Categorization rule 1]
          - [Categorization rule 2]
        tests:
          - not_null
          - accepted_values:
              values: [list_of_categories]
      
      # Required Metadata Columns
      - name: _loaded_at
        description: "Timestamp when the data was loaded into the data warehouse by the ETL pipeline (using current_timestamp)"
        tests:
          - not_null
      
      - name: _created_at
        description: "Timestamp when the record was created in the source system (OpenDental). [Source column mapping]. May be null for [specific conditions]."
        tests:
          - not_null:
              where: "[conditions where not null expected]"
      
      - name: _updated_at
        description: "Timestamp when the record was last updated in the source system (OpenDental). [Source column mapping]."
        tests:
          - not_null
      
      - name: _transformed_at
        description: "Timestamp when the record was processed by the dbt mart model (using current_timestamp)"
        tests:
          - not_null
      
      - name: _mart_refreshed_at
        description: "Timestamp when the mart model was last refreshed (using current_timestamp)"
        tests:
          - not_null
```

## Data Quality Documentation Standards

### Known Issues Section
Document all known data quality issues with:
- **Clear Description**: What the issue is
- **Business Impact**: How it affects analysis
- **Identification Date**: When discovered
- **Tracking**: JIRA ticket or reference
- **Test Reference**: Related test file if exists
- **Workaround**: How to handle the issue in analysis

### Business Rules Section
Document key business rules that affect data interpretation:
- **Rule Description**: What the rule is
- **Business Impact**: Why it matters
- **Implementation**: How it's enforced
- **Exceptions**: When the rule doesn't apply

### Usage Notes Section
Include critical information for analysts:
- **Scope and Coverage**: What data is included/excluded
- **Limitations**: What the data cannot be used for
- **Assumptions**: Key assumptions in the data model
- **Best Practices**: How to use the data effectively
- **Common Pitfalls**: What to avoid when using this data

## Test Standardization

### Required Tests by Column Type:
1. **Primary Keys**: unique, not_null, positive_values
2. **Foreign Keys**: relationships (with appropriate severity)
3. **Measures**: not_null (where applicable), range validation
4. **Flags**: not_null, accepted_values
5. **Categories**: not_null, accepted_values
6. **Financial**: non_negative_or_null (where appropriate)
7. **Dates**: date validation tests
8. **Metadata**: not_null for all metadata columns

### Model-Level Tests:
- Row count comparisons where appropriate
- Business rule validation
- Data quality expressions
- Custom validation tests
- Performance benchmarks

## Severity Guidelines

### Error Severity (severity: error):
- Data integrity violations
- Business rule violations that prevent analysis
- Primary key violations
- Critical foreign key relationships
- Financial calculation errors

### Warning Severity (severity: warn):
- Data quality issues that don't prevent analysis
- Historical or deprecated relationships
- Statistical outliers
- Business rule violations that require attention but don't block usage
- Performance concerns

## Implementation Phases

### Phase 1: Template Creation and Standards
- [x] Create standardization strategy document
- [ ] Create yml template files for each model type
- [ ] Document test standards
- [ ] Create implementation checklist

### Phase 2: High-Priority Models (Core Business Functions)
- [ ] Dimension Tables: dim_patient, dim_provider, dim_insurance
- [ ] Fact Tables: fact_appointment, fact_payment, fact_claim
- [ ] Summary Marts: mart_ar_summary, mart_provider_performance

### Phase 3: Supporting Models
- [ ] Remaining dimension tables
- [ ] Remaining fact tables
- [ ] Remaining summary marts

### Phase 4: Validation and Quality Assurance
- [ ] Review all updated documentation
- [ ] Validate test coverage
- [ ] Ensure consistency across all models
- [ ] Document any exceptions or special cases

## Success Metrics

1. **Consistency**: All mart models follow the same documentation pattern by type
2. **Completeness**: All business-critical information is documented
3. **Usefulness**: Documentation enables analysts to use data effectively
4. **Maintainability**: Documentation can be easily updated as business rules change
5. **Test Coverage**: Appropriate tests protect data quality without being overly restrictive
6. **Performance**: Documentation includes performance considerations and optimization notes

## Benefits Expected

1. **Improved Data Discovery**: Analysts can quickly understand model purpose and limitations
2. **Reduced Support Requests**: Self-service documentation reduces questions
3. **Better Data Quality**: Comprehensive testing catches issues early
4. **Faster Onboarding**: New team members can understand the data model quickly
5. **Audit Compliance**: Comprehensive documentation supports audit requirements
6. **Stakeholder Confidence**: Transparent data quality documentation builds trust
7. **Performance Optimization**: Clear performance notes enable better query optimization

## Maintenance Strategy

1. **Regular Reviews**: Quarterly review of documentation accuracy
2. **Issue Tracking**: Update known_issues section as problems are discovered/resolved
3. **Test Evolution**: Update tests as business rules change
4. **Version Control**: Track documentation changes with model changes
5. **Stakeholder Feedback**: Regular feedback from data consumers on documentation usefulness
6. **Performance Monitoring**: Regular review of performance notes and optimization opportunities
