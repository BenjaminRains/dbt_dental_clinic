# Intermediate Documentation Standardization Strategy

## Documentation-Specific Strategy

This strategy focuses exclusively on standardizing the `.yml` documentation files for intermediate models, building on the excellent business documentation patterns I observed in your models.

## Current Documentation Analysis

### Excellence Patterns Observed:

**From `int_adjustments.yml`:**
- ✅ Comprehensive business context with system identification
- ✅ Detailed refactoring notes and migration information
- ✅ Extensive data quality findings with specific examples
- ✅ Clear business rule documentation

**From `int_payment_split.yml`:**
- ✅ Detailed column descriptions with business context
- ✅ Comprehensive test coverage with business rationale
- ✅ Good handling of unused/legacy fields
- ✅ Clear validation rules and error messages

**From `int_ar_analysis.yml`:**
- ✅ Detailed aggregation logic documentation
- ✅ Comprehensive relationship testing
- ✅ Good data quality requirements in meta section
- ✅ Clear business impact descriptions

### Standardization Needs:

- ❌ Inconsistent model header structure
- ❌ Variable test coverage patterns
- ❌ Different approaches to business rule documentation
- ❌ Inconsistent meta section usage
- ❌ Variable relationship test patterns

## Documentation Template Framework

### Template 1: Financial/Transaction Models

Use for models like `int_adjustments`, `int_payment_split`, `int_ar_analysis`

```yaml
version: 2

models:
  - name: int_[entity_name]
    description: >
      [Primary business purpose in 1-2 sentences]
      
      This model serves as [role in business process] and provides [key business value].
      Part of System [X]: [System Name] workflow.
      
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
      materialized: [table|incremental]
      schema: intermediate
      unique_key: [primary_key]_id
    
    columns:
      [Column documentation following financial model patterns]
    
    tests:
      [Comprehensive financial validation tests]
    
    meta:
      owner: "dental_finance_team"
      contains_pii: true
      business_process: "[Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "High"
      data_quality_requirements:
        - [Financial requirement 1]
        - [Financial requirement 2]
        - [Validation requirement 1]
        - [Validation requirement 2]
```

### Template 2: Clinical/Operational Models

Use for models like `int_appointment_details`, `int_procedure_complete`

```yaml
version: 2

models:
  - name: int_[entity_name]
    description: >
      [Primary clinical/operational purpose in 1-2 sentences]
      
      This model provides [clinical functionality] and supports [operational processes].
      Part of System [X]: [System Name] workflow.
      
      Key Features:
      - [Clinical feature 1]: [Clinical impact and workflow support]
      - [Operational feature 2]: [Operational efficiency and tracking]
      - [Scheduling feature 3]: [Scheduling optimization and metrics]
      - [Patient care feature]: [Patient experience and care quality]
      
      Data Sources:
      - [clinical_source_1]: [Clinical data and purpose]
      - [operational_source_2]: [Operational data and metrics]
      - [scheduling_source_3]: [Scheduling data and patterns]
      
      Clinical Logic Features:
      - [Clinical workflow 1]: [Implementation and clinical rules]
      - [Patient tracking 2]: [Patient journey and care coordination]
      - [Provider workflow 3]: [Provider efficiency and scheduling]
      
      Operational Calculations:
      - [Efficiency metric 1]: [Calculation and operational value]
      - [Utilization metric 2]: [Calculation and business intelligence]
      - [Performance indicator 3]: [Calculation and quality measurement]
      
      Data Quality Notes:
      - [Clinical data issue 1]: [Impact on patient care and mitigation]
      - [Operational data issue 2]: [Impact on efficiency and handling]
      - [Scheduling data issue 3]: [Impact on workflow and resolution]
      
      Clinical Rules:
      - [Clinical rule 1]: [Patient safety and care standard]
      - [Operational rule 2]: [Workflow efficiency and compliance]
      - [Scheduling rule 3]: [Resource optimization and patient satisfaction]
    
    config:
      materialized: [table|incremental]
      schema: intermediate
      unique_key: [primary_key]_id
    
    columns:
      [Column documentation following clinical model patterns]
    
    tests:
      [Clinical and operational validation tests]
    
    meta:
      owner: "clinical_operations_team"
      contains_pii: true
      business_process: "[Clinical Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "High"
      data_quality_requirements:
        - [Clinical requirement 1]
        - [Operational requirement 2]
        - [Patient safety requirement 3]
```

### Template 3: Insurance/Benefits Models

Use for models like `int_insurance_coverage`, `int_claim_details`

```yaml
version: 2

models:
  - name: int_[entity_name]
    description: >
      [Primary insurance/benefits purpose in 1-2 sentences]
      
      This model manages [insurance functionality] and ensures [benefits optimization].
      Part of System [X]: [Insurance System Name] workflow.
      
      Key Features:
      - [Coverage feature 1]: [Coverage determination and patient benefits]
      - [Claims feature 2]: [Claims processing and reimbursement optimization]
      - [Benefits feature 3]: [Benefits tracking and utilization management]
      - [Verification feature 4]: [Coverage verification and eligibility]
      
      Data Sources:
      - [insurance_source_1]: [Coverage data and carrier information]
      - [benefits_source_2]: [Benefits structure and limitations]
      - [claims_source_3]: [Claims processing and payment data]
      
      Insurance Logic Features:
      - [Coverage determination]: [Rules and eligibility criteria]
      - [Benefits calculation]: [Methodology and limitation handling]
      - [Claims processing]: [Workflow and status tracking]
      - [Payment allocation]: [Distribution rules and reconciliation]
      
      Benefits Calculations:
      - [Coverage percentage]: [Calculation and plan-specific rules]
      - [Deductible tracking]: [Accumulation and reset logic]
      - [Annual maximum]: [Tracking and limitation enforcement]
      - [Copayment determination]: [Calculation and patient responsibility]
      
      Data Quality Notes:
      - [Coverage data issue 1]: [Impact on patient benefits and resolution]
      - [Claims data issue 2]: [Impact on reimbursement and handling]
      - [Benefits data issue 3]: [Impact on patient costs and mitigation]
      
      Insurance Rules:
      - [Coverage rule 1]: [Eligibility and benefit determination]
      - [Claims rule 2]: [Processing standards and compliance]
      - [Payment rule 3]: [Allocation methodology and reconciliation]
    
    config:
      materialized: [table|incremental]
      schema: intermediate
      unique_key: [primary_key]_id
    
    columns:
      [Column documentation following insurance model patterns]
    
    tests:
      [Insurance and benefits validation tests]
    
    meta:
      owner: "insurance_team"
      contains_pii: true
      business_process: "[Insurance Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "High"
      data_quality_requirements:
        - [Coverage requirement 1]
        - [Claims requirement 2]
        - [Benefits requirement 3]
        - [Compliance requirement 4]
```

### Template 4: System/Audit Models

Use for models like `int_opendental_system_logs`, `int_provider_profile`

```yaml
version: 2

models:
  - name: int_[entity_name]
    description: >
      [Primary system/audit purpose in 1-2 sentences]
      
      This model provides [system functionality] and maintains [audit/compliance requirements].
      Part of System [X]: [System Name] or [System monitoring/audit functionality].
      
      Key Features:
      - [System feature 1]: [System integrity and monitoring]
      - [Audit feature 2]: [Compliance and tracking capability]
      - [Security feature 3]: [Access control and user management]
      - [Data integrity feature]: [Data quality and validation]
      
      Data Sources:
      - [system_source_1]: [System data and audit trails]
      - [user_source_2]: [User management and permissions]
      - [security_source_3]: [Security and access control data]
      
      System Logic Features:
      - [System workflow 1]: [Implementation and system rules]
      - [Audit tracking 2]: [Compliance and regulatory requirements]
      - [User management 3]: [Permission and access control]
      
      System Calculations:
      - [Performance metric 1]: [Calculation and system value]
      - [Audit metric 2]: [Calculation and compliance measurement]
      - [Security indicator 3]: [Calculation and risk assessment]
      
      Data Quality Notes:
      - [System data issue 1]: [Impact on system integrity and resolution]
      - [Audit data issue 2]: [Impact on compliance and handling]
      - [Security data issue 3]: [Impact on system security and mitigation]
      
      System Rules:
      - [System rule 1]: [System integrity and operational standard]
      - [Audit rule 2]: [Compliance requirement and enforcement]
      - [Security rule 3]: [Access control and user management]
    
    config:
      materialized: [table|incremental]
      schema: intermediate
      unique_key: [primary_key]_id
    
    columns:
      [Column documentation following system model patterns]
    
    tests:
      [System integrity and audit validation tests]
    
    meta:
      owner: "system_administrators"
      contains_pii: [true|false]
      business_process: "[System Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "[High|Medium|Low]"
      data_quality_requirements:
        - [System requirement 1]
        - [Audit requirement 2]
        - [Security requirement 3]
        - [Compliance requirement 4]
```

### Template 5: Metrics/Aggregation Models

Use for models like `int_communication_metrics`, `int_collection_metrics`

```yaml
version: 2

models:
  - name: int_[entity_name]_metrics
    description: >
      [Primary metrics purpose in 1-2 sentences]
      
      This model aggregates [business area] data to provide [analytical functionality].
      Part of System [X]: [System Name] workflow.
      
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
      materialized: [table|incremental]
      schema: intermediate
      unique_key: [composite_key_fields]
    
    columns:
      [Column documentation following metrics model patterns]
    
    tests:
      [Metrics validation and business rule tests]
    
    meta:
      owner: "[business_team_name]"
      contains_pii: false
      business_process: "[Metrics Process Name]"
      refresh_frequency: "[frequency]"
      business_impact: "High"
      data_quality_requirements:
        - [Metrics requirement 1]
        - [Aggregation requirement 2]
        - [Performance requirement 3]
        - [Quality requirement 4]
```

## Advanced Column Documentation Patterns

### Pattern 7: Metrics/KPI Fields

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

### Pattern 8: System/Audit Fields

```yaml
- name: [system_field]
  description: >
    [System functionality and audit trail description]
    
    System Purpose:
    - Function: [What system function this field supports]
    - Audit Trail: [How this field supports compliance]
    - Security: [Access control or security implications]
    
    Data Governance:
    - Source: [Authoritative source system]
    - Update Frequency: [How often this field changes]
    - Retention: [Data retention requirements]
    
    Operational Impact:
    - Performance: [System performance considerations]
    - Integration: [How this field integrates with other systems]
    - Monitoring: [Monitoring and alerting requirements]
  tests:
    - not_null:
        config:
          description: "[System requirement for field presence]"
    - accepted_values:
        values: [list_of_system_values]
        config:
          description: "[System validation of acceptable values]"
    - dbt_utils.expression_is_true:
        expression: "[system_integrity_rule]"
        config:
          severity: error
          description: "[System integrity requirement]"
```

## Enhanced Test Documentation Patterns

### Metrics-Specific Tests:
```yaml
- dbt_expectations.expect_column_sum_to_equal:
    column_name: [sum_field]
    sum_total: [expected_total]
    config:
      severity: error
      description: >
        Metrics Validation: [Sum validation description]
        
        Business Rule: [Why this sum must equal expected total]
        Impact: [What incorrect sums indicate about data quality]
        Monitoring: [How this test supports operational monitoring]
```

### System Integrity Tests:
```yaml
- dbt_utils.expression_is_true:
    expression: "[system_integrity_check]"
    config:
      severity: error
      description: >
        System Integrity: [System rule description]
        
        Security Requirement: [Security implication]
        Audit Significance: [Why this matters for compliance]
        Operational Impact: [Impact on system operations]
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

### Communication Models Implementation:
```yaml
# Pattern for communication/interaction models
meta:
  owner: "communications_team"
  contains_pii: true
  business_process: "Patient Communication Management"
  refresh_frequency: "real-time"
  business_impact: "High"
  system_integration: "System F: Communications"
  data_quality_requirements:
    - "All communications must be properly categorized"
    - "Direction must be accurately determined"
    - "User attribution must be correct"
    - "Content analysis must be consistent"
    - "Response tracking must be accurate"
```

### Collections Models Implementation:
```yaml
# Pattern for financial/collections models
meta:
  owner: "collections_team"
  contains_pii: true
  business_process: "Collections and AR Management"
  refresh_frequency: "daily"
  business_impact: "High"
  system_integration: "System E: Collections"
  data_quality_requirements:
    - "Payment tracking must be accurate"
    - "Campaign attribution must be correct"
    - "Collection rates must be properly calculated"
    - "Statement effectiveness must be measured"
    - "Task completion must be tracked"
```

### System Models Implementation:
```yaml
# Pattern for system/audit models
meta:
  owner: "system_administrators"
  contains_pii: true
  business_process: "System Audit and Compliance"
  refresh_frequency: "real-time"
  business_impact: "High"
  system_integration: "System Monitoring"
  data_quality_requirements:
    - "All system activities must be logged"
    - "User attribution must be accurate"
    - "Audit trails must be complete"
    - "Security events must be tracked"
    - "System integrity must be maintained"
```

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

### Pattern 3: Boolean Fields with Macro Usage

```yaml
- name: is_[condition]
  description: >
    Flag indicating [business condition] (converted from OpenDental 0/1 integer)
    
    Source Transformation:
    - OpenDental source: "[SourceColumn]" (0/1 integer as stored)
    - Transformation: Uses convert_opendental_boolean() macro
    - Result: PostgreSQL boolean (true/false/null)
    
    Business Logic:
    - true when: [Specific business condition for true state]
    - false when: [Specific business condition for false state] 
    - null when: [Conditions that result in null values]
    
    Operational Usage:
    - [Workflow decisions based on flag]
    - [Reporting and filtering applications]
    - [Business rule enforcement]
  tests:
    - not_null:
        where: "[condition_when_flag_should_be_determined]"
        config:
          description: "[Business requirement for flag determination]"
    - accepted_values:
        values: [true, false]
        config:
          description: "[Boolean validation for business logic integrity]"
```

### Pattern 4: Metadata Fields (Standardized Approach)

```yaml
- name: _extracted_at
  description: >
    ETL pipeline extraction timestamp - when the record was extracted from the source system
    
    Source: ETL pipeline metadata (added during extraction process)
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
    dbt model processing timestamp - when this intermediate model was last run
    
    Source: current_timestamp at dbt model execution
    Purpose: Model execution tracking and debugging
    Usage: Understanding data processing timeline
  tests:
    - not_null
```

### Pattern 3: Calculated/Business Logic Fields

```yaml
- name: [calculated_field]
  description: >
    [Business purpose and calculated value description]
    
    Calculation Logic:
    - Input: [Source data and fields]
    - Method: [Calculation methodology]
    - Output: [Result interpretation and units]
    
    Business Rules:
    - [Business rule 1 affecting calculation]
    - [Business rule 2 affecting calculation]
    - [Edge case handling]
    
    Business Impact:
    - [How this field is used in business processes]
    - [Decision-making supported by this field]
    - [Downstream dependencies]
    
    Data Quality Notes:
    - [Known limitations or assumptions]
    - [Accuracy considerations]
    - [Monitoring requirements]
  tests:
    - not_null:
        where: "[business_condition_when_required]"
        config:
          description: "[Business requirement for non-null values]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: [business_minimum]
        max_value: [business_maximum]
        severity: [error|warn]
        config:
          description: >
            [Business rationale for acceptable range and impact of outliers]
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

### Pattern 6: Flag Fields with Business Logic

```yaml
- name: is_[condition]
  description: >
    Flag indicating [business condition and operational significance]
    
    Business Logic:
    - true when: [Specific business condition for true state]
    - false when: [Specific business condition for false state]
    - Impact: [Business process impact of flag state]
    
    Operational Usage:
    - [Workflow decisions based on flag]
    - [Reporting and filtering applications]
    - [Business rule enforcement]
    
    Data Quality:
    - [Flag determination reliability]
    - [Monitoring requirements]
    - [Business validation methods]
  tests:
    - not_null:
        config:
          description: "[Business requirement for flag determination]"
    - accepted_values:
        values: [true, false]
        config:
          description: "[Boolean validation for business logic integrity]"
```

## Test Documentation Patterns

### Model-Level Test Patterns

#### Business Rule Validation:
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

#### Financial Integrity Tests:
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

#### Data Quality Monitoring:
```yaml
- dbt_expectations.expression_is_true:
    expression: "[data_quality_rule]"
    config:
      severity: warn
      description: >
        Data Quality Monitor: [Quality aspect being monitored]
        
        Expectation: [What we expect to see]
        Tolerance: [Acceptable deviation if any]
        Action Required: [What to do if test fails]
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

This documentation strategy ensures that your sophisticated business logic is properly documented while maintaining consistency across all intermediate models.