# Source File Refactoring Prompt Template for AI Analysts

## Overview

This document provides the tactical implementation template and quality standards for refactoring OpenDental source files. It works in conjunction with:
- **`source_file_standardization_strategy.md`** - Provides comprehensive strategic context, patterns, and implementation framework
- **`source_refactor_two_layer_prompt_system.md`** - Explains how these documents work together in an integrated prompting system

Together, these documents provide a comprehensive framework for standardizing OpenDental source file documentation to support staging model development and business analytics.

---

## **Context and Objective**

You are an AI analyst tasked with refactoring OpenDental source file documentation for MDC Analytics, a dental clinic data pipeline. Your goal is to transform existing source files into comprehensive, business-focused documentation that follows established patterns and supports both technical and business users.

## **Quality Standards and Templates**

### **Excellent Reference Files (Use as Templates)**
- `billing_sources.yml` - Comprehensive meta sections, detailed business context, proper test patterns
- `claims_sources.yml` - Excellent relationship documentation, data quality assessments, business workflows

### **Target Quality Level**
Transform source files to match the excellence of the reference files above, ensuring every table has:
1. Comprehensive business context and workflows
2. Complete meta sections with data quality assessments
3. Detailed column documentation with business meanings
4. Appropriate test coverage with proper severity levels
5. Clear governance and ownership information

## **Required File Structure Template**

```yaml
version: 2

sources:
  - name: opendental
    description: "OpenDental dental practice management system data replicated to PostgreSQL analytics database"
    database: opendental_analytics
    schema: raw
    
    # Global configuration
    loaded_at_field: "_extracted_at"  # Or table-specific field
    freshness:
      warn_after: {count: [X], period: [hour|day]}
      error_after: {count: [Y], period: [hour|day]}
    
    tables:
      - name: [table_name]
        description: >
          [System Classification: System A-G] - [Primary business purpose in 1-2 sentences]
          
          [Detailed business context including]:
          - Core business function and workflow integration
          - Approximate record volume and growth patterns  
          - Key relationships to other OpenDental tables
          - Critical business rules and constraints
          - Integration points with clinical/administrative workflows
          - Known data quality characteristics and limitations
        
        # Table-specific overrides if needed
        loaded_at_field: "[specific_field]"
        freshness:
          warn_after: {count: [X], period: [hour|day]}
          error_after: {count: [Y], period: [hour|day]}
        
        columns:
          # [Follow column documentation patterns below]
        
        meta:
          # [Follow meta section template below]
```

## **System Classification Framework**

Classify each table according to the MDC Analytics system framework:

- **System A: Fee Processing & Verification** - Procedure codes, fees, verification workflows
- **System B: Insurance & Claims Processing** - Insurance plans, claims, payments, carriers
- **System C: Payment Allocation & Reconciliation** - Payments, adjustments, billing, statements
- **System D: Patient & Provider Management** - Patient demographics, provider information, clinics
- **System E: Clinical Operations** - Procedures, appointments, treatment plans, clinical documentation
- **System F: Communications & Referrals** - Patient communications, referrals, task management
- **System G: Scheduling & Resources** - Appointment scheduling, operatories, availability
- **System Support: Configuration & Reference** - System settings, definitions, lookup tables

## **Column Documentation Patterns**

### **Primary Key Pattern**
```yaml
- name: "[Entity]Num"
  description: >
    Primary key - [Business entity] identifier in OpenDental system.
    
    Technical characteristics:
    - Auto-incrementing integer starting from 1
    - Referenced by [list related tables] for referential integrity
    - [Any special business significance]
    
    Business significance:
    - [How this ID is used in workflows]
    - [Business rules about ID assignment]
    - [Relationship to external system IDs if applicable]
  tests:
    - unique
    - not_null
    - positive_values
```

### **Foreign Key Pattern**
```yaml
- name: "[RelatedEntity]Num"
  description: >
    Foreign key reference to [related_entity] table ([business_relationship]).
    
    Business relationship:
    - [Cardinality: one-to-many, many-to-one, etc.]
    - [Business rules governing the relationship]
    - [When/why this relationship is created or modified]
    
    Data quality considerations:
    - [Null handling and business meaning]
    - [Historical data considerations]
    - [Impact of missing relationships]
  tests:
    - relationships:
        to: source('opendental', '[related_table]')
        field: "[RelatedEntity]Num"
        severity: [warn|error]  # error for critical, warn for historical
        where: "[business_conditions if applicable]"
```

### **Status/Enumeration Pattern**
```yaml
- name: "[Status/Type]"
  description: >
    [Business workflow status] controlling [business process].
    
    OpenDental enumerated values with business impact:
    0 = [Status] - [Business meaning, workflow implications, user actions]
    1 = [Status] - [Business meaning, workflow implications, user actions]
    2 = [Status] - [Business meaning, workflow implications, user actions]
    
    Business rules:
    - [State transition rules if applicable]
    - [Who can change status and when]
    - [Impact on related records or processes]
    - [Workflow integration details]
  tests:
    - not_null
    - accepted_values:
        values: [0, 1, 2, etc.]
        config:
          severity: warn  # Allow for future OpenDental updates
```

### **Boolean/Flag Pattern**
```yaml
- name: "[Flag]"
  description: >
    [Business purpose and impact] controlling [business behavior].
    
    Stored as tinyint in OpenDental MySQL:
    0 = No/False - [business implication and workflow impact]
    1 = Yes/True - [business implication and workflow impact]
    
    Business rules:
    - [When flag is set and by whom]
    - [Impact on business processes]
    - [Integration with other systems]
  tests:
    - not_null
    - accepted_values:
        values: [0, 1]
```

### **Financial Amount Pattern**
```yaml
- name: "[Amount]"
  description: >
    [Business purpose] amount in USD, stored as decimal.
    
    Business context:
    - [How amount is calculated or determined]
    - [Relationship to other financial fields]
    - [Business rules for amount values]
    - [Impact on financial reporting]
    
    Data characteristics:
    - Precision: [decimal places]
    - Range: [typical business range]
    - [Handling of negatives, refunds, etc.]
  tests:
    - not_null:
        where: "[business_condition_when_required]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: [business_minimum]
        max_value: [business_maximum]
        severity: warn
```

### **Date/Time Pattern**
```yaml
- name: "[Date/Timestamp]"
  description: >
    [Business purpose and workflow significance]
    
    Technical details:
    - Stored as [datetime/date] in OpenDental MySQL
    - Timezone: [Central/Local practice timezone]
    - [Business rules about date relationships]
    - [Null handling and business meaning]
  tests:
    - not_null:
        where: "[business_condition_when_required]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: "'2000-01-01'::date"
        max_value: "current_date + interval '1 day'"
        severity: warn
```

### **OpenDental Audit Fields Pattern**
```yaml
- name: "SecDateTEdit"
  description: >
    OpenDental audit timestamp - automatically updated when record is modified.
    
    Critical for ETL pipeline:
    - Used for incremental data extraction
    - Enables change data capture
    - May not reflect all field changes (OpenDental limitation)
  tests:
    - not_null
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: "'2020-01-01'::timestamp"
        max_value: "current_timestamp + interval '2 hours'"

- name: "SecUserNumEntry"
  description: >
    OpenDental user who created this record.
    
    References userod.UserNum for audit trail and data lineage.
    May be 0 or null for system-generated records or data migrations.
  tests:
    - relationships:
        to: source('opendental', 'userod')
        field: "UserNum"
        severity: warn
        where: "SecUserNumEntry > 0"
```

## **Required Meta Section Template**

```yaml
meta:
  # System Classification
  system_integration: "System [A-G]: [System Name and Purpose]"
  business_criticality: "[High|Medium|Low]"
  
  # Data Characteristics
  record_count: "[approximate_count] (as of [date])"
  data_scope: "[date range or business scope description]"
  update_frequency: "[description of how often data changes]"
  
  # Data Quality Assessment (REQUIRED - follow billing_sources.yml pattern)
  data_quality_results:
    last_tested: 'YYYY-MM-DD'
    tests_passed: [number]
    tests_total: [number]
    quality_score: "[percentage if calculated]"
    quality_checks:
      - test: "primary_key_integrity"
        status: "[pass|warning|fail]"
        description: "[findings and business impact]"
        last_run: 'YYYY-MM-DD'
      - test: "foreign_key_integrity"
        status: "[pass|warning|fail]"
        description: "[findings and business impact]"
        last_run: 'YYYY-MM-DD'
      - test: "business_rule_validation"
        status: "[pass|warning|fail]"
        description: "[specific business rules tested]"
        last_run: 'YYYY-MM-DD'
      - test: "data_completeness"
        status: "[pass|warning|fail]"
        description: "[completeness assessment]"
        last_run: 'YYYY-MM-DD'
  
  # Business Context (REQUIRED)
  business_context:
    primary_workflows:
      - "[workflow 1 description with business impact]"
      - "[workflow 2 description with business impact]"
    integration_points:
      - system: "[external system name]"
        direction: "[inbound|outbound|bidirectional]"
        frequency: "[real-time|batch|manual]"
    performance_metrics:
      - "[metric 1 derived from this data]"
      - "[metric 2 derived from this data]"
    compliance_requirements:
      - "[HIPAA requirements if applicable]"
      - "[audit requirements if applicable]"
  
  # Known Issues (REQUIRED - document honestly)
  known_issues:
    - issue: "[clear description of the issue]"
      severity: "[high|medium|low]"
      impact: "[business impact description]"
      frequency: "[how often this occurs]"
      identified_date: 'YYYY-MM-DD'
      jira_ticket: "[TICKET-NUMBER if exists]"
      workaround: "[current workaround or handling]"
      test_coverage: "[related test name if exists]"
  
  # Technical Metadata
  indexes:
    - columns: ["[column_list]"]
      type: "[btree|brin|hash|unique]"
      description: "[performance purpose and query patterns]"
  
  # Governance
  contains_pii: [true|false]
  contains_phi: [true|false]  # Healthcare-specific
  business_owners: ["[team_names]"]
  technical_owners: ["data_engineering_team"]
```

## **Freshness Configuration Guidelines**

### **High-Frequency Updates** (real-time or hourly)
- Appointments, procedures, payments
- `warn_after: {count: 2, period: hour}`
- `error_after: {count: 8, period: hour}`

### **Daily Business Updates** (end-of-day processing)
- Claims, adjustments, statements
- `warn_after: {count: 6, period: hour}`
- `error_after: {count: 24, period: hour}`

### **Weekly/Administrative Updates** (configuration, definitions)
- System definitions, fee schedules, providers
- `warn_after: {count: 7, period: day}`
- `error_after: {count: 30, period: day}`

## **Test Severity Guidelines**

### **Error Severity** (severity: error)
- Primary key violations
- Critical foreign key relationships (current, active records)
- Business rule violations that prevent analysis
- Data integrity issues that break downstream processing

### **Warning Severity** (severity: warn)
- Historical or deprecated relationships
- Data quality issues that don't prevent analysis
- Statistical outliers that require attention
- Business rule violations that need review but don't block usage

## **Business Context Requirements**

### **For Each Table, Document:**
1. **Primary Business Purpose** - Why does this table exist?
2. **Key Workflows** - How is this data used in daily operations?
3. **Integration Points** - How does this connect to other systems?
4. **Performance Metrics** - What business metrics derive from this data?
5. **Data Volume and Growth** - How much data and how fast is it growing?
6. **Update Patterns** - When and how often does data change?

### **For Each Column, Document:**
1. **Business Meaning** - What does this field represent in business terms?
2. **Workflow Impact** - How do changes to this field affect operations?
3. **Data Quality Characteristics** - What are known issues or limitations?
4. **Relationships** - How does this connect to other data?

## **Quality Checklist for Completion**

### **File-Level Quality Gates**
- [ ] **System Classification**: Table classified according to A-G framework
- [ ] **Business Context**: Comprehensive description of business purpose and workflows
- [ ] **Meta Section**: Complete meta section following template exactly
- [ ] **Data Quality**: Honest assessment of known issues and limitations
- [ ] **Governance**: Clear ownership and responsibility assignments

### **Table-Level Quality Gates**
- [ ] **Business Purpose**: Clear explanation of why table exists and how it's used
- [ ] **Relationship Documentation**: All foreign keys documented with business context
- [ ] **Test Coverage**: Appropriate tests for all business-critical validations
- [ ] **Known Issues**: Documented data quality issues with business impact
- [ ] **Performance Context**: Index documentation includes query patterns

### **Column-Level Quality Gates**
- [ ] **Business Meaning**: Every column has clear business purpose
- [ ] **Enumeration Values**: All possible values documented with business impact
- [ ] **Null Handling**: Business rules for when nulls are acceptable
- [ ] **Data Quality**: Known limitations or issues documented
- [ ] **Test Appropriateness**: Tests match column type and business criticality

## **Deliverable Requirements**

### **Enhanced Source File Must Include:**
1. **Comprehensive table descriptions** with business context and workflows
2. **Complete meta sections** following the template exactly
3. **Detailed column documentation** with business meanings and relationships
4. **Appropriate test coverage** with proper severity levels
5. **Honest data quality assessment** including known issues and limitations
6. **Clear governance** with business and technical ownership

### **Documentation Quality Standards:**
- **Business-Focused**: Non-technical stakeholders can understand table purposes
- **Comprehensive**: All critical information for data consumers is included
- **Accurate**: Documentation reflects actual data behavior and business rules
- **Maintainable**: Structure supports ongoing updates and modifications
- **Actionable**: Known issues include clear impacts and workarounds

Use the reference files (`billing_sources.yml` and `claims_sources.yml`) as your quality benchmark. Every table should reach the same level of comprehensive documentation and business context as demonstrated in those excellent examples.