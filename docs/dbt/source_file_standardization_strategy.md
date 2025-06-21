# Source Model Refactoring Strategy for MDC Analytics

## Overview

This document provides comprehensive strategic context, patterns, and implementation framework for standardizing OpenDental source file documentation. It works in conjunction with:
- **`source_file_refactor_template.md`** - Provides tactical implementation template and quality standards for AI analysts
- **`source_refactor_two_layer_prompt_system.md`** - Explains how these documents work together in an integrated prompting system

Together, these documents provide a comprehensive framework for standardizing OpenDental source file documentation to support staging model development and business analytics.

---

## Current State Assessment

### Existing Source Files Analysis
Based on your source standardization strategy document, you have:

**Excellent Quality (Keep as Templates):**
- `billing_sources.yml` ✅ - Comprehensive financial tables with detailed meta sections
- `claims_sources.yml` ✅ - Excellent insurance/claims documentation

**Need Enhancement:**
- `appointment_sources.yml` - Good coverage but needs meta sections
- `coding_defs_sources.yml` - Minimal documentation, needs significant work

**Missing Critical Files:**
- Patient and provider management sources
- Fee processing and verification sources
- Communications and referral sources
- Core system configuration sources

## Source Refactoring Framework

### 1. Standardized Source File Template

```yaml
version: 2

sources:
  - name: opendental
    description: "OpenDental dental practice management system data replicated to PostgreSQL analytics database"
    database: opendental_analytics
    schema: raw
    
    # Global configuration for all tables
    loaded_at_field: "_extracted_at"  # ETL pipeline timestamp
    freshness:
      warn_after: {count: 6, period: hour}   # Business hours expectation
      error_after: {count: 24, period: hour} # Maximum acceptable delay
    
    tables:
      - name: [table_name]
        description: >
          [System Classification: System A-G] - [Primary business purpose]
          
          [Detailed business context including]:
          - Core business function and workflow integration
          - Approximate record volume and growth patterns
          - Key relationships to other OpenDental tables
          - Critical business rules and constraints
          - Integration points with clinical/administrative workflows
          - Known data quality characteristics and limitations
        
        # Table-specific freshness overrides
        loaded_at_field: "[specific_field_if_different]"
        freshness:
          warn_after: {count: [X], period: [hour|day]}
          error_after: {count: [Y], period: [hour|day]}
        
        # Comprehensive column documentation
        columns:
          # Primary Key Pattern
          - name: "[Entity]Num"
            description: >
              Primary key - [Business entity] identifier in OpenDental system.
              
              [Additional context]:
              - Auto-incrementing integer starting from 1
              - Used across [related tables] for referential integrity
              - [Business significance and usage patterns]
            tests:
              - unique
              - not_null
              - positive_values
          
          # Foreign Key Pattern
          - name: "[RelatedEntity]Num"
            description: >
              Foreign key reference to [related_entity] table.
              
              [Business relationship context]:
              - [Description of the business relationship]
              - [Cardinality and business rules]
              - [Handling of null values and business meaning]
            tests:
              - relationships:
                  to: source('opendental', '[related_table]')
                  field: "[RelatedEntity]Num"
                  severity: [warn|error]
                  where: "[business_conditions]"
          
          # Status/Enumeration Pattern
          - name: "[Status/Type]"
            description: >
              [Business purpose and workflow context]
              
              Enumerated values in OpenDental:
              0 = [Business meaning and implications]
              1 = [Business meaning and implications]
              2 = [Business meaning and implications]
              [etc.]
              
              [Additional business rules and usage notes]
            tests:
              - not_null
              - accepted_values:
                  values: [0, 1, 2, etc.]
                  config:
                    severity: warn  # Allow for future OpenDental updates
          
          # Boolean Pattern
          - name: "[Flag/Boolean]"
            description: >
              [Business purpose and impact]
              
              Stored as tinyint in OpenDental MySQL:
              0 = No/False - [business implication]
              1 = Yes/True - [business implication]
              
              [Business rules and usage context]
            tests:
              - not_null
              - accepted_values:
                  values: [0, 1]
          
          # Financial Amount Pattern
          - name: "[Amount/Fee]"
            description: >
              [Business purpose] stored as decimal in USD.
              
              [Business context]:
              - [Calculation method or source]
              - [Business rules for amounts]
              - [Relationship to other financial fields]
              - [Known precision and scale characteristics]
            tests:
              - not_null:
                  where: "[business_condition_when_required]"
              - dbt_expectations.expect_column_values_to_be_between:
                  min_value: [business_minimum]
                  max_value: [business_maximum]
                  severity: warn
                  config:
                    description: "[Business rationale for range]"
          
          # Date/Time Pattern
          - name: "[Date/Timestamp]"
            description: >
              [Business purpose and workflow significance]
              
              [Technical details]:
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
          
          # OpenDental Audit Fields Pattern
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
          
          - name: "SecDateEntry"
            description: >
              OpenDental creation timestamp - when record was originally created.
              
              Business significance:
              - Represents actual business transaction date in many cases
              - Used for historical analysis and compliance
              - More reliable than SecDateTEdit for creation timing
            tests:
              - not_null:
                  where: "SecDateEntry is not null"
              - dbt_expectations.expect_column_values_to_be_between:
                  min_value: "'2010-01-01'::timestamp"
                  max_value: "current_timestamp"
          
          # ETL Pipeline Metadata
          - name: "_extracted_at"
            description: >
              ETL pipeline timestamp when record was extracted from OpenDental source.
              
              Added by replication process for:
              - Data lineage tracking
              - Freshness monitoring
              - Debugging extraction issues
            tests:
              - not_null
              - dbt_expectations.expect_column_values_to_be_between:
                  min_value: "'2024-01-01'::timestamp"
                  max_value: "current_timestamp + interval '1 hour'"
        
        # Comprehensive metadata section
        meta:
          # System Classification
          system_integration: "System [A-G]: [System Name and Purpose]"
          business_criticality: "[High|Medium|Low]"
          
          # Data Characteristics
          record_count: "[approximate_count] (as of [date])"
          growth_pattern: "[daily/weekly/monthly growth description]"
          data_retention: "[retention policy if applicable]"
          
          # Data Quality Assessment
          data_quality_results:
            last_tested: 'YYYY-MM-DD'
            tests_passed: [number]
            tests_total: [number]
            quality_score: "[percentage]"
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
                description: "[specific rules tested]"
                last_run: 'YYYY-MM-DD'
              - test: "data_completeness"
                status: "[pass|warning|fail]"
                description: "[completeness assessment]"
                last_run: 'YYYY-MM-DD'
          
          # Known Issues Documentation
          known_issues:
            - issue: "[clear description of the issue]"
              severity: "[high|medium|low]"
              impact: "[business impact description]"
              frequency: "[how often this occurs]"
              identified_date: 'YYYY-MM-DD'
              jira_ticket: "[TICKET-NUMBER if exists]"
              workaround: "[current workaround or handling]"
              test_coverage: "[related test name if exists]"
          
          # Business Context
          business_context:
            primary_workflows: 
              - "[workflow 1 description]"
              - "[workflow 2 description]"
            integration_points:
              - system: "[external system name]"
                direction: "[inbound|outbound|bidirectional]"
                frequency: "[real-time|batch|manual]"
            compliance_requirements:
              - "[HIPAA requirements if applicable]"
              - "[audit requirements if applicable]"
            usage_patterns:
              peak_times: "[when system is most heavily used]"
              update_frequency: "[how often data changes]"
              query_patterns: "[common query types]"
          
          # Technical Metadata
          database_specifics:
            indexes:
              - columns: ["[column_list]"]
                type: "[btree|brin|hash|unique]"
                purpose: "[performance purpose]"
                statistics: "[usage statistics if available]"
            constraints:
              - type: "[foreign_key|check|unique]"
                definition: "[constraint definition]"
                business_rule: "[business rule enforced]"
            partitioning:
              method: "[range|list|hash if applicable]"
              key: "[partitioning column if applicable]"
              business_rationale: "[why partitioned]"
          
          # Access and Security
          data_classification:
            contains_pii: [true|false]
            contains_phi: [true|false]
            data_sensitivity: "[public|internal|confidential|restricted]"
            retention_requirements: "[legal/business retention requirements]"
          
          # Ownership and Governance
          ownership:
            business_owners: ["[team_name_1]", "[team_name_2]"]
            technical_owners: ["data_engineering_team"]
            subject_matter_experts: ["[sme_name if available]"]
          
          # Change Management
          change_history:
            - date: 'YYYY-MM-DD'
              change: "[description of significant schema or business rule change]"
              impact: "[impact on downstream processes]"
              migration_notes: "[any migration considerations]"
```

### 2. System-Based Source File Organization

Based on your established system classification (A-G), organize source files by business function:

#### **System A: Fee Processing & Verification**
**File**: `fee_processing_sources.yml`
```yaml
# Tables: procedurecode, fee, autocode, codegroup
# Focus: Procedure coding standards, fee schedules, automated coding rules
# Business Context: Revenue optimization, coding compliance, fee verification
```

#### **System B: Insurance & Claims Processing**
**File**: `insurance_claims_sources.yml` (enhance existing `claims_sources.yml`)
```yaml
# Tables: claim, claimproc, claimpayment, carrier, insplan, inssub, benefit
# Focus: Insurance processing workflow, claims lifecycle, payment allocation
# Business Context: Revenue cycle management, insurance verification, claims optimization
```

#### **System C: Payment Allocation & Reconciliation**
**File**: `payment_billing_sources.yml` (enhance existing `billing_sources.yml`)
```yaml
# Tables: payment, paysplit, adjustment, statement, dunning
# Focus: Payment processing, financial reconciliation, billing workflow
# Business Context: Cash flow management, patient billing, financial reporting
```

#### **System D: Patient & Provider Management**
**File**: `patient_provider_sources.yml`
```yaml
# Tables: patient, provider, clinic, operatory, userod
# Focus: Core entity management, demographics, provider scheduling
# Business Context: Patient care coordination, provider productivity, facility management
```

#### **System E: Clinical Operations**
**File**: `clinical_operations_sources.yml`
```yaml
# Tables: procedurelog, appointment, treatplan, treatplanattach
# Focus: Clinical workflow, treatment planning, procedure execution
# Business Context: Clinical efficiency, treatment outcomes, procedure tracking
```

#### **System F: Communications & Referrals**
**File**: `communications_referrals_sources.yml`
```yaml
# Tables: commlog, emailmessage, referral, task, recall
# Focus: Patient communications, referral management, task workflow
# Business Context: Patient engagement, referral tracking, communication compliance
```

#### **System G: Scheduling & Resources**
**File**: `scheduling_resources_sources.yml` (enhance existing `appointment_sources.yml`)
```yaml
# Tables: appointment, schedule, scheduleop, appointmentrule, blockout
# Focus: Appointment scheduling, resource management, availability tracking
# Business Context: Schedule optimization, resource utilization, patient access
```

#### **System Support: Configuration & Reference**
**File**: `configuration_reference_sources.yml` (enhance existing `coding_defs_sources.yml`)
```yaml
# Tables: definition, preference, program, programproperty
# Focus: System configuration, lookup tables, integration settings
# Business Context: System administration, business rule configuration, reference data
```

## 3. Enhanced Column Documentation Patterns

### Primary Key Enhancement
```yaml
- name: "[Entity]Num"
  description: >
    Primary key - [Entity] identifier in OpenDental system.
    
    Technical characteristics:
    - Auto-incrementing integer starting from 1
    - Referenced by [list related tables] as foreign key
    - Used in [specific business processes]
    
    Business significance:
    - [How this ID is used in workflows]
    - [Any business rules about ID assignment]
    - [Relationship to external system IDs if applicable]
  tests:
    - unique
    - not_null
    - positive_values
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: 1
        max_value: 999999999  # Reasonable business maximum
```

### Foreign Key Enhancement
```yaml
- name: "[RelatedEntity]Num"
  description: >
    Foreign key reference to [related_entity] table ([business_relationship]).
    
    Business relationship:
    - [Cardinality description: one-to-many, many-to-one, etc.]
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
        severity: [warn|error]
        where: "[business_conditions]"
        config:
          error_if: "> 100"  # Business threshold for relationship failures
          warn_if: "> 10"
```

### Status/Enumeration Enhancement
```yaml
- name: "[Status]"
  description: >
    [Business workflow status] controlling [business process].
    
    OpenDental enumerated values:
    0 = [Status] - [Business meaning, workflow implications, user actions]
    1 = [Status] - [Business meaning, workflow implications, user actions]
    2 = [Status] - [Business meaning, workflow implications, user actions]
    
    Business rules:
    - [State transition rules if applicable]
    - [Who can change status and when]
    - [Impact on related records or processes]
    - [Historical data considerations]
    
    Workflow integration:
    - [How status affects user interface]
    - [Impact on business reports and metrics]
    - [Integration with external systems]
  tests:
    - not_null
    - accepted_values:
        values: [0, 1, 2, etc.]
        config:
          severity: warn  # Allow for future OpenDental enum additions
    - dbt_expectations.expect_column_value_lengths_to_equal:
        value: 1
        severity: error
```

### Financial Field Enhancement
```yaml
- name: "[Amount]"
  description: >
    [Business purpose] amount in USD, stored as decimal(10,2).
    
    Business context:
    - [How amount is calculated or determined]
    - [Relationship to other financial fields]
    - [Business rules for amount values]
    - [Impact on financial reporting and analysis]
    
    Data characteristics:
    - Precision: 2 decimal places (cents)
    - Range: [typical business range]
    - Currency: Always USD
    - [Handling of refunds, adjustments, etc.]
  tests:
    - not_null:
        where: "[business_condition_when_required]"
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: [business_minimum]
        max_value: [business_maximum]
        severity: warn
        config:
          description: "Amounts outside normal business range"
    - dbt_expectations.expect_column_values_to_match_regex:
        regex: "^-?\\d{1,8}\\.\\d{2}$"
        severity: error
        config:
          description: "Ensure proper decimal formatting"
```

## 4. Data Quality Assessment Framework

### Quality Metrics by Table Type

#### **Transactional Tables** (high volume, frequent updates)
```yaml
data_quality_results:
  assessment_type: "transactional"
  quality_checks:
    - test: "primary_key_integrity"
      threshold: 99.99%
      business_impact: "Critical - affects all downstream processing"
    - test: "foreign_key_integrity"
      threshold: 95%
      business_impact: "High - may cause incomplete business analysis"
    - test: "required_field_completeness"
      threshold: 90%
      business_impact: "Medium - may affect specific analyses"
    - test: "business_rule_compliance"
      threshold: 85%
      business_impact: "Medium - requires business review"
```

#### **Reference Tables** (low volume, infrequent updates)
```yaml
data_quality_results:
  assessment_type: "reference"
  quality_checks:
    - test: "primary_key_integrity"
      threshold: 100%
      business_impact: "Critical - reference data must be complete"
    - test: "value_consistency"
      threshold: 100%
      business_impact: "Critical - inconsistent reference data affects all analyses"
    - test: "completeness"
      threshold: 95%
      business_impact: "High - missing reference data causes lookup failures"
```

#### **Configuration Tables** (system settings, definitions)
```yaml
data_quality_results:
  assessment_type: "configuration"
  quality_checks:
    - test: "configuration_completeness"
      threshold: 100%
      business_impact: "Critical - missing configuration affects system behavior"
    - test: "valid_configuration_values"
      threshold: 100%
      business_impact: "Critical - invalid configuration may cause system errors"
```

## 5. Implementation Phases

### **Phase 1: Foundation Setup (Week 1)**
- [ ] Create standardized source file templates
- [ ] Document quality assessment framework
- [ ] Establish naming conventions alignment
- [ ] Create implementation checklists

### **Phase 2: Core Business Entities (Week 2)**
- [ ] **Patient & Provider Management** (`patient_provider_sources.yml`)
  - Tables: patient, provider, clinic, userod
  - Focus: Core entity integrity and relationships
- [ ] **Fee Processing** (`fee_processing_sources.yml`)
  - Tables: procedurecode, fee, autocode
  - Focus: Revenue foundation data

### **Phase 3: Financial Core (Week 3)**
- [ ] **Enhanced Billing Sources** (build on existing `billing_sources.yml`)
  - Add comprehensive meta sections
  - Enhance column documentation
  - Add data quality assessments
- [ ] **Enhanced Claims Sources** (build on existing `claims_sources.yml`)
  - Standardize with template format
  - Add missing business context
  - Enhance test coverage

### **Phase 4: Clinical Operations (Week 4)**
- [ ] **Clinical Operations** (`clinical_operations_sources.yml`)
  - Tables: procedurelog, appointment, treatplan
  - Focus: Clinical workflow documentation
- [ ] **Enhanced Scheduling** (enhance existing `appointment_sources.yml`)
  - Complete meta sections
  - Add business context
  - Standardize test patterns

### **Phase 5: Supporting Systems (Week 5)**
- [ ] **Communications & Referrals** (`communications_referrals_sources.yml`)
  - Tables: commlog, emailmessage, referral, task
  - Focus: Communication workflow
- [ ] **Enhanced Configuration** (enhance existing `coding_defs_sources.yml`)
  - Significant expansion of documentation
  - Add business context for all definitions
  - Document configuration relationships

### **Phase 6: Integration and Validation (Week 6)**
- [ ] Cross-file relationship validation
- [ ] Test pattern consistency verification
- [ ] Business context completeness review
- [ ] Integration with staging model requirements
- [ ] Data quality assessment completion

## 6. Quality Gates and Success Criteria

### **Per-File Quality Gates**
- [ ] **Template Compliance**: Follows standardized structure exactly
- [ ] **Business Context**: Comprehensive business documentation for all tables
- [ ] **Column Documentation**: All columns have detailed business descriptions
- [ ] **Test Coverage**: Appropriate tests for all business-critical validations
- [ ] **Meta Sections**: Complete meta sections with quality assessments
- [ ] **Relationship Documentation**: All foreign keys properly documented and tested

### **System-Level Quality Gates**
- [ ] **Cross-File Consistency**: Related tables documented consistently across files
- [ ] **Relationship Integrity**: All foreign key relationships validated across files
- [ ] **Business Process Alignment**: Documentation supports end-to-end business understanding
- [ ] **Staging Model Support**: Source documentation enables staging model development
- [ ] **Data Quality Transparency**: Known issues documented with business impact

### **Integration Quality Gates**
- [ ] **Staging Alignment**: Source documentation maps cleanly to staging model requirements
- [ ] **Naming Convention Compliance**: All names follow established MDC Analytics conventions
- [ ] **ETL Pipeline Support**: Documentation supports current ETL pipeline implementation
- [ ] **Business User Accessibility**: Non-technical stakeholders can understand table purposes

## 7. Benefits and Expected Outcomes

### **Immediate Benefits**
1. **Consistent Foundation**: Standardized source documentation supporting staging models
2. **Improved Data Discovery**: Business users can quickly identify relevant data sources
3. **Enhanced Data Quality**: Comprehensive testing catches issues at source level
4. **Reduced Development Time**: Clear source documentation accelerates staging model development

### **Long-term Benefits**
1. **Audit Compliance**: Comprehensive documentation supports regulatory requirements
2. **Stakeholder Confidence**: Transparent data quality documentation builds trust
3. **Maintenance Efficiency**: Standardized documentation easier to maintain and update
4. **Knowledge Management**: Comprehensive business context preserved for team continuity

### **Integration Benefits**
1. **Seamless Staging Development**: Source documentation directly supports staging model creation
2. **Consistent Data Lineage**: Clear documentation trail from source through staging to marts
3. **Business Rule Preservation**: Business logic documented consistently across layers
4. **Quality Assurance**: Source-level quality documentation supports end-to-end data quality

## 8. Maintenance and Evolution Strategy

### **Regular Maintenance**
- **Quarterly Reviews**: Data quality assessments and business context updates
- **Change Documentation**: Update documentation when OpenDental schema changes
- **Test Evolution**: Update tests as business rules evolve
- **Stakeholder Feedback**: Regular review with business users and data consumers

### **Change Management Process**
1. **Impact Assessment**: Evaluate downstream impact of source documentation changes
2. **Staging Model Alignment**: Ensure changes maintain consistency with staging models
3. **Business Review**: Validate business context changes with stakeholders
4. **Version Control**: Track all documentation changes with implementation changes

This refactoring strategy creates a solid, standardized foundation for your source documentation that directly supports your successful staging model approach while maintaining consistency with your established naming conventions and business processes.