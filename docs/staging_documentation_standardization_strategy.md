# Staging Documentation Standardization Strategy

## Current State Analysis

After reviewing multiple staging model documentation files, I've identified significant inconsistencies:

### Quality Tiers Observed:
1. **Excellent**: `_stg_opendental__patient.yml` - Comprehensive descriptions, business context, data quality notes
2. **Good**: `_stg_opendental__appointment.yml` - Detailed but could be improved
3. **Basic**: `_stg_opendental__fee.yml` - Has some quality notes but inconsistent structure
4. **Minimal**: `_stg_opendental__programproperty.yml` - Basic documentation only

### Key Inconsistencies:
- **Description Quality**: Ranges from single line to comprehensive business context
- **Test Coverage**: Inconsistent application of data validation tests
- **Metadata Documentation**: Varies between models
- **Business Context**: Some models lack usage notes and known issues
- **Data Quality Notes**: Only present in some models despite widespread data quality issues

## Standardization Goals

1. **Consistent Structure**: All staging yml files follow the same template
2. **Comprehensive Documentation**: Include business context, usage notes, and data quality issues
3. **Standardized Testing**: Apply consistent test patterns across all models
4. **Clear Relationships**: Document all foreign key relationships with appropriate severity levels
5. **Metadata Standards**: Consistent metadata column documentation
6. **Data Quality Transparency**: Document known issues and their business impact

## Template Structure

### Model-Level Documentation
```yaml
version: 2

models:
  - name: stg_opendental__[table_name]
    description: >
      [Primary business purpose - 1-2 sentences]
      
      [Detailed description including]:
      - Business context and purpose
      - Record count and scope
      - Key relationships
      - Important business rules
    
    meta:
      # Data quality and business context
      record_count: [current count]
      data_scope: "[date range or scope description]"
      
      known_issues:
        - description: "[issue description]"
          severity: "[warn|error]"
          jira_ticket: "[ticket if exists]"
          identified_date: "[YYYY-MM-DD]"
          test: "[test_name if applicable]"
      
      business_rules:
        - rule: "[business rule description]"
          impact: "[business impact]"
      
      usage_notes: >
        [Key usage considerations, assumptions, or limitations]
    
    tests:
      # Model-level tests
      [standardized test patterns]
    
    columns:
      # Column documentation following standards
```

### Column Documentation Standards
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
          severity: [warn|error]  # warn for historical/optional relationships
          where: "[conditions if applicable]"
  
  # Business Columns
  - name: [column_name]
    description: >
      [Business description including]:
      - Purpose and usage
      - Value ranges or meanings
      - Business rules
      - Known data quality issues if applicable
    tests:
      [appropriate tests based on data type and business rules]
  
  # Status/Enum Columns
  - name: [status_column]
    description: >
      [Status description with all possible values]:
      0 = [meaning]
      1 = [meaning]
      etc.
    tests:
      - not_null
      - accepted_values:
          values: [0, 1, 2, etc.]
  
  # Boolean Columns
  - name: [boolean_column]
    description: "[Purpose] (0=No, 1=Yes converted to boolean)"
    tests:
      - not_null
      - boolean_values
  
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
  
  - name: _created_by_user_id
    description: "User who created the record in the source system (OpenDental). [Source column mapping]. May be null for [specific conditions]."
    tests:
      - not_null:
          where: "[conditions where not null expected]"
```

## Data Quality Documentation Standards

### Known Issues Section
Document all known data quality issues with:
- **Clear Description**: What the issue is
- **Business Impact**: How it affects analysis
- **Identification Date**: When discovered
- **Tracking**: JIRA ticket or reference
- **Test Reference**: Related test file if exists

### Business Rules Section
Document key business rules that affect data interpretation:
- **Rule Description**: What the rule is
- **Business Impact**: Why it matters
- **Implementation**: How it's enforced

### Usage Notes Section
Include critical information for analysts:
- **Scope and Coverage**: What data is included/excluded
- **Limitations**: What the data cannot be used for
- **Assumptions**: Key assumptions in the data model
- **Best Practices**: How to use the data effectively

## Test Standardization

### Required Tests by Column Type:
1. **Primary Keys**: unique, not_null, positive_values
2. **Foreign Keys**: relationships (with appropriate severity)
3. **Status/Enum**: not_null, accepted_values
4. **Boolean**: not_null, boolean_values
5. **Financial**: non_negative_or_null (where appropriate)
6. **Dates**: date validation tests
7. **Metadata**: not_null for all _loaded_at columns

### Model-Level Tests:
- Row count comparisons where appropriate
- Business rule validation
- Data quality expressions
- Custom validation tests

## Severity Guidelines

### Error Severity (severity: error):
- Data integrity violations
- Business rule violations that prevent analysis
- Primary key violations
- Critical foreign key relationships

### Warning Severity (severity: warn):
- Data quality issues that don't prevent analysis
- Historical or deprecated relationships
- Statistical outliers
- Business rule violations that require attention but don't block usage

## Implementation Phases

### Phase 1: Template Creation and Standards
- [x] Create standardization strategy document
- [ ] Create yml template file
- [ ] Document test standards
- [ ] Create implementation checklist

### Phase 2: High-Priority Models (Financial/Clinical Core)
- [ ] Patient, Provider, Procedure Code (foundation models)
- [ ] Fee, Payment, Claim (financial models)
- [ ] Appointment, Procedure Log (clinical models)

### Phase 3: Supporting Models
- [ ] Insurance-related models
- [ ] Communication and scheduling models
- [ ] Reference/lookup tables

### Phase 4: Administrative Models
- [ ] User management, task tracking
- [ ] System configuration models
- [ ] Logging and audit models

### Phase 5: Validation and Quality Assurance
- [ ] Review all updated documentation
- [ ] Validate test coverage
- [ ] Ensure consistency across all models
- [ ] Document any exceptions or special cases

## Success Metrics

1. **Consistency**: All staging models follow the same documentation pattern
2. **Completeness**: All business-critical information is documented
3. **Usefulness**: Documentation enables analysts to use data effectively
4. **Maintainability**: Documentation can be easily updated as business rules change
5. **Test Coverage**: Appropriate tests protect data quality without being overly restrictive

## Benefits Expected

1. **Improved Data Discovery**: Analysts can quickly understand model purpose and limitations
2. **Reduced Support Requests**: Self-service documentation reduces questions
3. **Better Data Quality**: Comprehensive testing catches issues early
4. **Faster Onboarding**: New team members can understand the data model quickly
5. **Audit Compliance**: Comprehensive documentation supports audit requirements
6. **Stakeholder Confidence**: Transparent data quality documentation builds trust

## Maintenance Strategy

1. **Regular Reviews**: Quarterly review of documentation accuracy
2. **Issue Tracking**: Update known_issues section as problems are discovered/resolved
3. **Test Evolution**: Update tests as business rules change
4. **Version Control**: Track documentation changes with model changes
5. **Stakeholder Feedback**: Regular feedback from data consumers on documentation usefulness