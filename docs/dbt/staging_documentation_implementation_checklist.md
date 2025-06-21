# Staging Documentation Implementation Checklist

## Per-Model Implementation Checklist

Use this checklist for each staging model yml file to ensure consistent, comprehensive documentation.

### ✅ Pre-Implementation Preparation
- [ ] Review corresponding SQL model file for business logic
- [ ] Check existing analysis files in `analysis/[table_name]/` for data quality insights
- [ ] Review stakeholder reports for known issues
- [ ] Identify current record count and data scope
- [ ] Review source table in OpenDental documentation if available

### ✅ Model-Level Documentation

#### **Header and Basic Info**
- [ ] **Model Name**: Follows `stg_opendental__[table_name]` pattern
- [ ] **Primary Description**: 1-2 sentence business purpose
- [ ] **Detailed Description**: Includes business context, scope, relationships
- [ ] **Version**: Set to `version: 2`

#### **Meta Section Implementation**
- [ ] **Record Count**: Document current approximate count
- [ ] **Data Scope**: Date range or scope description
- [ ] **Known Issues**: Document all known data quality problems
  - [ ] Issue description (clear and actionable)
  - [ ] Severity level (warn/error)
  - [ ] Identification date
  - [ ] JIRA ticket reference (if exists)
  - [ ] Related test name (if applicable)
- [ ] **Business Rules**: Document key business logic
  - [ ] Rule description
  - [ ] Business impact
- [ ] **Usage Notes**: Critical information for analysts
  - [ ] Scope and limitations
  - [ ] Key assumptions
  - [ ] Best practices

#### **Model-Level Tests**
- [ ] **Row Count Validation**: Compare to source where appropriate
- [ ] **Business Rule Tests**: Custom expressions for business logic
- [ ] **Data Quality Tests**: Overall model validation
- [ ] **Relationship Tests**: Multi-table validation where needed

### ✅ Column Documentation Standards

#### **Primary Key Columns**
- [ ] **Name**: Follows `[entity]_id` pattern
- [ ] **Description**: Includes business description and source mapping
- [ ] **Required Tests**:
  - [ ] `unique`
  - [ ] `not_null`
  - [ ] `positive_values`

#### **Foreign Key Columns**
- [ ] **Name**: Follows `[related_entity]_id` pattern
- [ ] **Description**: Explains business relationship
- [ ] **Required Tests**:
  - [ ] `relationships` test with appropriate model reference
  - [ ] `severity` level (error for required, warn for historical)
  - [ ] `where` clause if conditional relationship

#### **Status/Enumeration Columns**
- [ ] **Description**: Lists all possible values with meanings
  - Format: "0 = Meaning, 1 = Meaning, etc."
- [ ] **Required Tests**:
  - [ ] `not_null` (if required)
  - [ ] `accepted_values` with complete value list

#### **Boolean Columns**
- [ ] **Description**: Explains purpose with 0/1 mapping
  - Format: "[Purpose] (0=No, 1=Yes converted to boolean)"
- [ ] **Required Tests**:
  - [ ] `not_null`
  - [ ] `boolean_values` (custom test)

#### **Financial/Numeric Columns**
- [ ] **Description**: Includes business context and units
- [ ] **Appropriate Tests**:
  - [ ] `non_negative_or_null` (where business rules require)
  - [ ] Range validation (where applicable)
  - [ ] Statistical outlier tests (for amounts)

#### **Date/Time Columns**
- [ ] **Description**: Explains business purpose and timezone
- [ ] **Required Tests**:
  - [ ] Date format validation
  - [ ] Future date restrictions (where applicable)
  - [ ] Chronological order validation (where applicable)

#### **Text/String Columns**
- [ ] **Description**: Explains purpose and content expectations
- [ ] **Appropriate Tests**:
  - [ ] Length validation (where applicable)
  - [ ] Format validation (where applicable)
  - [ ] Non-empty validation (where required)

### ✅ Required Metadata Columns (ALL Models)

#### **_loaded_at**
- [ ] **Description**: "Timestamp when the data was loaded into the data warehouse by the ETL pipeline (using current_timestamp)"
- [ ] **Required Test**: `not_null`

#### **_created_at**
- [ ] **Description**: "Timestamp when the record was created in the source system (OpenDental). [Source column mapping]. May be null for [specific conditions]."
- [ ] **Required Test**: `not_null` with appropriate `where` clause if nulls are expected

#### **_updated_at**
- [ ] **Description**: "Timestamp when the record was last updated in the source system (OpenDental). [Source column mapping]."
- [ ] **Required Test**: `not_null`

#### **_created_by_user_id**
- [ ] **Description**: "User who created the record in the source system (OpenDental). [Source column mapping]. May be null for [specific conditions]."
- [ ] **Required Test**: `not_null` with appropriate `where` clause if nulls are expected

### ✅ Quality Assurance Checks

#### **Documentation Quality**
- [ ] **Consistency**: Terminology consistent with other models
- [ ] **Clarity**: Descriptions are clear and actionable
- [ ] **Completeness**: All columns have adequate descriptions
- [ ] **Accuracy**: Descriptions match actual data behavior
- [ ] **Business Context**: Includes relevant business rules and usage

#### **Test Coverage Validation**
- [ ] **Primary Keys**: All have required tests
- [ ] **Foreign Keys**: All have relationship tests with appropriate severity
- [ ] **Business Rules**: Critical rules have validation tests
- [ ] **Data Quality**: Known issues have associated tests
- [ ] **Performance**: Tests don't create unnecessary performance overhead

#### **Standard Compliance**
- [ ] **Naming**: Follows established conventions
- [ ] **Structure**: Matches template structure
- [ ] **Meta Section**: Includes all required sections
- [ ] **Test Patterns**: Uses standardized test configurations
- [ ] **Descriptions**: Follow format guidelines

### ✅ Post-Implementation Validation

#### **Technical Validation**
- [ ] **Compilation**: Model compiles without errors
- [ ] **Test Execution**: All tests pass or have documented exceptions
- [ ] **Performance**: No significant performance degradation
- [ ] **Dependencies**: All referenced models exist

#### **Business Validation**
- [ ] **Stakeholder Review**: Business users can understand the documentation
- [ ] **Analyst Feedback**: Data analysts find documentation useful
- [ ] **Issue Tracking**: Known issues are accurately documented
- [ ] **Usage Guidance**: Clear guidance on how to use the model

#### **Maintenance Setup**
- [ ] **Change Tracking**: Documentation changes are tracked in version control
- [ ] **Review Schedule**: Regular review schedule established
- [ ] **Issue Updates**: Process for updating known issues
- [ ] **Test Evolution**: Plan for updating tests as business rules change

## Implementation Priority Order

### **Phase 1: Foundation Models (Complete First)**
- [ ] `stg_opendental__patient`
- [ ] `stg_opendental__provider`
- [ ] `stg_opendental__procedurecode`
- [ ] `stg_opendental__clinic`

### **Phase 2: Financial Core**
- [ ] `stg_opendental__fee`
- [ ] `stg_opendental__payment`
- [ ] `stg_opendental__paysplit`
- [ ] `stg_opendental__adjustment`

### **Phase 3: Clinical Core**
- [ ] `stg_opendental__appointment`
- [ ] `stg_opendental__procedurelog`
- [ ] `stg_opendental__claim`
- [ ] `stg_opendental__claimproc`

### **Phase 4: Insurance System**
- [ ] `stg_opendental__insplan`
- [ ] `stg_opendental__inssub`
- [ ] `stg_opendental__carrier`
- [ ] `stg_opendental__benefit`

### **Phase 5: Supporting Systems**
- [ ] Communication models
- [ ] Scheduling models
- [ ] Reference/lookup tables
- [ ] Administrative models

## Quality Gates

### **Before Moving to Next Phase**
- [ ] All models in current phase meet checklist requirements
- [ ] All tests pass or have documented exceptions
- [ ] Stakeholder review completed for current phase
- [ ] Performance impact assessed and acceptable
- [ ] Documentation updated in main strategy document

### **Final Quality Gate**
- [ ] 100% of staging models follow standardized documentation
- [ ] All business-critical information is documented
- [ ] Test coverage meets established standards
- [ ] Stakeholders can effectively use the documentation
- [ ] Maintenance processes are established

## Common Pitfalls to Avoid

1. **Generic Descriptions**: Avoid copy-paste descriptions; each column should have specific business context
2. **Missing Business Rules**: Don't just describe the data; explain how it's used
3. **Inconsistent Severity**: Apply severity levels consistently across models
4. **Incomplete Known Issues**: Document all known problems, not just the obvious ones
5. **Over-Testing**: Don't add tests that don't provide business value
6. **Under-Testing**: Ensure critical business rules are validated
7. **Missing Context**: Include enough context for new team members to understand
8. **Stale Documentation**: Keep documentation current with data changes

## Success Indicators

- [ ] Analysts can work with new models without asking questions
- [ ] Data quality issues are caught early by tests
- [ ] Stakeholders trust the data documentation
- [ ] New team members can understand the data model quickly
- [ ] Documentation accurately reflects data behavior
- [ ] Maintenance burden is manageable