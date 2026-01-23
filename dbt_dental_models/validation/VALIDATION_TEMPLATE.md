# Validation Template

**Date**: [YYYY-MM-DD]  
**Status**: 🔍 **IN PROGRESS**  
**Investigation File**: `investigate_{issue_description}.sql`  
**Model**: `{layer}.{model_name}`  
**Field(s) Under Investigation**: `{field_name}` or `{field1}, {field2}, ...`

---

## Executive Summary

**Issue**: [Brief description of the data quality issue, business logic concern, or validation need]

**Current Logic** (from `{model_name}.sql` lines {start}-{end}):
```sql
[Paste relevant CASE statement or calculation logic]
```

**Documented Valid Values** (from `_{model_name}.yml`):
- [List documented values and their meanings]
- [Reference to staging model documentation if applicable]

**Investigation Goals**:
1. [Goal 1 - e.g., Verify all source values are properly mapped]
2. [Goal 2 - e.g., Check for NULL value handling issues]
3. [Goal 3 - e.g., Identify edge cases or unexpected categorizations]
4. [Goal 4 - e.g., Determine if additional mappings are needed]
5. [Goal 5 - e.g., Verify business rules are correctly implemented]

---

## Investigation Queries

Run the following investigation queries to understand the data:

**Location**: `validation/{layer}/{model_name}/investigate_{issue_description}.sql`

**Key Queries**:

### [Category 1] (Query Set 1):
1. **Query 1.1**: [Description]
2. **Query 1.2**: [Description]
3. **Query 1.3**: [Description]
4. **Query 1.4**: [Description]

### [Category 2] (Query Set 2):
1. **Query 2.1**: [Description]
2. **Query 2.2**: [Description]
3. **Query 2.3**: [Description]

### [Category 3] (Query Set 3):
1. **Query 3.1**: [Description]
2. **Query 3.2**: [Description]

### Combined Analysis (Query Set 4):
1. **Query 4.1**: [Description]
2. **Query 4.2**: [Description]

### Summary Statistics (Query Set 5):
1. **Query 5.1**: [Description]
2. **Query 5.2**: [Description]

---

## Expected Findings

### [Field/Category Name]:
- **Expected Mappings**:
  - `[condition]` → '[category]'
  - `[condition]` → '[category]'
  - `[condition]` → '[category]'
  - All other cases → '[catch-all category]'
- **Questions to Answer**:
  - [Question 1 - e.g., How should NULL values be handled?]
  - [Question 2 - e.g., Are there edge cases that need special handling?]
  - [Question 3 - e.g., Is the catch-all category appropriate for all unmapped cases?]

---

## Investigation Results

*Results will be documented here after running investigation queries*

### Query Set 1 Results:
[Paste query results here]

### Query Set 2 Results:
[Paste query results here]

### Query Set 3 Results:
[Paste query results here]

---

## Root Cause Analysis

*To be completed after investigation*

**Root Cause**: [Description of why the issue exists]

**Contributing Factors**:
- [Factor 1]
- [Factor 2]
- [Factor 3]

**Business Impact**:
- [Impact 1 - e.g., X% of records incorrectly categorized]
- [Impact 2 - e.g., Affects reporting accuracy]
- [Impact 3 - e.g., May impact business decisions]

---

## Business Rules Identified

*Document business rules discovered during investigation*

### Rule 1: [Rule Name]
- **Description**: [What the rule requires]
- **Source**: [Where this rule comes from - business requirement, data quality need, etc.]
- **Current Status**: [Is this rule currently enforced? How?]
- **Test Type**: [Recommended dbt test type: accepted_values, expression_is_true, custom macro, etc.]

### Rule 2: [Rule Name]
- **Description**: [What the rule requires]
- **Source**: [Where this rule comes from]
- **Current Status**: [Is this rule currently enforced?]
- **Test Type**: [Recommended dbt test type]

---

## Mapping Business Rules to dbt Tests

### Test Implementation Plan

#### Category 1: Simple Schema Tests
- [ ] **Rule**: [Rule name]
  - **Test Type**: `accepted_values` / `not_null` / `relationships`
  - **Location**: `models/{layer}/_{model_name}.yml`
  - **Test Definition**: [YAML snippet or description]

#### Category 2: Expression Tests
- [ ] **Rule**: [Rule name]
  - **Test Type**: `dbt_utils.expression_is_true` / `dbt_expectations.expression_is_true`
  - **Location**: `models/{layer}/_{model_name}.yml`
  - **Expression**: [SQL expression]
  - **Severity**: [error|warn]

#### Category 3: Custom Macros
- [ ] **Rule**: [Rule name]
  - **Test Type**: Custom macro
  - **Location**: `macros/tests/domain/test_{rule_name}.sql`
  - **Purpose**: [Why a custom macro is needed]

#### Category 4: Data Tests
- [ ] **Rule**: [Rule name]
  - **Test Type**: Data test (custom SQL)
  - **Location**: `tests/{layer}/{model_name}/test_{rule_name}.sql`
  - **Purpose**: [Why a data test is needed]

**Reference**: See `validation/marts/fact_claim/fact_claim_business_rules_to_dbt_tests.md` for complete mapping example

---

## Recommended Actions

### Immediate Actions (High Priority)
1. **[Action 1]**: [Description]
   - **Implementation**: [Steps to implement]
   - **Files to Update**: [List files]
   - **Expected Outcome**: [What this will fix]

2. **[Action 2]**: [Description]
   - **Implementation**: [Steps to implement]
   - **Files to Update**: [List files]
   - **Expected Outcome**: [What this will fix]

### Short-term Actions (Medium Priority)
1. **[Action 3]**: [Description]
   - **Timeline**: [When to implement]
   - **Dependencies**: [What needs to happen first]

### Long-term Actions (Low Priority)
1. **[Action 4]**: [Description]
   - **Link to TODO**: [Reference to TODO.md if applicable]
   - **Timeline**: [When to implement]

---

## Implementation Plan

### Phase 1: [Phase Name]
- [ ] [Task 1]
- [ ] [Task 2]
- [ ] [Task 3]

### Phase 2: [Phase Name]
- [ ] [Task 1]
- [ ] [Task 2]

### Phase 3: Verification
- [ ] Re-run investigation queries
- [ ] Verify fixes with dbt tests
- [ ] Update documentation
- [ ] Mark validation as complete

---

## Related Documentation

- **Investigation SQL**: `validation/{layer}/{model_name}/investigate_{issue_description}.sql`
- **Model Logic**: `models/{layer}/{model_name}.sql` (lines {start}-{end})
- **Model Documentation**: `models/{layer}/_{model_name}.yml` (lines {start}-{end})
- **Staging Model**: `models/staging/{source}/_{staging_model}.yml` (if applicable)
- **Related Validations**: [Links to related validation documents]

---

## Notes

- [Any additional notes, assumptions, or context]
- [References to business rules or requirements]
- [Links to related issues or discussions]
