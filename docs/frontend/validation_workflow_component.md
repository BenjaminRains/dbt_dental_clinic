# Validation Workflow Component - Feature Plan

## Overview

This document outlines a feature plan for adding a "Validation Workflow" component to the React portfolio. This feature demonstrates how mart models are validated through source reconciliation, business rule testing, and operationalized dbt tests—showcasing the difference between "building models" and "shipping trusted analytics."

**Target Audience**: SmarterDx-type reviewers who value QA/testing and customer data conformance

**Key Message**: "I don't just build marts; I validate them against source-of-truth and operationalize rules into automated dbt tests."

---

## Context & Foundation

This feature plan is based on existing validation work:

### 1. Validation Directory Structure
- **Location**: `dbt_dental_models/validation/`
- **Purpose**: Source vs transformed comparison, diagnostics, automation pathway
- **Reference**: `validation/README.md`

### 2. Fact Claim Validation Plan
- **Location**: `dbt_dental_models/validation/marts/fact_claim/fact_claim_validation_plan.md`
- **Structure**: Phased execution, expected results, FAIL/WARN thresholds, remediation workflows, monitoring
- **Key Features**: 
  - Row count reconciliation with <1% variance tolerance
  - Financial balance validation
  - Referential integrity checks
  - Diagnostic queries for root cause analysis

### 3. Business Rules to dbt Tests Mapping
- **Location**: `dbt_dental_models/validation/marts/fact_claim/BUSINESS_RULES_TO_DBT_TESTS.md`
- **Content**: Taxonomy of schema tests vs custom macros vs data tests vs diagnostics
- **Value**: Demonstrates understanding of how teams scale QA

### 4. Test Failure Analysis
- **Location**: `dbt_dental_models/validation/marts/fact_claim/TEST_FAILURES_ANALYSIS.md`
- **Content**: Real-world test failures, root cause analysis, remediation strategies
- **Insight**: Shows pragmatic approach to data quality (not perfectionism)

---

## Integration with Existing Portfolio

### Current Portfolio Structure
- **Project Components**: Existing section with "dbt Models" link
- **Data Quality Theme**: Already established in portfolio
- **dbt Docs Integration**: Links to dbt documentation

### New Component Placement
- **Location**: Add alongside existing "Project Components"
- **Label**: "Validation Workflow (QA)"
- **Description**: "Source reconciliation + business-rule tests + diagnostic runbooks"
- **Route**: `/validation` (or modal section on main page)

---

## Page/Section Structure

### 1. Overview Section

**Headline**: "Validation Workflow: Proving Mart Correctness"

**Content**:
- 1-2 sentence explanation: Validate marts by comparing raw/source data to downstream models, documenting expected outcomes, then converting durable rules into dbt tests
- **References**: 
  - `validation/README.md` (purpose and usage)
  - `fact_claim_validation_plan.md` (detailed methodology)

**Visual Elements**:
- **Callout chips**: Completeness • Accuracy • Business Logic • Financial Reconciliation • Referential Integrity
- **Source**: Validation objectives from `fact_claim_validation_plan.md`

---

### 2. Workflow Diagram

**Visual**: Simple Mermaid diagram showing data flow and validation lanes

**Structure**:
```
raw → staging → intermediate → marts
     ↓                          ↓
     └─── Validation Queries ───┘
     (manual + dbt run-operation)
     
     └─── dbt Tests ────────────┘
     (schema tests + custom tests + macros)
```

**Implementation**: Use existing `MermaidDiagram` component from portfolio code

**References**:
- `validation/README.md` (validation query usage)
- `fact_claim_validation_plan.md` (phased validation approach)

---

### 3. Case Study: fact_claim

**Why fact_claim**: Most detailed validation plan, demonstrates maturity

**Content Sections**:

#### 3.1 Model Context
- **Target Model**: `marts.fact_claim`
- **Source**: OpenDental (MySQL) → `raw` schema in PostgreSQL analytics database
- **Reference**: `fact_claim_validation_plan.md` (overview section)

#### 3.2 Validation Methodology
- **Format**: Phases + expected results + FAIL/WARN thresholds + remediation
- **Reference**: `fact_claim_validation_plan.md` (validation objectives and phases)

#### 3.3 Pragmatic Approach
- **Highlight**: "WARN <1% variance triggers investigation" 
- **Message**: Shows pragmatism, not perfectionism
- **Reference**: `fact_claim_validation_plan.md` (Phase 1.1 - Row Count Reconciliation)

---

### 4. Three "Proof" Validations

Select three validations that instantly convey real-world competency:

#### 4.1 Row Count Reconciliation (Completeness)

**Why it matters**: Ensures the mart contains expected records from raw

**Details**:
- **What**: Compare source row counts to fact table row counts
- **Expected Result**: Exact match (or <1% variance with investigation)
- **Failure Action**: Debug → fix → rerun
- **Reference**: `fact_claim_validation_plan.md` (Phase 1.1)

#### 4.2 Financial Balance Test (Business Logic)

**Why it matters**: Validates core business rule: billed = paid + writeoff + patient responsibility

**Details**:
- **What**: Financial amounts must balance (with real exclusions)
- **Expected Result**: 0 rows returned (or WARN for edge cases)
- **Failure Action**: Investigate discrepancies, check exclusions
- **Reference**: `fact_claim_validation_plan.md` (Phase 4.1)
- **Note**: Excludes `claim_id = 0` and `patient_responsibility = -1.0` (placeholders)

#### 4.3 Sentinel/Exception Handling

**Why it matters**: Demonstrates first-class handling of data anomalies

**Details**:
- **What**: `patient_responsibility = -1.0` is a placeholder, not "negative money"
- **Expected Result**: Documented and excluded correctly in validations
- **Failure Action**: Ensure exclusions are properly applied
- **Reference**: `fact_claim_validation_plan.md` (Phase 4.1 - Financial Balance)

**For Each Validation, Show**:
- "Why this matters"
- "Expected result" (often "0 rows returned" or PASS/WARN/FAIL)
- "What I do when it fails" (debug → fix → rerun)

---

### 5. Operationalization: Business Rules → Tests

**Purpose**: Demonstrate production-minded approach to data quality

**Content**: Simple table or list showing taxonomy:

#### 5.1 Schema Tests (YAML)
- **Use Case**: Enums, relationships, not-null constraints
- **Example**: `accepted_values`, `relationships`, `not_null`
- **Reference**: `BUSINESS_RULES_TO_DBT_TESTS.md` (Category 1)

#### 5.2 Custom Macro Tests
- **Use Case**: Complex rules (payment status categories, financial balance)
- **Example**: Custom macros for multi-column validations
- **Reference**: `BUSINESS_RULES_TO_DBT_TESTS.md` (Category 3)

#### 5.3 Data Tests
- **Use Case**: Source comparison (must compare against raw.*)
- **Example**: Tests that query raw tables directly
- **Reference**: `BUSINESS_RULES_TO_DBT_TESTS.md` (Category 4)

#### 5.4 Diagnostics
- **Use Case**: Not everything should be a test
- **Example**: Root cause analysis queries, exploratory validations
- **Reference**: `BUSINESS_RULES_TO_DBT_TESTS.md` (Category 5)
- **Note**: `fact_claim_validation_plan.md` includes diagnostic queries for troubleshooting

**Value Proposition**: This taxonomy demonstrates understanding of how teams scale QA

---

### 6. Real-World Impact

**Closing Section**: Short "impact" statement

**Key Points**:
1. **Prevents KPI Drift**: "Why did numbers change?" - validation catches issues early
2. **Speeds Onboarding**: Conformance of new source data is faster with established patterns
3. **Creates Guardrails**: Repeatable validation for production pipelines
4. **Turns Investigations into Tests**: Reusable tests over time
5. **Reference**: `fact_claim_validation_plan.md` (monitoring and remediation sections)
6. **Reference**: `validation/README.md` (best practices)

---

## Assets & Links

**Minimal but High Value**: Link to 2-3 repo artifacts only

### Primary Links:
1. **`validation/README.md`**
   - Explains purpose and usage patterns
   - Documents validation best practices

2. **`fact_claim_validation_plan.md`**
   - Real runbook with phases
   - Shows detailed validation methodology

3. **`BUSINESS_RULES_TO_DBT_TESTS.md`**
   - "How I operationalize" documentation
   - Demonstrates test taxonomy and decision-making

### Optional Additional Links:
- `TEST_FAILURES_ANALYSIS.md` - Shows real-world problem-solving
- `FAILING_NOT_NULL_TESTS_ANALYSIS.md` - Demonstrates investigation process

---

## Optional Enhancement: Validation Results Widget

**Purpose**: Add visual interest and product-like feel

**Content** (synthetic/demo numbers for public portfolio):
- **Total checks**: Number of validation queries/tests
- **PASS / WARN / FAIL counts**: Visual breakdown
- **Top 3 recurring failure types**: Shows pattern recognition

**Implementation**: Simple React component with mock data

**Note**: Keep it safe for public portfolio (no real sensitive data)

---

## Implementation Order

### Phase 1: Foundation
1. Add "Validation Workflow" tile under Project Components
   - **Style**: Match existing "dbt Models" tile
   - **Reference**: `Portfolio_v2` component structure

### Phase 2: Content Creation
2. Create `/validation` page with structure above
3. Add Mermaid diagram (workflow visualization)
4. Add 3 proof validations (detailed examples)
5. Add "rules→tests" taxonomy section

### Phase 3: Integration
6. Add 2-3 links to repo documentation
7. Ensure consistent styling with existing portfolio

### Phase 4: Enhancement (Optional)
8. Add synthetic "results" widget
9. Add any additional visual elements

---

## Key Differentiators

This feature demonstrates:

1. **Maturity**: Not just building models, but validating them systematically
2. **Pragmatism**: WARN thresholds, not perfectionism
3. **Operationalization**: Converting manual checks into automated tests
4. **Real-World Problem Solving**: Actual test failures and remediation
5. **Scalability**: Understanding of how teams scale QA processes

---

## Success Metrics

**For Portfolio Reviewers**:
- Clear demonstration of data quality mindset
- Evidence of production-ready thinking
- Understanding of validation vs testing taxonomy
- Real-world examples with actual documentation

**For Technical Review**:
- Links to actual validation artifacts
- Demonstrates understanding of dbt testing patterns
- Shows progression from manual to automated validation
