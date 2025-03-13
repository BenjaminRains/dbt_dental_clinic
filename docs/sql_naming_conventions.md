# SQL Naming Conventions

## Overview

This document outlines the standardized naming conventions for SQL code in the MDC Analytics project. Following these guidelines will ensure consistency, readability, and maintainability of our database queries, reports, and data extraction tools.

## General Principles

- **Readability**: Names should clearly indicate the purpose of the object
- **Consistency**: Similar objects should follow similar naming patterns
- **Precision**: Names should be specific enough to avoid ambiguity
- **Brevity**: Names should be concise while maintaining clarity

## Naming Conventions

### Database Column References

**Rule**: Use CamelCase for all references to raw database columns.

**Rationale**: This maintains consistency with the actual database schema, reducing the risk of errors when referencing database fields.

**Examples**:
- `DatePay` - Payment date column from database
- `PatNum` - Patient number primary key
- `PayType` - Payment type identifier
- `SplitAmt` - Payment split amount

### Derived/Calculated Fields

**Rule**: Use snake_case for all derived or calculated fields.

**Rationale**: This visually distinguishes derived data from raw database columns, making it immediately clear which fields are calculated versus direct database references.

**Examples**:
- `total_payment` - Sum of multiple payment fields
- `percent_current` - Calculated percentage of current payments
- `days_since_payment` - Calculated number of days
- `average_payment_amount` - Average of payment amounts

### Common Table Expression (CTE) Names

**Rule**: Use CamelCase for CTE definition names in SQL.

**Rationale**: CTEs represent database-like objects/entities, so using CamelCase aligns with database entity naming and distinguishes them from variables/attributes.

**Examples**:
- `PaymentTypeDef` - Payment type definitions CTE
- `PatientBalances` - Patient balance information CTE
- `UnearnedTypeDefinition` - Unearned income type definitions
- `AllPaymentTypes` - Aggregated payment types

### SQL File Names

**Rule**: Use snake_case for all SQL file names.

**Rationale**: This follows Pythonic conventions for file naming, making files easier to work with in the Python environment.

**Examples**:
- `unearned_income_payment_type.sql` - SQL file containing payment type queries
- `payment_split_analysis.sql` - SQL file for split analysis
- `monthly_trend_report.sql` - Monthly trend report queries

## Visual Differentiation Benefits

This dual-case approach provides immediate visual cues about the nature of each element:

```sql
-- CTE with CamelCase name
WITH PatientPayments AS (
    SELECT
        pt.PatNum,                   -- Raw DB column (CamelCase)
        pt.LName,                    -- Raw DB column (CamelCase)
        SUM(ps.SplitAmt) AS total_payments,   -- Calculated (snake_case)
        COUNT(*) AS payment_count,            -- Calculated (snake_case)
        AVG(ps.SplitAmt) AS average_payment   -- Calculated (snake_case)
    FROM patient pt
    JOIN paysplit ps ON ps.PatNum = pt.PatNum
    GROUP BY pt.PatNum, pt.LName
)
```

## Implementation Notes

- When refactoring existing code, prioritize consistency within individual queries over immediate full compliance
- New code should adhere to these conventions from the start
- Comments should be used to clarify naming in complex cases

## Exceptions

In some special cases where direct SQL compatibility with external systems is required, these conventions may be modified. Such exceptions should be documented in the code.

---

*Document Version: 1.0 - Last Updated: March 6, 2025* 