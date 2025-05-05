# Collection Task Description Data Quality Issues

## Overview

The `int_collection_tasks` model processes free-text task descriptions from the dental practice management system to extract structured information. This document explains the challenges with the current data entry practices and demonstrates how the model attempts to standardize and extract meaningful data.

## Current Challenge

Staff members create collection tasks with important information embedded in the free-text description field rather than in structured data fields. This creates several challenges:

1. **Inconsistent formatting** of key information (dates, amounts, action types)
2. **Varied terminology** for the same concepts
3. **Missing context** requiring inference
4. **Extraction difficulties** requiring complex regex patterns

## Example Transformations

Below are examples showing raw task descriptions and how the model transforms them:

### Example 1: Payment Promise

**Raw Description:**
```
Call pt re: balance of $247.50 - pt promised to pay on 4/15
```

**Extracted Fields:**
```json
{
  "task_type": "call",
  "collection_type": "direct_collection",
  "collection_amount": 247.50,
  "outcome": "payment_promised",
  "promised_payment_amount": 247.50,
  "promised_payment_date": "2023-04-15",
  "follow_up_required": false,
  "has_amount": true,
  "has_outcome": true
}
```

### Example 2: Follow-up Required

**Raw Description:**
```
Insurance denied claim - follow up with Delta on 3/22 about missing tooth clause
```

**Extracted Fields:**
```json
{
  "task_type": "other",
  "collection_type": "insurance_follow_up",
  "outcome": "denied",
  "follow_up_required": true,
  "follow_up_date": "2023-03-22",
  "has_follow_up": true,
  "has_outcome": true
}
```

### Example 3: Payment Received

**Raw Description:**
```
Pt paid $125 cash today for outstanding balance
```

**Extracted Fields:**
```json
{
  "task_type": "other",
  "collection_type": "direct_collection",
  "collection_amount": 125.00,
  "outcome": "payment_received",
  "actual_payment_amount": 125.00,
  "actual_payment_date": "2023-01-15", // from task completion date
  "follow_up_required": false,
  "has_amount": true,
  "has_outcome": true
}
```

### Example 4: Inconsistent Notation

**Raw Description:**
```
F/u with patient 6/2 re: outstanding balance of $312
```

**Extracted Fields:**
```json
{
  "task_type": "other",
  "collection_type": "balance_inquiry",
  "collection_amount": 312.00,
  "outcome": "pending",
  "follow_up_required": true,
  "follow_up_date": "2023-06-02",
  "has_amount": true,
  "has_follow_up": true
}
```

## SQL Implementation

To handle these variations, the model uses numerous `CASE WHEN` statements with regex patterns:

```sql
-- Extract dollar amounts
CASE
    WHEN t.description ~ '\$[0-9]+(\.[0-9]{2})?' THEN 
        CAST(REGEXP_REPLACE(t.description, '.*\$([0-9]+(\.[0-9]{2})?).*', '\1', 'g') AS DECIMAL(10,2))
    ELSE NULL
END AS collection_amount

-- Identify follow-up dates with different formats
CASE
    WHEN t.description ~ '(f/u|follow up|follow-up)' THEN
        CASE
            WHEN t.description ~ '(f/u|follow up|follow-up).*?([0-9]{1,2}/[0-9]{1,2})' THEN
                TO_DATE(REGEXP_REPLACE(t.description, '.*(f/u|follow up|follow-up).*?([0-9]{1,2}/[0-9]{1,2}).*', '\2', 'g'), 'MM/DD')
            WHEN t.description ~ '(f/u|follow up|follow-up).*?([0-9]{1,2} [A-Za-z]{3})' THEN
                TO_DATE(REGEXP_REPLACE(t.description, '.*(f/u|follow up|follow-up).*?([0-9]{1,2} [A-Za-z]{3}).*', '\2', 'g'), 'DD Mon')
            ELSE NULL
        END
    ELSE NULL
END AS follow_up_date
```

## Recommendations

### For Office Staff

1. **Use consistent formatting** for critical information:
   - Always include dollar amounts with a "$" symbol (e.g., "$125.00")
   - Use consistent date formats (e.g., "MM/DD/YYYY")
   - Use standard terminology for outcomes (e.g., "paid", "promised", "denied")

2. **Structure your descriptions** in a predictable format:
   ```
   [Action]: [Amount] - [Outcome] - [Next step] on [Date]
   ```
   
   For example:
   ```
   Call: $247.50 - Patient promised payment - Follow up on 04/15/2023
   ```

3. **Use standard abbreviations** that the system recognizes:
   - f/u = follow up
   - pt = patient
   - ins = insurance

### For Developers

1. **Consider implementing structured forms** for task creation with specific fields for:
   - Amount
   - Due date
   - Follow-up date
   - Outcome type (dropdown)

2. **Create validation rules** to enforce data quality at entry time

3. **Track data quality metrics** to identify users who may need additional training

4. **Use this transformation layer as a temporary solution** while working toward a more structured data entry approach

## Impact

Poor data quality in task descriptions leads to:

1. Reduced collection effectiveness
2. Difficulty tracking outcomes accurately
3. Extra work for data transformation
4. Potential missed follow-ups
5. Inconsistent reporting

By improving the quality of data entry at the source, we can significantly reduce these issues.