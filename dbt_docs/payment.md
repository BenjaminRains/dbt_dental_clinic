{% docs stg_opendental__payment %}

# Payment Staging Model

## Overview
This model standardizes payment records from OpenDental, implementing consistent naming, 
data types, and business rules.

## Payment Types
- **Type 71**: Regular payments (avg $293.25)
- **Type 0**: Administrative entries ($0 only)
- **Type 69**: Higher value payments (avg $760.20)
- **Type 72**: Refunds (negative amounts only)
- **Type 574**: Very high value (avg $16,661.66)
- **Type 412**: Newer payment type
- **Type 634**: Newest payment type (since Sept 2024)

## Technical Notes
- Incremental model based on payment_date
- Includes proper handling of PostgreSQL data types
- Implements null handling for text fields
- Maintains original index structure

## Business Rules
1. Type 0 payments must have $0 amount
2. Type 72 payments must be negative (refunds)
3. High value thresholds:
   - Type 69: Flag if > $5,000
   - Type 574: Flag if > $50,000

{% enddocs %}