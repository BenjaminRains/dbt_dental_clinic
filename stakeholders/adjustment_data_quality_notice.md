# Adjustment Data Quality Notice

## Overview
This document highlights important observations about adjustment data quality that
stakeholders should be aware of.

## Large Adjustments on Zero-Base Fee Procedures

### Issue Description
Several records show large adjustments being applied to procedures with $0 base fees. 
This is unusual and potentially problematic as adjustments should typically be relative
 to a non-zero base fee.

### Examples Found
1. Adjustment ID: 277290
   - Procedure: Delivery (D2919)
   - Base Fee: $0
   - Adjustment Amount: -$14,340.00
   - Type: Admin Error

2. Adjustment ID: 276851
   - Procedure: WAX TRY IN (D5109)
   - Base Fee: $0
   - Adjustment Amount: -$11,372.60
   - Type: Discount Per Dr. Kamp

3. Adjustment ID: 276471
   - Procedure: Unspecified
   - Base Fee: $0
   - Adjustment Amount: -$9,904.00
   - Type: Unspecified

4. Adjustment ID: 278133
   - Procedure: Unspecified
   - Base Fee: $0
   - Adjustment Amount: -$9,848.00
   - Type: Unspecified

5. Adjustment ID: 278301
   - Procedure: Unspecified
   - Base Fee: $0
   - Adjustment Amount: -$7,630.00
   - Type: Unspecified

### Impact
- Distorts fee analysis and reporting
- Makes it difficult to calculate meaningful adjustment percentages
- May indicate data entry errors or missing base fee information

## Very Small Adjustments on Zero-Fee Procedures

### Issue Description
Several records show very small adjustments (both positive and negative)
being applied to procedures with $0 base fees. These might be rounding differences
 or data entry artifacts.

### Examples Found
1. Adjustment ID: 280515
   - Procedure: Unspecified
   - Base Fee: $0
   - Adjustment Amount: $0.01
   - Type: Unspecified

2. Adjustment ID: 279642
   - Procedure: Unspecified
   - Base Fee: $0
   - Adjustment Amount: $20.00
   - Type: Unspecified

3. Adjustment ID: 276803
   - Procedure: Unspecified
   - Base Fee: $0
   - Adjustment Amount: -$31.00
   - Type: Unspecified

### Impact
- May indicate rounding issues in the system
- Could be data entry artifacts
- Makes it difficult to distinguish between actual adjustments and system artifacts

## Large Adjustments on Low-Cost Procedures

### Issue Description
Some procedures with relatively low base fees are receiving disproportionately large adjustments.

### Examples Found
1. Adjustment ID: 276457
   - Procedure: Extraction (D7210)
   - Base Fee: $330
   - Adjustment Amount: -$27,680.00
   - Type: Discount Per Dr. Kamp

2. Adjustment ID: 276413
   - Procedure: Extraction (D7210)
   - Base Fee: $330
   - Adjustment Amount: -$10,499.00
   - Type: Discount Per Dr. Kamp

3. Adjustment ID: 277364
   - Procedure: Partial Denture (D5820)
   - Base Fee: $736
   - Adjustment Amount: -$10,246.00
   - Type: MDC EDP

### Impact
- Makes it challenging to analyze typical adjustment patterns
- May indicate special cases that need separate handling in analysis
- Could indicate missing or incorrect base fee information

## Missing Procedure Information

### Issue Description
Some adjustments are not linked to any procedure information, making it impossible
to analyze their context.

### Example Found
- Adjustment ID: 276786
  - No procedure_id
  - No procedure_code
  - No procedure_description
  - No procedure_fee
  - Adjustment Amount: -$27,730.00
  - Type: Discount / Write off

### Impact
- Incomplete data for analysis
- Missing context for large adjustments
- Potential data integrity issues

## Recommendations
1. Review and potentially correct adjustments on zero-base fee procedures
2. Investigate the "Admin Error" adjustment for potential correction
3. Consider implementing data validation rules to prevent large adjustments on zero-base fee procedures
4. Review the business process for handling special cases like large discounts
5. Investigate and fix records with missing procedure information
6. Consider allowing very small adjustments (< $1) on zero-fee procedures if they are legitimate rounding differences
7. Review the business logic for large discounts on low-cost procedures

## Next Steps
1. Business stakeholders should review these cases to determine if they represent legitimate business scenarios
2. Data team should implement additional validation rules to catch similar cases in the future
3. Consider creating separate reporting categories for special cases like large discounts
4. Review system rounding and data entry processes to minimize very small adjustments on zero-fee procedures 