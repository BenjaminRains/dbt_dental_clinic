# Adjustment Data Quality Notice

## Overview
This document highlights important observations about adjustment data quality that stakeholders should be aware of.

## Large Adjustments on Zero-Base Fee Procedures

### Issue Description
Several records show large adjustments being applied to procedures with $0 base fees. This is unusual and potentially problematic as adjustments should typically be relative to a non-zero base fee.

### Examples Found
1. Adjustment ID: 276851
   - Procedure: WAX TRY IN (D5109)
   - Base Fee: $0
   - Adjustment Amount: -$11,372.60
   - Type: Discount Per Dr. Kamp

2. Adjustment ID: 277290
   - Procedure: Delivery (D2919)
   - Base Fee: $0
   - Adjustment Amount: -$14,340.00
   - Type: Admin Error

### Impact
- Distorts fee analysis and reporting
- Makes it difficult to calculate meaningful adjustment percentages
- May indicate data entry errors or missing base fee information

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

### Impact
- Makes it challenging to analyze typical adjustment patterns
- May indicate special cases that need separate handling in analysis

## Missing Procedure Information

### Issue Description
Some adjustments are not linked to any procedure information, making it impossible to analyze their context.

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

## Next Steps
1. Business stakeholders should review these cases to determine if they represent legitimate business scenarios
2. Data team should implement additional validation rules to catch similar cases in the future
3. Consider creating separate reporting categories for special cases like large discounts 