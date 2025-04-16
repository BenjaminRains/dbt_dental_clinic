# Large Provider Discounts and Write-offs Report

## Overview
This report documents cases where adjustments result in negative adjusted fees (procedure_fee + adjustment_amount < 0). These cases primarily involve large provider discounts, employee/family benefits, and account write-offs.

## Key Findings

### 1. Largest Adjustments by Type

#### Provider Discounts (Dr. Kamp)
- **Largest Discount**: $27,680 on a $330 extraction (D7210)
- **Zero-Fee Cases**: $14,340 on a $0 delivery (D2919)
- **High-Value Procedures**: $12,636 on a $1,288 crown (D2740)

#### Employee/Family Benefits
- **MDC EDP**: $10,246 on a $736 partial denture (D5820)
- **Doctors Family**: $5,795 on a $1,288 crown (D2740)
- **MDC SPOUSE/CHILDREN/SIG OTHER**: $1,288 on various procedures

#### Write-offs and Account Adjustments
- **Write off Acct Balance**: $4,205 on a $0 delivery
- **FixedFeeTXP**: $9,848 on a $0 post-op
- **Discount / Write off**: Various amounts on zero-fee procedures

### 2. Common Procedure Types with Large Adjustments

| Procedure Code | Description | Typical Fee | Common Adjustment Types |
|----------------|-------------|-------------|-------------------------|
| D2740 | Crown - porcelain/ceramic | $1,288 | Provider discounts, Employee benefits |
| D7210 | Extraction with bone removal | $330 | Large provider discounts |
| D2919 | Delivery | $0 | Various write-offs |
| D6058 | Abutment supported crown | $1,288 | Provider discounts |
| D5820 | Partial denture | $736 | Employee benefits |

### 3. Zero-Fee Procedures with Adjustments

Several procedures with $0 fees have significant adjustments:
- Deliveries (D2919)
- Post-ops (00040)
- Denture adjustments (N4102)
- Treatment consults (TXCONSULT)

### 4. Failing Records Analysis

#### Zero-Fee Procedures with Non-Zero Adjustments
- Multiple `D2919` (Delivery) procedures with $0 fees but adjustments ranging from -$14,340 to -$31
- Other zero-fee procedures like `00040` (Post Op), `N4102` (Denture Adjust) with significant adjustments
- These violate the rule: "Zero-fee procedures must have zero adjustment"

#### Large Provider Discounts on Low-Fee Procedures
- `D7210` (Extraction) with $330 fee but $27,680 discount
- `D5820` (Partial denture) with $736 fee but $10,246 discount
- These violate the rule: "Procedures ≤ 1,000: adjustment between -10,000 and 10,000"

#### Missing Procedure Information
- Several records with no procedure_id, procedure_fee, or procedure_code
- These are handled by the "adjustments without procedure context" rules

#### Reallocations Outside Expected Range
- One reallocation of $1,348.40 with no procedure context
- Violates the rule: "Reallocations and small discounts between -1,000 and 1,000"

#### Employee/Family Discounts Exceeding Limits
- MDC EDP discount of $10,246 on a $736 procedure
- Violates the rule: "Employee and family discounts between -5,000 and 5,000"

## Recommendations

1. **Review Process for Large Discounts**
   - Consider implementing additional approval steps for discounts exceeding $10,000
   - Document justification for large provider discounts

2. **Zero-Fee Procedures**
   - Review why these procedures have $0 fees but significant adjustments
   - Consider setting minimum fees for procedures that typically require adjustments

3. **Employee/Family Benefits**
   - Review benefit policies for consistency
   - Consider standardizing discount amounts for specific procedure types

4. **Monitoring**
   - Implement alerts for adjustments exceeding certain thresholds
   - Regular review of large adjustments by adjustment type

## Data Quality Notes

- Some procedures show missing or zero fees with large adjustments
- Several cases involve historical procedures that may have been archived
- The relationship between procedure fees and adjustment amounts varies significantly by adjustment type

## Next Steps

1. Review the largest adjustments (>$10,000) with providers
2. Document policies for zero-fee procedures
3. Consider implementing additional validation rules for large adjustments
4. Regular monitoring of adjustment patterns by type 