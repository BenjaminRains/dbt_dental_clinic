# Fee Schedule System Analysis
February 2024

## Executive Summary

The dental practice operates with an intentional out-of-network strategy, prioritizing patient-centric fee setting over insurance fee schedules. This approach allows the practice to:
- Set fees independently
- Maintain patient affordability
- Avoid insurance contract constraints
- Focus on patient care over maximum reimbursement

## System State

- 99.9% of insurance plans process as out-of-network (intentional)
- 0.1% of plans have assigned fee schedules (selective in-network relationships)
- Payment variability (40-42%) is expected and accepted as part of the business model

## Key Findings

1. **Insurance Claim Analysis**
   - Total Claims: 38,930 analyzed
   - Valid Claims: 35,781 (91.9%)
   - Invalid Claims: 3,149 (8.1%)
   - Open-ended Plans: 36,297 (93.2%)
   - Expired Plans: 2,569 (6.6% of total)

2. **Batch Success Patterns**
   - Optimal Size (2-3 claims): 82-91% success
   - Mixed-Value Batches: 46% success
   - Oversized Batches (>4): 27% success
   - Single High-Value: 91% success

3. **Payment Processing**
   - Normal Split Patterns (99.3%): Single claim per procedure
   - Complex Split Patterns (0.7%): Multiple claims per procedure
   - Insurance reprocessing (Type 71): 78% of complex cases
   - Patient payments (Type 69): 7.3% of complex cases

## Questions for Stakeholders

1. **Batch Optimization**
   - Should we implement strict batch size limits (2-3 claims)?
   - How do we handle urgent claims that break optimal patterns?
   - Do we need automated batch optimization tools?

2. **Risk Management**
   - How should we handle identified risk factors:
     * Batch size > 4 claims (-20% success)
     * Mixed fee types (-25% success)
     * High-value claim mixing (-30% success)
     * Multiple same-day claims (-15% success)

3. **Payment Processing**
   - Should we create separate workflows for:
     * Normal vs complex split patterns?
     * Different payment types (71, 69, etc.)?
     * High-value vs standard claims?

4. **Success Metrics**
   - Are these targets appropriate:
     * Payment ratio > 70%
     * Zero payment rate < 20%
     * Maximum 3 same-day claims
     * 1-2 days between batches

## Business Strategy Alignment

The current system implementation aligns with the practice's core philosophy while revealing optimization opportunities:
- Patient-friendly pricing maintained
- Fee setting autonomy preserved
- Flexible care options supported
- Clear path to improved success rates

## Proposed Success Criteria Updates

1. **Batch Optimization Rules**
   ```sql
   CASE WHEN
       cl.BatchSize BETWEEN 2 AND 3
       AND cl.UniqueFees <= 3
       AND (
           -- Standard claims
           cl.MaxFee < 1000
           OR
           -- High-value isolation
           (cl.MaxFee >= 1000 AND cl.BatchSize = 1)
       )
       AND cl.SameDayCount <= 3
   ```

2. **Split Pattern Validation**
   ```sql
   CASE WHEN
       -- Normal splits (99.3%)
       (sp.ClaimsPerProc = 1 AND sp.SplitCount <= 3)
       OR
       -- Complex splits (0.7%)
       (sp.ClaimsPerProc <= 2 AND sp.PayType IN (71, 69, 391, 70, 412))
   ```

## Next Steps

1. Implement batch optimization rules
2. Create separate high-value claim workflow
3. Develop split pattern monitoring
4. Establish risk factor alerts
5. Track success metrics by pattern

Note: The high percentage of out-of-network claims and variable payments remain intentional business strategy elements, while batch optimization presents clear opportunity for improved success rates.