# Unearned Income Analysis
Last updated: [Current Date]

## Overview
Analysis of unearned income transactions from OpenDental, focusing on types 288 and 439. Based on 8,221 transactions from 2022-2024.

## Key Findings

### Transaction Types Distribution
- **Type 288**: 7,887 transactions (95.9%)
  - Average amount: -$13.39
  - Range: -$24,967 to $43,900
  - Primarily small transactions
  - High reversal rate (65.7%)

- **Type 439**: 334 transactions (4.1%)
  - Average amount: $159.30
  - Range: -$11,248 to $16,071
  - More balanced reversal rate (46.4%)

### Transaction Size Distribution
| Size Category | Count | Percentage |
|---------------|-------|------------|
| Small         | 6,385 | 77.7%      |
| Medium        | 1,515 | 18.4%      |
| Large         | 285   | 3.5%       |
| Very Large    | 36    | 0.4%       |

### Provider Activity
Top 5 Providers by Transaction Volume:
1. Provider 1: 2,063 transactions (-$2,599.41 total)
2. Provider 28: 1,928 transactions ($54,491.94 total)
3. Provider 47: 1,013 transactions ($7,044.17 total)
4. Provider 20: 436 transactions ($301.12 total)
5. Provider 52: 283 transactions ($972.60 total)

### Largest Transactions
Notable transactions over $15,000:
- $43,900.00 (Type 288, 2024-07-26)
- $38,252.00 (Type 288, 2024-01-10)
- $27,485.00 (Type 288, 2024-10-29)
- $25,500.00 (Type 288, 2024-05-07)
- $16,977.50 (Type 288, 2024-11-07)
- $16,850.00 (Type 288, 2024-02-12)
- $16,071.60 (Type 439, 2024-12-17)

### Transaction Direction Summary
| Direction | Count | Total Amount | Average |
|-----------|-------|--------------|---------|
| Negative  | 5,338 | -$786,898.14 | -$147.41 |
| Positive  | 2,880 | $734,488.51  | $255.03  |
| Zero      | 3     | $0.00        | $0.00    |

## Data Quality Notes
- No future-dated transactions
- All required fields present except:
  - Clinic ID (missing for all records)
  - Provider ID (missing for 19.3% of transactions)
- Only 3 zero-amount transactions
- 36 very large transactions (≥$5,000)

## Recommendations for Staff
1. **Provider Documentation**
   - Ensure provider ID is recorded, especially for large transactions
   - Currently missing in 19.3% of cases

2. **Large Transaction Handling**
   - Extra verification for transactions over $5,000
   - Document reason for large adjustments
   - Note: Only 0.4% of transactions are "very large"

3. **Reversal Patterns**
   - Type 288: Review high reversal rate (65.7%)
   - Type 439: Monitor reversal patterns (46.4%)

4. **Transaction Size Guidelines**
   - Small (< $100): Standard processing
   - Medium ($100-$999): Regular verification
   - Large ($1,000-$4,999): Manager review
   - Very Large (≥$5,000): Special approval required

## Next Steps
1. Investigate reason for missing clinic IDs
2. Develop provider-specific monitoring
3. Establish clear guidelines for large transaction approval process
4. Create regular monitoring report for reversal patterns
