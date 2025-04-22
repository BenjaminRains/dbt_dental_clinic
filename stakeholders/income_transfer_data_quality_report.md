# Income Transfer Data Issue Report
**Date:** [Current Date]

## Overview
We've identified widespread issues with income transfers in our payment system that need attention. These issues affect 60 patients and could significantly impact our financial reporting if not corrected.

## What Needs to be Fixed
### 1. Immediate Fixes Required (High Priority)
- **Patient 22194** (October 31, 2024)
  * Fix 139 outgoing transfers: Change unearned_type from 0 to 288
  * Fix 43 incoming transfers: Change unearned_type from 288 to 0
  * Total: 376 records to fix

- **Patient 884** (October 31, 2024)
  * Fix 124 outgoing transfers: Change unearned_type from 0 to 288
  * Fix 5 incoming transfers: Change unearned_type from 288 to 0
  * Total: 272 records to fix

- **Patient 26793** (October 30, 2024)
  * Fix 31 outgoing transfers: Change unearned_type from 0 to 288
  * Fix 22 incoming transfers: Change unearned_type from 288 to 0
  * Total: 218 records to fix

### 2. Historical Fixes Required (Lower Priority)
- **Patient 8457** (April 24, 2020)
  * Fix 234 outgoing transfers: Change unearned_type from 0 to 288
  * Fix 1 incoming transfer: Change unearned_type from 288 to 0
  * Total: 468 records to fix

### 3. System Setup Changes Required
- [ ] Set Paysplits to "Rigorous" mode
- [ ] Enable Show Income Transfer Manager
- [ ] Enable Claim Pay by Total splits
- [ ] Verify PaymentCreate permissions
- [ ] Enable audit trail logging

## Issue Summary
- **Total Affected Patients:** 60
- **Total Affected Transfers:** 2,401 records
- **Date Range:** April 24, 2020 to November 20, 2024
- **Primary Issue Type:** Incorrect unearned type assignments
- **Most Affected Period:** October 30-31, 2024 and November 4, 2024

## The Rules: How Transfers Should Work

### Income Transfer Rules
1. Income transfers must be $0 payments with offsetting splits:
   - Negative split (outgoing): **MUST** use unearned type 288
   - Positive split (incoming): **MUST** use unearned type 0
   - Total of all splits must sum to zero
2. Each transfer appears as "Txfr" code on patient accounts
3. Transfers affect income allocation but don't change total practice income

### System Requirements
1. **Allocations Setup:**
   - Paysplits should be set to "Rigorous" mode
   - Show Income Transfer Manager should be enabled
   - Claim Pay by Total splits should be enabled
2. **Permissions Required:**
   - PaymentCreate permission for income transfers
   - All transfers are logged in Audit Trail

## Examples

### ✅ Correct Income Transfer
A properly configured income transfer pair should look like this:

1. **Outgoing Transfer:**
   - Payment Type: 0 (Administrative)
   - Amount: -$100.00 (negative)
   - Unearned Type: 288 ✅
   - Payment Notes: "INCOME TRANSFER"
   - Date: 2024-11-04

2. **Matching Incoming Transfer:**
   - Payment Type: 0 (Administrative)
   - Amount: +$100.00 (positive)
   - Unearned Type: 0 ✅
   - Payment Notes: "INCOME TRANSFER"
   - Date: 2024-11-04

### ❌ Incorrect Income Transfer
Common mistakes to fix:

1. **Outgoing Transfer (Wrong):**
   - Payment Type: 0 (Administrative)
   - Amount: -$100.00 (negative)
   - Unearned Type: 0 ❌ (WRONG - must be 288)
   - Payment Notes: "INCOME TRANSFER"
   - Date: 2024-11-04

2. **Matching Incoming Transfer (Wrong):**
   - Payment Type: 0 (Administrative)
   - Amount: +$100.00 (positive)
   - Unearned Type: 288 ❌ (WRONG - must be 0)
   - Payment Notes: "INCOME TRANSFER"
   - Date: 2024-11-04

## Impact if Not Fixed
These issues will affect:
- ❌ Financial reporting accuracy
- ❌ Accounts receivable calculations
- ❌ Revenue recognition
- ❌ Patient account balances
- ❌ Historical financial data (some issues date back to 2020)

## How to Fix: Step-by-Step Guide

### 1. System Setup Verification
1. Check Allocations Setup:
   - Verify Paysplits is set to "Rigorous"
   - Enable Show Income Transfer Manager
   - Enable Claim Pay by Total splits
2. Verify PaymentCreate permissions for staff
3. Enable audit trail logging for transfers

### 2. Fix Process for Each Patient
1. Use Income Transfer Manager:
   - Access via Account Module > Payment dropdown
   - Review Provider/Family Balances section
   - Check Show Breakdown for detailed view

2. For each transfer pair:
   - Double-click transfer in Income Transfer Manager
   - For outgoing transfers (negative amounts): Change to type 288
   - For incoming transfers (positive amounts): Change to type 0
   - Verify amounts match and sum to zero

3. Verify your changes:
   - Check payment window
   - Verify paysplit allocations
   - Confirm unearned types are correct
   - Ensure transfers appear as "Txfr" in account

### Fix Priority Order
1. 🔴 Recent transfers first (October-November 2024)
   - Patient 22194: 182 transfers
   - Patient 884: 129 transfers
   - Patient 26793: 53 transfers

2. 🟡 High-volume patients (100+ transfers)
   - Any remaining patients with 100+ transfers

3. 🟢 Historical transfers (2020-2023)
   - Patient 8457: 235 transfers
   - Remaining historical patients

## Next Steps
1. Create correction plan for each patient
2. Prioritize based on:
   - Volume of affected transfers
   - Date of transfers (recent first)
   - Impact on financial reporting
3. Implement corrections in batches
4. Verify all corrections
5. Notify finance team once complete

## OpenDental Manual References
For detailed guidance, refer to:
- [Income Transfer Documentation](https://opendental.com/manual243/incometransfer.html)
- [Income Transfer Manager](https://opendental.com/manual243/incometransfermanager.html)
- [Unearned Type Definitions](https://opendental.com/manual243/definitionspaysplitunearned.html)
- [Allocations Setup](https://opendental.com/manual243/allocationssetup.html)

## Questions?
If you need clarification or assistance with these corrections, please contact [appropriate contact person].

---

**⚠️ Important:** All transfers must be properly paired and have correct unearned types to ensure accurate financial reporting and patient account balances. Double-check each correction before moving to the next one.

## Detailed Analysis of Transfer Issues (November 2024)

### Pattern Analysis
Our recent analysis of 5,006 transfer records revealed distinct patterns of issues:

1. **Single Match with Correct Sign (1,839 records)**
   - **Pattern:** Transfers have correct unearned types but may have timing issues
   - **Example:**
     ```
     Patient 676 (Nov 4, 2024)
     - Outgoing: -$9.00, unearned_type=288 ✅
     - Incoming: +$9.00, unearned_type=0 ✅
     - Issue: Multiple duplicate pairs (12 matching pairs)
     ```

2. **Single Match with Wrong Sign (803 records)**
   - **Pattern:** Amount signs don't match unearned types
   - **Example:**
     ```
     Patient 1964 (Oct 31, 2024)
     - Outgoing: -$2.40, unearned_type=0 ❌ (should be 288)
     - Incoming: +$2.40, unearned_type=47 ❌ (should be 0)
     ```

3. **Multiple Matches (1,073 records)**
   - **Pattern:** Duplicate transfer entries causing confusion
   - **Example:**
     ```
     Patient 11961 (Oct 30, 2024)
     - Original: -$3.48, unearned_type=288
     - Duplicate 1: -$3.48, unearned_type=288
     - Duplicate 2: -$3.48, unearned_type=288
     - All with same incoming match
     ```

4. **No Matching Pairs (1,291 records)**
   - **Pattern:** Orphaned transfers without counterparts
   - **Example:**
     ```
     Patient 5927 (Nov 4, 2024)
     - Outgoing: -$160.45, unearned_type=0 ❌
     - No matching incoming transfer found
     ```

### Impact Analysis

1. **Financial Impact**
   - Largest single transfer: $10,013.00
   - Median amounts by issue type:
     - Single Match (Correct): $23.13
     - Single Match (Wrong): $40.00
     - Multiple Matches: $29.00
     - No Matches: $68.00-$73.50

2. **Temporal Pattern**
   - All issues occurred between Oct 30, 2024 and Nov 20, 2024
   - Peak issues on:
     - Oct 30-31, 2024
     - Nov 4, 2024
     - Nov 13, 2024

3. **Patient Impact**
   - Total affected patients: 446
   - Distribution:
     - 185 patients with single correct matches
     - 67 patients with sign issues
     - 94 patients with multiple matches
     - 100 patients with missing pairs

### Root Cause Analysis

1. **System Configuration Issues**
   - Recent changes in late October 2024
   - Possible permission or setting changes
   - Audit trail gaps during peak issue periods

2. **Process Issues**
   - Inconsistent application of unearned types
   - Duplicate transfer creation
   - Missing transfer pair creation

3. **Data Entry Patterns**
   - Higher error rates with larger amounts
   - Provider-specific patterns in unearned type assignments
   - Time-of-day clustering of issues

### Recommended Actions

1. **Immediate Fixes**
   - Review and correct all transfers from Oct 30-Nov 20, 2024
   - Focus on largest amounts first
   - Verify system settings and permissions

2. **Process Improvements**
   - Implement transfer validation before save
   - Add automated pair verification
   - Enhance audit trail logging

3. **Monitoring**
   - Set up alerts for unusual transfer patterns
   - Monitor transfer creation in real-time
   - Regular validation of transfer pairs

### Example Correction Process

For a typical case:
```
Patient 676 (Nov 4, 2024)
1. Identify all transfers for the date
2. Group by amount (e.g., $9.00 pairs)
3. Verify unearned types:
   - Negative amounts → 288
   - Positive amounts → 0
4. Remove duplicates
5. Verify net zero balance
```