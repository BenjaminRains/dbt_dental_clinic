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