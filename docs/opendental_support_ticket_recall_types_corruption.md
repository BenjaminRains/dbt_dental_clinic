# OpenDental Support Ticket: UI-Database Conversion Bug in RecallType Table

**Date:** June 12, 2025  
**Severity:** MEDIUM - Data Quality Issue  
**Module:** Appointments > Recall Types  
**Database Table:** `recalltype`  

---

## **PROBLEM SUMMARY**

OpenDental database contains corrupted data in the `recalltype` table due to a UI-to-database conversion bug. The OpenDental UI displays correct interval values (e.g., "6m1d") but stores corrupted binary values in the database (e.g., 393,217 days = 1,077 years).

**Important:** Patient operations are unaffected as the practice uses Practice by Numbers (PbN) for all recall communications, which operates independently of these OpenDental values.

---

## **SYSTEM INFORMATION**

- **OpenDental Version:** 24.3.36.0 (Database: 24.3.35.0)
- **Registration Key:** 6Q37JCWHQYXKCHIR
- **Last Update:** 01/03/2025 (from v24.3.33.0)
- **Database:** MySQL 5.5.14 (MyISAM), Server: 192.168.2.10

---

## **CORRUPTED DATA EVIDENCE**

### **Current Database Values:**
```
RecallType       | DefaultInterval | Expected | Calculated Years
Prophy          | 393,217        | ~183     | 1,077 years
Child Prophy    | 0              | ~183     | 0 days  
Perio           | 196,609        | ~90      | 538 years
4BW/2BW         | 16,777,217     | ~365     | 45,965 years
Pano/FMX        | 83,886,081     | ~730     | 229,825 years
Exam            | 393,217        | ~183     | 1,077 years
```

### **Binary Pattern Evidence:**
- **393,217** = 0x60001 (hexadecimal)
- **196,609** = 0x30001 (hexadecimal)  
- **16,777,217** = 2²⁴ + 1
- **83,886,081** = 5 × 2²⁴ + 1

These mathematical patterns are impossible for human input, confirming software corruption.

---

## **BUG REPRODUCTION**

**Test Performed:** Created new recall type "TEST_CONVERSION_BUG"
- **UI Input:** 6 months
- **Expected Database Value:** ~183 days
- **Actual Database Value:** 393,216 days
- **Result:** ✅ Bug actively reproduced - conversion issue confirmed

**UI vs Database Display:**
- Users see correct values in OpenDental interface (6m1d, 1y1d, etc.)
- Database stores corrupted binary values
- This confirms a **UI-to-database conversion bug**

---

## **BUSINESS IMPACT: ZERO OPERATIONAL EFFECT**

**Why Patients Are Unaffected:**
1. **OpenDental Native Recall System:** DISABLED (`IsEnabled = 0`)
2. **Active System:** Practice by Numbers (PbN) handles all patient recalls
3. **PbN Integration:** 93,459+ automated messages via ProgramNum 137
4. **Patient Experience:** Cleaning reminders and appointments work normally

**PbN System Coverage:**
- Tracks: Prophy ✓, Child Prophy ✓, Perio ✓
- Uses industry standard intervals (6 months Prophy, 3 months Perio)
- Operates independently of OpenDental DefaultInterval values

---

## **REQUESTED ASSISTANCE**

### **Primary Questions:**
1. **Is this a known UI-to-database conversion bug** in v24.3.36.0?
2. **Was this introduced** in the recent update from v24.3.33.0?
3. **Is there a patch/hotfix** available?
4. **How to safely repair** the database without breaking the UI?

### **Suggested Database Repair:**
```sql
UPDATE recalltype SET DefaultInterval = 183 WHERE RecallTypeNum IN (1,2,7); -- 6 months
UPDATE recalltype SET DefaultInterval = 90 WHERE RecallTypeNum = 3;         -- 3 months  
UPDATE recalltype SET DefaultInterval = 365 WHERE RecallTypeNum IN (4,8);   -- 1 year
UPDATE recalltype SET DefaultInterval = 730 WHERE RecallTypeNum IN (5,6);   -- 2 years
```

---

## **CONTACT INFORMATION**

**Primary Contact:** [Your Name]  
**Email:** [Your Email]  
**Phone:** [Your Phone]

---

**Priority:** MEDIUM - Data quality issue requiring attention  
**Response Requested:** Within 3-5 business days  
**Preferred Resolution:** UI bug fix + database remediation guidance 