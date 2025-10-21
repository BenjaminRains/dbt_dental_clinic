# ToothInitial Data Quality Issues: Empty Tooth Number

## ⚠️ CRITICAL UPDATE (October 20, 2025)
**This is an ONGOING DATA QUALITY ISSUE requiring immediate action!**

- **Current Count:** 360 records (up from 271)
- **Recent Activity:** 279 records (77.5%) were created or updated in 2024-2025
- **Latest Record:** October 13, 2025 (LAST WEEK!)
- **Status:** Staff are actively entering incomplete records RIGHT NOW
- **Severity:** ERROR - Blocking dbt builds until resolved

## Issue Summary
During data quality checks, we identified **360 records** in the `toothinitial` table where the `tooth_num` field is empty or NULL. According to business rules, every `toothinitial` record should be associated with a specific tooth number. These records violate that rule and represent a data quality concern.

**This is NOT legacy data** - the majority of these records are recent, indicating an active workflow problem or lack of form validation in OpenDental.

## Business Rules for Interpreting `toothinitial`
- **Every record must reference a specific tooth**: Each `toothinitial` record must have a valid `tooth_num` (e.g., 1–32 for adult teeth, A–T for primary teeth, or other accepted codes).
- **Each record must be linked to a patient**: The `PatNum` field must always be present and valid.
- **Accepted tooth number formats**: Only certain formats are allowed for `tooth_num` (1–32, A–T, and some non-standard codes like 51, 57, etc.).
- **No empty or null tooth numbers**: Empty or null `tooth_num` values are not allowed, as they make the record ambiguous and unusable for clinical or analytical purposes.
- **Clinical meaning**: Each record describes the initial state (e.g., rotation, tipping, intrusion) or planned movement for the tooth. The `InitialType` and `Movement` fields provide clinical context.

## Scope and Impact
- **Affected Patients:** 120+ unique patients have at least one anomalous record.
- **Distribution:** Most affected patients have 1–4 such records, but a few have more (up to 14).
- **Example Patient IDs:** 29221, 28048, 9258, 28694, 816, 31503, 23938, 31689, 24822, 26692, etc.
- **UI Impact:** These records do not appear as normal tooth entries in the OpenDental odontogram or chart. They may show up as blank or orphan entries in the Ortho or Perio modules, or may only be visible in the database or audit logs.

## Clinical Interpretation
- **Valid Record:** A record with a valid `tooth_num`, `PatNum`, and clinical details means that a specific tooth for a patient is being tracked for a clinical reason.
- **Invalid/Anomalous Record:** A record with an empty `tooth_num` cannot be interpreted clinically—it does not correspond to any specific tooth and should be excluded from analysis.
- **Missing Teeth:** If a tooth is missing, it is typically not present in the `toothinitial` table at all, or is explicitly marked as missing with a valid `tooth_num` and a status indicating absence.

## What This Is Not
- These records **do not represent actual missing teeth**. In normal clinical workflows, missing teeth are simply not present in the perio measurement data and are not recorded as `toothinitial` records with empty tooth numbers.
- The number of anomalous records is much smaller than the number of actual missing teeth in the patient population.

## How to Investigate in OpenDental
- **Step 1:** Use the patient ID from the list above to open the patient's chart in OpenDental.
- **Step 2:** Check the Ortho Chart or Tooth Initials module for entries with no associated tooth number, or look for blank/orphan lines.
- **Step 3:** These records may also be visible in the audit log or raw data viewer, but will not be linked to a specific tooth in the odontogram.

## Root Cause Analysis (Updated Oct 2025)
**Previous Hypothesis (Incorrect):**
- ~~These records are likely the result of legacy data import~~ 

**Current Finding:**
- **77.5% of records created in 2024-2025** - This is NOT a legacy issue
- **Active workflow problem:** Staff are entering records without tooth numbers TODAY
- **Missing form validation:** OpenDental UI allows NULL tooth numbers to be saved
- **Training gap:** Staff may not understand that tooth number is mandatory
- **Possible UI confusion:** Form may not clearly indicate tooth number is required
- Records are not systematically linked to extractions, missing teeth, or other clinical events

## Immediate Action Required
**Priority: HIGH - Must be resolved within 1 week**

### 1. Data Cleanup (Immediate)
- Fix all 360 existing records with missing tooth numbers
- Either assign valid tooth numbers or delete incomplete records
- Coordinate with clinical staff to identify which teeth these records were intended for

### 2. Staff Training (This Week)
- Train all staff who enter orthodontic records
- Emphasize that tooth number is MANDATORY for every toothinitial record
- Review proper workflow for recording tooth initial conditions
- Share examples of correct vs. incorrect entries

### 3. System Validation (Within 1 Week)
- Add form validation in OpenDental to prevent NULL tooth number entries
- Make tooth_num a required field in the UI
- Consider dropdown selection instead of free text to prevent errors
- Test validation with staff before deploying

### 4. Monitoring (Ongoing)
- dbt tests will continue to ERROR if new records are created without tooth numbers
- Weekly review of any new violations
- Track which staff members need additional training

## Recommended Actions (Original)
- **Short-term:** Exclude these records from analytics and reporting that require a valid tooth number.
- **Long-term:** ~~Investigate the workflow or process that creates these records and update the system to prevent creation of `toothinitial` records without a valid tooth number.~~ **[COMPLETED ABOVE - NOW ACTIONABLE]**

## Summary Statement for Stakeholders
> **URGENT:** The presence of 360 `toothinitial` records with empty tooth numbers is an **active, ongoing data quality issue** that requires immediate intervention. With 77.5% of these records created in 2024-2025 (latest: October 13, 2025), staff are currently entering incomplete data. This is NOT legacy data. Immediate actions required: 1) Clean up 360 existing records, 2) Train staff on proper data entry, 3) Add form validation in OpenDental to prevent future occurrences. dbt builds will continue to ERROR until this is resolved.

## Test Configuration
- **dbt Test:** `not_null` on `tooth_num` column
- **Severity:** ERROR (blocking builds)
- **Location:** `models/staging/opendental/_stg_opendental__toothinitial.yml`
- **Last Updated:** October 20, 2025 