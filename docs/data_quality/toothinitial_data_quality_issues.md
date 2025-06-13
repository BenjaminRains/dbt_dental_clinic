# ToothInitial Data Quality Issues: Empty Tooth Number

## Issue Summary
During data quality checks, we identified **271 records** in the `toothinitial` table where the `tooth_num` field is empty. According to business rules, every `toothinitial` record should be associated with a specific tooth number. These records violate that rule and represent a data quality concern.

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

## Root Cause Hypothesis
- These records are likely the result of a specific workflow, batch process, or legacy data import that did not require or capture a tooth number.
- They are not systematically linked to extractions, missing teeth, or other clinical events.

## Recommended Actions
- **Short-term:** Exclude these records from analytics and reporting that require a valid tooth number.
- **Long-term:** Investigate the workflow or process that creates these records and update the system to prevent creation of `toothinitial` records without a valid tooth number.

## Summary Statement for Stakeholders
> The presence of `toothinitial` records with empty tooth numbers is a data quality issue, not a reflection of actual missing teeth. These records should be excluded from clinical analytics and their creation process should be reviewed and corrected. 