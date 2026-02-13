# Appointment Type Classification Investigation

**Related TODO:** [Appointment Workflow Improvement - Type Classification](../../../../TODO.md#appointment-workflow-improvement---type-classification)  
**Status:** Investigation complete (analytics side); workflow review is operational  
**Date:** 2026-02-06

---

## Summary

Appointments with `appointment_type_id = 0` (OpenDental "None") are **valid** in the source system but are strongly associated with data quality issues in our tests. This document answers the TODO action items from the perspective of what we can determine in the analytics codebase and what remains for OpenDental workflow review.

---

## Findings vs. TODO Action Items

### 1. Review appointment creation workflow in OpenDental system

**Conclusion:** Cannot be done in code. This is an **operational/OpenDental** review.

- Workflow lives in OpenDental UI/settings (e.g. how new appointments get a default type).
- **Recommendation:** Have practice staff or OpenDental admin review: **Setup → Appointments → Appointment Types** and the flow when creating a new appointment (whether a type is required or defaults to "None").

---

### 2. Identify why appointments are created with `appointment_type_id = 0`

**What we know from data:**

- **`appointment_type_id = 0` is a defined type** in OpenDental: `stg_opendental__appointmenttype` documents it as **"None"** — "a valid appointment that hasn't had specific details appended yet."
- **Correlation with DQ issues:**
  - **68.7%** of broken appointments without procedures (that triggered the old test) had `appointment_type_id = 0`.
  - **87.5%** of past appointments still marked as scheduled had `appointment_type_id = 0`.
- So in our data, "unclassified" (type 0) appointments are much more likely to also have missing procedure info or stale status.

**Why they might be created with type 0:**

- Default when no type is selected at creation.
- Quick scheduling without choosing a type.
- Legacy or imported data.
- Consultations / placeholders that are never updated.

**Recommendation:** Run the **monitoring query** (see below) to see overall rate and trend of type 0; then in OpenDental, confirm whether new appointments default to "None" and whether that’s intentional.

---

### 3. Determine if `appointment_type_id = 0` is intentional default or workflow gap

**Conclusion:** **Both.**

- **Intentional:** It is a valid, documented type ("None") in OpenDental and in our `_stg_opendental__appointmenttype.yml`.
- **Workflow gap:** The high share of type 0 among problematic appointments suggests that many appointments that *should* be classified (e.g. "Consultation", "New Patient") are left as "None", which then correlates with:
  - No procedures linked (consultation-style or incomplete scheduling).
  - Status not updated (e.g. still "Scheduled" after the visit).

So the gap is **incomplete use of classification**, not that "0" is invalid.

---

### 4. Implement validation or training to ensure proper classification

**What we implemented in the repo:**

| Item | Status |
|------|--------|
| **Test logic** | `appt_broken_wo_procs` updated to only flag broken appointments **with** linked procedures; consultations (often type 0) no longer fail the test. |
| **Documentation** | `_stg_opendental__appointment.yml` documents that type 0 is valid and that it’s associated with higher DQ issue rates (see below). |
| **Monitoring query** | DBeaver-ready SQL added to track **unclassified (type 0) rate over time** (see `monitor_unclassified_appointment_rate.sql`). |

**Training / process:** Belongs to practice operations (e.g. "Always choose an appointment type when scheduling").

---

### 5. Create monitoring query to track `appointment_type_id = 0` rate over time

**Done.** Use the query in:

- **File:** `validation/staging/appointment/monitor_unclassified_appointment_rate.sql`
- **Target:** Analytics DB (PostgreSQL), schema where staging appointment model is built (e.g. `public_staging` if that’s where `stg_opendental__appointment` lives).

It provides:

- Monthly counts and percentage of appointments with `appointment_type_id = 0`.
- Optional threshold check (e.g. alert if rate &gt; 50% in a given month).

Run periodically (e.g. weekly or after each load) and track the rate; if it rises, trigger the workflow review in OpenDental.

---

### 6. Set up alerts if unclassified appointment rate increases

**Options:**

1. **Manual:** Run `monitor_unclassified_appointment_rate.sql` on a schedule and review the `pct_unclassified` (or threshold) column.
2. **dbt:** Add a **singular test** that fails when the current unclassified rate exceeds a chosen threshold (e.g. 50%); run it in CI or your dbt schedule.
3. **Orchestration:** In Airflow (or similar), run the monitoring query and fail the DAG or send a Slack/email if the rate is above X%.

The monitoring SQL is written so a threshold can be applied in the same query or in a downstream alert step.

---

## Reference

- **Broken appointments / procedure descriptions:** `BROKEN_APPOINTMENTS_FINDINGS.md`
- **Investigation SQL (both issues):** `investigate_appointment_data_quality.sql`
- **Staging model docs:** `dbt_dental_models/models/staging/opendental/_stg_opendental__appointment.yml`
- **Appointment type definitions:** `dbt_dental_models/models/staging/opendental/_stg_opendental__appointmenttype.yml`
