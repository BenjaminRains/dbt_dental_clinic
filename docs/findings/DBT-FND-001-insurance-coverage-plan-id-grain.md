# DBT-FND-001 — `int_insurance_coverage` uses `patplan_id` as `insurance_plan_id`

**Type:** Mart / dbt defect  
**Status:** Open  
**Found:** 2026-07-09 (dbt test failure triage Batch 7c)  
**Primary models:** `int_insurance_coverage`, consumers such as `int_claim_details`

## Summary

`int_insurance_coverage` exposes `pp.patplan_id AS insurance_plan_id`, but OpenDental claim/claimproc `plan_id` is **`PlanNum`** (`insplan`). Joining claims to coverage on that column almost never matches; any “hits” are accidental ID collisions between different entity spaces.

## Evidence (clinic analytics, 2026-07-09)

| Check | Result |
| --- | --- |
| `int_claim_details` rows with `insurance_plan_id` null | **109,148 / 109,148** (100%) |
| Staging claimproc / claim with `plan_id > 0` | **All** rows have plan ids |
| `int_insurance_coverage` row count | **4,616** (= `stg_opendental__patplan`) |
| `stg_opendental__insplan` row count | **6,373** |
| Claims whose `plan_id` equals some coverage `insurance_plan_id` | **340 / 17,431** (~2%) — collisions, not real joins |
| Sample claims (26727, 25745, 25209) | `PlanNum` present; no matching `patplan_id` |

In `int_insurance_coverage.sql` the insurance-plan join is also wrong:

```sql
left join insurance_plan_enhanced ip
    on pp.patplan_id = ip.insurance_plan_id  -- PatPlanNum = PlanNum (coincidental)
```

`int_claim_details` then does:

```sql
left join insurance_coverage ic
    on coalesce(cp.patient_id, c.patient_id) = ic.patient_id
    and coalesce(cp.plan_id, c.plan_id) = ic.insurance_plan_id  -- PlanNum = PatPlanNum
```

## Impact

- All claim-detail insurance attributes from coverage (`insurance_plan_id`, carrier, subscriber, plan type, verification flags) are null even for valid primary claims.
- dbt tests: primary-plan requirement (~43k) and related not_null/FK rules fail; masks real plan association quality.
- Any mart/KPI that trusts `int_claim_details.insurance_plan_id` or coverage-derived fields is under-informed.

## Proposed fix (not applied in Batch 7c soft-rule pass)

1. Re-key coverage grain: keep patient-plan rows if needed for patient-level coverage, but expose a true **`plan_id` / `PlanNum`** (from `insplan` via `patplan` → `inssub` → plan, or whatever OD path this project already uses elsewhere).
2. Stop aliasing `patplan_id` as `insurance_plan_id`; rename to `patient_plan_id` / `patplan_id` if that grain is retained.
3. Fix `patplan` ↔ `insplan` join to use the real foreign key (subscriber / plan link), not `patplan_id = insurance_plan_id`.
4. Rebuild `int_insurance_coverage+` and re-validate claim-detail plan fill rate (expect near 100% for `claim_id > 0` primary claims with `plan_id > 0`).

## Temporary triage disposition (Batch 7c)

- Soften `claim_status` / `claim_type` not_null and claim FK with `where: claim_id > 0` (separate issue: unattached claimprocs with `claim_id = 0`).
- Primary-plan / all-null plan fields: **warn or hold** until this finding is fixed — do not treat as expected OD data.

## Related

- [DBT_TEST_FAILURE_TRIAGE.md](../dbt/DBT_TEST_FAILURE_TRIAGE.md) Batch 7c-A
