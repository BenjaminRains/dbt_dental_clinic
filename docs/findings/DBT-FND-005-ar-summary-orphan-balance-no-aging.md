# DBT-FND-005 — `mart_ar_summary` orphan balance (dim total, no aging buckets)

**Type:** Mart / dbt defect  
**Status:** Fixed (2026-07-09 Batch 9c)  
**Found:** 2026-07-09 (dbt test failure triage Batch 9c)  
**Primary model:** `mart_ar_summary`

## Summary

`mart_ar_summary` uses `dim_patient.total_balance` as the source of truth and scales
aging buckets from `int_ar_balance` proportionally. When a patient has a positive
dim balance but **no** aging rows (bucket sum = 0), the scale `CASE` set every
bucket to `0.00`. Percent columns (`pct_current` … `pct_over_90`) were then all
`0.00`, so the “pct sum ≈ 100” test failed for **150** patients (~$63.8k).

Individual `pct_current` / `pct_over_90` reconcile tests still passed (0 = 0/total).

## Fix

When bucket sum = 0 and `total_balance > 0`, assign the full amount to
`balance_over_90_days` (conservative aging). Percentages then sum to 100 and
risk category reflects aged AR.

## Related

- Bucket scaling logic in `ar_aggregated` CTE of `mart_ar_summary.sql`
- Upstream gap: `dim_patient.total_balance` without matching `int_ar_balance` rows
  (investigate separately if needed)
