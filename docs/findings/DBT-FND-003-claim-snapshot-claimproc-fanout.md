# DBT-FND-003 — `int_claim_snapshot` fan-out on claim+procedure joins

**Type:** Mart / dbt defect  
**Status:** Fixed (2026-07-09 Batch 8c)  
**Found:** 2026-07-09 (dbt test failure triage Batch 8c)  
**Primary model:** `int_claim_snapshot`

## Summary

`int_claim_snapshot` joined `int_claim_details` and ranked `int_claim_payments` on
`(claim_id, procedure_id)`. OpenDental often has **multiple claimprocs** for the same
claim+procedure (resubmits / supplementals). That multiplied identical snapshot rows
(~3,430 extras; unique `claim_snapshot_id` failed with ~3,208).

Example claim 19991 / procedure 1097903: five claim_detail rows and five payment rows
(claim_procedure_ids 215528, 218927, 227813, 260100, 260106) → five identical snapshot
rows for snapshot 51710.

## Fix

Join and rank on **`claim_procedure_id`** (snapshot’s native grain).

## Related YAML softs (same batch)

- Fee / estimate ceilings → 15k warn; allowed warn (typos)
- `most_recent_payment` band **[-2000, 15k] warn** (709 were negatives, not >10k)
