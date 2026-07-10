# Incremental `_loaded_at` refactor — status

**Last updated:** 2026-07-09

## Summary

| Phase | Scope | Status |
| --- | --- | --- |
| 1–2 | 18 models (HIGH/MEDIUM early wave) | Done (prior work) |
| **3** | 9 remaining MEDIUM models | **Done** (2026-07-09) |
| 4 | Full-project `dbt build` + regression monitor | Pending (deferred; not blocking Phase 3) |

## Phase 3 checklist

### Staging — add `is_incremental()` filters

| Model | Source filter column | Status |
| --- | --- | --- |
| `stg_opendental__tasknote` | `"DateTimeNote"` | Done |
| `stg_opendental__document` | `"DateTStamp"` | Done |
| `stg_opendental__timeadjust` | `"TimeEntry"` (keeps 2023 floor) | Done |
| `stg_opendental__toothinitial` | `"SecDateTEdit"` | Done |

Pattern: compare cleaned source edit timestamp to `COALESCE(MAX(_loaded_at), '1900-01-01')` on `{{ this }}`.

### Collection intermediates — fix wrong watermark

| Model | Was | Now | Status |
| --- | --- | --- | --- |
| `int_collection_tasks` | `entry_timestamp > MAX(model_created_at)` | `t._loaded_at > cutoff`; emit `t._loaded_at` | Done |
| `int_collection_communication` | `communication_datetime > MAX(model_created_at)` | `cl._loaded_at > cutoff`; emit `cl._loaded_at` | Done |

`model_created_at` / `model_updated_at` retained for YAML compatibility; they are **not** the incremental watermark.

### Payment intermediates — add upstream `_loaded_at` filters

| Model | Filter | Status |
| --- | --- | --- |
| `int_insurance_payment_allocated` | `cp._loaded_at OR ip.payment_loaded_at > cutoff`; emit `GREATEST(...)` | Done |
| `int_patient_payment_allocated` | `p._loaded_at OR ps._loaded_at > cutoff`; emit `GREATEST(...)` | Done |
| `int_payment_split` | `ps._loaded_at OR p._loaded_at > cutoff` in `base_splits`; emit `GREATEST(...)` | Done |

## Local verification (2026-07-09)

No ETL. Per wave: `--full-refresh`, then incremental `run`, then `test`.

| Wave | Incremental result | Notes |
| --- | --- | --- |
| Staging (4) | `INSERT 0 0` all models | Pre-existing: `not_null` on `toothinitial.tooth_num` (554); several WARN |
| Collection (2) | `INSERT 0 0` | Pre-existing: `collection_tests` SQL syntax error |
| Payment (3) | `INSERT 0 0` | Pre-existing data-quality FAILs (null bank_branch, etc.) — not introduced by watermark changes |

## Phase 4 (optional follow-up)

- Full `dbt build` triage remains under Tier 5 “Review dbt build failures”
- Monitor first nightly after merge for zero-row / watermark regressions on these 9 models
