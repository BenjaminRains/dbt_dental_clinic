# Archive: dbt planning & one-off docs

**Status:** Historical · **Living docs:** `../` (parent `docs/dbt/`)

Completed plans, superseded checklists, and point-in-time investigations moved out of
`docs/dbt/` so the parent folder stays current.

## Archived files

| File | Why archived |
|------|----------------|
| `EXPOSURES_DEVELOPMENT_PLAN.md` | Marked DEPRECATED — exposures/dashboards complete |
| `int_implementation_plan.md` | Marked DEPRECATED — intermediate layer built |
| `staging_documentation_implementation_checklist.md` | Marked DEPRECATED — staging YAML done |
| `HYGIENE_RETENTION_DASHBOARD_PLAN.md` | Plan shipped (`mart_hygiene_retention` + UI) |
| `dbt_source_freshness_refactor.md` | Freshness `loaded_at_field` pattern in sources |
| `marts_documentation_standardization.md` | One-time YAML standardization plan |
| `macro_implementation_checklist.md` | Companion to archived naming migration; macros in use |
| `explicit_column_selection_findings.md` | Staging `select *` refactor findings; work done |
| `marts_models_plan.md` | Early mart wishlist; many names diverge from current marts |
| `dbt_lineage_hints_implementation.md` | Feature implemented (API + InfoTooltip) |
| `test_data_quality_reivew.md` | Oct 2025 baseline checklist (typo in filename) |
| `DBT_TROUBLESHOOTING.md` | Resolved `'config' is undefined` incident write-up |
| `dbt_test_failures_demo_analysis.md` | Point-in-time demo test failure dump |
| `diagnose_mart_ar_summary.sql` | Demo AR empty-results diagnostics (see also `docs/_archive/dbt_balance_fix_2026-01/`) |

Related older standardization strategies live under
[`docs/_archive/dbt_standardization_2025/`](../../_archive/dbt_standardization_2025/).
