# KPI Validation (OpenDental Report Benchmarking)

**Purpose**: Compare dashboard KPIs and mart models against OpenDental's built-in reports.

This track is separate from `validation/marts/` (raw → staging → marts reconciliation). Here the
benchmark is what the office sees in **Reports** in OpenDental, not just ETL fidelity.

---

## Directory layout

```
validation/kpi/
├── README.md                      # This file
├── KPI_VALIDATION_REGISTRY.md     # KPI ↔ OD report mapping and known deltas
├── golden/                        # Drop zone for OD report CSV exports (PHI — not committed)
│   ├── golden_manifest.yml        # Optional: aggregate totals + file index (no row-level PHI)
│   └── golden_manifest.example.yml
├── compare/                       # SQL to diff marts vs golden totals or MySQL reconstruction
└── screenshots/                   # Optional: OD filter dialogs for reproducibility (local only)
```

Repo root `.gitignore` already excludes `*.csv`, `*.xlsx`, and `*.xls`. Files in `golden/` stay
local unless you copy anonymized totals into `golden_manifest.yml`.

---

## Workflow

### 1. Register the KPI

Add or update a row in `KPI_VALIDATION_REGISTRY.md`:

- Our mart model and field
- OpenDental report name and menu path
- Manual URL (from `reference/opendental_manual/` or opendental.com)
- Date basis, filters, grain, tolerance, known deltas

Cross-reference: `models/marts/exposures.yml`, `frontend/src/pages/KPIDefinitions.tsx`.

### 2. Export from OpenDental

1. Pick a **frozen validation window** (document it in the registry).
2. Run the OD report with fixed parameters; screenshot filters → `screenshots/` if helpful.
3. Export CSV → `golden/` using this naming convention:

   ```
   {report_slug}_{date_from}_{date_to}_{exported_yyyy-mm-dd}.csv
   ```

   Examples:

   - `daily_payments_2025-07-01_2025-09-30_2026-06-25.csv`
   - `production_income_by_provider_2025-07-01_2025-09-30_2026-06-25.csv`

4. Record the export in `golden/golden_manifest.yml` (copy from `golden_manifest.example.yml`):
   file name, parameters, and **aggregate totals only** — not patient-level rows.

### 3. Compare

Write or run SQL in `compare/`:

- **Track A**: Reconstruct OD logic in MySQL (`opendental`) and compare to `marts.*` (PostgreSQL).
- **Track B**: Load manifest totals and diff against mart aggregates for the same window.

Run comparison queries in DBeaver against `opendental_analytics` (`analytics_user`).

Use PASS / WARN / FAIL thresholds from `validation/marts/fact_claim/fact_claim_validation_plan.md`
(e.g. exact match, or &lt;1% variance = WARN).

### 4. Document deltas

When numbers do not match, record **why** in the registry (`known_deltas`). Common causes:

- Procedure date vs payment date
- Refunds / income transfers / unearned prepayment handling
- Unmapped `payment_type_id` values (see `mart_daily_payments.sql`)
- Hidden providers, test patients, clinic filters

### 5. Operationalize (optional)

Promote stable checks to `dbt_dental_models/tests/kpi/` as warn-level data tests. Keep
exploratory work in `compare/` until definitions are stable.

---

## Priority KPIs (starter set)

| Priority | KPI | Our model | OD report (verify in UI) |
| --- | --- | --- | --- |
| 1 | Daily net collections | `mart_daily_payments` | Daily Payments / deposit log |
| 2 | Production by provider | `mart_provider_performance` | Production and Income |
| 3 | AR aging totals | `mart_ar_summary` | Accounts Receivable Aging |

See `KPI_VALIDATION_REGISTRY.md` for full mapping template and slots to fill in.

---

## Related documentation

- `validation/README.md` — internal layer validation (staging / intermediate / marts)
- `validation/marts/fact_claim/fact_claim_validation_plan.md` — phased plan and thresholds
- `models/marts/_mart_daily_payments.yml` — collections mart intended for OD reconciliation
- `stakeholders/income_transfer_provider_assignment.md` — example OD manual report links
- `powerbi/report_checklist.md` — mart-to-mart SQL reconciliation (complementary)
