# Daily Production by Procedure — golden parser

`parse_od_daily_production_by_procedure_golden.py` reads the OpenDental **Daily → Production by
Procedure** CSV and writes a snapshot YAML (procedure codes, quantities, fees — no patient PHI).

Report date is taken from the **filename stem** (not the CSV `Date:` line when they disagree).

## Usage

```bash
cd dbt_dental_models/validation/kpi/daily-production-by-procedure
python scripts/parse_od_daily_production_by_procedure_golden.py \
  golden/od_daily_production_by_procedure_06102026_06102026.csv
```

Output: `golden/snapshots/od_daily_production_by_procedure_06102026_06102026.snapshot.yml`

Use `od_totals.total_fees` and `od_rows` in compare SQL (`compare/*.sql`).
