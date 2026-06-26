# Daily Payments golden parser

`parse_od_daily_payments_golden.py` converts an OpenDental **Daily Payments CSV export** into a
**PHI-free YAML snapshot** of section subtotals and row counts.

## Why it exists

Golden CSVs contain **full PHI** (patient names, check numbers). They stay on the clinic secure
site and are gitignored — they must not be copied into compare SQL, findings in shared repos, or
chat prompts.

You still need structured OD numbers for validation:

- Net collections, patient vs insurance section totals
- Per-section subtotals (Credit Card, Check, carrier names, …)
- Detail row counts (OD provider-split grain vs mart header grain)

Copying those by hand from a long CSV is error-prone. This script reads the golden file locally
and writes a safe summary.

## What it does

1. Parses OD’s section-based export format (section name → header → detail rows → subtotal row).
2. Classifies sections: `provider_split` (patient PayTypes) vs `carrier_split` (insurance).
3. Computes totals using the same rules as the OD report:
   - `patient_sections_amount` — patient sections, excluding Income Transfer
   - `insurance_sections_amount` — carrier sections
   - `net_collections` — all sections except Income Transfer
   - Income Transfer tracked separately (should net to $0)
4. Writes `golden/snapshots/od_daily_payments_{stem}.snapshot.yml` (amounts and counts only).

## What it does not do

- Does **not** query the warehouse or run dbt
- Does **not** compare mart vs OD (use SQL in `../compare/`)
- Does **not** replace the golden CSV (CSV remains source of truth for row-level validation)

## Usage

From `validation/kpi/daily-payments/`:

```bash
python scripts/parse_od_daily_payments_golden.py golden/od_daily_payments_11082025_11082025.csv
```

Output (default): `golden/snapshots/od_daily_payments_11082025_11082025.snapshot.yml`

Custom output:

```bash
python scripts/parse_od_daily_payments_golden.py golden/od_daily_payments_10072025_10072025.csv \
  -o golden/snapshots/od_daily_payments_10072025_10072025.snapshot.yml
```

## Where snapshot values go

| Consumer | How |
| --- | --- |
| `compare/compare_daily_payments_all_fields.sql` | Paste `od_sections` into the `VALUES` CTE (Block C/D) |
| `compare/compare_daily_collections.sql` | Set `od_total` net collections from `od_totals.net_collections` |
| `findings/{date}.md` | Reference section totals without PHI |
| `golden_manifest.yml` | Optional `od_totals` fields (copy from snapshot) |

## Example output (2025-11-08)

```yaml
od_totals:
  patient_sections_amount: 3791.65
  insurance_sections_amount: 0
  net_collections: 3791.65
od_sections:
  - od_section: 'Credit Card'
    amount: 3791.65
    detail_row_count: 11
    grain: provider_split
```

That `net_collections` value is the OD side of mart compare — e.g. `$3,791.65` vs
`mart_daily_payments.net_collections_amount`.

## Golden CSV naming

```
od_daily_payments_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv
```

Single day: same date twice (e.g. `od_daily_payments_11082025_11082025.csv` for 2025-11-08).
