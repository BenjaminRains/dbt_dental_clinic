# Daily Payments — OD Report ↔ Mart Field Map

**Goal:** Same business logic as OpenDental's Daily Payments report — this map is the spec our
mart should implement; compare SQL and golden totals verify it on specific dates.

**Audience:** Clinic secure site validation (VPN / allowlisted). Golden exports retain **full PHI**
(patient names, check numbers) — required for row-level reconciliation, not just totals.

**Mart:** `marts.mart_daily_payments` (one row per `payment_date`)  
**OD report:** Reports → Standard → Daily Payments  
**Golden files:** `golden/od_daily_payments_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv` (e.g. `od_daily_payments_06242026_06242026.csv`)

---

## OD report structure

Each export is a sequence of **sections** (PayType or carrier name). Within a section:

| OD column | Patient sections | Insurance sections |
| --- | --- | --- |
| Date | ✓ | ✓ |
| Paying Patient | ✓ | — |
| Carrier | — | ✓ |
| Patient Name | — | ✓ (insurance) |
| Provider | ✓ | ✓ |
| Check# | ✓ | ✓ |
| Amount | ✓ | ✓ |

Section **subtotal** row (leading empty columns) is the section amount. **Income Transfer** must
net to **$0.00**; it is not included in `net_collections_amount`.

**Grain:** OD detail rows are **provider splits** (and carrier splits for insurance). Mart
`payment_count` uses **payment / claim headers**, not OD detail row count — see count rules below.

---

## Mart column validation rules

| Mart column | OD / staging source | Validation |
| --- | --- | --- |
| `payment_date` | Report date filter | Exact |
| `patient_payment_amount` | Sum of patient section subtotals (excl. Income Transfer) | = OD `patient_sections_amount` |
| `insurance_payment_amount` | Sum of insurance/carrier section subtotals | = OD `insurance_sections_amount` |
| `net_collections_amount` | Patient + insurance sections (Income Transfer excluded) | = OD `net_collections` |
| `total_payment_amount` | Same as net for this mart | = `patient_payment_amount` + `insurance_payment_amount` |
| `other_payment_amount` | Patient PayTypes not in mapped clinic list (69, 70, 71, 391, …) | = staging unmapped types |
| `income_amount` | Sum of positive `payment_amount` + positive `check_amount` | Staging-derived; not an OD section total |
| `refund_amount` | Sum of negative amounts | Staging-derived |
| `payment_count` | Non-zero patient headers + claimpayment rows | ≠ OD detail row count |
| `patient_payment_count` | Non-zero `stg_opendental__payment` headers | ≠ OD patient section detail rows |
| `insurance_payment_count` | `stg_opendental__claimpayment` rows on `check_date` | May differ from OD insurance detail rows |
| `other_payment_count` | Unmapped PayType header count | Staging-derived |

### Section-level (per PayType / carrier)

Map each OD section subtotal to staging:

| OD section pattern | Staging source |
| --- | --- |
| Carrier header (`Carrier` column) | `stg_opendental__claimpayment` by `check_date` + `payment_type_id` / definition name |
| Check, Cash, Credit Card, Cherry, Care Credit, … | `stg_opendental__payment` by `payment_date` + `payment_type_id` |
| Income Transfer | Must total $0; investigate via paysplit / transfer PayTypes if non-zero |

### Golden parser (PHI → snapshot)

OpenDental golden CSVs contain patient names and check numbers. Run locally on the clinic site:

[`scripts/parse_od_daily_payments_golden.py`](./scripts/parse_od_daily_payments_golden.py)

→ [`scripts/README.md`](./scripts/README.md) for purpose, usage, and where snapshot values go.

Produces `golden/snapshots/od_daily_payments_{date}.snapshot.yml` (section subtotals + row counts
only). Use snapshot totals in compare SQL; keep the full CSV for row-level validation only.

Row-level: open the golden CSV alongside `compare/investigate_daily_collections_{date}.sql` or
`compare/compare_daily_payments_row_level.sql` (join staging on date, amount, provider).

---

## API surface (clinic)

`GET /kpi/daily-collections` reads mart columns **without transformation** — see
[`compare/compare_daily_collections_api.sql`](./compare/compare_daily_collections_api.sql)
(layer 4: API SQL vs mart vs OD golden).

| API field | Mart column | Shown in UI |
| --- | --- | --- |
| `net_collections_amount` | `net_collections_amount` | Hero amount (Practice Manager Home) |
| `patient_payment_amount` | `patient_payment_amount` | Subtitle |
| `insurance_payment_amount` | `insurance_payment_amount` | Subtitle |
| `payment_count` | `payment_count` | Subtitle |
| `has_data` | row exists | Empty state when false |

Live smoke test (all three golden dates):

```bash
python api/tests/kpi/verify_daily_collections.py
```

Full mart columns are validated in the warehouse before API fields are promoted to `validatedKpis.ts`.

---

## Comparison SQL index

| File | Purpose |
| --- | --- |
| `compare/compare_daily_payments_all_fields.sql` | All mart columns + OD section totals |
| `compare/compare_daily_collections.sql` | Mart vs OD net total |
| `compare/compare_daily_collections_staging.sql` | Mart vs staging reconstruction |
| `compare/compare_daily_collections_api.sql` | API-equivalent SQL vs mart vs OD (frontend path) |
| `compare/compare_daily_payments_row_level.sql` | Staging line items for golden row match |
| `compare/investigate_daily_collections_{date}.sql` | Date-specific drill-down |
