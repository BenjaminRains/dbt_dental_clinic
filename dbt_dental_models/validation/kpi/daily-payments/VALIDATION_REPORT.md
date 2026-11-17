# Validation Report: Daily Net Collections vs OpenDental Daily Payments

| Field | Value |
| --- | --- |
| **Report ID** | `kpi-daily-payments-001` |
| **Report date** | 2026-06-26 |
| **Workflow** | **Complete** — all four validation layers signed off |
| **Registry status** | `within_tolerance` |
| **KPI name** | Daily net collections |
| **Subject model** | `marts.mart_daily_payments` |
| **Primary measure** | `net_collections_amount` |
| **Reference report** | OpenDental → Reports → Standard → Daily → Payments |
| **Validator** | Analytics / dbt validation workflow (clinic secure site) |

---

## 1. Executive summary

We compared `mart_daily_payments` to OpenDental Daily Payments exports on three clinic dates
(a weekday with insurance, a second weekday after the mart rebuild, and a patient-only Saturday).
After the 2026-06-24 mart rebuild and an ETL sync, the net, the patient subtotal, the insurance
subtotal, and the exported section totals match to the cent.

That is output agreement on those dates. [FIELD_MAP.md](./FIELD_MAP.md) is the rule set the mart
is written to follow. These compares sum the report's Amount column. They do not read the
OpenDental report query, so they do not show that the mart SQL and that query are the same rules.
A section that totals $0.00 matches whether or not the mart filters those rows out. Income Transfer
on 2025-10-07 is that case. Row identity is out of scope: two different rows inside one section can
still sum to the section total.

**Conclusion:** All three golden dates **PASS** (exact). The clinic API reads one mart row and does
no further math, so the practice-manager card shows that number.

**Recommendation:** Daily net collections is registered in
`frontend/src/config/validatedKpis.ts`. Re-run golden compare after material
changes to `mart_daily_payments.sql`, staging payment models, or ETL scope.

---

## 2. Objective

Confirm that `net_collections_amount` equals the OpenDental Daily Payments net on golden dates,
at section grain where the export has sections.

The field map states the rules the mart is written to follow:

- Which payments and claim checks belong on a calendar date
- How patient vs insurance sections are grouped
- How an Income Transfer section that totals $0.00 is treated
- Treatment of zero-amount payment headers

The golden compares test the dollars those rules produce on the exported dates. Tolerance (±$10 or
±0.5%) is the pass bar for that dollar check. See [KPI validation README](../README.md#tolerance).

---

## 3. Scope

### In scope

| Layer | Artifact | Role |
| --- | --- | --- |
| **Reference** | OD Daily Payments CSV export | Golden source of truth |
| **dbt mart** | `models/marts/mart_daily_payments.sql` | Business API logic under test |
| **dbt staging** | `stg_opendental__payment`, `stg_opendental__claimpayment` | Upstream reconstruction |
| **Measure** | `net_collections_amount`, `patient_payment_amount`, `insurance_payment_amount` | Primary KPI fields |
| **OD field** | Section subtotals and net total derived from **Amount** column | Benchmark |

### Out of scope (documented, not failing)

| Item | Notes |
| --- | --- |
| Row-level Amount vs paysplit grain | OD detail rows are provider splits; mart counts payment/claim **headers**. Not part of this sign-off. A PASS does not mean each report line was matched. Drill-down: `compare/compare_daily_payments_row_level.sql`. |
| API integration test | API performs `SELECT` from mart; validated by inheritance. No separate Pydantic business logic. |
| Same-day ETL freshness | Late-posted payments may lag until ETL runs. Documented on 2026-06-24. |
| `int_insurance_payment_allocated` | Uses `insurance_finalized_date`, not `check_date` — not used for this KPI. |

### Downstream consumers

| Consumer | Fields exposed | Logic source |
| --- | --- | --- |
| `GET /kpi/daily-collections` | `net_collections_amount`, `patient_payment_amount`, `insurance_payment_amount`, `payment_count` | Pass-through from `mart_daily_payments` |
| Frontend `validatedKpis.ts` | Registry metadata for daily net collections | Points to validated mart field |

---

## 4. Business rules (written spec)

[FIELD_MAP.md](./FIELD_MAP.md) is the spec the mart is written to follow. The golden dates check
the totals that spec produces. They do not, by themselves, show that each rule below is the rule
in the OpenDental report query.

### 4.1 Date assignment

| Collection type | OpenDental | Warehouse source | Date column |
| --- | --- | --- | --- |
| Patient / practice payments | PayDate | `stg_opendental__payment` | `payment_date` |
| Insurance claim checks | CheckDate | `stg_opendental__claimpayment` | `check_date` |

### 4.2 Net collections formula

```
net_collections_amount = patient_payment_amount + insurance_payment_amount
```

Where:

- **Patient:** sum of non-zero `payment_amount` on `payment_date`
- **Insurance:** sum of `check_amount` on `check_date` (from claim payment table, not patient PayType 261/303/464)
- **Income Transfer:** the OpenDental section must total $0.00. The compare drops it from the report net. The mart sums every non-zero payment header. A $0.00 section matches either way, so these dates do not show that the mart excludes those rows.
- **Zero-amount headers:** the mart count uses `payment_amount <> 0`. A zero does not change the dollar total whether or not the row is filtered. The OpenDental figure on these dates is the amount.

### 4.3 Patient PayType mapping (clinic)

Standard patient types, used to split `other_payment_amount` out of the patient sum:
69, 70, 71, 391, 412, 417, 574, 634, 676 (Check, Cash, Credit Card, Care Credit, Cherry, etc.).
`net_collections_amount` still includes every non-zero patient header. These golden dates do not
contain an unmapped pay type, so the list is not what the totals tested.

### 4.4 OD report parameters (all golden dates)

- **Menu path:** Reports → Standard → Daily → Payments
- **Providers:** All
- **Include Unearned:** Yes
- **Export:** CSV to `golden/od_daily_payments_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv`

---

## 5. Methodology

### 5.1 Workflow

1. Document rules in `FIELD_MAP.md` and registry row in [KPI_VALIDATION_REGISTRY.md](../KPI_VALIDATION_REGISTRY.md).
2. Export OD golden CSV on clinic secure site (PHI retained locally, gitignored).
3. Parse section totals: `python scripts/parse_od_daily_payments_golden.py golden/od_daily_payments_*.csv`
   (see [scripts/README.md](./scripts/README.md) — PHI-free snapshot for compare SQL)
4. Run warehouse compare SQL in DBeaver (`opendental_analytics`, schema `marts`).
5. Record results in `findings/{date}.md`.
6. Promote to `within_tolerance` when the golden totals match. Record which rules those dates do not isolate.

### 5.2 Compare layers

| Layer | SQL | Purpose |
| --- | --- | --- |
| Staging reconstruction | `compare/compare_daily_collections_staging.sql` | Mart = staging (warehouse fidelity) |
| OD total | `compare/compare_daily_collections.sql` | Mart vs OD net collections |
| All fields + sections | `compare/compare_daily_payments_all_fields.sql` | Blocks A–E (internal, staging, OD, sections, grain) |
| Date drill-down | `compare/investigate_daily_collections_{date}.sql` | PayType and mart breakdown |

### 5.3 Pass criteria

| Result | Condition |
| --- | --- |
| **PASS** | `abs(mart − od) ≤ $10` **or** percent diff `< 0.5%` |
| **WARN** | Within 2× percent tolerance (`< 1.0%`) |
| **FAIL** | Outside 2× tolerance — investigate logic before accepting |

Both OD and mart zero → PASS.

---

## 6. Test cases and results

### 6.1 Summary

| Golden date | Day type | OD net | Mart net | Difference | Patient | Insurance | Status | Findings |
| --- | --- | ---: | ---: | ---: | --- | --- | --- | --- |
| 2026-06-24 | Weekday (Tue) | $11,197.40 | $11,197.40 | $0.00 | $7,219.60 | $3,977.80 | PASS | [2026-06-24.md](./findings/2026-06-24.md) |
| 2025-10-07 | Weekday (Tue) | $21,747.30 | $21,747.30 | $0.00 | $8,577.82 | $13,169.48 | PASS (exact) | [2025-10-07.md](./findings/2025-10-07.md) |
| 2025-11-08 | Weekend (Sat) | $3,791.65 | $3,791.65 | $0.00 | $3,791.65 | $0.00 | PASS (exact) | [2025-11-08.md](./findings/2025-11-08.md) |

**Golden files:** `golden/od_daily_payments_{MMDDYYYY}_{MMDDYYYY}.csv`  
**Snapshots:** `golden/snapshots/od_daily_payments_{MMDDYYYY}_{MMDDYYYY}.snapshot.yml`

### 6.2 Test case: 2026-06-24 (initial validation + mart fix)

**Purpose:** First OD golden; discovered and fixed mart logic gaps.

**OD sections (representative):** Check, Cash, Credit Card, Cherry (patient); Metlife EFT
(insurance via claimpayment).

**Issues resolved:**

1. **ETL lag (−$156.30):** Two patient payments posted in OD before analytics sync. Resolved after
   targeted ETL on `payment` / `paysplit`.
2. **Mart logic:** Prior mart omitted insurance claim payments and mis-filtered patient PayTypes.
   Rebuilt `mart_daily_payments` to union patient staging + claim payment staging.

**Outcome:** Staging and mart match the report sections after the fix. The missing claim-payment path and the patient-header grain were necessary for this date. Rules a $0.00 section cannot falsify are unchanged by that result. Status `within_tolerance`.

### 6.3 Test case: 2025-10-07 (mixed activity)

**Purpose:** Weekday with **both** patient and insurance sections; multiple PayTypes.

**OD sections validated:**

| Section | OD amount | Warehouse |
| --- | ---: | --- |
| Check (insurance) | $13,169.48 | claimpayment |
| Check (patient) | $265.05 | payment type 69 |
| Cash | $180.00 | payment type 70 |
| Credit Card | $7,843.37 | payment type 71 |
| Care Credit | $289.40 | payment type 391 |
| Income Transfer | $0.00 | Section totals $0.00. The compare omits it from the report net. The mart has no separate exclusion, so this row does not show one. |
| **Net** | **$21,747.30** | `net_collections_amount` |

**Outcome:** Exact match. Status `within_tolerance`.

### 6.4 Test case: 2025-11-08 (patient-only weekend)

**Purpose:** Saturday with **no insurance** activity; single PayType section.

**OD vs mart (formal compare query):**

| Field | OD | Mart | Diff | Status |
| --- | ---: | ---: | ---: | --- |
| Net collections | $3,791.65 | $3,791.65 | $0.00 | PASS |
| Patient sections | $3,791.65 | $3,791.65 | $0.00 | PASS |
| Insurance sections | $0.00 | $0.00 | $0.00 | PASS |

**Section:** Credit Card $3,791.65 (7 payment headers; 11 OD provider-split detail rows — expected
grain difference).

**Outcome:** Exact match. Status `within_tolerance`.

---

## 7. Defects found and corrective actions

| ID | Date found | Issue | Root cause | Resolution | Retest |
| --- | --- | --- | --- | --- | --- |
| DP-001 | 2026-06-24 | Mart −$3,977.80 vs OD | Mart used `fact_payment` only; no `claimpayment` path | Rebuilt `mart_daily_payments.sql` with insurance from staging claim payments | PASS |
| DP-002 | 2026-06-24 | Mart patient under-count | Wrong PayType IDs; unallocated paysplits dropped | Mart reads staging payment headers directly | PASS |
| DP-003 | 2026-06-24 | Staging −$156.30 vs OD | ETL lag on same-day payments | Run ETL before validate; documented operational note | PASS |

No open defects as of report date.

---

## 8. API and presentation layer

The clinic API does **not** implement independent collection logic. The frontend displays API
fields directly — validating the app is a **fourth layer** after mart vs OD.

### 8.1 What the frontend shows

**Page:** Practice Manager Home (`frontend/src/pages/homes/PracticeManagerHome.tsx`)

| UI element | API field | Mart column |
| --- | --- | --- |
| Hero dollar amount | `net_collections_amount` | `net_collections_amount` |
| Patient / insurance subtitle | `patient_payment_amount`, `insurance_payment_amount` | same |
| Payment count | `payment_count` | `payment_count` |
| "Latest" date picker | `GET /kpi/daily-collections/latest-date` | `MAX(payment_date) WHERE payment_count > 0` |

Custom date picker passes `payment_date=YYYY-MM-DD` to the API (Central calendar dates).

### 8.2 API SQL (must match mart)

```python
# api/services/kpi_service.py — get_daily_collections_kpi()
SELECT payment_date, net_collections_amount, patient_payment_amount,
       insurance_payment_amount, payment_count
FROM marts.mart_daily_payments
WHERE payment_date = :payment_date
```

If no row: API returns zeros and `has_data: false` (UI shows empty-state message).

### 8.3 Layer-4 validation (SQL)

Run [`compare/compare_daily_collections_api.sql`](./compare/compare_daily_collections_api.sql)
in DBeaver. It runs the **same SELECT** as the API for each golden date and compares to OD totals.
Every row should be **PASS**.

### 8.4 Layer-4 validation (live API / UI)

1. Publish analytics to the environment the API uses (`mdc publish analytics --env clinic`).
2. Run smoke script (API must be up, `API_KEY` set):

   ```bash
   python api/tests/kpi/verify_daily_collections.py
   ```

3. Open Practice Manager Home → pick the golden date (Custom) → confirm hero amount matches
   findings (e.g. **$3,791.65** on 2025-11-08).

**Recorded API smoke (local, 2026-06-26):** all three golden dates **3/3 PASS** via
`verify_daily_collections.py`:

| Date | Expected net | API `net_collections_amount` | Patient | Insurance | Count | Status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2026-06-24 | $11,197.40 | $11,197.40 | $7,219.60 | $3,977.80 | 39 | PASS |
| 2025-10-07 | $21,747.30 | $21,747.30 | $8,577.82 | $13,169.48 | 59 | PASS |
| 2025-11-08 | $3,791.65 | $3,791.65 | $3,791.65 | $0.00 | 7 | PASS |

**Sign-off:** Mart vs OD shows the totals match on the golden dates. The service query is one
`SELECT` of that mart row. The live smoke test shows the practice manager receives that row.

---

## 9. Limitations and ongoing monitoring

| Risk | Mitigation |
| --- | --- |
| New PayTypes not in patient type list | Monitor `other_payment_amount`; update mart ID list if OD adds sections |
| ETL delay on current day | Do not claim same-day validation until ETL completes |
| OD report filter drift | Document filters in golden manifest; screenshot optional |
| Logic change in dbt | Re-run golden compare on at least one weekday + one insurance day |
| Rules these dates do not isolate | Income-transfer exclusion, zero-dollar header exclusion, unearned turned off, and an unmapped pay type are in the field map. A $0.00 section matches either way. Row-level compare is out of scope. |

**Suggested re-validation triggers:** change to `mart_daily_payments.sql`, payment/claimpayment
staging, or OD report version upgrade.

---

## 10. Conclusion and sign-off

| Criterion | Met? |
| --- | --- |
| Business rules documented (`FIELD_MAP.md`) | Yes |
| Registry row with tolerance and known deltas | Yes |
| ≥1 weekday with insurance validated | Yes (2025-10-07, 2026-06-24) |
| ≥1 alternate scenario (weekend / patient-only) | Yes (2025-11-08) |
| Mart vs OD net collections within tolerance | Yes (exact on all three dates) |
| Section totals match on the exported days | Yes (2025-10-07, 2026-06-24; 2025-11-08 is one patient section) |
| Mart vs staging reconciliation | Yes |
| API SQL matches mart on golden dates | Yes — `compare/compare_daily_collections_api.sql` |
| Live API smoke on golden dates | Yes — **3/3 PASS** (2026-06-26 local, `verify_daily_collections.py`) |
| OpenDental report query compared to mart SQL | No. The sign-off sums the exported Amount column. |
| Each field-map rule isolated by a date that would fail if the rule differed | Partial. 2026-06-24 required the claim-payment path and patient-header grain. Income transfer, zero-dollar counts, unearned off, and unmapped pay types were not isolated. |
| Findings recorded per golden date | Yes |

**Final status:** **`within_tolerance` — totals workflow complete.** On 2026-06-24, 2025-10-07, and
2025-11-08, `mart_daily_payments.net_collections_amount` equals the OpenDental Daily Payments net,
and the API returns that row to Practice Manager Home. The field map remains the written spec.
These dates do not show that the mart SQL and the OpenDental report query are the same rules.

---

## 11. References

| Document | Path |
| --- | --- |
| KPI validation README | [../README.md](../README.md) |
| Validation registry | [../KPI_VALIDATION_REGISTRY.md](../KPI_VALIDATION_REGISTRY.md) |
| Field map (logic spec) | [FIELD_MAP.md](./FIELD_MAP.md) |
| dbt mart model | `models/marts/mart_daily_payments.sql` |
| Mart column docs | `models/marts/_mart_daily_payments.yml` |
| Compare SQL | `compare/` |
| API layer-4 compare | `compare/compare_daily_collections_api.sql` |
| API smoke script | `api/tests/kpi/verify_daily_collections.py` |
| Frontend validated KPIs | `frontend/src/config/validatedKpis.ts` |
| API service | `api/services/kpi_service.py` |

---

## Appendix A: Golden export naming

```
od_daily_payments_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv
```

Single-day example: `od_daily_payments_11082025_11082025.csv` for 2025-11-08.

Parser:

```bash
cd dbt_dental_models/validation/kpi/daily-payments
python scripts/parse_od_daily_payments_golden.py golden/od_daily_payments_11082025_11082025.csv
```
