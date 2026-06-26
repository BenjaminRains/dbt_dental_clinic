# Daily Production by Procedure — OD Report ↔ Warehouse Field Map

**Goal:** Same business logic as OpenDental Daily → Production by Procedure.

**Mart (candidate):** Aggregate `marts.fact_procedure` by `procedure_date` + procedure code (dedicated
`mart_daily_production_by_procedure` TBD if needed for API).

**OD report:** Reports → Standard → Daily → Production by Procedure  
**Golden files:** `golden/od_daily_production_by_procedure_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv`

---

## OD report structure

First line: `Date: MM/DD/YYYY`  
Header: `Category, Code, Description, Quantity, Average Fee, Total Fees`

| OD column | Meaning |
| --- | --- |
| Category | OD procedure category (e.g. Exams & Xrays, Cleanings) |
| Code | Procedure code (CDT or clinic product code) |
| Description | From procedure code table |
| Quantity | Count of procedure rows in report |
| Average Fee | Total Fees ÷ Quantity (OD calculated) |
| Total Fees | Sum of production fees for that code on the report date |

Final row (empty Category/Code): **report total** in Total Fees column.

---

## Warehouse mapping (draft — validate on golden date)

| OD measure | Warehouse source | Notes |
| --- | --- | --- |
| Report date | `date_complete` on `stg_opendental__procedurelog` | OD groups by **date set complete**, not ProcDate |
| Total Fees (report) | Sum of `procedure_fee` where `date_complete` = report date | Status = Complete (2) only |
| Total Fees (by code) | Group by `stg_opendental__procedurecode.procedure_code` on `date_complete` | Primary compare layer |
| Quantity | Count of procedure rows per code | Complete (2) only |
| Category | `stg_opendental__definition` / procedure category | Map in compare Block D |

### Procedure status (confirmed)

OD Production by Procedure includes **ProcStatus = 2 (Complete) only**.  
Do **not** include status 4 (Existing Prior) — it inflates hygiene counts vs OD.

`fact_procedure` currently uses `procedure_date` + status `(2, 4)` — mart compare will diverge
until aligned to `date_complete` + status 2.

---

## Golden parser

[`scripts/parse_od_daily_production_by_procedure_golden.py`](./scripts/parse_od_daily_production_by_procedure_golden.py)
→ [`scripts/README.md`](./scripts/README.md)

---

## Comparison SQL index

| File | Purpose |
| --- | --- |
| `compare/compare_daily_production_by_procedure_total.sql` | Report total vs warehouse aggregate |
| `compare/compare_daily_production_by_procedure_staging.sql` | Staging reconstruction vs mart |
| `compare/investigate_daily_production_by_procedure_{date}.sql` | Pay-type / code drill-down |
| `compare/compare_daily_production_by_procedure_by_code.sql` | Row-level code totals vs golden snapshot |
