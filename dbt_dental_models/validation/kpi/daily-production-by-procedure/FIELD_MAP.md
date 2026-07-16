# Daily Production by Procedure — OD Report ↔ Warehouse Field Map

**Goal:** Same business logic as OpenDental Daily → Production by Procedure.

**Mart:** `marts.mart_daily_production_by_procedure`  
**Grain:** one row per `production_date` × `procedure_code`

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

## Warehouse mapping

| OD measure | Warehouse source | Notes |
| --- | --- | --- |
| Report date | `production_date` = `date_complete` | OD groups by **date set complete**, not ProcDate |
| Total Fees (report) | `sum(total_fees)` on mart for date | Status = Complete (2) only |
| Total Fees (by code) | `total_fees` on mart row | Primary compare layer |
| Quantity | `procedure_quantity` | Complete (2) only |
| Average Fee | `average_fee` | total_fees / procedure_quantity |
| Category | `procedure_category` | definition ItemName via ProcCat |
| Description | `procedure_description` | from procedurecode |

### Procedure status (confirmed)

OD Production by Procedure includes **ProcStatus = 2 (Complete) only**.  
Do **not** include status 4 (Existing Prior).

**Do not use `fact_procedure` for this KPI** — that fact keys `date_id` off ProcDate and includes status 2/4. See TODO: Align `fact_procedure` date/status with OpenDental production KPIs.

---

## Golden parser

[`scripts/parse_od_daily_production_by_procedure_golden.py`](./scripts/parse_od_daily_production_by_procedure_golden.py)
→ [`scripts/README.md`](./scripts/README.md)

---

## Comparison SQL index

| File | Purpose |
| --- | --- |
| `compare/compare_daily_production_by_procedure_total.sql` | Report total vs mart + staging |
| `compare/compare_daily_production_by_procedure_staging.sql` | Staging vs mart (DateComplete + status 2) |
| `compare/compare_daily_production_by_procedure_by_code.sql` | Code-level vs golden snapshot |
| `compare/compare_daily_production_api.sql` | API day rollup vs OD golden |
| `compare/investigate_daily_production_by_procedure_{date}.sql` | Drill-down |
