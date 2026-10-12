# Production and Income — OD Report ↔ Warehouse Field Map

**Goal:** Same business logic as OpenDental's Production and Income report. This map is the spec.
Compare SQL checks it against the 2025 and 2026 monthly grids.

**OD report:** Reports → Standard → Production and Income  
**Grain:** one row per calendar month (This Year / custom year). Provider detail is a later golden.  
**Warehouse today:** no single mart implements this report. Reconstruction is from `raw.*`.
Existing marts are listed under known deltas — they answer different OD reports.

Proven against the clinic grids on 2026-10-08 (full year 2025, and 2026 through the open October).

---

## Report parameters for these goldens

| Parameter | Value on the two grids |
| --- | --- |
| Providers | All |
| Clinics | All (single clinic) |
| Production date | Procedure date (`ProcDate`), not date completed |
| Write-off date | Claim procedure date (`DateCP`) |
| Adjustment date | `AdjDate` |
| Patient / unearned date | Paysplit `DatePay` |
| Insurance date | `claimproc.DateCP` |

Date completed is the rule for **Daily → Production by Procedure**
(`mart_daily_production_by_procedure`). It is not the rule for this report.

---

## Column rules

Totals use signed amounts. Tot Prod and Total Income are identities, not separate queries.

| OD column | Rule | Sign |
| --- | --- | --- |
| Production | `procedurelog` where `ProcStatus = 2` (Complete). Sum `ProcFee` by `ProcDate`. Unit quantity does not change the clinic total. | Fee as stored |
| Adjustments | Sum `adjustment.AdjAmt` by `AdjDate`. | As stored (credits negative) |
| Write-off | Sum `claimproc.WriteOff` by `DateCP`, any status. | Report prints **−WriteOff**. A reversal stored as negative prints positive. |
| Tot Prod | Production + Adjustments + displayed Write-off | Identity |
| Pt Income | Sum `paysplit.SplitAmt` by `DatePay` where `UnearnedType = 0` | As stored |
| Unearned Pt Income | Sum `SplitAmt` by `DatePay` where `UnearnedType <> 0` and the unearned definition `ItemValue` is not `X` | As stored |
| Ins Income | Sum `claimproc.InsPayAmt` by `DateCP` where `Status` is Received (1) or Supplemental (4) | As stored |
| Total Income | Pt Income + Unearned Pt Income + Ins Income | Identity |

### Unearned types (definition category PaySplit unearned)

`ItemValue = X` means OpenDental leaves that type off this report.

| DefNum | Name | ItemValue | On this report |
| --- | --- | --- | --- |
| 288 | Prepayment | blank | Yes — this is the Unearned column |
| 439 | Treat Plan Prepayment | `X` | No |
| 598 | Payment Plan Prepay | `X` | No |
| 641 | Refund | blank, hidden | No rows in 2025–2026 |

2026 has no type 439 splits, so "all unearned" and "Prepayment only" are the same number.
2025 January–May has negative type 439 splits. That is the whole gap vs `mart_daily_payments`.

---

## What existing models do

| Model | Relation to this report |
| --- | --- |
| `staging.stg_opendental__procedurelog` | Production matches when summed by `procedure_date` and `procedure_status = 2`. |
| `mart_daily_production_by_procedure` | Different report. Uses `date_complete`. Do not expect it to match Production. |
| `raw.adjustment` | Adjustments match by `AdjDate`. |
| `staging.stg_opendental__adjustment` | 2025 matches. 2026 is behind raw: the incremental filter keeps rows only when `AdjDate` is later than the last load time, so edits to older dates never refresh. |
| `staging.stg_opendental__paysplit` | Pt Income and included unearned match. The model does not apply the `ItemValue = X` exclusion; do that at read time. |
| `staging.stg_opendental__claimproc` | Drops rows whose procedure date is before 2023-01-01. 2025 insurance is short by those payments. 2026 had none. |
| `mart_daily_payments` | `insurance_payment_amount` matches Ins Income. `patient_payment_amount` is Pt Income + **all** unearned, including `ItemValue = X`. `net_collections_amount` matches Total Income only in months with no excluded unearned. `income_amount` drops refunds and does not match. |
| Tot Prod | No mart. |

---

## Tolerance

Source reconstruction vs the monthly golden: **±$0.01** (penny rounding). A larger gap is a rule miss, not noise.

Mart gaps above are known deltas, not tolerance misses. They stay visible on the compare query until a model implements this field map.
