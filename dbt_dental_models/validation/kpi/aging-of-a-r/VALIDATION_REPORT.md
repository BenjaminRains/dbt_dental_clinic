# Validation Report: Aging of A/R vs OpenDental

| Field | Value |
| --- | --- |
| **Report ID** | `kpi-aging-of-a-r-001` |
| **Report date** | 2026-10-12 |
| **Workflow** | Same-day `patient` full refresh, mart rebuild, live `BalTotal` source proxy |
| **Registry status** | `within_tolerance` (practice total) |
| **KPI name** | AR total (Aging of A/R) |
| **Subject model** | `marts.mart_ar_summary` |
| **Primary measure** | `SUM(total_balance)` / API `total_ar_outstanding` |
| **Reference report** | OpenDental → Reports → Standard → Monthly → Aging of A/R |
| **Validator** | Analytics / dbt validation workflow (clinic secure site) |

---

## 1. Executive summary

On 2026-10-12 the local warehouse `patient` table was full-refreshed from live OpenDental and `mart_ar_summary` was rebuilt the same day. The practice total matches the locked source proxy exactly.

| Layer | Result |
| --- | --- |
| Live MySQL `patient.BalTotal` (PatStatus 0 and 2, `BalTotal > 0`) | **$282,624.23** / 736 |
| Analytics `raw.patient` (same filter) | **$282,624.23** / 736 |
| `mart_ar_summary` / `dim_patient` | **$282,624.23** / 736 — **PASS** |
| Aging buckets vs stored `Bal_0_30` … `BalOver90` | Secondary **FAIL** (different split, same total) |
| `insurance_estimate` vs `InsEst` | Secondary. Not part of the total. |
| API smoke | Not re-run this close |

The 2026-07-16 fail was warehouse lag ($320,427.04 mart vs $240,865.22 live). That gap is closed. A printed Aging of A/R CSV is still not in `golden/`. This sign-off is the stored guarantor balance OpenDental last wrote, which is what the account module shows. The report itself recalculates when printed.

---

## 2. Same-day snapshot (2026-10-12)

Source `DateTStamp` maximum: **2026-10-12 17:47:16**. Mart `snapshot_date`: **2026-10-12**.

| Source | Total AR | Accounts |
| --- | ---: | ---: |
| Live MySQL, Patient + Inactive, `BalTotal > 0` | $282,624.23 | 736 |
| `raw.patient` / `mart_ar_summary` | $282,624.23 | 736 |
| Patient status only (Inactive excluded) | $243,974.72 | — |
| Patient + Inactive, credits included | $187,169.37 | — |

The mart includes Inactive guarantors and drops credit balances. That is the locked population in [FIELD_MAP.md](./FIELD_MAP.md).

Stored OD buckets on the locked population foot to $282,624.23. The mart scales procedure-age buckets onto the same total, so the split differs:

| Bucket | Stored OD | Mart |
| --- | ---: | ---: |
| 0–30 | $93,531.82 | $49,945.21 |
| 31–60 | $38,326.36 | $25,484.84 |
| 61–90 | $28,388.60 | $15,742.24 |
| > 90 | $122,377.45 | $191,451.93 |

Bucket drift does not change the total sign-off.

---

## 3. Business rules

Documented in [FIELD_MAP.md](./FIELD_MAP.md):

- Primary = sum of positive guarantor `BalTotal` for Patient and Inactive
- Same-day `snapshot_date` only
- Buckets secondary
- DSO, collection rate, and the priority queue are out of scope

---

## 4. API endpoints in scope

| Endpoint | Role |
| --- | --- |
| `GET /ar/snapshot-dates` | Latest snapshot helper |
| `GET /ar/kpi-summary` | `total_ar_outstanding` |
| `GET /ar/aging-summary` | Bucket amounts |

Smoke when the API is up:

```bash
python api/tests/kpi/verify_aging_ar.py --expected-total 282624.23
```

---

## 5. Out of scope

| Item | Notes |
| --- | --- |
| Historical as-of goldens | Mart has no historical aging store |
| Simplified DSO | Still a known difference in exposures |
| Insurance Aging / Custom Aging | Separate reports |
| Practice Manager Home allowlist | Aging is not a home-card KPI |

---

## 6. Sign-off checklist

- [x] Folder scaffold (`README`, `FIELD_MAP`, compare SQL)
- [x] `patient` full refresh and same-day `mart_ar_summary` rebuild
- [x] Live MySQL, `raw`, and mart total agree ($282,624.23 / 736)
- [x] Staging compare: mart total equals `dim_patient`
- [x] Registry → `within_tolerance`
- [ ] Printed Aging of A/R CSV, if the on-screen grand total should replace stored `BalTotal`
