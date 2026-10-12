# Validation Report: Aging of A/R vs OpenDental

| Field | Value |
| --- | --- |
| **Report ID** | `kpi-aging-of-a-r-001` |
| **Report date** | 2026-07-16 |
| **Workflow** | Scaffold + same-day mart rebuild + source-proxy golden; **UI CSV pending** |
| **Registry status** | `delta_documented` |
| **KPI name** | AR total (Aging of A/R) |
| **Subject model** | `marts.mart_ar_summary` |
| **Primary measure** | `SUM(total_balance)` / API `total_ar_outstanding` |
| **Reference report** | OpenDental → Reports → Standard → Monthly → Aging of A/R |
| **Validator** | Analytics / dbt validation workflow (clinic secure site) |

---

## 1. Executive summary

We scaffolded the Aging of A/R validation folder, rebuilt `mart_ar_summary` for
**2026-07-16**, and compared practice total AR to a **live MySQL BalTotal source proxy**
(UI Aging CSV not yet exported).

| Layer | Result |
| --- | --- |
| Mart ↔ `dim_patient` (staging reconstruction) | **PASS** (exact $320,427.04) |
| Mart ↔ live OD `patient.BalTotal` (PatStatus 0,2) | **FAIL** — delta **$79,561.82** (33%) |
| API ↔ mart (`/ar/kpi-summary`, `/ar/aging-summary`) | Smoke vs mart total (layer 4) |

**Root cause:** local analytics `raw.patient` / warehouse BalTotal (**$326,933.40**) does not
match live OpenDental BalTotal (**$240,865.22**). Mart correctly aggregates warehouse
`dim_patient`; it cannot match today's OD report until patient ETL is refreshed (then rebuild
mart + re-compare). This is **not** a mart formula bug.

**Recommendation:** Registry `delta_documented`. After ETL patient refresh + optional UI CSV,
re-run total compare toward `within_tolerance` (±$50 / ±0.5%).

---

## 2. Same-day snapshot (2026-07-16)

| Source | Total AR | Patients |
| --- | ---: | ---: |
| Live MySQL BalTotal > 0, PatStatus ∈ {0,2} | $240,865.22 | 673 |
| Analytics `raw.patient` BalTotal > 0 | $326,933.40 | 903 |
| `mart_ar_summary` / `dim_patient` (Patient/Inactive) | $320,427.04 | 884 |

Buckets (mart only; OD bucket columns pending UI export):

| Bucket | Mart sum |
| --- | ---: |
| 0–30 / Current | $38,827.58 |
| 31–60 | $30,891.87 |
| 61–90 | $17,973.05 |
| 90+ | $232,734.54 |

Findings: [findings/2026-07-16.md](./findings/2026-07-16.md)  
Source-proxy snapshot: [golden/snapshots/od_aging_of_a_r_07162026_source_proxy.snapshot.yml](./golden/snapshots/od_aging_of_a_r_07162026_source_proxy.snapshot.yml)

---

## 3. Business rules

Documented in [FIELD_MAP.md](./FIELD_MAP.md):

- Primary = practice sum of outstanding balances (`BalTotal` / `total_balance`)
- Same-day `snapshot_date` only (mart rebuilds as of today)
- Buckets secondary (scaled from `int_ar_balance`)
- DSO / collection-rate / priority-queue **out of scope**

---

## 4. API endpoints in scope

| Endpoint | Role |
| --- | --- |
| `GET /ar/snapshot-dates` | Latest snapshot helper |
| `GET /ar/kpi-summary` | `total_ar_outstanding` |
| `GET /ar/aging-summary` | Bucket amounts |

Smoke: `python api/tests/kpi/verify_aging_ar.py --expected-total 320427.04`

---

## 5. Out of scope

| Item | Notes |
| --- | --- |
| Historical as-of goldens | Mart has no historical aging store |
| Simplified DSO | Registry known_delta |
| Insurance Aging / Custom Aging | Separate registry rows |
| UI validated chip | Defer until total `within_tolerance` |

---

## 6. Sign-off checklist

- [x] Folder scaffold (`README`, `FIELD_MAP`, compare SQL, gitignores)
- [x] Same-day `mart_ar_summary` rebuild; totals recorded
- [x] Source-proxy golden + findings (UI CSV still recommended)
- [x] Staging compare PASS; OD total FAIL documented
- [x] Registry → `delta_documented`
- [ ] UI Aging of A/R CSV + option screenshot
- [ ] ETL refresh → re-compare → `within_tolerance`
