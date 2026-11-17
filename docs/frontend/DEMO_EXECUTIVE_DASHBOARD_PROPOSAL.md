# Demo executive dashboard: validated KPIs only

> Status: **Proposed — Phase 0 in progress** on `feature/executive-dashboard-phase-0`
> Last updated: **2026-11-17**
> Audience: Platform maintainer, practice manager (clinic sign-off), portfolio readers
> Related: [FRONTEND_EVOLUTION_PROPOSAL.md](./FRONTEND_EVOLUTION_PROPOSAL.md), [KPI validation README](../../dbt_dental_models/validation/kpi/README.md), [KPI_VALIDATION_REGISTRY.md](../../dbt_dental_models/validation/kpi/KPI_VALIDATION_REGISTRY.md), [OPENDENTAL_REPORTS_INDEX.md](../../dbt_dental_models/validation/kpi/OPENDENTAL_REPORTS_INDEX.md)

---

## 1. Decision

The executive dashboard shows a measure only after that measure is in the validated-KPI allowlist. The allowlist gains an entry only when the registry status is `within_tolerance`: golden-date totals match an OpenDental standard report inside the tolerance written on that registry row.

Two measures qualify today:

| Allowlist id | Measure | Mart | OpenDental report | Signed off |
| --- | --- | --- | --- | --- |
| `payments` | Daily net collections | `mart_daily_payments.net_collections_amount` | Reports → Standard → Daily → Payments | 2026-06-26 |
| `daily-production-by-procedure` | Daily production by procedure | `mart_daily_production_by_procedure` sum of `total_fees` | Reports → Standard → Daily → Production by Procedure | 2026-07-16 |

Patients, recovery lost, recovery potential, recovery rate, and active providers stay off that page until each one has the same sign-off. Recovery potential and recovery rate are model estimates. They stay on the revenue page and do not enter the allowlist.

The public demo (`dbtdentalclinic.com`, `@mdc/portfolio`) and the clinic portal (`clinic.dbtdentalclinic.com`, `@mdc/clinic`) both mount the same page: `frontend/packages/analytics-ui/src/pages/Dashboard.tsx` at `/dashboard`. Phase 0 changes both hosts. Clinic Practice Manager Home already follows this rule. The executive route should match it.

Demo dollars come from the synthetic warehouse. A clinic golden-date sign-off proves the measure's rules, and the demo page shows that same measure. The synthetic banner stays. The card cites the OpenDental report and the sign-off date. It does not claim the synthetic dollars were exported from OpenDental.

---

## 2. What `/dashboard` does today

`GET /reports/dashboard/kpis` (`api/routers/reports.py`) returns two blocks:

- Revenue: `sum(mart_revenue_lost.lost_revenue)` and `sum(estimated_recoverable_amount)`, filtered on `appointment_date`.
- Providers: `count(distinct provider_id)`, `sum(total_production)`, `sum(total_collections)`, and collections ÷ production, from `mart_provider_performance` filtered on `production_date`.

`Dashboard.tsx` renders those as Revenue Lost, Recovery Potential, Recovery Rate, and Active Providers, plus Total Production, Total Collection, a recovery-rate repeat, a revenue trend chart, and Top 10 Patient Balances. Recovery rate is computed in the browser:

`(total_recovery_potential / total_revenue_lost) × 100`

Three trend chips are constants in the component (`-5.2%`, `+12.5%`, `+2.1%` versus last month). They are not query results.

The Patients nav item (`/patients`) is a paginated list from `GET /patients/`. It has no count, no status filter, and no OpenDental comparison.

`frontend/apps/clinic/src/config/validatedKpis.ts` is the clinic allowlist. The portfolio KPI Validation page keeps a hand-copied `SIGNED_OFF_KPIS` array and a comment that says not to import the clinic app. Those two lists are the thing Phase 0 unifies.

---

## 3. What each requested measure can become

### 3.1 Patients → Active patients

| Item | Choice |
| --- | --- |
| Card label | Active patients |
| OpenDental report | Reports → Standard → Lists → Active Patients |
| Folder | `dbt_dental_models/validation/kpi/active-patients/` |
| Warehouse measure | `count(*)` from `marts.dim_patient` where `patient_status = 'Patient'` |
| Source rule already in dbt | `int_patient_profile`: PatStatus `0` → `'Patient'`. `1` NonPatient, `2` Inactive, `3` Archived, `4` Deceased, `5` Deleted stay out of the count |
| Grain | One count as of the export. This is a list report, not a date window |
| Tolerance | Exact count. Any OD filter that removes rows (hidden clinics, guarantor-only, status checkboxes) goes in `known_deltas` after the first export, and the mart filter is updated to match |

New Patients (Lists → New Patients) is a different report and a different card. It is out of this proposal.

The first export is the field map. Run Active Patients once with the clinic's usual checkboxes, save the CSV under `active-patients/golden/`, and record every filter in `FIELD_MAP.md` before writing compare SQL. If the report's default is not "PatStatus = Patient", the mart predicate changes to the report. The card does not ship on a guessed status list.

### 3.2 Active providers

The exposure text and the API disagree.

| Source | Definition |
| --- | --- |
| `exposures.yml` and `KPIDefinitions.tsx` | Distinct providers with scheduled appointments in the selected range |
| `GET /reports/dashboard/kpis` | Distinct `provider_id` on `mart_provider_performance` in the production-date range |

Sign-off compares one of those to one OpenDental export. The production-by-procedure golden cannot settle it: that report's columns are Category, Code, Description, Quantity, Average Fee, Total Fees. There is no provider column.

Phase 2 starts by exporting **one shared date** (use an existing production golden date, `2026-06-10`) from both of these reports:

1. Reports → Standard → Daily → Procedures
2. Reports → Standard → Lists → Appointments

Whichever CSV has a provider column becomes the benchmark. Prefer **distinct providers with at least one completed procedure that day** (ProcStatus `2`, date set complete = report date) when the Procedures export lists a provider, because that date and status rule is already signed off on Production by Procedure. Use **distinct providers with an appointment that day** when only the Appointments export lists a provider, and change the API and the exposure to that definition in the same phase.

The card's subtitle states the definition that was signed off. `mart_provider_performance` is not the comparison subject until a field map shows it uses ProcStatus 2 and DateComplete. The compare subject is a small mart or a documented query with that grain: one row per provider per day, or a single distinct count for the date.

Tolerance: exact distinct count on each golden date. Three dates, same scenarios as payments: a weekday with a full schedule, a Saturday, and a hygiene-heavy day.

### 3.3 Recovery lost → one Broken Appointments measure

`mart_revenue_lost.lost_revenue` adds four constructions over windows we chose:

| Slice | Rule in `mart_revenue_lost.sql` | OpenDental report that could match a slice |
| --- | --- | --- |
| Missed appointment | No-show or broken, last 1 year, `scheduled_production_amount > 0` | Lists → Broken Appointments |
| Claim | Denied, Rejected, or Pending; billed > paid; last 1 year | None as a single lost-revenue total |
| Treatment plan delay | Active plan, idle > 90 days, plans dated in the last 2 years | None |
| Adjustment | Non-zero adjustment, last 1 year | Daily → Write-offs, and only for the write-off subset |

The executive card is the Broken Appointments slice alone. The other three stay on `/revenue` as warehouse analysis.

Phase 3 opens with one Broken Appointments export for a single day. The field map records the columns.

| If the CSV has… | Executive measure |
| --- | --- |
| A fee or production column that maps to `scheduled_production_amount` | Sum of that column for the day. Tolerance ±$10 or ±0.5%, same rule as payments |
| Appointment rows and no dollar column | Broken-appointment **count** for the day. Tolerance: exact count. Dollars stay off the executive page |

The composite `sum(lost_revenue)` does not get a registry row. A total with no OpenDental counterpart cannot reach `within_tolerance`.

Write-offs as their own KPI are already a folder under `validation/kpi/write-offs/` and are tracked with the rest of the report index. They are not an executive card in this proposal.

### 3.4 Recovery potential and recovery rate

These stay on `/revenue`, labeled **Model estimate**.

`estimated_recoverable_amount` is `lost_revenue` times a rate we assigned:

| `recovery_potential` | Rate |
| --- | --- |
| High | 0.8 |
| Medium | 0.5 |
| Low | 0.2 |
| None | 0 |

OpenDental has no report for that rate, so there is no golden total to match. Recovery rate is that estimate divided by lost revenue, calculated again in `Dashboard.tsx`. Phase 0 removes it from `/dashboard`. Phase 4 keeps it on the revenue page with the formula visible and removes it from the Executive Dashboard section of `KPIDefinitions.tsx`.

The cancellation band is also unstable. For broken appointments the model sets Medium when `extract(hours from appointment_datetime - current_timestamp) < 24`, and Low otherwise. For any appointment in the past that difference is negative, so historical broken appointments land in Medium, and the category changes if the row is rebuilt after the appointment time crosses the 24-hour line. Phase 4 stops using `current_timestamp`. No-show stays High. Broken stays one band until a stored notice interval is identified in OpenDental (appointment `DateTStamp` or a broken-time column — confirm in the source table before picking a column). The rebuilt figure is still an estimate. It becomes a stable estimate.

---

## 4. Phases

| Phase | Goal | Needs a clinic export? | Status |
| --- | --- | --- | --- |
| **0** | `/dashboard` renders the allowlist only | No | In progress |
| **1** | Active patients signed off and added to the allowlist | Yes — Active Patients list | Proposed |
| **2** | Active providers definition locked, then signed off | Yes — Procedures and Appointments for one shared date, then 2 more dates | Proposed |
| **3** | Broken Appointments slice signed off as count or dollars | Yes — one scout export, then 2 more dates | Proposed |
| **4** | Recovery potential and recovery rate labeled as estimates; classification stops depending on `current_timestamp` | No | Proposed |

Phases 1–3 are sequential because each one consumes clinic export time. Phase 4 can run beside them. A measure appears on `/dashboard` in the same change that sets its registry row to `within_tolerance`.

Promotion checklist, applied at the end of Phases 1, 2, and 3:

1. `{report}/VALIDATION_REPORT.md` written, registry status `within_tolerance`.
2. Entry added to the shared allowlist module (Phase 0).
3. API route is a pass-through of the signed-off query. Compare SQL shows API-equivalent SQL = mart = golden.
4. Card on `Dashboard.tsx` shows the value, the report name, the menu path, and the sign-off date.
5. Portfolio KPI Validation page picks the new entry up from the shared module.
6. `exposures.yml` executive dashboard description matches the field map.

---

## 5. Implementation plan

### Phase 0 — Allowlist page

Code only. Ship this before any new golden work so the demo stops presenting unsigned numbers as the executive view.

**Single allowlist**

Add `frontend/packages/analytics-ui/src/config/validatedKpis.ts` with the two current entries (same fields as `frontend/apps/clinic/src/config/validatedKpis.ts`: `id`, `name`, `model`, `field`, `odReport`, `odMenuPath`, `registryStatus`).

- `frontend/apps/clinic/src/config/validatedKpis.ts` re-exports that module.
- `frontend/apps/portfolio/src/pages/KpiValidation.tsx` imports it and deletes the hand-copied `SIGNED_OFF_KPIS` array. Links to each `VALIDATION_REPORT.md` stay beside the metadata, in the shared module, so the portfolio page and the clinic home cannot drift.

**Replace the executive page**

Rewrite `frontend/packages/analytics-ui/src/pages/Dashboard.tsx` so its body is the two signed-off KPIs for one Central report date. Lift the date control already in `frontend/apps/clinic/src/pages/homes/PracticeManagerHome.tsx` (Latest / Yesterday / Today / pick a date, shared date = the later of the two marts' latest dates) into `frontend/packages/analytics-ui/src/pages/ValidatedKpiPanel.tsx`. Practice Manager Home and `Dashboard.tsx` both render that panel.

Remove from `Dashboard.tsx`:

- Calls to `dashboardApi.getKPIs`, `revenueApi.getTrends`, and `patientApi.getTopBalances`.
- The previous-business-week summary, the four KPI cards, the trend chart, quick statistics, and top balances.
- The hardcoded `trend` props.

The page title stays **Executive Dashboard**. The subtitle names Daily Payments and Production by Procedure and states that the figures use the signed-off definitions. Portfolio layout already mounts `SyntheticDataBanner`.

**Landing link**

In `frontend/apps/portfolio/src/pages/Portfolio.tsx`, the dashboard-examples button points at `/dashboard` with the label "Validated executive dashboard". The section lead sentence states that this page shows measures reconciled to OpenDental, and that the other analytics routes are exploratory.

**Leave the exploratory routes in the nav**

`/revenue`, `/patients`, `/providers`, `/ar-aging`, and the rest stay reachable. Phase 0 does not delete them and does not add a banner to every page. Phase 4 adds the estimate label on `/revenue` only.

**Acceptance**

- Portfolio and clinic `/dashboard` show daily net collections and daily production by procedure for the selected date, including the production-by-code table the Practice Manager home already shows.
- The page contains no revenue-lost, recovery, provider-count, collection-rate, or patient-balance figure.
- No trend percentage is hardcoded.
- Changing the allowlist in the shared module changes the clinic home metadata and the portfolio KPI Validation page together.
- Existing API smoke for the two KPIs still passes (`api/tests/kpi/verify_daily_collections.py` and the production day-rollup compare).

### Phase 1 — Active patients

**Clinic export (once, before SQL)**

1. On the clinic machine: Reports → Standard → Lists → Active Patients → CSV.
2. Save as `dbt_dental_models/validation/kpi/active-patients/golden/od_active_patients_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv`. The two dates are the export date repeated. This report is a snapshot, not a range.
3. Screenshot the filter dialog into `active-patients/screenshots/` (gitignored with the rest of `screenshots/` and `golden/`).
4. Write every checkbox and the row count into `active-patients/FIELD_MAP.md`.

**Warehouse**

1. Registry row: status `not_started`, folder `active-patients`, model `dim_patient`, measure `count(*) where patient_status = 'Patient'`, tolerance exact count.
2. Parser script `active-patients/scripts/parse_od_active_patients_golden.py` writes a PHI-free snapshot YAML with `patient_count` only. Same pattern as `daily-payments/scripts/`.
3. Compare SQL uses the `marts` schema, same as the existing daily-payments compare scripts:
   - `compare/compare_active_patients_staging.sql` — staging patient status counts vs `dim_patient`.
   - `compare/compare_active_patients.sql` — mart count vs snapshot `patient_count`.
   - `compare/compare_active_patients_api.sql` — the new endpoint's SQL vs the mart.
4. Run in DBeaver as `analytics_user` on `opendental_analytics`. Record `findings/{export-date}.md`.
5. One snapshot is enough to move status to `within_tolerance` for a list report, provided the field map accounts for every filter that changed the count. A second export a week later is the regression check. It can land after the card ships if the first export matches exactly. If it does not match, the card waits.

**API**

`GET /kpi/active-patients` returns `{ active_patient_count, as_of }`. The query is the field-map count. No status filter in the client.

**UI**

After the registry row is `within_tolerance`, add the allowlist entry and a card on `ValidatedKpiPanel`: the count, "Lists → Active Patients", and the sign-off date. This card is a snapshot, so it ignores the report-date picker. The card says "as of {export date}" until a later export refreshes the sign-off. The live count is still the current mart; the caption separates "count right now" from "rules last checked on".

**Acceptance**

- Mart count equals the golden row count.
- API count equals the mart.
- `/dashboard` shows the card. `/patients` remains the row browser and is not described as the KPI.

### Phase 2 — Active providers

**Scout export**

For `2026-06-10` (already a production golden date), export Daily → Procedures and Lists → Appointments. Record columns in `validation/kpi/active-providers/FIELD_MAP.md`. Lock the definition using the rule in §3.2. Update `exposures.yml` and the provider bullet in `KPIDefinitions.tsx` to that sentence in the same PR as the field map, before compare SQL is written.

**Model and API**

Add a query, backed by a small mart if the distinct count is awkward to keep correct in the router:

- Completed-production definition: distinct provider on procedure rows with ProcStatus 2 and DateComplete = `:report_date`. Reuse the staging filters documented in `daily-production-by-procedure/FIELD_MAP.md`.
- Appointment definition: distinct provider on appointments scheduled that day, with the inclusion rules copied from the Appointments export (broken and no-show in or out — whatever the scout CSV contains).

`GET /kpi/active-providers?report_date=` returns `{ report_date, active_provider_count }`.

**Golden set**

Three dates. Parser writes `provider_count` and, when the CSV allows it, the set of provider ids for a row-level diff in a local-only findings file. Compare SQL returns PASS/FAIL with exact-count tolerance.

**UI**

The card uses the same report date as collections and production. Subtitle is the locked definition plus the OpenDental menu path.

**Acceptance**

- Three findings files at PASS.
- `VALIDATION_REPORT.md` states which report was the benchmark and which definition was rejected.
- `/dashboard` shows the count for the selected date.
- The exposure sentence and the API query describe the same population.

### Phase 3 — Broken appointments

**Scout export**

One day, Lists → Broken Appointments. Field map lists every column. Choose count or dollars using the table in §3.3. Registry row uses that measure only. Suggested folder name: `broken-appointments` (already in the report index).

**Model**

The comparison subject is the missed-appointment slice, not `sum(lost_revenue)`.

- Count: appointments with the broken/no-show flags the field map documents, for that appointment date.
- Dollars: `sum(scheduled_production_amount)` on that same set, and only if the CSV column maps to it.

If `fact_appointment` flags do not reproduce the export, fix the flags or document the OD rule. Do not point the executive card at the composite mart to close the gap.

**API**

`GET /kpi/broken-appointments?report_date=` returns the signed-off measure and nothing else. No recovery potential field on this payload.

**UI**

Card title is "Broken appointments" or "Broken appointment production", matching the measure. Menu path: Lists → Broken Appointments.

**Acceptance**

- Three dates PASS at the tolerance in §3.3.
- `/revenue` still shows the four-slice analysis.
- `/dashboard` shows the one signed-off slice.
- `GET /reports/dashboard/kpis` is unused by the executive page. Leave the route in place until a follow-up confirms no other client calls it. Power BI is called out in §7.

### Phase 4 — Estimates

Can merge independently of Phases 1–3.

1. `frontend/packages/analytics-ui/src/pages/Revenue.tsx`: the recovery potential figure and any recovery rate are labeled **Model estimate**, with the 80 / 50 / 20 rates in the caption.
2. `KPIDefinitions.tsx`: move Recovery Potential and Recovery Rate out of the Executive Dashboard group into Revenue Analytics. The calculation block states that the rates are assigned in `mart_revenue_lost` and are not an OpenDental report.
3. `exposures.yml` `executive_dashboard` description: drop revenue lost, recovery potential, recovery rate, and active providers from "Key Metrics Displayed" until each promotion PR adds back only the measure that signed off. Phase 0 can land the deletion; Phase 4 is the revenue-page label.
4. `mart_revenue_lost.sql`: stop classifying broken appointments with `current_timestamp`. No-show → High. Broken → one band (use Low, the existing non-urgent band) until a stored notice column is confirmed. Adjust the dbt test in `_mart_revenue_lost.yml` that encodes the 24-hour split so it matches the new rule. Say in the model description that `estimated_recoverable_amount` is an internal score.

**Acceptance**

- Rebuilding the mart on two different days does not change a historical row's `recovery_potential`.
- `/dashboard` still has no recovery rate.
- `/revenue` shows the estimate label on the recovery potential total.

---

## 6. File map

| Phase | Touch |
| --- | --- |
| 0 | `frontend/packages/analytics-ui/src/config/validatedKpis.ts` (new), `pages/Dashboard.tsx`, `pages/ValidatedKpiPanel.tsx` (new), `apps/clinic/src/config/validatedKpis.ts`, `apps/clinic/src/pages/homes/PracticeManagerHome.tsx`, `apps/portfolio/src/pages/KpiValidation.tsx`, `apps/portfolio/src/pages/Portfolio.tsx` |
| 1 | `validation/kpi/active-patients/**`, `KPI_VALIDATION_REGISTRY.md`, `api` route + types, allowlist entry, `ValidatedKpiPanel` card |
| 2 | `validation/kpi/active-providers/**`, registry, exposure text, `KPIDefinitions.tsx`, API, panel card |
| 3 | `validation/kpi/broken-appointments/**`, registry, API, panel card. Composite mart stays the source for `/revenue` |
| 4 | `Revenue.tsx`, `KPIDefinitions.tsx`, `exposures.yml`, `mart_revenue_lost.sql`, `_mart_revenue_lost.yml` |

Compare SQL is written for DBeaver against PostgreSQL `opendental_analytics` as `analytics_user`, using the `marts` schema the way `daily-payments/compare/` already does. Source checks against MySQL `opendental` stay ad hoc. The golden CSV is the benchmark.

---

## 7. Out of scope

- **Total production, total collection, and collection rate** on the old executive page. Phase 0 takes them off `/dashboard`. Promoting production and collections is Phase 1A of [FRONTEND_EVOLUTION_PROPOSAL.md](./FRONTEND_EVOLUTION_PROPOSAL.md), and that work uses the marts already signed off (`mart_daily_payments`, `mart_daily_production_by_procedure`). `mart_provider_performance` is a different model. Its collection rate is not an allowlist candidate here.
- **Top 10 patient balances.** Aging of A/R is `delta_documented` in the registry: the mart matches stored balances, and OpenDental rebuilds buckets from transactions. Balances return to an executive surface only after that gap is closed.
- **New patients, retention, hygiene, referral production.** Separate reports, separate registry rows.
- **A composite "revenue lost" KPI.** No OpenDental report equals the sum of the four slices.
- **Power BI.** `power_bi_executive_pilot` mirrors this exposure. After Phase 0, that report should drop the same cards. Track it as a follow-up in `powerbi/`; this proposal does not edit the `.pbix`.
- **Replacing `/patients`, `/revenue`, `/providers`.** They stay available as exploratory pages.

---

## 8. Order of work

1. Phase 0. The demo executive page matches the clinic rule with the two KPIs already signed off.
2. Phase 4 in parallel. The revenue page tells the truth about the estimate, and the mart stops moving historical recovery bands.
3. Phase 1. Active patients is a count against a list report. One export unlocks the field map.
4. Phase 2. Scout the provider column, lock the definition, then three dates.
5. Phase 3. Scout Broken Appointments, lock count or dollars, then three dates.

Each of 3–5 ends with the promotion checklist. The executive page grows by one card per sign-off.
