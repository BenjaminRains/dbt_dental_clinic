# OpenDental Reports — UI Index

Transcribed from the clinic **Reports** window (OpenDental). Folder names under `validation/kpi/`
use the **report name** in kebab-case (not the section header).

**Sections** group the UI only; each report row below is a potential validation target.

---

## Production and Income

Shortcuts into the Production and Income report with preset date ranges:

| OD report name | Folder |
| --- | --- |
| Today | [today](./today/) |
| Yesterday | [yesterday](./yesterday/) |
| This Month | [this-month](./this-month/) |
| Last Month | [last-month](./last-month/) |
| This Year | [this-year](./this-year/) |
| More Options | *(opens report dialog — no export folder)* |
| Monthly Production Goal | [monthly-production-goal](./monthly-production-goal/) |

Full custom report: use **More Options** or the standard Production and Income export workflow;
mart mapping lives in [production-and-income](./production-and-income/) when validated.

---

## Daily

| OD report name | Folder | Notes |
| --- | --- | --- |
| Adjustments | [adjustments](./adjustments/) | |
| Payments | [payments](./payments/) | Active KPI validation (mart_daily_payments) |
| Write-offs | [write-offs](./write-offs/) | |
| Procedures | [procedures](./procedures/) | |
| Incomplete Procedure Notes | [incomplete-procedure-notes](./incomplete-procedure-notes/) | |
| Routing Slips | [routing-slips](./routing-slips/) | |
| Unfinalized Insurance Payments | [unfinalized-insurance-payments](./unfinalized-insurance-payments/) | |
| Patient Portion Uncollected | [patient-portion-uncollected](./patient-portion-uncollected/) | |
| Production by Procedure | [production-by-procedure](./production-by-procedure/) | Scroll list continues below fold |

---

## Monthly

| OD report name | Folder |
| --- | --- |
| Aging of A/R | [aging-of-a-r](./aging-of-a-r/) |
| Unearned Income | [unearned-income](./unearned-income/) |
| Claims Not Sent | [claims-not-sent](./claims-not-sent/) |
| Capitation Utilization | [capitation-utilization](./capitation-utilization/) |
| Finance Charge Report | [finance-charge-report](./finance-charge-report/) |
| Outstanding Insurance Claims | [outstanding-insurance-claims](./outstanding-insurance-claims/) |
| Procedures Not Billed to Insurance | [procedures-not-billed-to-insurance](./procedures-not-billed-to-insurance/) |
| PPO Write-offs | [ppo-write-offs](./ppo-write-offs/) |
| Payment Plans | [payment-plans](./payment-plans/) |
| Receivables Breakdown | [receivables-breakdown](./receivables-breakdown/) |
| Insurance Overpaid | [insurance-overpaid](./insurance-overpaid/) |
| Presented TreatPlan Production | [presented-treatplan-production](./presented-treatplan-production/) |
| Treatment Presentation Statistics | [treatment-presentation-statistics](./treatment-presentation-statistics/) |
| Ins Pay Plans Past Due | [ins-pay-plans-past-due](./ins-pay-plans-past-due/) |
| Insurance Aging Report | [insurance-aging-report](./insurance-aging-report/) |
| Custom Aging | [custom-aging](./custom-aging/) |
| Procedures Overpaid | [procedures-overpaid](./procedures-overpaid/) |
| Payment Plans Overcharged | [payment-plans-overcharged](./payment-plans-overcharged/) |

---

## Lists

| OD report name | Folder |
| --- | --- |
| Active Patients | [active-patients](./active-patients/) |
| Appointments | [appointments](./appointments/) |
| Birthdays | [birthdays](./birthdays/) |
| Broken Appointments | [broken-appointments](./broken-appointments/) |
| Insurance Plans | [insurance-plans](./insurance-plans/) |
| New Patients | [new-patients](./new-patients/) |
| Patients - Raw | [patients-raw](./patients-raw/) |
| Patient Notes | [patient-notes](./patient-notes/) |
| Prescriptions | [prescriptions](./prescriptions/) |
| Procedure Codes - Fee Schedules | [procedure-codes-fee-schedules](./procedure-codes-fee-schedules/) |
| Referrals - Raw | [referrals-raw](./referrals-raw/) |
| Referral Analysis | [referral-analysis](./referral-analysis/) |
| Referred Proc Tracking | [referred-proc-tracking](./referred-proc-tracking/) |
| Treatment Finder | [treatment-finder](./treatment-finder/) |
| Web Sched Appointments | [web-sched-appointments](./web-sched-appointments/) |
| Discount Plans | [discount-plans](./discount-plans/) |
| Hidden Payment Splits | [hidden-payment-splits](./hidden-payment-splits/) |
| ERAs Automatically Processed | [eras-automatically-processed](./eras-automatically-processed/) |
| Insurance Pending Supplementals | [insurance-pending-supplementals](./insurance-pending-supplementals/) |

---

## Public Health

| OD report name | Folder |
| --- | --- |
| Raw Screening Data | [raw-screening-data](./raw-screening-data/) |
| Raw Population Data | [raw-population-data](./raw-population-data/) |
| FQHC Dental Sealant Measure | [fqhc-dental-sealant-measure](./fqhc-dental-sealant-measure/) |
| UDS+ Dental Services | [uds-dental-services](./uds-dental-services/) |

---

## Other UI (not report export folders)

- **Setup** — report configuration
- **User Query** — custom query tool
- **Laser Labels** — label printing

Footer notes (not separate report folders):

- Appointment lists (recall, confirmation) print from their own windows
- Labels, letters, postcards from various windows
- Deposit slips, accounting, sent claims, images from their own windows

---

## Golden export naming

Per-report CSV prefix (when validation starts):

```
od_{folder}_{mm}{dd}{yyyy}_{mm}{dd}{yyyy}.csv
```

Example for Payments: `od_payments_10072025_10072025.csv`

Legacy files may still use `od_daily_payments_*` until renamed.

**Snapshot YAML** (section totals for compare SQL): same stem as the golden CSV with `.snapshot.yml`:

```
od_daily_payments_10072025_10072025.snapshot.yml
```
