# Marts Layer

The marts layer provides reporting-ready datasets for analytics, dashboards, and operational
insights. It includes dimensions (`dim_*`), facts (`fact_*`), and summary marts
(`mart_*`) that standardize definitions and deliver business-ready metrics.

## Validation Status

Most marts are in-progress validation. See `dbt_dental_models/validation/README.md`
for the validation workflow and `dbt_dental_models/validation/marts/` for current
investigations and analyses.

Currently tracked validation artifacts:
- `validation/marts/dim_patient/`
- `validation/marts/dim_provider/`
- `validation/marts/fact_claim/`
- `validation/marts/fact_payment/`
- `validation/marts/mart_hygiene_retention/`
- `validation/marts/mart_patient_retention/`

## Exposures

Exposures are defined in `exposures.yml` and describe dashboard/app consumption.

- `executive_dashboard` uses `mart_revenue_lost`, `mart_provider_performance`
- `revenue_analytics_dashboard` uses `mart_revenue_lost`
- `provider_performance_dashboard` uses `mart_provider_performance`, `mart_appointment_summary`
- `appointment_analytics_dashboard` uses `mart_appointment_summary`
- `patient_management_dashboard` uses `dim_patient`
- `accounts_receivable_aging_dashboard` uses `mart_ar_summary` (planned exposure)

## Model Catalog

### Dimensions

| Model | Purpose | Clinical/Operational Value | Exposures |
| --- | --- | --- | --- |
| `dim_patient` | Patient demographics, status, and key identifiers | Enables accurate patient lookup, segmentation, and continuity-of-care reporting | `patient_management_dashboard` |
| `dim_provider` | Provider attributes, type, and status | Supports provider benchmarking, scheduling alignment, and clinical staffing analysis | `provider_performance_dashboard` |
| `dim_procedure` | Procedure codes, descriptions, and categories | Standardizes clinical procedure reporting and service mix analysis | — |
| `dim_insurance` | Insurance plan and carrier reference data | Improves claim and coverage analytics and payer mix insights | `accounts_receivable_aging_dashboard` |
| `dim_fee_schedule` | Fee schedule definitions and references | Supports pricing analysis and contract alignment | — |
| `dim_clinic` | Clinic/location reference data | Enables location-based performance and capacity planning | — |
| `dim_date` | Standard date dimension | Consistent time-based slicing for all marts | `revenue_analytics_dashboard`, `appointment_analytics_dashboard` |

### Facts

| Model | Purpose | Clinical/Operational Value | Exposures |
| --- | --- | --- | --- |
| `fact_procedure` | Completed procedures with financial amounts | Tracks clinical production and procedure mix | — |
| `fact_appointment` | Appointment-level events and outcomes | Monitors utilization, cancellations, and no-show patterns | `appointment_analytics_dashboard`, `provider_performance_dashboard` |
| `fact_payment` | Payment transactions and payment direction | Supports cash-flow tracking and collections analysis | — |
| `fact_claim` | Insurance claim lifecycle and amounts | Enables claims performance, denial analysis, and payer accountability | — |
| `fact_communication` | Patient communication events and engagement | Measures outreach effectiveness and patient engagement | — |

### Summary Marts

| Model | Purpose | Clinical/Operational Value | Exposures |
| --- | --- | --- | --- |
| `mart_appointment_summary` | Aggregated appointment KPIs by provider/date | Drives schedule optimization, access to care, and no-show reduction | `appointment_analytics_dashboard`, `provider_performance_dashboard` |
| `mart_ar_summary` | Accounts receivable aging, risk, and priority | Supports collections focus and cash-flow stability | `accounts_receivable_aging_dashboard` |
| `mart_hygiene_retention` | Hygiene recall and retention metrics | Improves preventive care adherence and recall effectiveness | — |
| `mart_new_patient` | New patient acquisition and onboarding metrics | Supports growth, referral analysis, and intake capacity planning | — |
| `mart_patient_retention` | Patient lifecycle, churn risk, and engagement | Protects continuity of care and long-term patient outcomes | — |
| `mart_procedure_acceptance_summary` | Treatment acceptance and conversion metrics | Identifies barriers to care acceptance and improves case completion | — |
| `mart_production_summary` | Production and collection rollups | Enables revenue tracking and clinical throughput insights | — |
| `mart_provider_performance` | Provider productivity and efficiency metrics | Supports coaching, scheduling balance, and quality oversight | `provider_performance_dashboard`, `executive_dashboard` |
| `mart_revenue_lost` | Missed opportunity and revenue leakage analysis | Prioritizes recovery workflows and prevents future revenue loss | `revenue_analytics_dashboard`, `executive_dashboard` |
| `mart_treatment_plan_summary` | Treatment plan pipeline and status | Monitors care plan follow-through and future production | — |
