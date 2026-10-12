# Referral source KPI dataset (BI and analysis)

## Canonical source

Build target is the dbt model **`mart_referral_source_kpis`** in schema **`marts`** on the analytics warehouse (database **`opendental_analytics`**; connect as **`analytics_user`** per project convention).

The file **`referral_dataset.csv`** in this folder is an optional **manual export** for sharing. It is not refreshed by the pipeline. After each dbt run, regenerate exports from the table below so column definitions stay in sync.

## Refresh export (DBeaver / PostgreSQL)

```sql
SELECT *
FROM marts."mart_referral_source_kpis"
ORDER BY reporting_month, period_basis_sort_order, referral_display_name;
```

Use **DirectQuery or import from PostgreSQL** in Power BI against the same table for production dashboards; avoid relying on a stale CSV when numbers matter.

## Grain

One row per:

- **`reporting_month`** (first day of calendar month),
- **`referral_id`** (OpenDental referrer; joined to **`dim_referral`**),
- **`period_basis`** (how the patient cohort is defined).

Do **not** sum **`distinct_patient_count`** across multiple **`referral_id`** values in the same month without **deduplicating patients** in the BI layer—the same patient can appear under more than one referrer.

## Measures

| Column | Type | Definition |
|--------|------|------------|
| **`distinct_patient_count`** | Integer | Distinct patients in the cohort for that month, referrer, and `period_basis`. |
| **`production_value_in_period`** | Money | **Production**, not cash: sum of **`fact_procedure.actual_fee`** for procedures dated in **`reporting_month`** (completed / existing-prior only). |
| **`net_collections_in_period`** | Money | **Collections**: same net rule as **`mart_daily_payments`**—sum of **`fact_payment.payment_amount`** in **`reporting_month`** where **`payment_direction`** is **Income** or **Refund** (refunds negative). |

Stakeholder-facing “cash” reporting should emphasize **`net_collections_in_period`**.

## `period_basis` (choose one slice per analysis)

Use **`period_basis_sort_order`** or **`period_basis_description`** in filters and legends.

| `period_basis` | Sort | Meaning (short) |
|----------------|------|-------------------|
| `referral_link` | 1 | Referral attachment **RefDate** falls in the month. |
| `new_patient_first_visit` | 2 | Patient’s **first_visit_date** in **`dim_patient`** falls in the month and they have a referral link on file. |
| `production_in_period` | 3 | Patient had **production** in the month and has a referral link (link may be from any prior date). |

## Referral / “provider” names

Name columns are **referring sources** from OpenDental’s **referral** list (**`dim_referral`**), not **`dim_provider`** (in-house treating providers).

- **`referral_display_name`**: Primary label.
- **`referral_source_segment`**: `physician_or_clinical` | `organization` | `individual` for charts.
- **`referral_is_doctor`**, **`referral_not_person`**: Raw flags from OpenDental.

## Time columns for BI

- **`reporting_year`**, **`reporting_month_number`**: Slicers and hierarchies.
- **`reporting_year_month`**: `YYYY-MM` axis labels.

## Lineage

dbt: **`stakeholders` → not part of dbt**; model lives under `dbt_dental_models/models/marts/mart_referral_source_kpis.sql` and is documented in **`_mart_referral_source_kpis.yml`**.

Upstream facts: **`fact_procedure`**, **`fact_payment`**, **`dim_referral`**, **`dim_patient`**, **`stg_opendental__refattach`**.
