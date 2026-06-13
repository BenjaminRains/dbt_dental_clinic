# Power BI — pilot dataset (executive KPI slice)

This pilot mirrors the **Executive Dashboard** exposure in dbt: revenue-lost and provider performance KPIs, aligned with the custom app’s first-line metrics. Source: [exposures.yml](../../dbt_dental_models/models/marts/exposures.yml) (`executive_dashboard`).

> **Local development:** this pilot is being implemented first against the **local**
> PostgreSQL warehouse (`localhost:5432`, no gateway needed). Implementation artifacts —
> connection guide, verified model design, DAX measures, report checklist — live in
> [powerbi/](../../powerbi/README.md). The dbt exposure is `power_bi_executive_pilot`.

## Network and Aurora (read this first)

The analytics warehouse is **Aurora PostgreSQL in a private VPC** — **not on the public internet**. See [power_bi_postgresql_access.md](power_bi_postgresql_access.md) for the full picture and links to [NETWORK_INFRASTRUCTURE_EXPLAINED.md](../deployment/NETWORK_INFRASTRUCTURE_EXPLAINED.md).

- **Power BI Desktop:** use a host that already has a path to Aurora (VPN, Session Manager tunnel, RDP to a VPC EC2 with Desktop, etc.).
- **Scheduled refresh in the Power BI service:** use a **data gateway** installed on a host that can reach the **Aurora endpoint** on port **5432**; bind this dataset’s refresh to that gateway.

## Target tables (schema `marts`)

| Table | Role in model |
| ----- | ------------- |
| `mart_revenue_lost` | Facts / metrics for lost revenue and recovery potential |
| `mart_provider_performance` | Production, collections, provider-level KPIs |
| `dim_provider` | Provider attributes and keys for filters/slicers |
| `dim_date` | Calendar for time intelligence (if needed for reporting grain) |

Fully qualified names: `marts.mart_revenue_lost`, `marts.mart_provider_performance`, `marts.dim_provider`, `marts.dim_date`.

## Model design (star-oriented)

1. Decide **grain** per table from column names in those models (do not assume a single fact without checking keys).
2. **Relationships** in Power BI:
   - Link facts to `dim_provider` on the same provider key used in marts.
   - Link facts to `dim_date` on the date key if both sides share a consistent date surrogate or calendar date column.
3. **Hide** foreign-key columns from report view if you expose only descriptions.
4. **Measures**: Prefer DAX measures for ratios and time-window logic; keep definitions documented and aligned with API/dashboard copy in exposures where possible.

## Connectivity — Power BI Desktop

1. **Get Data** → **PostgreSQL database** (or **Database** → **PostgreSQL** depending on build).
2. From a machine that can reach **private Aurora**, enter the **cluster (or reader) endpoint**, database name, and `bi_user` per [power_bi_postgresql_access.md](power_bi_postgresql_access.md). `bi_user` has `SELECT` only on the tables above; extend grants in [sql/bi_user_marts_grants.sql](sql/bi_user_marts_grants.sql) when you add tables.
3. Select the four tables in Navigator, or use **Advanced** SQL only if you need a constrained pre-filter (prefer importing base tables first).
4. Before publishing: note the **gateway** you will use in the service so connection parameters (host, SSL, database) match.

## Storage mode and refresh

- Use **Import** for the pilot unless you have a strong near-real-time requirement (**DirectQuery** keeps persistent connections to Aurora and is usually more expensive operationally).
- In the **Power BI service**, configure **scheduled refresh** on the **same gateway** that can reach Aurora, **after** the dbt/ETL window (often **15–30 minutes** after jobs typically finish).

## Minimal report checklist

- Slicers: date range, provider (from `dim_provider`).
- Cards or KPI visuals: production and collections (`mart_provider_performance`); revenue lost and recovery potential (`mart_revenue_lost`) — align with exposures where practical.
- One trend visual: daily or weekly series if a suitable date column exists on the fact or via `dim_date`.

## Lineage (document in workspace or repo)

```text
Power BI dataset "Dental Executive Pilot"
  → Aurora PostgreSQL (private VPC) → marts.mart_revenue_lost
  → marts.mart_provider_performance
  → marts.dim_provider
  → marts.dim_date
  → dbt exposures: executive_dashboard
```

## After the pilot

- Promote the dataset to a **shared workspace**; confirm **gateway** high availability if refresh becomes business-critical.
- Extend with additional `marts` tables only after adding explicit `GRANT SELECT` for `bi_user` and verifying refresh duration impact on Aurora.
