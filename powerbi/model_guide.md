# Pilot data model guide

Verified against the local warehouse (schema `marts`) on 2026-06-12.

## Tables and grains

| Table | Grain | Rows (local) | Notes |
| ----- | ----- | ------------ | ----- |
| `mart_provider_performance` | One row per **provider per production day** (`provider_id`, `date_id`) | 2,531 | Daily production/collections plus appointment, claim, and patient metrics. Includes scheduled (future) dates. |
| `mart_revenue_lost` | One row per **lost-revenue opportunity** (`opportunity_id`, `date_id`) | 521 | Missed/cancelled appointments, claim rejections, capacity gaps. |
| `dim_provider` | One row per **provider** (`provider_id`) | 58 | Includes hidden and non-person (system) providers — see filter note below. |
| `dim_date` | One row per **calendar day** (`date_id`) | 4,018 (2020-01-01 → 2030-12-31) | Covers all fact dates; safe to mark as the model date table. |

## Relationships (all many-to-one, single direction, dims filter facts)

| From (fact, many side) | To (dim, one side) |
| ---------------------- | ------------------ |
| `mart_provider_performance[date_id]` | `dim_date[date_id]` |
| `mart_provider_performance[provider_id]` | `dim_provider[provider_id]` |
| `mart_revenue_lost[date_id]` | `dim_date[date_id]` |
| `mart_revenue_lost[provider_id]` | `dim_provider[provider_id]` |

Keep cross-filter direction **Single** (dim → fact). Do not relate the two facts to each
other — they share dimensions, which is how a measure like `Revenue Lost` slices by the
same provider/date filters as `Total Production`.

Both facts carry their own date copies (`production_date`, `appointment_date`) and
denormalized date attributes (`year`, `month`, `quarter`, `day_name`, `is_weekend`,
`is_holiday`). Use **`date_id` for the relationships** and prefer `dim_date` columns in
visuals so both facts filter consistently.

## Power Query cleanups (before Close & Apply)

1. **`mart_revenue_lost`: remove `missed_procedures`** — it is a PostgreSQL `ARRAY`
   column; the connector imports it as a list/record column that bloats the model and
   cannot be aggregated directly.
2. Optionally remove ETL metadata columns from all tables: `_loaded_at`, `_created_at`,
   `_updated_at`, `_created_by`, `_transformed_at`, `_mart_refreshed_at` (keep one
   `_mart_refreshed_at` somewhere if you want a "data as of" card).
3. Confirm `date_id` columns typed as **Date** (they are `date` in PostgreSQL; no
   timezone conversion needed).

## Model settings

- **Mark `dim_date` as date table** (Table tools → Mark as date table → `date_id`).
  This makes the time-intelligence measures in [measures.dax](measures.dax) valid.
- **Hide from report view**: `provider_id` and `date_id` on both facts (use the dim
  attributes instead); raw FK/staging columns on `dim_provider` you don't need
  (`fee_schedule_id`, `specialty_id`, color columns, web-schedule columns).
- **Sort** `dim_date[month_name]` by `month`, `dim_date[day_name]` by `day_of_week`.
- **Format**: currency (no decimals) for production/collections/lost-revenue columns;
  percentage for rate measures.

## Useful attribute columns

- Slicers: `dim_date[date_id]` (range), `dim_provider[provider_specialty_category]`,
  `dim_provider[provider_status_category]`.
- `mart_revenue_lost` breakdowns: `opportunity_type`, `opportunity_subtype`,
  `recovery_potential` (text bucket: categorizes recoverability), `preventability`,
  `revenue_impact_category`, `recovery_timeline`, `time_period`.
- Provider filter hygiene: `dim_provider` includes hidden/system rows. For most visuals
  add page/report filters `is_hidden = FALSE` and `is_not_person = FALSE`, or rely on
  `provider_status_category`.

## Known data characteristics (local synthetic/dev data)

- `mart_provider_performance` spans 2023-01-02 → 2027-06-08 (future rows are scheduled
  production); `mart_revenue_lost` spans 2025-06-16 → 2026-08-08. Default report date
  range should cover mid-2025 onward or visuals comparing the two facts will look empty.
- `lost_revenue` is `double precision`, `estimated_recoverable_amount` is `numeric` —
  both import as Decimal Number; no action needed.
