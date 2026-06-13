# Power BI — Dental Executive Pilot

Power BI implementation of the `executive_dashboard` dbt exposure: revenue-lost and
provider-performance KPIs built on the dbt `marts` layer of the local PostgreSQL
analytics warehouse.

This directory holds the git-friendly artifacts for the pilot:

| File | Purpose |
| ---- | ------- |
| [model_guide.md](model_guide.md) | Tables, grains, relationships, columns to hide/remove |
| [measures.dax](measures.dax) | Starter DAX measures (copy into the model) |
| [report_checklist.md](report_checklist.md) | Pilot report page layout and acceptance checklist |
| `DentalExecutivePilot/` | Power BI project (PBIP) — created when you save the report here |
| `screenshots/` | Exported report images for the portfolio |

Canonical runbooks (deployed/Aurora architecture, gateway, costs):
[docs/analytics/power_bi_postgresql_access.md](../docs/analytics/power_bi_postgresql_access.md)
and [docs/analytics/power_bi_pilot_dataset.md](../docs/analytics/power_bi_pilot_dataset.md).

---

## 1. Prerequisites

- **Power BI Desktop** (free): Microsoft Store or
  [direct download](https://www.microsoft.com/en-us/download/details.aspx?id=58494).
- Local PostgreSQL running with the `opendental_analytics` database and a populated
  `marts` schema (`dbt run` completed).
- The read-only **`bi_user`** role (next section). Credentials live in
  `etl_pipeline/.env_local` (`POSTGRES_BI_USER` / `POSTGRES_BI_PASSWORD` — gitignored).

## 2. Create `bi_user` (one-time, superuser required)

The grant script is [docs/analytics/sql/bi_user_marts_grants.sql](../docs/analytics/sql/bi_user_marts_grants.sql)
(`USAGE` on `marts`, `SELECT` on the four pilot tables only). Run it as the `postgres`
superuser with the password substituted from `.env_local`. From PowerShell at the repo root:

```powershell
$bipw = (Select-String etl_pipeline\.env_local -Pattern '^POSTGRES_BI_PASSWORD=(.+)').Matches[0].Groups[1].Value
$sql  = (Get-Content docs\analytics\sql\bi_user_marts_grants.sql -Raw) -replace '<strong_password>', $bipw
$sql | & "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h localhost -p 5432 -U postgres -d opendental_analytics
# psql prompts for the postgres superuser password
```

Verify least privilege:

```powershell
# Should return a count (granted) ...
$env:PGPASSWORD = $bipw
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h localhost -U bi_user -d opendental_analytics -c "SELECT count(*) FROM marts.mart_revenue_lost;"
# ... and this should fail with "permission denied" (not granted):
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h localhost -U bi_user -d opendental_analytics -c "SELECT count(*) FROM marts.mart_ar_summary;"
```

> **Re-grant after dbt runs.** Mart tables are dropped and recreated by
> `dbt run` (table materialization), which discards per-table grants. Re-run the
> `GRANT SELECT` statements (the script is idempotent except `CREATE ROLE`) after a
> rebuild, or move the pilot tables to dbt `+grants` config later.

## 3. Connect Power BI Desktop (local, no gateway)

1. **Get Data → PostgreSQL database**.
2. Server: `localhost`  Database: `opendental_analytics`. Data Connectivity mode: **Import**.
3. Credentials: **Database** tab → `bi_user` + password from `.env_local`.
4. In Navigator select the four pilot tables (schema `marts`):
   `mart_revenue_lost`, `mart_provider_performance`, `dim_provider`, `dim_date`.
5. **Transform Data** and apply the Power Query cleanups in [model_guide.md](model_guide.md)
   (remove the `missed_procedures` array column, etc.), then **Close & Apply**.
6. Build relationships and paste measures per [model_guide.md](model_guide.md) and
   [measures.dax](measures.dax).

## 4. Save as PBIP (source control)

1. **File → Options and settings → Options → Preview features** → enable
   **Power BI Project (.pbip) save option** (already GA in recent builds).
2. **File → Save as** → choose this `powerbi/` folder → type **Power BI project files (*.pbip)**
   → name it `DentalExecutivePilot`.
3. Commit the resulting `DentalExecutivePilot.SemanticModel/` and
   `DentalExecutivePilot.Report/` folders — they are TMDL/JSON text and diff cleanly.
   User-specific cache files (`.pbi/localSettings.json`, `.pbi/cache.abf`) are gitignored.
4. Export a PNG of each report page into `screenshots/` for the portfolio.

## 5. Deployed architecture (interview story)

Locally there is no gateway: Desktop talks straight to `localhost:5432`. In the deployed
design the warehouse is **Aurora PostgreSQL in a private VPC**, so:

- **Desktop authoring** happens from a host with VPC reachability (VPN, SSM port-forward,
  or RDP to an EC2 in the VPC).
- **Power BI Service scheduled refresh** requires an **on-premises data gateway** on a
  Windows host that can reach the Aurora endpoint on 5432; the dataset refresh is bound
  to that gateway.
- The same least-privilege `bi_user` (SELECT on named mart tables only) is used in both
  environments, so the model built locally publishes unchanged.

Details and cost bands: [power_bi_postgresql_access.md](../docs/analytics/power_bi_postgresql_access.md).

## 6. Lineage

```text
Power BI dataset "DentalExecutivePilot"
  → PostgreSQL (local dev: localhost; deployed: private Aurora)
      → marts.mart_revenue_lost
      → marts.mart_provider_performance
      → marts.dim_provider
      → marts.dim_date
  → dbt exposures: executive_dashboard, power_bi_executive_pilot
```
