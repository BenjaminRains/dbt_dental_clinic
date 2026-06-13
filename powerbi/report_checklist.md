# Pilot report checklist

One page is enough for the pilot ("Executive Overview"); add the optional second page
if you want a deeper revenue-lost drill-down. Mirrors the `executive_dashboard` dbt
exposure so the Power BI report and the React dashboard tell the same story.

## Page 1 — Executive Overview

**Slicers (top or left rail)**

- [ ] Date range — `dim_date[date_id]` (Between slicer; default mid-2025 → today, see
      data-range note in [model_guide.md](model_guide.md))
- [ ] Provider — `dim_provider[provider_specialty_category]` or provider name/custom id
- [ ] Page filters: `dim_provider[is_hidden] = False`, `dim_provider[is_not_person] = False`

**KPI cards (single row)**

- [ ] Total Production
- [ ] Total Collections
- [ ] Collection Rate
- [ ] Revenue Lost
- [ ] Recovery Potential Amt
- [ ] Active Providers

**Visuals**

- [ ] Line chart — `Revenue Lost` and `Recovery Potential Amt` by `dim_date[date_id]`
      (weekly grain reads better than daily for 521 opportunities)
- [ ] Stacked bar — `Revenue Lost` by `mart_revenue_lost[opportunity_type]`,
      legend `recovery_potential`
- [ ] Table — provider leaderboard: provider, `Total Production`, `Total Collections`,
      `Collection Rate`, `Revenue Lost`; sort by `Total Production` desc
- [ ] Card or footer text — "data as of" using `_mart_refreshed_at` if kept

## Page 2 (optional) — Revenue Recovery Drill-Down

- [ ] Decomposition tree or matrix: `Revenue Lost` by `opportunity_type` →
      `opportunity_subtype` → `preventability`
- [ ] Scatter: `recovery_priority_score` vs `lost_revenue`, legend `recovery_timeline`
- [ ] Table of recent recoverable opportunities (`recoverable = True`,
      `days_since_opportunity` ascending)

## Quality checks before committing

- [ ] Cross-filtering works: clicking a provider bar filters both facts (shared dims)
- [ ] Time intelligence sanity: `Production YoY %` returns values for 2026 dates
- [ ] Totals reconcile with SQL (see below) for the same date window
- [ ] All currency/percentage formats applied; FK columns hidden
- [ ] Saved as PBIP in `powerbi/`; screenshots exported to `powerbi/screenshots/`

**Reconciliation query (DBeaver, analytics DB)**

```sql
SELECT
    SUM(pp.total_production)  AS total_production,
    SUM(pp.total_collections) AS total_collections
FROM marts.mart_provider_performance pp
WHERE pp.date_id BETWEEN DATE '2025-07-01' AND DATE '2026-06-12';

SELECT
    SUM(rl.lost_revenue)                  AS revenue_lost,
    SUM(rl.estimated_recoverable_amount)  AS recovery_potential
FROM marts.mart_revenue_lost rl
WHERE rl.date_id BETWEEN DATE '2025-07-01' AND DATE '2026-06-12';
```
