# dbt Performance Investigation and Optimization

## Is ~52 minutes expected for 157 models?

**Short answer:** For 157 models (45 views, 46 tables, 66 incremental), total run time is usually dominated by a small number of slow models. In your last run, **one model accounted for almost all of the 52 minutes**: `mart_patient_retention` (~52 min). The other 156 models together ran in only a few minutes.

So the total time is **not** “expected” in the sense of being evenly spread; it’s expected that a few heavy marts can dominate. The goal is to find those and optimize.

---

## How to investigate

### 1. List slow models after every run

Use the project script (run from `dbt_dental_models`):

```powershell
cd dbt_dental_models
.\scripts\list_slow_models.ps1
```

Optional: show top 40 and use a different run results file:

```powershell
.\scripts\list_slow_models.ps1 -Top 40 -ResultsPath "target\run_results.json"
```

This reads `target/run_results.json` and prints total run time and the top N slowest models by `execution_time`, with percentage of total time.

### 2. Inspect `run_results.json` manually

- **Path:** `dbt_dental_models/target/run_results.json`
- **Fields:** For each model, `unique_id`, `execution_time` (seconds), `status`.
- Sort by `execution_time` descending to see the heaviest models.

### 3. See the SQL the database runs

1. Compile one model:
   ```bash
   dbt compile --select mart_patient_retention
   ```
2. Open the compiled SQL in `target/run/dbt_dental_models/models/marts/mart_patient_retention.sql`.
3. In DBeaver (or any PostgreSQL client), run:
   ```sql
   EXPLAIN (ANALYZE, BUFFERS) <paste the compiled SELECT here>;
   ```
4. Use the output to see:
   - Which steps take the most time
   - Large sequential scans
   - Missing or unused indexes

### 4. Check parallelism (threads)

- **Where:** `dbt_dental_models/profiles.yml` → each target’s `threads`.
- **Current:** Often 4. If the warehouse has spare CPU and I/O, try 8 (or more) to run more models in parallel. This does **not** speed up a single slow model; it shortens total wall-clock time when many models are run together.

---

## What you can do

### Global / project-level

| Action | Effect |
|--------|--------|
| **Increase `threads`** in `profiles.yml` (e.g. 4 → 8) | More models run in parallel; total run time usually drops (unless one model dominates). |
| **Use selectors** | Run only what’s needed (e.g. `dbt run --selector daily_refresh`, `--selector incremental_only`) instead of full `dbt run` when possible. |
| **Run slow models less often** | If a mart is only needed weekly, run it in a separate job on a schedule. |

### Per-model (heavy marts like `mart_patient_retention`)

| Action | Effect |
|--------|--------|
| **Pre-aggregate in intermediates** | Move heavy aggregations (e.g. by `patient_id`) into incremental or table models; mart only joins pre-aggregated tables. Reduces repeated full scans of fact tables. |
| **Incremental or snapshot** | If the mart is a “point-in-time snapshot” (e.g. one row per patient per day), consider incremental by date or a snapshot so you don’t rebuild the whole table every run. |
| **Limit date range** | If only recent data is needed, filter in the mart or in upstream models (e.g. last 2 years) to reduce data volume. |
| **Indexes** | Already defined in config; ensure the warehouse actually has them and that key filters/joins use indexed columns. |
| **Simplify joins** | e.g. Join to `dim_date` only for “today” via a single-row subquery instead of scanning the full dimension. |

### Staging / incremental

- **Incremental models:** Already 66; they should be relatively fast on subsequent runs when only new data is processed. If an incremental model is slow, check that the incremental filter (e.g. `_loaded_at > (select max(...) from {{ this }})`) is selective and that the unique key is correct.
- **Views:** 45 views don’t store data; they add compute only when they’re built (and when downstream models read from them). Slow views are worth optimizing or materializing if they’re on the critical path.

---

## Current bottleneck: `mart_patient_retention`

From your last run:

- **Execution time:** ~3095 s (~52 min)  
- **Share of total:** Almost 100% of the run

The model:

- Scans four fact tables (`fact_appointment`, `fact_payment`, `fact_claim`, `fact_communication`) with large aggregations by `patient_id`.
- Joins `dim_patient`, `dim_date` (for `current_date = date_day`), and `dim_provider`.
- Builds a full table every run (no incremental).

**Recommended next steps:**

1. **Immediate:** Join to `dim_date` only for “today” (e.g. single-row subquery or `where date_day = current_date` in a small CTE) so the plan doesn’t scan the whole dimension.
2. **Short term:** Introduce intermediate models that do the patient-level aggregations (e.g. one table per fact: appointments, payments, claims, communications) and make those incremental or at least reusable, then have `mart_patient_retention` join these pre-aggregated tables.
3. **Optional:** Consider making the mart incremental by “snapshot date” (e.g. only (re)build today’s rows) or run it on a separate schedule (e.g. nightly) so it doesn’t block the rest of the pipeline.

After changes, run:

```bash
dbt run --select mart_patient_retention
```

and use `list_slow_models.ps1` and `EXPLAIN (ANALYZE, BUFFERS)` to confirm improvement.
