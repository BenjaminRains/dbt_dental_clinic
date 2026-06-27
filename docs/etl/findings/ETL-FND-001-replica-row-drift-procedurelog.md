# ETL-FND-001: Replica row drift — `procedurelog`

**Status:** Mitigated (local KPI PASS 2026-06-26, Phase 2 re-validated 2026-06-27) — branch `fix/etl-fnd-001-procedurelog-row-drift` (P0 drift check + P1 lookback re-sync + Phase 2 replicator/loader alignment). Clinic RDS deploy + nightly automation pending before close.

**Platform roadmap (pre-CDC):** [ETL_REPLICA_FIDELITY_ROADMAP.md](../ETL_REPLICA_FIDELITY_ROADMAP.md) — schema analyzer fix, Layer 0 checks, Sunday full refresh, CDC sketch.
**Severity:** High (blocks production KPI validation; affects any metric keyed on complete procedures)  
**Discovered via:** [daily-production-by-procedure KPI validation](../../../dbt_dental_models/validation/kpi/daily-production-by-procedure/findings/2026-06-10.md) (2026-06-10)  
**Owner:** ETL / data platform  
**Tracking:** [TODO.md — ETL-FND-001](../../../TODO.md#etl-fnd-001--replica-row-drift-procedurelog)  

## Classification

| Dimension | Value |
| --- | --- |
| **Finding type** | **Pipeline defect** — incremental sync does not reliably propagate in-place row updates |
| **Secondary type** | **Observability gap** — no automated check detected drift before KPI validation |
| **Layer** | MySQL (`opendental`) → replication → `raw.procedurelog` → dbt staging |
| **Not a data quality finding** | Source MySQL and OD golden agree; the clinic data is correct |
| **Not a dbt / mart defect** | `raw` = staging = mart for affected rows; transform logic is faithful to stale raw |
| **Not a golden / KPI definition issue** | Golden date and OD report rules validated against MySQL |
| **Not SCD (dimensional)** | Not Kimball Type 1/2 history on a dimension; fact rows are **stale copies** of source |
| **Not “ETL didn’t run”** | Daily incremental runs occurred; the design skips rows that already passed the watermark |

Use **replica row drift** as the precise term (defined below), not “ETL lag” (which we reserve for
late-arriving **new** rows, e.g. same-day payments in daily-payments 2026-06-24).

---

## What “row drift” means here

**Row drift** = a row with the **same primary key** exists in both OpenDental MySQL and analytics,
but **one or more business attributes differ** because the pipeline copied the row once and never
applied a later source-side update.

We are **not** talking about:

| Term | Meaning | This finding? |
| --- | --- | --- |
| **Missing row** | `ProcNum` in MySQL, absent from `raw` | Partially (some rows may never have been inserted; primary pattern here is stale copy) |
| **Phantom row** | `ProcNum` in `raw`, deleted/hard-removed in MySQL | Different CDC gap (deletes not propagated) |
| **Aggregate drift** | Totals differ but row-level relationship unclear | Symptom; root issue is row-level stale attributes |
| **ETL lag** | New source row not yet copied | Different mechanism (watermark never saw a **new** key) |

### Drift taxonomy (replica sync)

```
                    SOURCE (MySQL)              REPLICA (raw)
                    ─────────────              ──────────────
Missing row         ProcNum 12345 exists       (no row)

Stale row drift     ProcStatus = 2             ProcStatus = 1   ← THIS FINDING
                    ProcFee = 672              ProcFee = 660
                    DateComplete = 2026-06-10   (unchanged / wrong)

Phantom row         (deleted)                  ProcStatus = 2   ← separate issue
```

**Drift** specifically means: **PK match, attribute mismatch, source is authoritative.**

---

## Concrete example: D1206 on 2026-06-10

**OpenDental Production by Procedure** (complete only) expects D1206: **21 procedures, $672.00**.

### MySQL (`opendental`) — ground truth

| ProcStatus | Meaning | Count | Fees |
| --- | --- | ---: | ---: |
| 2 | Complete | **21** | **$672.00** |
| 1 | Treatment Planned | 4 | $128.00 |
| 6 | Deleted | 3 | $96.00 |

### Analytics (`staging.stg_opendental__procedurelog`) — drifted

| procedure_status | Count | Fees |
| --- | ---: | ---: |
| 1 (TP) | **21** | $660.00 |
| 2 (Complete) | 0 | — |

**Same 21 `procedure_id` values** (same procedures, same day context), but analytics still stores
them as **Treatment Planned** with slightly stale fees ($660 vs $672).

That is **stale row drift**: the replica row is not missing — it is **wrong**.

### Day-level aggregate drift (symptom)

| Layer | Complete rows | Complete fees |
| --- | ---: | ---: |
| MySQL | 140 | $15,239 |
| `raw` / staging | 48 | $3,719 |

~92 complete procedures appear as TP in analytics (status mix: MySQL 46 TP vs warehouse 135 TP).

---

## Root cause

### Mechanism

The ETL pipeline uses **high-watermark incremental copy + upsert** (see
`docs/etl/ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md`):

1. Copy rows where `incremental_column > last_watermark`.
2. Upsert into replica on primary key (`ProcNum`).

**In-place updates** (e.g. `ProcStatus` 1 → 2 when a procedure is set complete) are only captured
if the chosen incremental column **advances past the watermark** on that update.

If OpenDental updates `ProcStatus` / `ProcFee` / `DateComplete` **without** bumping
`DateTStamp` (or the configured incremental column) above the watermark, the row **never re-enters**
the incremental batch. The replica retains the **first version** copied — typically Treatment
Planned.

### Contributing pipeline factors (code / config)

| Factor | Location | Risk |
| --- | --- | --- |
| Watermark-only incremental | `simple_mysql_replicator.py`, `postgres_loader.py` | Misses in-place updates when timestamp unchanged |
| **`and_logic` on `procedurelog`** | `analyze_opendental_schema.py` conservative table list | Stricter than OR; multiple columns must all pass watermark |
| Stale-state fallback | `postgres_loader._prepare_load()` | Only triggers when incremental returns **0 rows**, not when rows drift |
| dbt staging incremental | `stg_opendental__procedurelog.sql` | `DateTStamp > max(_loaded_at)` — won't refresh old-timestamp rows even after raw fix unless full-refresh |
| No row-drift reconciliation | — | Drift accumulates silently until manual KPI compare |

### OpenDental behavior (to verify)

OpenDental has **no MySQL triggers** to guarantee timestamp updates. Whether `DateTStamp` changes
on TP → Complete must be **empirically verified** (spot-edit test). If OD does not bump
`DateTStamp`, watermark incremental **cannot** be durable without a compensating design.

---

## What is NOT an acceptable durable fix

| Approach | Why insufficient |
| --- | --- |
| **Weekly full refresh of `procedurelog`** | Masks drift up to 7 days; heavy load; does not detect drift between refreshes; same class of bug can affect `payment`, `claimproc`, etc. |
| **“Run ETL daily”** | Already running; drift persisted 16+ days on 2026-06-10 |
| **dbt-only changes** | `raw` is wrong first; mart cannot fix upstream stale rows |
| **Widen KPI tolerance** | Hides production misstatement |

Full refresh remains valid as a **one-time backfill** after fixing detection/reconciliation.

---

## Durable fixes (in priority order)

### 1. Row drift detection (required — observability)

**Goal:** Catch drift automatically, not only during manual KPI validation.

**Production reconciliation check** (run after each ETL load or nightly):

Compare MySQL vs `raw.procedurelog` for a **rolling business-date window** (1 month / 30 days):

```sql
-- Same predicate on both sides; alert if rows or fees differ beyond tolerance
SELECT COUNT(*) AS complete_rows, ROUND(SUM(ProcFee), 2) AS complete_fees
FROM procedurelog
WHERE ProcStatus = 2
  AND DateComplete >= CURRENT_DATE - INTERVAL 30 DAY;
```

PostgreSQL equivalent on `raw.procedurelog`. Extend to **per-code** or **per-ProcNum sample** for
debugging.

**Row-level drift query** (MySQL vs raw on sample keys):

```sql
-- Example pattern: ProcNums complete in MySQL but not complete in raw
-- (implement as ETL script joining replication to source on ProcNum)
```

Wire to alerting (Airflow task failure, Slack, etc.). Document in KPI validation README as **Layer 0**
before golden compare.

### 2. Lookback window re-sync (recommended code change)

**Goal:** Fix drift without full table scan.

Augment incremental load for `procedurelog`:

```
incremental_batch = (
  rows WHERE DateTStamp > watermark
  UNION
  rows WHERE DateComplete >= today - 30 days   -- 1-month lookback; or ProcDate in same window
)
```

Always re-copy recent business dates so TP → Complete updates reach replica even when
`DateTStamp` is stale. Upsert on `ProcNum` applies new `ProcStatus`.

Default **1 month (30 days)**; tune to clinic volume if needed. **This is the primary durable
mitigation** short of log-based CDC.

### 3. Incremental config review (required)

On clinic `tables.yml` for `procedurelog`:

- Confirm `primary_incremental_column` = **`DateTStamp`** (not `ProcDate` — business date never
  changes on status complete).
- Re-evaluate **`and_logic`** vs **`or_logic`** for incremental columns.
- Document decision in `tables.yml` comments.

### 4. Extend loader stale-state detection (code)

Today: fallback to full load only when incremental count = 0.

Enhance `_check_analytics_needs_updating()` (or post-load hook) for critical tables:

- Compare aggregate checksums (complete production by day) source vs raw for lookback window.
- Trigger **targeted re-sync** or **table full refresh** when drift detected.

### 5. dbt staging: lookback merge (secondary)

After raw is corrected, staging incremental on `DateTStamp` alone can miss rows. Options:

- Merge strategy with lookback filter on `ProcDate` / `DateComplete`, or
- Depend on raw lookback upsert + staging `merge` on `procedure_id` without narrow timestamp filter
  for recent dates.

### 6. Log-based CDC (long-term)

Binlog / Debezium captures **every** update and delete. Only architecture that guarantees zero
drift for all mutation types. Higher operational cost.

---

## Immediate remediation (2026-06-10 validation)

**Local validation (2026-06-26):** PASS — Query 8 and KPI compare 140 / $15,239 on golden date
[2026-06-10](../../../dbt_dental_models/validation/kpi/daily-production-by-procedure/findings/2026-06-10.md).

**Phase 2 re-validation (2026-06-27):** After replicator/loader alignment (`replica_sync_config.py`),
incremental ETL copies ~10,632 lookback rows (~2s replicate + ~1.2 min load) instead of ~815k
full-table scans. Drift check PASS; KPI totals + 28 procedure codes PASS unchanged.

```bash
mdc etl run --env local --profile full -- --tables procedurelog
mdc etl invoke --env local -- check-procedurelog-drift --warn-only
mdc dbt run --env local -- --select stg_opendental__procedurelog+
```

**Clinic RDS (pending):** repeat ETL on `--env clinic`, then `mdc dbt run --env local` + publish or
EC2 dbt per [CLINIC_ANALYTICS_WORKFLOW.md](../../CLINIC_ANALYTICS_WORKFLOW.md).

---

## Acceptance criteria (close this finding)

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Automated drift check runs post-ETL and alerts on MySQL vs raw mismatch for `procedurelog` | **Done** — `mdc etl invoke -- check-procedurelog-drift` |
| 2 | 2026-06-10 KPI compare PASS after backfill | **Done** (local, 2026-06-26) |
| 3 | Lookback re-sync **or** verified `DateTStamp` behavior + config fix deployed | **Done** (30-day lookback + Phase 2 replicator/loader alignment); clinic deploy pending |
| 4 | Documented in KPI validation workflow as Layer 0 (replica fidelity before golden compare) | **Done** — [kpi/README.md](../../../dbt_dental_models/validation/kpi/README.md) |

---

## Related artifacts

| Document | Role |
| --- | --- |
| [ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](../ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md) | Current incremental design |
| [daily-production-by-procedure/findings/2026-06-10.md](../../../dbt_dental_models/validation/kpi/daily-production-by-procedure/findings/2026-06-10.md) | KPI instance that discovered this |
| [audit_table_row_counts.py](../../../etl_pipeline/scripts/audit_table_row_counts.py) | Row-count audit (extend toward drift) |
| `postgres_loader.py` `_prepare_load`, `_check_analytics_needs_updating` | Stale-state hooks |

---

## Finding IDs cross-reference

| ID | Scope |
| --- | --- |
| **ETL-FND-001** | Platform — replica row drift on `procedurelog` (this document) |
| **DPBP-2026-06-10-001** | KPI instance — daily production by procedure **PASS** on 2026-06-10 (local, 2026-06-26) |
