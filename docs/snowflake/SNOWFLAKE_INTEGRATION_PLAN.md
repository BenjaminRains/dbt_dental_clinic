# Snowflake Integration Plan — Payments / Collections Mini Warehouse

| Attribute | Value |
|-----------|--------|
| **Status** | Wave 1 loaded and matched to demo Postgres (2026-09-30); portfolio page next |
| **Branch** | `feature/snowflake-wave1-reload` |
| **Decisions locked** | 2026-07-17 |
| **Last updated** | 2026-09-30 |
| **Priority** | Tier 4 #1 — hiring differentiator |
| **Scope** | Portfolio / demo only — not clinic production |
| **Live checklist** | `TODO.md` → Snowflake Integration |

---

## Current state (2026-09-30)

Wave 1 rails are on `main` ([PR #79](https://github.com/BenjaminRains/dbt_dental_clinic/pull/79), squash `43e442bf`). Work since then is on `feature/snowflake-wave1-reload`.

Done on the account:

- RSA key-pair for `CONCRETE1866`. `mdc dbt validate --env snowflake` reconfirmed 2026-09-30 (`WH_DEMO_XS`, `OPENDENTAL_SF`)
- Wave 1 export: `RAW.PAYMENT` 21,279 rows, `RAW.CLAIMPAYMENT` 1,817 rows. This account folds quoted identifiers to uppercase, so dbt's `"raw"."payment"` is that table. Do not run `sql/03_recreate_raw_quoted.sql` — it drops the live load
- `mdc dbt invoke --env snowflake -- build --select tag:snowflake` → `PASS=76 WARN=1 ERROR=0`. The warning is `claimpayment.bank_branch` (1,817 nulls; the generator inserts `''`). Relationship tests to models outside the wave, and `not_null` on `payment_type_id` / `sec_user_num_entry`, run only when `target.type != 'snowflake'`
- `PayAmt`, `MerchantFee`, and `CheckAmt` are `numeric(18, 2)`. Float casts made daily sums differ by cents to a few dollars
- [`scripts/snowflake/compare_mart_daily_payments.py`](../../scripts/snowflake/compare_mart_daily_payments.py) matches all 61 dates against local demo Postgres and against the EC2 tunnel. Sample: 2025-11-13 `45473.55` / 372 payments; 2026-01-12 `39676.25` / 334 payments. EC2 `raw.payment` and `raw.claimpayment` were copied from the local generation before that rebuild; other EC2 raw tables were not

**Next:** Phase 5 portfolio tile and page. Then Phase 6 `fact_payment`.

---

## Purpose

Add a **small but meaningful Snowflake analytics warehouse** alongside the existing PostgreSQL stack. This is a **portfolio and hiring** piece: demonstrate dual-warehouse dbt, Snowflake ingestion patterns, and cost-aware platform habits — without cloning the full ~200-model Postgres warehouse.

Clinic RDS Postgres remains the source of truth for production operations.

---

## Locked decisions

| Decision | Choice |
|----------|--------|
| Architecture | **Mini parallel warehouse** that can grow to full demo coverage |
| Domain (v1) | **Payments / collections** first slice — not a permanent ceiling |
| Data source | **Export from demo Postgres** (`opendental_demo`) via table manifest |
| dbt shape | Existing `dbt_dental_models` + **`snowflake` enablement tag** |
| Scale path | Same DB/schemas/export/dbt rails; expand tables + tags over time |
| Cost | Acceptable at ~$20/mo after trial; XS + auto-suspend |
| Visibility | **Full portfolio UI tile + dedicated page** |
| PHI | **Synthetic / demo only** — clinic data never lands in Snowflake |

---

## Scale path (design for whole warehouse later)

v1 ships a thin payments slice. The **rails** are built so expansion is additive, not a rewrite.

### What already scales (keep)

| Rail | Why it scales |
|------|----------------|
| One dbt project + `--target snowflake` | Full warehouse = more models on the same target, not a fork |
| `OPENDENTAL_SF` + quoted `"raw"` / `"staging"` / `"int"` / `"marts"` | Same layout as Postgres (`quoting: true`); new domains just land more tables |
| Sources use `schema: raw`, DB from profile | No per-target source rewrites for new tables |
| Export introspects Postgres columns | Adding a table ≠ hand-writing Snowflake DDL |
| `mdc dbt --env snowflake` | Env pattern stays stable as the project grows |
| Demo-only / no PHI | Volume stays portfolio-sized even at “full” demo coverage |

### What changes as we grow (intentionally later)

| Growth step | Trigger | Work |
|-------------|---------|------|
| Wave 2 payment detail | After Wave 1 parity | More RAW tables + tag `fact_payment` lineage |
| Demo-core (~34 generator tables) | Want fuller marts on SF | Expand `export_tables.yml`; tag more staging/int/marts |
| Near-full dbt on Snowflake | Hiring / demos need breadth | Fix Postgres-only SQL as models are enabled; eventually `dbt build --target snowflake` without a narrow tag |
| Clinic-scale / Airbyte→SF | Separate product decision | New ingest path; **out of portfolio v1** |

**Demo ceiling:** generator only populates ~34 tables. “Whole warehouse” on Snowflake means **all models that have demo data**, not cloning empty 400-table shells unless we choose to. Clinic PHI still stays on Postgres.

### Tag convention (enablement, not “payments forever”)

- `tag:snowflake` = **enabled on Snowflake** (membership grows over time).
- Optional second tag for demos: `payments` / `revenue_cycle` (domain), so you can still run a thin slice:
  - Thin: `--select tag:snowflake,tag:payments` (or path + tag)
  - Broad: `--select tag:snowflake` (everything enabled so far)
  - Eventual: untagged full `dbt build --target snowflake` once most models are portable

Do **not** create a second dbt project for Snowflake.

### Portability rule (avoid a rewrite cliff)

When enabling a model on Snowflake, fix dialect issues **in that model’s dependency cone** (adapter-aware config/macros), not a big-bang rewrite of 200 models. Common Postgres-only traps: `indexes=`, `ON CONFLICT`, `::double precision`, freshness on missing `_loaded_at`.

Wave 1 already applied this rule: ID macros and the daily-payments aggregate were made adapter-aware, the Postgres tracking upsert is skipped on this target, and payment money columns are `numeric(18, 2)` instead of `double precision`. Do not add source freshness until exported tables have `_loaded_at`.

### Export manifest

Table lists live in [`scripts/snowflake/export_tables.yml`](../../scripts/snowflake/export_tables.yml) (`wave1_payments`, `wave2_payment_detail`, …). Scaling export = edit YAML + re-run, not rewrite the loader.

---

## Context: demo data today

Demo is **not** a full ETL replica of OpenDental.

| Layer | Reality |
|-------|---------|
| Schema | ~420 OpenDental-shaped tables (DDL shell) |
| Populated | **~34 core tables** via `etl_pipeline/synthetic_data_generator/` |
| Load path | Generator → Postgres `opendental_demo.raw` (no ETL, no Airflow) |
| dbt | `mdc dbt … --env demo` |
| Consumers | Demo API + `@mdc/portfolio` |

Payments/collections tables **are** populated on demo (`payment`, `paysplit`, `claimpayment`, `claimproc`, `adjustment`, etc.). That makes this domain viable without generator changes.

---

## Target architecture

```text
Synthetic generator
        │
        ▼
PostgreSQL opendental_demo.raw     ← unchanged clinic path stays separate
        │
        │  export (CSV / Parquet)
        ▼
Snowflake internal stage
        │
        │  COPY INTO
        ▼
Snowflake "OPENDENTAL_SF"."raw".<table>
        │
        ▼
dbt --target snowflake --select tag:snowflake
   staging → (wave 2 ints) → marts
        │
        ▼
Portfolio UI tile + page
   (architecture, proof, synthetic banner)
```

**Not in scope for v1:** clinic PHI, Snowpipe continuous ingest, Airbyte, full staging catalog, AR aging mart, system_e collection campaigns/statements.

---

## Domain story

**Hero KPI:** `mart_daily_payments` — OpenDental Daily Payments / **net collections** (patient payments + insurance claim checks). Already validated against OD golden exports on Postgres.

Wave 1 lineage is intentionally thin:

- `stg_opendental__payment`
- `stg_opendental__claimpayment`
- `mart_daily_payments`

Wave 2 adds payment detail (`fact_payment` via `int_payment_split`) for a richer collections narrative without expanding into unrelated domains.

---

## Implementation phases

### Phase 0 — Branch & docs (this document)

- [x] Feature branch (scaffold `feature/snowflake-payments-warehouse`; rails merged via PR #79)
- [x] Plan doc under `docs/snowflake/`
- [x] `TODO.md` Tier 4 Snowflake section tracks this plan

### Phase 1 — Snowflake sandbox

- [x] Bootstrap SQL + setup guide in-repo ([SETUP.md](SETUP.md), [sql/01_bootstrap.sql](sql/01_bootstrap.sql))
- [x] Quoted landing for existing accounts: [sql/03_recreate_raw_quoted.sql](sql/03_recreate_raw_quoted.sql) (drops unquoted `RAW`, creates `"raw"`)
- [x] Env aligned with [ENVIRONMENT_FILES.md](../deployment/ENVIRONMENT_FILES.md): `dbt_dental_models/.env_snowflake` + `mdc dbt --env snowflake`
- [x] Grants use `SNOWFLAKE_USER` (no hardcoded Snowsight user)
- [x] Snowflake account created; RSA key-pair (Snowsight passkey blocks password auth from Python/dbt)
- [x] Bootstrap run in Snowsight (XS, auto-suspend 60s, roles, schemas)
- [x] Local `dbt_dental_models/.env_snowflake` filled from the template (gitignored)
- [x] `mdc dbt validate --env snowflake` reconfirmed 2026-09-30

**Exit criteria:** Can connect from local machine; warehouse suspends when idle; zero clinic credentials involved. Met.

### Phase 2 — Export from demo Postgres → Snowflake `"raw"`

- [x] Export script: [`scripts/snowflake/export_demo_to_snowflake.py`](../../scripts/snowflake/export_demo_to_snowflake.py)
  - Reads from `opendental_demo.raw` only (hard refuse clinic DB names)
  - `PUT` → internal stage → `COPY INTO` quoted `"OPENDENTAL_SF"."raw"."<table>"` (`MATCH_BY_COLUMN_NAME`)
- [x] Wave 1 (`payment`, `claimpayment`) is loaded. On this account the quoted names are stored as `RAW.PAYMENT` / `RAW.CLAIMPAYMENT`, and dbt reads them. Do not drop `RAW`
- [x] Refresh cadence: manual / on-demand for the portfolio demo, not a nightly clinic job. PayType `definition` is in `wave2_payment_detail`, not Wave 1

**Exit criteria:** Row counts in Snowflake `"raw"` match demo Postgres for exported tables.

### Phase 3 — dbt Snowflake target + Wave 1 tag

- [x] Add `snowflake` target to `profiles.yml.template` (+ local `profiles.yml`)
- [x] `dbt-snowflake~=1.10` in the dbt Pipfile
- [x] `mdc` stage: `DBT_STAGES` + `dbt_env.py` load `dbt_dental_models/.env_snowflake`
- [x] Tag Wave 1 models with `snowflake`:
  - `stg_opendental__payment`
  - `stg_opendental__claimpayment`
  - `mart_daily_payments`
- [x] Skip Postgres ETL tracking hooks on Snowflake target
- [x] Wave 1 cone dialect fixes: Snowflake `transform_id_columns`; `mart_daily_payments` aggregates without Postgres `FILTER`
- [x] `mdc dbt invoke --env snowflake -- build --select tag:snowflake` → `PASS=76 WARN=1 ERROR=0` (2026-09-30)
- [x] Wave 1 test scope: relationship tests to models outside the wave, and `not_null` on `claimpayment.payment_type_id` / `sec_user_num_entry`, are enabled only when `target.type != 'snowflake'`. `bank_branch` stays `severity: warn`

**Exit criteria:** `mart_daily_payments` builds on Snowflake from the live `RAW` landing; tests in the Wave 1 scope pass. Met.

### Phase 4 — Parity proof

- [x] Compare `mart_daily_payments` on demo Postgres vs Snowflake (`net_collections_amount`, `payment_count`) via [`scripts/snowflake/compare_mart_daily_payments.py`](../../scripts/snowflake/compare_mart_daily_payments.py). Exit 0 on 2026-09-30 for local demo and for the EC2 tunnel: all 61 dates match
- [x] Results recorded here and in `TODO.md` (sample: 2025-11-13 `45473.55` / 372; 2025-12-13 `83152.80` / 401; 2026-01-12 `39676.25` / 334)
- [x] Known type difference: summing `double precision` payment amounts diverged by cents to a few dollars. Staging now casts `PayAmt`, `MerchantFee`, and `CheckAmt` to `numeric(18, 2)` before the mart sums them

**Exit criteria:** Collections totals match within agreed tolerance (same spirit as KPI registry: tight $/%). Met — exact cents on every date.

### Phase 5 — Portfolio UI (max visibility)

- [ ] Capability tile on portfolio home (`Portfolio.tsx` pattern)
- [ ] Dedicated page (route under `@mdc/portfolio`), including:
  - Architecture diagram (demo PG → stage/COPY → Snowflake → dbt → mart)
  - Domain story: payments / net collections
  - Synthetic-only banner (reuse `SyntheticDataBanner` pattern)
  - Proof summary (parity note + links)
  - Cost / RBAC callouts (XS, auto-suspend, roles) — hiring signal
  - Links to this plan + repo paths
- [ ] Nav / evidence section entry so the piece is discoverable

**Exit criteria:** Live portfolio tile + page on demo frontend; no clinic data exposed.

### Phase 6 — Wave 2 payment detail (same domain)

- [ ] Export additional raw tables as required by lineage: e.g. `paysplit`, `adjustment`, `procedurelog`, `patient`, `provider`, `userod`
- [ ] Tag `int_payment_split`, `fact_payment`, and required upstream staging/int models
- [ ] Remove or gate Postgres-only `indexes=` (and similar) via adapter-aware config
- [ ] Rebuild `tag:snowflake`; extend parity notes

**Exit criteria:** `fact_payment` builds on Snowflake; portfolio page mentions detail layer.

### Deferred (explicitly out of v1)

- `mart_ar_summary` and full AR aging chain
- `system_e_collection` (statements / tasks / campaigns) — weak synthetic coverage
- Snowpipe / continuous tasks
- Airbyte → Snowflake path
- BI live connection (Power BI / Tableau) — optional later polish
- Teaching the synthetic generator a native Snowflake destination

---

## dbt selector convention

```bash
# Everything enabled on Snowflake so far (grows over time)
mdc dbt invoke --env snowflake -- build --select tag:snowflake

# Thin payments slice (v1 hero path)
mdc dbt invoke --env snowflake -- build --select tag:snowflake,mart_daily_payments
```

`tag:snowflake` = **enabled on this warehouse** (not “payments-only forever”). Domain tags (`revenue_cycle`, etc.) stay available for thin demos. Default target remains Postgres for clinic/demo.

Export waves:

```bash
python scripts/snowflake/export_demo_to_snowflake.py --wave wave1_payments
python scripts/snowflake/export_demo_to_snowflake.py --wave wave2_payment_detail
```

---

## Cost guardrails

| Control | Setting |
|---------|---------|
| Warehouse size | X-Small only |
| Auto-suspend | 60 seconds |
| Continuous features | Off (no Snowpipe / no always-on tasks in v1) |
| Budget comfort | ~$20/mo after trial is fine |
| Monitoring | Resource monitor + credit alert |
| Data | Synthetic only (keeps volume small) |

Trial credits (~$400 / ~30 days typical) should cover build + parity if warehouses are not left running.

---

## Effort ballpark

| Slice | Rough effort |
|-------|----------------|
| Phase 1 sandbox | 0.5–1 day |
| Phase 2 export + COPY | 1–2 days |
| Phase 3 Wave 1 dbt | 1–2 days |
| Phase 4 parity | 0.5 day |
| Phase 5 portfolio UI | 2–3 days |
| Phase 6 Wave 2 | 2–4 days |
| **Total** | **~1–2 weeks** focused work |

---

## Success criteria

1. Snowflake holds a **payments/collections** mini warehouse fed from **demo Postgres export**.
2. `mdc dbt invoke --env snowflake -- build --select tag:snowflake` produces `mart_daily_payments` (Wave 1) and later `fact_payment` (Wave 2).
3. Parity with demo Postgres collections totals is documented.
4. Portfolio has a **full tile + page** explaining dual-warehouse architecture, synthetic boundary, and cost/RBAC habits.
5. Clinic PHI never appears in Snowflake; clinic Postgres path unchanged.

---

## Key paths

| Path | Role |
|------|------|
| `docs/snowflake/SNOWFLAKE_INTEGRATION_PLAN.md` | This plan |
| `docs/snowflake/sql/03_recreate_raw_quoted.sql` | One-time drop of unquoted `RAW` → quoted `"raw"` |
| `docs/deployment/ENVIRONMENT_FILES.md` | Env file SoT (`dbt_dental_models/.env_snowflake`) |
| `TODO.md` → Tier 4 Snowflake | Live checklist |
| `etl_pipeline/synthetic_data_generator/` | Demo row source (+ `.env_demo` for export) |
| `dbt_dental_models/.env_snowflake.template` | Snowflake secrets template |
| `dbt_dental_models/models/marts/mart_daily_payments.sql` | Hero mart |
| `dbt_dental_models/models/marts/fact_payment.sql` | Wave 2 mart |
| `frontend/apps/portfolio/` | Tile + page |
| `scripts/snowflake/export_tables.yml` | Export wave manifest (grows with domains) |
| `scripts/snowflake/` | Export / COPY helpers (no secrets) |

---

## Related roadmap

- Tier 4 hiring order: **Snowflake (this)** → Open Dental Airbyte (gated on Tier 3A) → Kafka → QuickBooks → BI connectors
- Snowflake can proceed **in parallel** with Tier 3 Track A; Airbyte cannot
- See also: [EVENT_DRIVEN_ANALYTICS_PROPOSAL.md](../streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md) (separate portfolio track)
