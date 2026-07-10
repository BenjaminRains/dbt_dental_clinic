# Proposal: Machine Learning on the Dental Practice Analytics Platform

> **Status: DRAFT — not scheduled for implementation.**
> Parked for planning and portfolio refinement.
> Audience: analytics maintainer, practice stakeholders evaluating predictive use cases.

**Related:**
- [README.md](README.md) — what lives in `docs/ml/` vs other folders
- [ML_SERVING_ARCHITECTURE.md](ML_SERVING_ARCHITECTURE.md) — serving design (outline; fill in at implementation)
- [../dbt/marts_models_plan.md](../dbt/marts_models_plan.md) — planned marts with ML-adjacent metrics
- [../streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md](../streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md) — optional near-real-time feature path
- [databricks_lakehouse_proposal.md](databricks_lakehouse_proposal.md) — lakehouse / Spark ML at scale (parallel track)

---

## Executive summary

The platform already has a **production-grade analytics warehouse** — ETL, dbt marts, FastAPI,
React dashboards, and nightly Airflow orchestration — but **no ML training or serving layer**.
What exists today are **rule-based risk scores** (churn, onboarding, collections, revenue recovery)
built in SQL/dbt.

This proposal describes how to **extend the existing stack** with supervised learning for
operational decisions (retention, scheduling, collections, claims) without replacing batch ETL
or duplicating business logic outside dbt.

**Recommended first targets:** patient churn, appointment no-show, new-patient early value.

**Recommended environment:** train and demo on `opendental_demo` (synthetic); validate on clinic
data locally only (`opendental_analytics`, PHI).

---

## Current state

### What exists

| Layer | Implementation | ML relevance |
| --- | --- | --- |
| Source | OpenDental MySQL (~432 tables) | Rich behavioral + clinical raw data |
| ETL | `etl_pipeline/` → PostgreSQL `raw` | Nightly incremental refresh |
| Transform | `dbt_dental_models/` staging → int → marts | Feature and label engineering home |
| Orchestration | Airflow nightly DAG | Batch feature refresh cadence |
| Consumption | FastAPI + React + Power BI pilot | Score delivery surface |
| Demo data | `synthetic_data_generator/` | HIPAA-safe ML sandbox |

### What does not exist

- No `ml/` code package, feature store, or model registry
- No training pipelines or evaluation harness
- No inference endpoints beyond rule-based SQL scores
- No point-in-time feature tables (current marts are **current-state snapshots** — leakage risk for training)
- `scikit-learn` and Jupyter in `dbt_dental_models/Pipfile` are exploratory only

### Adjacent but separate

- **`consult_audio_pipe/`** — Whisper transcription + Claude analysis of consultation audio.
  Not integrated with the warehouse; treat as a future unstructured-data source, not Phase 1.

---

## Available datasets for ML

### Patient-grain spine (primary feature/label source)

| Model | Grain | Role |
| --- | --- | --- |
| `mart_patient_journey` | 1 row / patient | Lifecycle labels: `journey_stage`, milestone dates, stage lags |
| `mart_patient_retention` | 1 row / patient / day | Churn labels, engagement, production, communication |
| `mart_new_patient` | 1 row / new patient / day | Early-life features + 90-day outcomes |
| `mart_hygiene_retention` | patient × provider × hygienist × day | Recall compliance, hygiene lapse |
| `dim_patient` | 1 row / patient | Demographics, insurance, tenure |
| `int_patient_profile` | 1 row / patient | Diseases, documents, family links, notes |

### Event-grain (sequence and appointment models)

| Model | Grain | Role |
| --- | --- | --- |
| `fact_appointment` | 1 row / appointment | `is_no_show`, `is_broken`, timing, provider |
| `fact_procedure` | 1 row / procedure | Production, fees, completion |
| `fact_payment` | 1 row / payment | Payment behavior, timing |
| `fact_claim` | 1 row / claim line | Denial, reimbursement, payer |
| `fact_communication` | 1 row / comm event | Outreach and response patterns |
| `stg_opendental__histappointment` | appointment history | Reschedule / cancel sequences |

### Aggregated marts (cohort labels and baselines)

| Model | Grain | Role |
| --- | --- | --- |
| `mart_appointment_summary` | date × provider × clinic | No-show rates, utilization |
| `mart_procedure_acceptance_summary` | date × provider × clinic | Treatment acceptance KPIs |
| `mart_ar_summary` | date × patient × provider | AR aging, `collection_priority_score` |
| `mart_revenue_lost` | opportunity events | `recovery_potential`, `recovery_priority_score` |
| `mart_claim_summary` | date × provider | Denial / reimbursement KPIs |
| `mart_untapped_implant_revenue` | patient | Rule-defined implant conversion candidates |

### Staged, not yet marted (future clinical features)

Staging exists for `perioexam`, `periomeasure`, `disease`, `allergy`, `medication`, `recall`,
`treatplan`. Useful for clinical risk models after intermediate/mart work. Note: treatment-plan
usage at the clinic is very low (~220 plans vs 40K+ patients); procedure-based acceptance
(`int_procedure_acceptance`) is the reliable path.

---

## Existing rule-based scores (baselines)

These heuristics in dbt are **not ML** but provide benchmarks, weak labels, and production
fallbacks if models are unavailable.

| Score | Model | Range / values |
| --- | --- | --- |
| `churn_risk_score` | `mart_patient_retention` | 0–100 |
| `churn_risk_category` | `mart_patient_retention` | Low / Medium / High Risk |
| `onboarding_success_score` | `mart_new_patient` | 0–100 |
| `collection_priority_score` | `mart_ar_summary` | 0–100 |
| `recovery_priority_score` | `mart_revenue_lost` | 0–100 |

Any ML model should be evaluated against these rules before replacing them in production.

---

## Candidate ML targets

### Tier 1 — highest value, data ready

| Target | Label source | Features | Model type |
| --- | --- | --- | --- |
| **Patient churn** | `retention_status = 'Lost'`, 12–18 month lapse, `journey_stage` | Visit frequency, no-show rate, days since last visit, hygiene, comm response, production | Binary classification or survival |
| **No-show** | `fact_appointment.is_no_show`, `is_broken` | Patient history, lead time, DOW, appt type, prior no-shows, confirmations | Binary classification at booking |
| **New patient early value** | 90-day / 1-year production, `returned_within_90_days` | First-visit completion, early production, referral source, insurance, demographics | Regression or high/low classification |

### Tier 2 — strong business case, moderate effort

| Target | Label source | Features | Model type |
| --- | --- | --- | --- |
| **Treatment acceptance** | `int_procedure_acceptance.is_accepted` | Presented amount, procedure category, provider, payer history | Binary / amount regression |
| **Collection probability** | Payment within N days, balance cleared | Aging buckets, payment velocity, insurance vs patient portion | Binary or survival (days-to-pay) |
| **Claim denial** | `fact_claim.payment_status_category` | Carrier, procedure code, billed amount, provider, payer history | Binary classification |
| **Hygiene recall miss** | Recall overdue, `hygiene_status` in `mart_hygiene_retention` | Interval consistency, no-show history, last hygiene vs target | Binary classification |

### Tier 3 — future / needs new marts

| Target | Blocker | Notes |
| --- | --- | --- |
| **Patient LTV projection** | `mart_patient_lifetime_value` planned, not built | See `docs/dbt/marts_models_plan.md` |
| **Clinical perio risk** | Perio data staged only | Build `int_perio_*` first |
| **Implant conversion** | Small positive class | Start from `mart_untapped_implant_revenue` candidates |
| **Revenue recovery** | Opportunity-level labels sparse | Propensity to recover from `mart_revenue_lost` |

---

## Architecture

### Batch-first (recommended Phase 1)

```text
OpenDental → ETL → PostgreSQL raw
                    ↓
              dbt staging → int → marts
                    ↓
              dbt ml_* models (NEW)
              point-in-time features + training exports
                    ↓
         ┌──────────┴──────────┐
         │                     │
   Offline training         Batch scoring
   (Python / scikit-learn)  (nightly → scored mart table)
         │                     │
         └──────────┬──────────┘
                    ↓
              FastAPI (optional score endpoints)
                    ↓
              React / Power BI / outreach workflows
```

**Design principles:**

1. **Features and labels live in dbt** — same tests, docs, and lineage as existing marts.
2. **Point-in-time joins** — training tables must use `as_of_date` and only data known at that date.
3. **Scores land in PostgreSQL** — batch-scored tables in `marts` or a new `ml` schema; API reads them like any other mart.
4. **Clinic vs demo** — never train on PHI in public infra; synthetic generator for portfolio demos.
5. **Rule-based fallback** — keep existing SQL scores until ML beats them on held-out clinic data.

### Optional: near-real-time (Phase 3+)

See [EVENT_DRIVEN_ANALYTICS_PROPOSAL.md](../streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md).
Kafka replay from marts can refresh features between nightly runs; not required for first models.

### Optional: lakehouse scale (parallel track)

See [databricks_lakehouse_proposal.md](databricks_lakehouse_proposal.md) for Spark/Delta
if multi-clinic volume or feature complexity outgrows Postgres batch scoring.

---

## Proposed dbt additions

### New schema / tag

- Schema: `ml` (or `marts` with `ml_` prefix — prefer **`ml`** for clear separation)
- dbt tag: `ml` for selective `dbt run --select tag:ml`

### Suggested models (Phase 1)

| Model | Grain | Purpose |
| --- | --- | --- |
| `ml_patient_features_snapshot` | patient × `as_of_date` | Point-in-time features for churn / LTV |
| `ml_appointment_features` | appointment | No-show model features at booking time |
| `ml_churn_training` | patient × `observation_date` | Features + label (`churned_within_N_days`) |
| `ml_churn_scores` | patient × `score_date` | Batch inference output joined to `dim_patient` |

### Leakage rules (non-negotiable)

- No `current_date` snapshots as training rows without an explicit `as_of_date` column.
- Labels must be defined **after** the observation window (e.g. observe through T, label churn by T+180).
- Exclude future appointments from features unless the model is scored at booking time with only pre-booking data documented.

---

## Training and evaluation

### Environment

| Stage | Database | Data |
| --- | --- | --- |
| Explore / portfolio | `opendental_demo` | Synthetic via `synthetic_data_generator` |
| Validate | `opendental_analytics` | Clinic PHI — local only |
| CI | `opendental_test` | Fixtures; smoke tests only |

### Tooling (proposed)

- **Training:** Python package at repo root `ml/` (or `tools/ml/`) with `scikit-learn` / `xgboost`
- **Notebooks:** `dbt_dental_models/notebooks/` for exploration; not production path
- **Orchestration:** Airflow task after `dbt_build` for batch scoring (Phase 2)
- **Versioning:** `ml/models/registry/` with model artifact + training metadata JSON

### Evaluation checklist

- Compare AUC / precision-recall vs rule-based `churn_risk_score` (and equivalents)
- Calibration plots for probability outputs used in outreach prioritization
- Segment stability: performance by provider, payer, new vs established patients
- Monitor score distribution drift month-over-month

---

## Serving

Detailed serving design lives in **[ML_SERVING_ARCHITECTURE.md](ML_SERVING_ARCHITECTURE.md)**.

Summary:

| Mode | When | Where scores appear |
| --- | --- | --- |
| **Batch scoring (default)** | Nightly after dbt | `ml.ml_*_scores` table → existing dashboards |
| **API enrichment** | Staff workflows need live lookup | New FastAPI routes in `api/routers/` reading scored marts |
| **Online inference** | Only if sub-daily latency required | Deferred; adds ops burden without clear clinic ROI today |

Existing API already exposes rule-based priorities (`collection_priority_score`, revenue recovery).
ML serving should **extend the same patterns** — read scored tables, do not embed model inference
in the API process until batch latency is proven insufficient.

---

## Phased roadmap

### Phase 0 — Planning (current)

- [x] Inventory datasets and targets (this document)
- [ ] Stakeholder pick: one Tier 1 target (recommend churn or no-show)
- [ ] Define success metric with practice (e.g. top-decile churn capture rate)

### Phase 1 — Feature mart + baseline

- [ ] Add `ml_churn_training` (or no-show equivalent) with point-in-time logic
- [ ] Export training set from demo DB; train sklearn baseline
- [ ] Evaluate vs `churn_risk_score`; document in `ML_EVALUATION.md`

### Phase 2 — Batch scoring in production path

- [ ] `ml_churn_scores` mart populated nightly
- [ ] Airflow task: train/score script after dbt
- [ ] Frontend or API: "ML churn risk" column alongside rule-based score

### Phase 3 — Second model + monitoring

- [ ] No-show or new-patient value model
- [ ] Drift checks, model registry, rollback to rule-based scores

### Phase 4 — Optional enhancements

- Streaming feature refresh (Kafka proposal)
- Clinical features (perio mart)
- Databricks path for multi-entity scale

---

## Risks and constraints

| Risk | Mitigation |
| --- | --- |
| **PHI exposure** | Train on demo; clinic validation local only; no model artifacts in public repos |
| **Label leakage** | Point-in-time dbt models; peer review feature lists |
| **Low positive class** | Implant / recovery models need careful sampling; start with churn/no-show |
| **Stakeholder trust** | Show side-by-side with rule scores; explain top features |
| **Ops overhead** | Batch-first; defer online inference |
| **Sparse treatment plans** | Use procedure-based acceptance, not `treatplan` signatures |

---

## Open decisions

1. **First target:** churn vs no-show vs new-patient value?
2. **Schema name:** `ml` vs `marts` prefix?
3. **Code location:** `ml/` at repo root vs `tools/ml/`?
4. **Replace or augment rule scores** in UI when ML is available?
5. **Outreach integration:** export scores to OpenDental comms / recall workflows?

---

## Document map

| Question | Read |
| --- | --- |
| What can we predict? What data exists? | This document |
| How do scores get to staff / dashboards? | [ML_SERVING_ARCHITECTURE.md](ML_SERVING_ARCHITECTURE.md) |
| How are features built in SQL? | Future `ML_FEATURE_ENGINEERING.md`; until then `dbt_dental_models/models/marts/` |
| How do BI tools consume scores? | `docs/analytics/` |
| Near-real-time features? | `docs/streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md` |
