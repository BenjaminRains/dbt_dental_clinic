# ML Serving Architecture (Outline)

> **Status: OUTLINE — fill in when implementing Phase 2 batch scoring.**
> Parent strategy: [ML_ANALYTICS_PROPOSAL.md](ML_ANALYTICS_PROPOSAL.md)

This document defines **how trained models reach users**. Feature engineering and target selection
belong in the proposal; **this file is the home for inference, delivery, and operations**.

---

## Why a separate serving doc

| Concern | Proposal (`ML_ANALYTICS_PROPOSAL.md`) | Serving (this doc) |
| --- | --- | --- |
| What to predict | Targets, labels, datasets | — |
| How to train | Tooling, evaluation, leakage rules | Training job → artifact handoff |
| How scores run in prod | High-level batch vs API | Concrete paths, SLAs, rollback |
| Who consumes scores | Stakeholder use cases | Endpoints, tables, dashboard wiring |

Keeping serving separate avoids a single monolithic doc and matches how other domains split
planning (`docs/streaming/`) from deployment runbooks (`docs/deployment/`).

---

## Serving modes (planned)

### 1. Batch scoring — default

**When:** Nightly after ETL + dbt (same cadence as existing KPIs).

```text
Airflow dbt_build
    → Python score script (reads ml_*_training features, writes scores)
    → PostgreSQL ml.ml_*_scores (or marts.ml_*_scores)
    → FastAPI / React / Power BI read scored table
```

**Properties:**

- Aligns with clinic batch ETL; no new latency requirements
- Model inference runs once per day on EC2/clinic host, not in API workers
- Rollback = skip score task and fall back to rule-based marts

**Open items to specify at implementation:**

- [ ] Table grain (`patient_id` × `score_date` vs appointment-level)
- [ ] Retention policy for historical scores
- [ ] Idempotent upsert vs append-only score history

### 2. API read-through — staff workflows

**When:** Dashboards or integrations need per-patient lookup of latest score.

**Pattern:** Extend existing FastAPI services (`api/services/`) to `SELECT` from scored mart —
same as `collection_priority_score` and revenue recovery today.

**Do not** load sklearn/xgboost inside uvicorn workers for Phase 1–2.

**Open items:**

- [ ] New routes vs extend `api/routers/patient.py`
- [ ] Pydantic response models in `api/models/`
- [ ] `bi_user` / API grants for score columns

### 3. Online inference — deferred

**When:** Sub-daily scoring is required (e.g. score at appointment booking in OpenDental).

**Cost:** Model server, versioning, autoscaling, monitoring — disproportionate for current
clinic volume.

Revisit only after batch scoring is in production and latency gap is documented.

---

## Model artifact handling

| Artifact | Location (proposed) | Notes |
| --- | --- | --- |
| Serialized model | `ml/models/artifacts/<model_name>/<version>/` | Not committed for clinic-trained weights |
| Metadata | `model_card.json` — features, metrics, train date | Committed for demo models only |
| Registry index | `ml/models/registry.json` | Points to active version per environment |

**Environment rule:** Demo/portfolio may commit a small demo model; clinic artifacts stay on host
or private object storage, never in public git.

---

## Integration with existing platform

| Consumer | Integration |
| --- | --- |
| React dashboard | New KPI or patient list column from API |
| Power BI | `GRANT SELECT` on score table; see `docs/analytics/` |
| Airflow | Task after `dbt_build` in nightly DAG |
| dbt exposures | Optional `exposures.yml` entry for `ml_churn_scores` |
| OpenDental outreach | Future export CSV / webhook — out of scope Phase 2 |

---

## Monitoring and rollback

| Check | Action |
| --- | --- |
| Score distribution drift | Compare monthly histogram to training baseline |
| Missing scores | Alert if row count ≠ eligible population |
| Model failure | Airflow task fails → previous night's scores remain; UI shows stale badge |
| Quality regression | Auto-fallback flag to rule-based `churn_risk_score` |

Detailed runbooks belong here as checklists when Phase 2 starts.

---

## Security

- Scored tables contain PHI at clinic — same RDS access controls as `marts`
- API key auth unchanged; no public demo of clinic ML scores
- Audit log for bulk score exports if added

---

## Next steps

1. Pick first model (see proposal).
2. Define `ml_churn_scores` table contract (columns, grain, indexes).
3. Implement batch score script + Airflow task.
4. Add API route reading latest score per patient.
5. Expand this outline into operational runbooks.
