# `docs/ml/` — Machine Learning

Documentation for ML strategy, feature engineering, training, and serving on top of the
existing analytics warehouse.

## What belongs here

| Document | Purpose | When to write |
| --- | --- | --- |
| [ML_ANALYTICS_PROPOSAL.md](ML_ANALYTICS_PROPOSAL.md) | Strategy: available datasets, candidate targets, architecture, phased roadmap | **Now** — draft / planning |
| [ML_SERVING_ARCHITECTURE.md](ML_SERVING_ARCHITECTURE.md) | Inference paths: batch scoring, FastAPI endpoints, model registry, monitoring, rollback | When moving past exploration |
| `ML_FEATURE_ENGINEERING.md` (future) | dbt models for point-in-time features, leakage rules, training exports | When building first training mart |
| `ML_EVALUATION.md` (future) | Metrics, baselines vs rule-based scores, drift checks | When first model is trained |

## What belongs elsewhere

| Topic | Location | Why |
| --- | --- | --- |
| dbt mart definitions and business logic | `docs/dbt/`, `dbt_dental_models/models/marts/` | Source of truth for features and rule-based scores |
| BI / dashboard consumption of scores | `docs/analytics/` | Power BI, Looker, exposures — read scored marts or API |
| Near-real-time feature refresh | `docs/streaming/EVENT_DRIVEN_ANALYTICS_PROPOSAL.md` | Optional streaming path; not required for batch ML |
| Lakehouse / Spark ML at scale | `docs/databricks_lakehouse_proposal.md` | Parallel portfolio path |
| Consultation audio NLP (Whisper + Claude) | `consult_audio_pipe/README.md` | Separate pipeline; not warehouse-integrated today |
| Synthetic training sandbox | `etl_pipeline/synthetic_data_generator/` | HIPAA-safe demo data for public experiments |

## Related code (today)

- Rule-based scores: `mart_patient_retention`, `mart_new_patient`, `mart_ar_summary`, `mart_revenue_lost`
- API surfaces scores indirectly: `api/services/` (AR, revenue, hygiene)
- Exploratory stack: `dbt_dental_models/Pipfile` (`scikit-learn`, Jupyter)
