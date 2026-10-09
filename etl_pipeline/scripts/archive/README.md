# Archived ETL scripts

Not used by the nightly pipeline. See `scripts/archive/README.md` for the two-quarter deletion rule.

| File | Replacement |
|------|-------------|
| `purge_raw_payment_phantoms.py` | `etl_pipeline/scripts/purge_raw_phantoms.py` (pass `payment` plus PayNum values) |
| `test_performance_optimizations.py` | Manual benchmark from an early loader pass. Unit coverage is `etl_pipeline/tests/unit/core/simple_mysql_replicator/test_performance_optimizations_unit.py`. |

Still active, on purpose:

- `investigate_payment_drift.py` and `investigate_replica_phantoms.py` — PK-level phantom checks documented in `docs/etl/ETL_SYNC_SEMANTICS.md`
- `method_tracker.py` — imported by the replicator and CLI
- `analyze_connection_usage.py` — connection review described in `etl_pipeline/README.md`
- `test_root_connections.py` — prerequisite called out by `grant_etl_privileges.py`
