# OpenDental source — design notes for analytics

This folder documents **how OpenDental writes to clinic MySQL**, inferred from
schema, clinic behavior, and ETL findings. It is the source-side companion to
our ingestion docs under [`docs/etl/`](../etl/).

| Doc | Role |
| --- | --- |
| [WRITE_LAYER_AND_ETL.md](WRITE_LAYER_AND_ETL.md) | OD write-layer software design and what it implies for watermark ETL |
| [INTERACTIVE_WRITE_BEHAVIOR_TESTS.md](INTERACTIVE_WRITE_BEHAVIOR_TESTS.md) | Hands-on staff/portal experiments + MySQL before/after queries + results worksheet |

**Pipeline contract (ours):** [ETL_SYNC_SEMANTICS.md](../etl/ETL_SYNC_SEMANTICS.md)  
**Why incremental is not CDC:** [ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](../etl/ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md)  
**Fidelity work:** [ETL_REPLICA_FIDELITY_ROADMAP.md](../etl/ETL_REPLICA_FIDELITY_ROADMAP.md)

Keep OD product facts here. Keep replicator/loader behavior in `docs/etl/`.
Record interactive test outcomes in the worksheet (or a finding) and fold durable conclusions into `WRITE_LAYER_AND_ETL.md`.
