# OpenDental write layer — design and ETL implications

**Document type:** Source-system design notes (inferred from schema, DDL, clinic
behavior, and ETL findings). Not an Open Dental Inc. official architecture guide.

**Audience:** Anyone changing ETL ingestion, `tables.yml` sync profiles, Layer 0
checks, or deciding whether watermark sync is enough vs binlog CDC.

**Companions (our pipeline):**

| Doc | Role |
| --- | --- |
| [ETL_SYNC_SEMANTICS.md](../etl/ETL_SYNC_SEMANTICS.md) | What *we* insert / upsert / never delete |
| [ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md](../etl/ETL_CDC_IMPLEMENTATION_AND_OPTIONS.md) | Watermark design + CDC options |
| [ETL_REPLICA_FIDELITY_ROADMAP.md](../etl/ETL_REPLICA_FIDELITY_ROADMAP.md) | Lookback, Layer 0, Sunday full, deferred CDC |

---

## 1. One-sentence model

OpenDental treats clinic MySQL as a **mutable, application-owned datastore**:
thick-client (.NET) code issues INSERT/UPDATE/DELETE against wide MyISAM tables
with auto-increment `*Num` keys, soft lifecycle flags, light row-level audit
columns, and **no declarative FKs, no triggers, and no change-data product**.

Our ETL hitchhikes on those audit timestamps and business dates. That is a
**convenience**, not a contract OpenDental ships for analytics.

---

## 2. Software design at the write layer

### 2.1 Stack shape (inferred)

```text
OpenDental UI / forms / modules
        │
        ▼
Application CRUD / domain logic (C#)
        │  parameterized INSERT / UPDATE / DELETE
        │  occasional side writes (securitylog, entrylog, hist*)
        ▼
MySQL / MariaDB  — database `opendental`
        │  MyISAM tables, indexes, ON UPDATE timestamps (where declared)
        ▼
(no outbox, no binlog consumer required by the product)
```

- **Integrity lives in the app**, not the database.
- **Identity is insert-once:** new entities get a new `*Num`; edits keep the same PK.
- **Domain state is column-encoded:** `ProcStatus`, `AptStatus`, `PatStatus`, etc.
- **Audit is mixed:** row `Sec*` / `DateTStamp` fields + append-only logs + selective history tables.

This matches a long-lived dental PMS evolved since the mid-2000s: table-per-entity
CRUD, clinic-local multi-client concurrency, operational correction preferred over
immutable ledgers.

### 2.2 Storage characteristics we observe

| Observation | Evidence in this repo | Engineering meaning |
| --- | --- | --- |
| **MyISAM**, no `FOREIGN KEY` | Captured DDLs under `analysis/*/…_ddl.sql` | Relationships are logical ints + indexes; cascades are app code |
| Auto-increment `*Num` PKs | `ProcNum`, `PatNum`, `PayNum`, … in `tables.yml` | Stable PK across edits → ETL upsert is viable |
| `0` as “no FK” | Staging treats `0` as null link (e.g. appointment) | Pre-nullable / sentinel design |
| Sentinel empty dates | `0001-01-01`, `1900-01-01` cleaned in dbt macros | .NET / OD default “unset” dates |
| No triggers | Confirmed empty `information_schema.triggers` for `opendental` | Nothing DB-side enforces audit beyond column defaults |
| ~446 tables | Analyzer / `tables.yml` | Schema accretes with product features |

### 2.3 How rows change (the mutation model)

Clinical and financial entities are **stateful rows**, not event streams:

| Product action (examples) | Typical DB effect |
| --- | --- |
| Chart a treatment plan procedure | `INSERT procedurelog` (new `ProcNum`) |
| Complete the procedure | `UPDATE` same `ProcNum` (`ProcStatus`, fees, dates, …) |
| Edit patient phone | `UPDATE` same `PatNum` |
| Adjust a payment / claimproc | `UPDATE` same `PayNum` / `ClaimProcNum` |
| Soft-remove patient / sheet | Status / `IsDeleted` / `IsHidden` — often **no hard DELETE** |
| Delete appointment (some paths) | Hard DELETE and/or `histappointment` snapshot with action = Deleted |

**Implication for ETL:** ~50 tables are classified `sync_profile: in_place_updates`
because the product **rewrites** them. Append-only watermark-on-PK only captures
*new* rows; it cannot see attribute churn on an existing PK.

Mutation seed list (analyzer; extend carefully):

`procedurelog`, `claimproc`, `payment`, `adjustment`, `claim`, `patplan`,
`paysplit`, `commlog`, `patient`

### 2.4 Lifecycle: soft state vs hard delete

OpenDental prefers **soft lifecycle** for many entities, but **hard deletes still
occur**.

| Pattern | Examples | ETL consequence |
| --- | --- | --- |
| Status enum | `PatStatus`, `ProcStatus`, `AptStatus` | Upsert if watermark/lookback sees the row; filter in dbt |
| Explicit soft delete / hide | `sheet.IsDeleted`, `IsHidden*` | Row remains; models must filter |
| History event | `histappointment.HistApptAction` (incl. Deleted) | Append-style history; source of truth for “what happened” may be hist, not live row |
| Hard DELETE | Observed via phantom PKs in `raw` | Incremental path **never** removes the replica row ([ETL_SYNC_SEMANTICS §7](../etl/ETL_SYNC_SEMANTICS.md)) |

Do not assume “OD never hard-deletes.” Assume “OD often soft-deletes, sometimes
hard-deletes, and never publishes a delete stream to analytics.”

### 2.5 Audit and “last modified” columns

Three overlapping mechanisms:

**A. Engine auto-touch (`ON UPDATE CURRENT_TIMESTAMP`)**  
Many mutation tables declare e.g.:

```sql
`DateTStamp` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
```

Same pattern appears on `SecDateTEdit` for payments/claims/etc. (see
`analysis/procedurelog/procedurelog_ddl.sql`, `payment`, `claim`, …).

On a normal `UPDATE` that changes any column, MySQL can bump that timestamp
**without the application listing the column**. This is *not* a trigger and *not*
universal:

- Not every table has such a column.
- Not every child/detail table that mutates has a usable stamp (`sheetfield`-class).
- Edge write paths (bulk tools, partial updates, unusual SQL) are unverified.
- Product upgrades can change column defaults.

**B. Application-set security / entry fields**  
`SecUserNumEntry`, `SecDateEntry` / `SecDateTEntry`, sometimes `SecurityHash` —
create-time / accountability fields. `SecDateTEdit` may be both app-set and
`ON UPDATE`-backed depending on table DDL.

**C. Append-only and snapshot side tables**  
`securitylog`, `entrylog` — immutable-ish event logs (good PK watermarks).  
`histappointment`, `taskhist`, `insverifyhist` — selective snapshots when the
product chooses to record history.

**Practical rule for ingestion:** Treat timestamp columns as **best-effort change
signals**. Validate with spot-edits ([roadmap §1.5](../etl/ETL_REPLICA_FIDELITY_ROADMAP.md#15-spot-edit-verification-matrix-manual-one-time));
compensate with lookback + periodic full refresh; do not treat them as CDC.

### 2.6 What OpenDental does *not* provide

| Capability | Status for our clinic source |
| --- | --- |
| Declarative referential integrity | Absent (MyISAM, no FKs) |
| Triggers enforcing audit | Absent |
| Soft-delete flag on every table | Partial / inconsistent |
| Guaranteed bump of watermark on every edit | Unverified per table; empirically insufficient alone ([ETL-FND-001](../findings/ETL-FND-001-replica-row-drift-procedurelog.md)) |
| Published change stream / outbox | Not a product feature |
| Binlog as supported analytics API | Clinic/hosting policy dependent; roadmap Phase 5 only |

---

## 3. Mapping OD design → our ETL contract

```text
OD write behavior                          Our ingestion response
─────────────────                          ─────────────────────
INSERT new *Num                            Watermark / PK capture → upsert
UPDATE same *Num + stamp advances       Timestamp watermark → upsert
UPDATE same *Num, stamp silent         Missed until lookback / full refresh
Hard DELETE                                Phantom row until purge / full refresh
Soft delete / status                       Upsert + filter in dbt
Schema column add/remove                   Schema analyzer → tables.yml
```

### 3.1 Sync profiles (analyzer → `tables.yml`)

| `sync_profile` | OD meaning | Ingestion meaning |
| --- | --- | --- |
| `append_only` | New PK ≈ new fact (logs, or no usable mutation stamp) | PK or single watermark for *inserts*; in-place edits may need full refresh / dbt |
| `in_place_updates` | Same PK is rewritten in product workflows | Prefer `DateTStamp` / `SecDateTEdit` as `replicator_watermark_column`; never PK-as-watermark |

PK-only modeled tables that still mutate in OD (e.g. `sheetfield`) are
**misclassified as durable incremental** if treated as `in_place_updates` without
a timestamp — see [ETL-FND-002](../findings/ETL-FND-002-sync-profile-pk-only-misclassification.md).

### 3.2 Why lookback exists

Business-date lookback (`DateComplete`, `PayDate`, …) re-upserts rows that still
exist in a recent window even if the mutation timestamp did not advance past the
watermark. That is a **compensation for OD’s mutable-row design**, not a second
source of truth.

Lookback does **not** delete missing PKs.

### 3.3 Why Layer 0 and Sunday full refresh exist

| Control | Addresses OD gap |
| --- | --- |
| Layer 0 aggregate drift | Silent missed UPDATEs / phantoms affecting KPIs |
| Sunday scoped full refresh | Accumulated drift + hard deletes on mutation tables |
| Binlog CDC (deferred) | Only path that captures every UPDATE/DELETE without trusting stamps |

---

## 4. Domain cheat sheet (ingestion-relevant)

| Domain | Representative tables | Write style | Typical watermark | Notes |
| --- | --- | --- | --- | --- |
| Clinical production | `procedurelog` | Heavy in-place | `DateTStamp` | TP→Complete proven drift case |
| Appointments | `appointment`, `histappointment` | Mutate + hist | `DateTStamp` / hist PK | Deletes may be hist + hard remove |
| Patient | `patient` | In-place demographics | `DateTStamp` | Soft statuses incl. deleted |
| Payments / AR | `payment`, `paysplit`, `adjustment` | In-place | `SecDateTEdit` | Lookback on business dates |
| Claims | `claim`, `claimproc` | In-place | `SecDateTEdit` | Status/fee churn |
| Comms | `commlog` | In-place | `DateTStamp` | Same `CommlogNum` on edit |
| Forms | `sheet`, `sheetfield` | Mutate; field often weak stamp | Mixed | Field tables: PK capture only risk |
| Security / entry | `securitylog`, `entrylog` | Append-only | PK | Large; chunked incremental OK |

---

## 5. Open questions / validation backlog

Keep answers here (or link findings) so ETL decisions stay evidence-based.

**Protocol:** [INTERACTIVE_WRITE_BEHAVIOR_TESTS.md](INTERACTIVE_WRITE_BEHAVIOR_TESTS.md) (staff client + optional portal; MySQL before/after; results worksheet).

| Question | Why it matters | How to answer | Status |
| --- | --- | --- | --- |
| Does every UI edit on mutation tables bump `DateTStamp` / `SecDateTEdit`? | Trust in timestamp watermark | Interactive tests Tier A–C | **Open** — run protocol |
| Does Patient Portal write path match staff client stamps? | Second code path | Interactive test A3 | Open |
| Which tables hard-delete in normal clinic ops? | Phantom rate | Tier D + phantom scripts + Layer 0 | Partial |
| Do bulk / import / eConnector paths skip `ON UPDATE` stamps? | Edge missed updates | Profile after vendor tools run | Open |
| OD version / schema changelog vs our analyzer | New columns, new stamps | Sunday schema DAG + changelog | Ongoing |

---

## 6. Evidence index

| Claim | Where to look |
| --- | --- |
| `ON UPDATE` timestamps | `analysis/**/**_ddl.sql` (e.g. `procedurelog.DateTStamp`) |
| Mutation seed + lookback | `etl_pipeline/scripts/analyze_opendental_schema.py` (`IN_PLACE_UPDATE_TABLES`, `LOOKBACK_RESYNC_BY_TABLE`) |
| Per-table sync config | `etl_pipeline/etl_pipeline/config/tables.yml` |
| Missed in-place update (proven) | [ETL-FND-001](../findings/ETL-FND-001-replica-row-drift-procedurelog.md) |
| PK-only sync misclassification | [ETL-FND-002](../findings/ETL-FND-002-sync-profile-pk-only-misclassification.md) |
| Staging metadata mapping | [metadata_strategy_guide.md](../dbt/metadata_strategy_guide.md) |
| Sentinel date cleaning | `dbt_dental_models/macros/utils/clean_opendental_dates.sql` |
| Interactive spot-edit protocol | [INTERACTIVE_WRITE_BEHAVIOR_TESTS.md](INTERACTIVE_WRITE_BEHAVIOR_TESTS.md) |

---

## 7. Doc maintenance

- When OpenDental upgrades change DDL defaults or add stamps, update §2.5 and §4.
- When [interactive tests](INTERACTIVE_WRITE_BEHAVIOR_TESTS.md) complete, fold §6 worksheet into this §5 and tick roadmap §1.5.
- Do **not** duplicate replicator/loader SQL here — link `docs/etl/` instead.

---

*Document version: 1.1 (2026-07-21). Linked interactive write-behavior test protocol.*
