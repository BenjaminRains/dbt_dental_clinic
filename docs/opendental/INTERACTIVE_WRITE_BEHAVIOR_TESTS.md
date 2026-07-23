# Interactive OpenDental write-behavior tests

**Purpose:** Empirically verify how OpenDental writes to clinic MySQL when a human
uses the product — so ETL watermark / lookback decisions rest on evidence, not
DDL guesses.

**Who runs it:** You (staff OD client + optional patient/portal path), with DBeaver
on **source** MySQL `opendental`.

**Companion design notes:** [WRITE_LAYER_AND_ETL.md](WRITE_LAYER_AND_ETL.md)  
**Roadmap checkbox this closes:** [ETL_REPLICA_FIDELITY_ROADMAP §1.5](../etl/ETL_REPLICA_FIDELITY_ROADMAP.md#15-spot-edit-verification-matrix-manual-one-time)

---

## 0. Rules of engagement

| Rule | Why |
| --- | --- |
| Prefer a **dedicated test patient** (or your own chart only for low-risk demographic edits) | Avoid corrupting production clinical/financial history |
| One experiment at a time; wait ~2–5s after OD save before querying | Avoid racing the client’s commit |
| Record **wall-clock** of the click/save (clinic local time) | Correlate with `DateTStamp` / `SecDateTEdit` |
| Do **not** run ETL mid-experiment unless the step says so | Keep “OD wrote X” separate from “pipeline copied X” |
| Revert soft changes when done (phone, notes, status) | Leave the chart tidy |
| Financial / claim / delete experiments are **optional Tier C/D** — use throwaway amounts or reverse immediately | AR and claims are easy to bruise |

**Pass criteria (per experiment):**

| Result | Meaning for ETL |
| --- | --- |
| **PASS — stamp advanced** | Watermark incremental *can* see this edit (same day) |
| **FAIL — stamp silent** | Need lookback / full refresh / CDC for this path |
| **SOFT** | Status/`IsDeleted` only; row still present — dbt filter, not delete-sync |
| **HARD** | Row gone from source — phantoms until purge/full refresh |

Fill results in [§6 worksheet](#6-results-worksheet).

---

## 1. Setup (once)

### 1.1 Pick subjects

| Role | Value (fill in) | Notes |
| --- | --- | --- |
| Test patient name | RainsTEST | Prefer unused / clearly labeled test chart |
| `PatNum` | **32390** | From OD Patient Info or query below |
| Your staff `UserNum` (optional) | | For `SecUserNumEntry` / securitylog checks |
| OD version (Help → About) | | Record for upgrade diffs later |
| Clinic local timezone | | e.g. America/Los_Angeles |

Note: `32390` has `PatStatus = 4` (deceased). Fine for A1/A2/D; for **B1 charting**, either change status to Patient (0) temporarily or use an active test chart (e.g. `28837` Test Kid / `30321` Tester Test).


```sql
-- DBeaver / source MySQL (opendental)
-- Find patient by name fragment
SELECT PatNum, LName, FName, PatStatus, DateTStamp, SecDateEntry
FROM patient
WHERE LOWER(LName) LIKE '%test%' OR LOWER(FName) LIKE '%test%'
   OR LOWER(LName) LIKE '%patient%' OR LOWER(FName) LIKE '%patient%'
ORDER BY PatNum DESC
LIMIT 20;
```

### 1.2 Baseline snapshot helper

Before every experiment, run the “BEFORE” query for that table and paste/save:

- Primary key
- Current watermark column (`DateTStamp` or `SecDateTEdit`)
- 1–2 business columns you will change

After save, run the matching “AFTER” query. Compare:

1. Did the **PK stay the same**? (expect yes for edits)
2. Did the **watermark column increase**?
3. Did any **side table** get a new row? (`securitylog`, `entrylog`, `histappointment`, …)

### 1.3 Optional: confirm no triggers (sanity)

```sql
SELECT trigger_schema, trigger_name, event_object_table
FROM information_schema.triggers
WHERE trigger_schema = 'opendental';
```

Expect **0 rows**.

---

## 2. Tier A — low risk (do these first)

These edits are easy to reverse and high value for ETL (`patient`, `commlog`).

### A1 — Patient demographic edit (staff client)

**Hypothesis:** Editing phone/address bumps `patient.DateTStamp`; `PatNum` unchanged.

| Step | Action |
| ---: | --- |
| 1 | BEFORE query (below) |
| 2 | In OD: open test patient → Edit → change **HmPhone** or **Address** by one digit/letter → OK |
| 3 | AFTER query |
| 4 | Revert the field to original |

```sql
-- A1 BEFORE / AFTER (same query)
-- Replace 32390 with your test PatNum if different (RainsTEST = 32390)
SELECT PatNum, HmPhone, Address, WirelessPhone,
       DateTStamp, SecDateEntry, SecUserNumEntry
FROM patient
WHERE PatNum = 32390;
```

**Also check (optional):** new `securitylog` row?

```sql
SELECT SecurityLogNum, PermType, LogDateTime, UserNum, FKey, LogText
FROM securitylog
WHERE FKey = 32390
  AND LogDateTime >= NOW() - INTERVAL 15 MINUTE
ORDER BY SecurityLogNum DESC
LIMIT 20;
```

| Field | Expected | Observed |
| --- | --- | --- |
| `PatNum` | unchanged | |
| `DateTStamp` | advances | |
| `securitylog` | maybe new row | |

---

### A2 — Commlog create then edit (staff)

**Hypothesis:** Create = new `CommlogNum` + stamp; edit = **same** `CommlogNum`, `DateTStamp` advances.

| Step | Action |
| ---: | --- |
| 1 | Add Comm Log on test patient (type/note something unique, e.g. `ETL-TEST-A2`) → save |
| 2 | Query newest row for that patient (CREATE snapshot) |
| 3 | Re-open that commlog → change note or mode/status → save |
| 4 | Re-query **same** `CommlogNum` (EDIT snapshot) |
| 5 | Delete or leave marked as test note |

```sql
-- A2 after create: find the row
SELECT CommlogNum, PatNum, CommDateTime, Mode_, SentOrReceived, Note, DateTStamp
FROM commlog
WHERE PatNum = 32390
  AND Note LIKE '%ETL-TEST-A2%'
ORDER BY CommlogNum DESC
LIMIT 5;

-- A2 after edit: pin by CommlogNum (replace 0 with the value from create)
SELECT CommlogNum, Note, Mode_, DateTStamp
FROM commlog
WHERE CommlogNum = 0;
```

| Check | Expected | Observed |
| --- | --- | --- |
| Create → new `CommlogNum` | yes | |
| Edit → same `CommlogNum` | yes | |
| Edit → `DateTStamp` advances | ? | |

---

### A3 — Patient portal / online account change (optional second path)

Only if you have Patient Portal (or similar) access for the test chart.

**Hypothesis:** Portal writes may use a **different code path** than the staff client (stamp / user fields may differ).

| Step | Action |
| ---: | --- |
| 1 | Staff BEFORE on `patient` (A1 query) |
| 2 | As patient: change an allowed field (email, phone, address — whatever portal permits) → save |
| 3 | AFTER on `patient` |
| 4 | Compare to A1: same stamp behavior? `SecUserNumEntry` vs portal user? |

Record under worksheet row **A3** separately from A1 — do not merge results.

---

## 3. Tier B — clinical (test patient only)

### B1 — Procedure: Treatment Planned → Complete

**Hypothesis:** Same `ProcNum`; `ProcStatus` changes; `DateTStamp` advances.  
This is the [ETL-FND-001](../findings/ETL-FND-001-replica-row-drift-procedurelog.md) path.

| Step | Action |
| ---: | --- |
| 1 | Chart a **harmless** TP procedure on test patient (or use an existing TP you can complete/delete) |
| 2 | BEFORE query on that `ProcNum` |
| 3 | Set procedure **Complete** (and set a clear `DateComplete` if prompted) |
| 4 | AFTER query |
| 5 | Prefer **delete/set back** only if clinic policy allows on test chart; otherwise leave TP fee $0 / note `ETL-TEST` |

```sql
-- Replace 0 with your ProcNum
SELECT ProcNum, PatNum, ProcStatus, ProcFee, ProcDate, DateComplete,
       DateTStamp, DateEntryC, SecUserNumEntry
FROM procedurelog
WHERE ProcNum = 0;
```

| Check | Expected | Observed |
| --- | --- | --- |
| `ProcNum` unchanged | yes | |
| `ProcStatus` TP → Complete | yes (often 1 → 2) | |
| `DateTStamp` advances | **critical** | |

If `DateTStamp` does **not** advance here, watermark-only sync for `procedurelog` is proven insufficient for this UI path (lookback already compensates for recent `DateComplete` / `ProcDate`).

---

### B2 — Appointment edit (optional)

**Hypothesis:** Same `AptNum`; `DateTStamp` advances; may insert `histappointment`.

```sql
-- Replace 0 with your AptNum
SELECT AptNum, PatNum, AptStatus, AptDateTime, Note, DateTStamp
FROM appointment
WHERE AptNum = 0;

SELECT HistApptNum, AptNum, HistApptAction, AptDateTime, DateTStamp
FROM histappointment
WHERE AptNum = 0
ORDER BY HistApptNum DESC
LIMIT 10;
```

Edit note or time on a **future test appointment** only.

---

## 4. Tier C — financial (optional, careful)

Use **tiny** amounts on the test patient and reverse immediately. Skip if you are unsure.

### C1 — Payment amount tweak

```sql
-- Replace 0 with your PayNum
SELECT PayNum, PatNum, PayDate, PayAmt, PayType, SecDateTEdit, SecDateEntry, SecUserNumEntry
FROM payment
WHERE PayNum = 0;
```

Edit amount by $0.01 → save → re-query → **put amount back**.

### C2 — Adjustment amount tweak

```sql
-- Replace 0 with your AdjNum
SELECT AdjNum, PatNum, AdjDate, AdjAmt, AdjType, SecDateTEdit, SecDateEntry
FROM adjustment
WHERE AdjNum = 0;
```

Same pattern: ±$0.01 → verify `SecDateTEdit` → revert.

### C3 — Claimproc (only if you have a safe test claim)

```sql
-- Replace 0 with your ClaimProcNum
SELECT ClaimProcNum, ClaimNum, ProcNum, Status, InsPayAmt, WriteOff, DateCP, SecDateTEdit
FROM claimproc
WHERE ClaimProcNum = 0;
```

Prefer status/note-ish fields over paid amounts if the UI allows; otherwise skip.

---

## 5. Tier D — delete / soft-delete (optional)

Run only on **test-patient-owned** rows you created in Tier A/B.

### D1 — Soft patterns

| Action | Tables to watch | What “soft” looks like |
| --- | --- | --- |
| Archive / inactive patient | `patient` | `PatStatus` change; row remains |
| Delete sheet (if you created one) | `sheet` | `IsDeleted = 1` (typical) |
| Hide provider/carrier (do **not** on production catalogs) | — | skip |

### D2 — Hard delete

| Action | Watch | What “hard” looks like |
| --- | --- | --- |
| Delete the A2 test `commlog` | `commlog` | `SELECT` by `CommlogNum` returns **0 rows** |
| Delete a future test appointment | `appointment` + `histappointment` | Live row gone; hist may gain action=Deleted |

```sql
-- After attempted delete (replace 0 with the PK you deleted)
SELECT COUNT(*) AS still_there FROM commlog WHERE CommlogNum = 0;
SELECT COUNT(*) AS still_there FROM appointment WHERE AptNum = 0;
```

If `still_there = 0`, ETL incremental **cannot** remove the replica row — phantom risk confirmed for that table.

---

## 6. Results worksheet

Copy outcomes here (or into a dated note under `docs/findings/` if something surprising appears).

| ID | Date | OD path (staff / portal) | PK | Watermark col | Advanced? (Y/N/Δsec) | Side tables | Verdict | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A1 | | staff | PatNum= | DateTStamp | | securitylog? | | |
| A2 create | | staff | CommlogNum= | DateTStamp | | | | |
| A2 edit | | staff | same? | DateTStamp | | | | |
| A3 | | portal | PatNum= | DateTStamp | | | | |
| B1 | | staff | ProcNum= | DateTStamp | | | | |
| B2 | | staff | AptNum= | DateTStamp | | hist? | | |
| C1 | | staff | PayNum= | SecDateTEdit | | | | |
| C2 | | staff | AdjNum= | SecDateTEdit | | | | |
| C3 | | staff | ClaimProcNum= | SecDateTEdit | | | | |
| D1 | | | | | | | SOFT/… | |
| D2 | | | | | | | HARD/… | |

**ETL decision summary (fill after Tier A+B):**

| Question | Answer |
| --- | --- |
| Trust timestamp watermark for `patient` / `commlog`? | |
| Trust timestamp watermark for `procedurelog` TP→Complete? | |
| Any FAIL → stamp silent paths? | |
| Any HARD deletes observed? | |
| Portal path differs from staff? | |

When complete: update [WRITE_LAYER_AND_ETL.md §5](WRITE_LAYER_AND_ETL.md) status column and tick roadmap §1.5.

---

## 7. Optional follow-on: “did ETL see it?”

Only after stamps are understood. Same night or next incremental:

1. Note watermark columns + PKs from a PASS experiment.
2. After nightly (or a targeted `mdc etl` for that table), check `raw.<table>` for the PK and updated attributes.
3. If OD stamp advanced but `raw` is stale → pipeline bug (config/replicator), not OD.
4. If OD stamp silent and `raw` stale → expected without lookback/full refresh.

Do not conflate the two failure modes in the worksheet.

---

## 8. Suggested session order (≈60–90 minutes)

1. Setup §1 + trigger check  
2. **A1** patient phone (5 min)  
3. **A2** commlog create+edit (10 min)  
4. **B1** TP→Complete on test proc (15–20 min)  
5. **D2** delete the test commlog (5 min)  
6. Optional: A3 portal, B2 appt, C1 payment ±$0.01  
7. Fill §6 summary → paste key cells into WRITE_LAYER §5  

---

*Document version: 1.0 (2026-07-21). Interactive protocol; results belong in §6 or a finding doc.*
