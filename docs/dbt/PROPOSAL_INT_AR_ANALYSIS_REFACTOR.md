# Proposal: Split `int_ar_analysis` into smaller intermediate models

**Status:** Proposal only — no model changes are planned yet.  
**Target:** `models/intermediate/system_d_ar_analysis/int_ar_analysis.sql`  
**Related:** `dbt_project_evaluator` rule `fct_too_many_joins` (high join count on this model).

---

## Context

`int_ar_analysis` is a patient-grain AR snapshot: one row per active patient, combining balances, payment behavior, adjustments, claim activity, demographics, and insurance context. It is already built from well-factored upstream models (`int_ar_balance`, `int_patient_profile`, `int_claim_details`, etc.) and uses clear, named CTEs.

The evaluator’s `fct_too_many_joins` surfaced this model with a high **join count** (e.g. 14 in a sample run). That metric counts **joins across the entire compiled SQL**, including nested CTEs—not only the five `LEFT JOIN`s in the final `SELECT`. The concentration of joins is largely in:

1. **`ClaimActivity`** — claim status, tracking, claim payments, and payment-type metrics (nested sub-CTEs and multiple joins).
2. **`PaymentActivity`** — patient vs insurance payment totals, plus split / merchant / system metrics (`int_payment_split` + `int_insurance_payment_allocated`).

## Assessment

- **Conceptual complexity is appropriate:** A single wide patient-level AR intermediate is a reasonable pattern for downstream marts.
- **Mechanical complexity is high in two blocks:** `ClaimActivity` and `PaymentActivity` are the best candidates for extraction if we want shorter files, easier tests, and a lower join count in `int_ar_analysis` itself.

## Proposed direction (when we choose to implement)

### Phase 1 — `int_ar_claim_activity` (patient grain)

- **Purpose:** One row per `patient_id` with all columns currently produced by the **`ClaimActivity`** CTE block (pending/denied claims, amounts, claim payments, payment-type breakdowns, benefit fields, etc.).
- **Implementation:** Lift the existing `ClaimActivity` SQL (and its inner CTEs) into `int_ar_claim_activity.sql`, with `unique_key: patient_id` (or equivalent) and tests/docs aligned with `_int_ar_analysis.yml` / new YAML.
- **`int_ar_analysis` change:** Replace the inline CTE with `LEFT JOIN {{ ref('int_ar_claim_activity') }}` on `patient_id` and select the same output columns as today.

### Phase 2 — `int_ar_payment_activity` (patient grain)

- **Purpose:** One row per `patient_id` with all columns from **`PaymentActivity`** (patient vs insurance payment counts, totals, splits, merchant fees, check/bank metadata, etc.).
- **Implementation:** Same pattern as Phase 1: move CTE body to `int_ar_payment_activity.sql`, then ref from `int_ar_analysis`.

### Phase 3 (optional) — Patient + insurance context

- **Purpose:** Only if we want further thinning or reuse elsewhere: extract **`BasePatientInfo`**, **`InsuranceCoverage`**, and **`PatientInfo`** into something like `int_ar_patient_insurance_context` (patient grain). **Lower priority** unless another mart needs the same bundle.

## Expected benefits

- **Smaller, easier-to-review** `int_ar_analysis` focused on joining pre-aggregated patient-grain pieces.
- **Targeted tests** on claim vs payment intermediates without running the full AR stack.
- **Likely reduction** in `fct_too_many_joins` count for `int_ar_analysis.sql` (evaluator counts joins in the file).
- **Clearer ownership** boundaries (claims vs payments vs final AR assembly).

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Duplicate or drifted logic if intermediates change | Keep column contracts identical; add schema tests; run `dbt build` + evaluator before/after. |
| More models to maintain | Acceptable tradeoff; aligns with existing `int_*` layering. |
| YAML / docs duplication | Split or reference shared column descriptions; update `_int_ar_analysis.yml` incrementally. |

## Explicitly out of scope for this proposal

- Changing **business logic** or **grain** (still one row per patient in `int_ar_analysis`).
- Renaming existing downstream columns consumed by marts (unless we add a compatibility pass).
- Implementing the refactor in this iteration — **this document is planning only.**

## Suggested next steps (when prioritized)

1. Confirm downstream refs (marts, API, BI) rely only on **`int_ar_analysis`** outputs (not raw CTE names — they are internal only).
2. Implement Phase 1 (`int_ar_claim_activity`), run full test suite + `dbt_project_evaluator`, compare row counts and key metrics to baseline.
3. Implement Phase 2 (`int_ar_payment_activity`), same validation.
4. Revisit Phase 3 only if needed for reuse or further simplification.

---

**References**

- Source model: `dbt_dental_models/models/intermediate/system_d_ar_analysis/int_ar_analysis.sql`
- Model tests/docs: `dbt_dental_models/models/intermediate/system_d_ar_analysis/_int_ar_analysis.yml`
- Evaluator rules: [dbt-project-evaluator — rules](https://dbt-labs.github.io/dbt-project-evaluator/latest/)
