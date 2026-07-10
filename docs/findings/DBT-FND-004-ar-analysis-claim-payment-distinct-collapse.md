# DBT-FND-004 — `int_ar_analysis` claim payment DISTINCT collapses line pays

**Type:** Mart / dbt defect  
**Status:** Fixed (2026-07-09 Batch 9b)  
**Found:** 2026-07-09 (dbt test failure triage Batch 9b)  
**Primary model:** `int_ar_analysis`

## Summary

`PatientPayments` / `PaymentTypeMetrics` built from
`UniquePaymentCombinations`:

```sql
SELECT DISTINCT patient_id, claim_payment_id, paid_amount
FROM int_claim_payments
JOIN int_claim_details ON claim_id AND procedure_id
```

`int_claim_payments.paid_amount` is **claimproc-level** (`InsPayAmt`). Many
checks have multiple lines with the **same dollar amount**. `DISTINCT` kept one
row per amount per check, undercounting ~$234k across 688 patients.

Example patient 33056 / `claim_payment_id` 21792: 9×$644 + 5×$155 + $34 + 2×$27
= **$6,659** true; DISTINCT amounts sum to **$860**.

`COUNT(DISTINCT claim_payment_id)` still matched (688 amount mismatches, 0 count
mismatches). Reconcile tests comparing to naive `SUM(paid_amount)` from
`int_claim_payments` correctly failed (~34k expression rows).

## Fix

Aggregate `int_claim_payments` by `patient_id` directly (model already has
`patient_id`). Drop the `claim_details` join and the DISTINCT CTE. Keep
`claim_payment_id IS NOT NULL` (includes `0`) so counts stay aligned with
existing reconcile tests.

**Verified 2026-07-09:** all three claim-payment reconcile `expression_is_true`
tests PASS. Two patients now exceed the `$25k` `total_claim_payments` band
(expected after correcting the undercount).

## Related

- [DBT-FND-003](./DBT-FND-003-claim-snapshot-claimproc-fanout.md) — similar
  claim+procedure grain hazard; that case was fan-out, this was collapse.
