-- KPI investigation: mart_daily_payments vs OD Daily Payments (2026-06-24)
-- Run in DBeaver against PostgreSQL opendental_analytics (analytics_user).
--
-- Schemas: raw | staging | int | marts
-- Column names:
--   raw.*           OpenDental names ("PayNum", "PayAmt", "PayDate", "PayType", "PatNum")
--   staging/int/marts  dbt snake_case (payment_id, payment_amount, payment_date, ...)
--
-- OD golden (od_daily_payments_06242026_06242026.csv):
--   net_collections  11197.40  (Metlife 3977.80 + Check 1051.30 + Cash 677 + CC 4172.10 + Cherry 1319.20)
--   mart result      5644.10   → gap -5553.30

-- =============================================================================
-- Query 1: Mart breakdown for the day
-- =============================================================================
SELECT
    payment_date,
    net_collections_amount,
    patient_payment_amount,
    insurance_payment_amount,
    other_payment_amount,
    income_amount,
    refund_amount,
    payment_count
FROM marts.mart_daily_payments
WHERE payment_date = DATE '2026-06-24';

-- =============================================================================
-- Query 2: Insurance claim payments (claimpayment path) — NOT in fact_payment
-- OD "Metlife EFT" section comes from this table, not payment/paysplit.
-- Use staging (renamed columns). raw.claimpayment has "CheckAmt" / "CheckDate".
-- =============================================================================
SELECT
    COUNT(*) AS claim_payment_count,
    SUM(check_amount)::numeric(18, 2) AS claim_payment_total
FROM staging.stg_opendental__claimpayment
WHERE check_date = DATE '2026-06-24';

-- =============================================================================
-- Query 3: Insurance allocations in intermediate layer (same date basis as int model)
-- =============================================================================
SELECT
    COUNT(*) AS allocation_rows,
    SUM(split_amount)::numeric(18, 2) AS insurance_split_total
FROM int.int_insurance_payment_allocated
WHERE payment_date = DATE '2026-06-24';

-- =============================================================================
-- Query 4: fact_payment vs int_payment_split split sums (grain mismatch check)
-- OD report sums split rows by provider; fact_payment uses payment header PayAmt.
-- =============================================================================
WITH header AS (
    SELECT
        SUM(payment_amount)::numeric(18, 2) AS fact_payment_header_total,
        COUNT(*) AS fact_payment_count
    FROM marts.fact_payment
    WHERE payment_date = DATE '2026-06-24'
),
splits AS (
    SELECT
        SUM(split_amount)::numeric(18, 2) AS int_split_total,
        COUNT(*) AS int_split_count
    FROM int.int_payment_split
    WHERE payment_date = DATE '2026-06-24'
)
SELECT * FROM header CROSS JOIN splits;

-- =============================================================================
-- Query 5: Payments in staging missing from fact_payment (filtered out upstream)
-- =============================================================================
SELECT
    p.payment_id,
    p.payment_type_id,
    p.payment_amount,
    p.payment_date,
    p.check_number,
    COUNT(ps.paysplit_id) AS staging_split_count,
    COUNT(ips.paysplit_id) AS int_split_count
FROM staging.stg_opendental__payment p
LEFT JOIN staging.stg_opendental__paysplit ps
    ON p.payment_id = ps.payment_id
LEFT JOIN int.int_payment_split ips
    ON ps.paysplit_id = ips.paysplit_id
LEFT JOIN marts.fact_payment fp
    ON p.payment_id = fp.payment_id
WHERE p.payment_date = DATE '2026-06-24'
  AND fp.payment_id IS NULL
GROUP BY 1, 2, 3, 4, 5
ORDER BY ABS(p.payment_amount) DESC;

-- =============================================================================
-- Query 6: Unallocated splits excluded by int_payment_split.is_valid_allocation
-- (procedure_id IS NULL AND adjustment_id IS NULL → filtered out)
-- =============================================================================
SELECT
    p.payment_id,
    p.payment_type_id,
    p.payment_amount AS header_amount,
    ps.paysplit_id,
    ps.split_amount,
    ps.provider_id,
    ps.procedure_id,
    ps.adjustment_id
FROM staging.stg_opendental__payment p
INNER JOIN staging.stg_opendental__paysplit ps
    ON p.payment_id = ps.payment_id
LEFT JOIN int.int_payment_split ips
    ON ps.paysplit_id = ips.paysplit_id
WHERE p.payment_date = DATE '2026-06-24'
  AND ips.paysplit_id IS NULL
  AND ps.procedure_id IS NULL
  AND ps.adjustment_id IS NULL
ORDER BY ABS(ps.split_amount) DESC;

-- =============================================================================
-- Query 7: payment_type_id breakdown in fact_payment (mapping gap)
-- fact_payment only maps 0=Patient, 1=Insurance; clinic uses 69/70/71/261/303/464/etc.
-- =============================================================================
SELECT
    payment_type_id,
    payment_type,
    COUNT(*) AS payment_count,
    SUM(payment_amount)::numeric(18, 2) AS total_amount
FROM marts.fact_payment
WHERE payment_date = DATE '2026-06-24'
GROUP BY 1, 2
ORDER BY 4 DESC;

-- =============================================================================
-- Query 8: Reconstructed OD-style total (patient splits + claim payments)
-- Hypothesis: gap closes when claim payments and missing unallocated patient pays are added.
-- =============================================================================
WITH patient_splits AS (
    SELECT COALESCE(SUM(split_amount), 0)::numeric(18, 2) AS total
    FROM int.int_payment_split
    WHERE payment_date = DATE '2026-06-24'
),
claim AS (
    SELECT COALESCE(SUM(check_amount), 0)::numeric(18, 2) AS total
    FROM staging.stg_opendental__claimpayment
    WHERE check_date = DATE '2026-06-24'
),
od AS (
    SELECT 11197.40::numeric(18, 2) AS total
)
SELECT
    od.total AS od_net_collections,
    ps.total AS int_payment_split_sum,
    c.total AS claimpayment_sum,
    (ps.total + c.total) AS reconstructed_total,
    (ps.total + c.total) - od.total AS difference_vs_od
FROM od
CROSS JOIN patient_splits ps
CROSS JOIN claim c;

-- =============================================================================
-- Query 9: Staging header reconstruction (patient + insurance vs OD)
-- =============================================================================
WITH patient AS (
    SELECT SUM(payment_amount)::numeric(18, 2) AS total
    FROM staging.stg_opendental__payment
    WHERE payment_date = DATE '2026-06-24'
      AND payment_amount <> 0
),
insurance AS (
    SELECT SUM(check_amount)::numeric(18, 2) AS total
    FROM staging.stg_opendental__claimpayment
    WHERE check_date = DATE '2026-06-24'
)
SELECT
    11197.40 AS od_total,
    p.total AS patient_staging,
    i.total AS insurance_staging,
    p.total + i.total AS reconstructed,
    (p.total + i.total) - 11197.40 AS diff
FROM patient p
CROSS JOIN insurance i;

-- =============================================================================
-- Query 10a: Type 71 (Credit Card) — payments where split sum ≠ header
-- Run this block alone in DBeaver (includes its own WITH).
-- =============================================================================
WITH by_payment AS (
    SELECT
        p.payment_id,
        p.payment_amount AS header_amount,
        COALESCE(SUM(ps.split_amount), 0)::numeric(18, 2) AS split_sum,
        (COALESCE(SUM(ps.split_amount), 0) - p.payment_amount)::numeric(18, 2) AS split_minus_header
    FROM staging.stg_opendental__payment p
    LEFT JOIN staging.stg_opendental__paysplit ps
        ON p.payment_id = ps.payment_id
    WHERE p.payment_date = DATE '2026-06-24'
      AND p.payment_type_id = 71
      AND p.payment_amount <> 0
    GROUP BY p.payment_id, p.payment_amount
)
SELECT *
FROM by_payment
WHERE split_minus_header <> 0
ORDER BY ABS(split_minus_header) DESC;

-- =============================================================================
-- Query 10b: Type 71 totals — compare to OD Credit Card section (4172.10)
-- Run this block alone in DBeaver (separate statement from 10a).
-- =============================================================================
WITH by_payment AS (
    SELECT
        p.payment_id,
        p.payment_amount AS header_amount,
        COALESCE(SUM(ps.split_amount), 0)::numeric(18, 2) AS split_sum
    FROM staging.stg_opendental__payment p
    LEFT JOIN staging.stg_opendental__paysplit ps
        ON p.payment_id = ps.payment_id
    WHERE p.payment_date = DATE '2026-06-24'
      AND p.payment_type_id = 71
      AND p.payment_amount <> 0
    GROUP BY p.payment_id, p.payment_amount
)
SELECT
    4172.10::numeric(18, 2) AS od_credit_card_section,
    SUM(header_amount)::numeric(18, 2) AS type71_header_total,
    SUM(split_sum)::numeric(18, 2) AS type71_split_total,
    (SUM(split_sum) - SUM(header_amount))::numeric(18, 2) AS split_header_gap,
    (SUM(split_sum) - 4172.10)::numeric(18, 2) AS split_total_vs_od
FROM by_payment;

-- =============================================================================
-- Query 11a: All patient payment types on 2026-06-24 (find missing CC types)
-- OD may group multiple PayType definitions under "Credit Card", not only 71.
-- =============================================================================
SELECT
    p.payment_type_id,
    COUNT(*) AS payment_count,
    SUM(p.payment_amount)::numeric(18, 2) AS header_total
FROM staging.stg_opendental__payment p
WHERE p.payment_date = DATE '2026-06-24'
  AND p.payment_amount <> 0
GROUP BY p.payment_type_id
ORDER BY header_total DESC;

-- =============================================================================
-- Query 11b: PayType definitions for types active on 2026-06-24
-- PayType on payment/claimpayment = definition.definition_id (not item_name search).
-- Compare patient_header_total to OD section subtotals from golden CSV.
-- =============================================================================
WITH day_payments AS (
    SELECT
        payment_type_id,
        COUNT(*) AS payment_count,
        SUM(payment_amount)::numeric(18, 2) AS header_total
    FROM staging.stg_opendental__payment
    WHERE payment_date = DATE '2026-06-24'
      AND payment_amount <> 0
    GROUP BY payment_type_id
),
day_claim AS (
    SELECT
        payment_type_id,
        COUNT(*) AS claim_payment_count,
        SUM(check_amount)::numeric(18, 2) AS check_total
    FROM staging.stg_opendental__claimpayment
    WHERE check_date = DATE '2026-06-24'
    GROUP BY payment_type_id
),
active_types AS (
    SELECT payment_type_id FROM day_payments
    UNION
    SELECT payment_type_id FROM day_claim
)
SELECT
    at.payment_type_id,
    d.item_name,
    d.category_id,
    d.is_hidden,
    COALESCE(dp.payment_count, 0) AS patient_payment_count,
    COALESCE(dp.header_total, 0)::numeric(18, 2) AS patient_header_total,
    COALESCE(dc.claim_payment_count, 0) AS claim_payment_count,
    COALESCE(dc.check_total, 0)::numeric(18, 2) AS claim_check_total,
    CASE at.payment_type_id
        WHEN 69 THEN 'OD Check 1051.30'
        WHEN 70 THEN 'OD Cash 677.00'
        WHEN 71 THEN 'OD Credit Card 4172.10 (staging 4015.80; gap -156.30)'
        WHEN 634 THEN 'OD Cherry 1319.20'
        ELSE NULL
    END AS od_golden_section
FROM active_types at
LEFT JOIN staging.stg_opendental__definition d
    ON d.definition_id = at.payment_type_id
LEFT JOIN day_payments dp
    ON dp.payment_type_id = at.payment_type_id
LEFT JOIN day_claim dc
    ON dc.payment_type_id = at.payment_type_id
ORDER BY
    COALESCE(dp.header_total, 0) + COALESCE(dc.check_total, 0) DESC;

-- =============================================================================
-- Query 11c: Type 71 splits by paysplit date vs payment date
-- OD may key off DatePay on split rather than PayDate on payment header.
-- =============================================================================
SELECT
    CASE
        WHEN p.payment_date = DATE '2026-06-24' THEN 'payment_date match'
        ELSE 'payment_date other'
    END AS payment_date_bucket,
    SUM(ps.split_amount)::numeric(18, 2) AS split_total
FROM staging.stg_opendental__paysplit ps
INNER JOIN staging.stg_opendental__payment p
    ON ps.payment_id = p.payment_id
WHERE ps.payment_date = DATE '2026-06-24'
  AND p.payment_type_id = 71
GROUP BY 1;

-- =============================================================================
-- Query 12: Line-level type 71 (Credit Card) payments on 2026-06-24
-- Compare to OD golden CSV Credit Card section (4172.10).
-- =============================================================================
SELECT
    p.payment_id,
    p.payment_type_id,
    p.payment_amount,
    p.check_number,
    pt.last_name || ', ' || pt.first_name AS patient_name
FROM staging.stg_opendental__payment p
LEFT JOIN staging.stg_opendental__patient pt
    ON p.patient_id = pt.patient_id
WHERE p.payment_date = DATE '2026-06-24'
  AND p.payment_type_id = 71
  AND p.payment_amount <> 0
ORDER BY p.payment_amount DESC;

-- =============================================================================
-- Query 13a: Golden CSV gap patients — confirm names exist in patient table
-- OD CC section: Skinner Anderson F ($50), Teller Nancy ($106.30) not in Query 12.
-- =============================================================================
SELECT
    patient_id,
    last_name,
    first_name
FROM staging.stg_opendental__patient
WHERE last_name ILIKE '%skinner%'
   OR last_name ILIKE '%teller%'
   OR first_name ILIKE '%anderson%'
ORDER BY last_name, first_name;

-- =============================================================================
-- Query 13b: CC-sized amounts on 2026-06-24 (any pay type)
-- Skinner $50; Teller $92.50 + $13.80 splits or $106.30 header.
-- =============================================================================
SELECT
    p.payment_id,
    p.payment_type_id,
    d.item_name AS pay_type_name,
    p.payment_amount,
    p.payment_date,
    pt.last_name || ', ' || pt.first_name AS patient_name
FROM staging.stg_opendental__payment p
LEFT JOIN staging.stg_opendental__definition d
    ON d.definition_id = p.payment_type_id
LEFT JOIN staging.stg_opendental__patient pt
    ON p.patient_id = pt.patient_id
WHERE p.payment_date = DATE '2026-06-24'
  AND (
      p.payment_amount IN (50.00, 106.30, 92.50, 13.80)
      OR p.payment_amount BETWEEN 106.00 AND 107.00
  )
ORDER BY p.payment_amount DESC;

-- =============================================================================
-- Query 13c: Paysplit rows matching Teller / Skinner CC amounts on 2026-06-24
-- =============================================================================
SELECT
    ps.paysplit_id,
    ps.payment_id,
    p.payment_type_id,
    d.item_name AS pay_type_name,
    ps.split_amount,
    ps.payment_date,
    pt.last_name || ', ' || pt.first_name AS patient_name
FROM staging.stg_opendental__paysplit ps
INNER JOIN staging.stg_opendental__payment p
    ON ps.payment_id = p.payment_id
LEFT JOIN staging.stg_opendental__definition d
    ON d.definition_id = p.payment_type_id
LEFT JOIN staging.stg_opendental__patient pt
    ON ps.patient_id = pt.patient_id
WHERE ps.payment_date = DATE '2026-06-24'
  AND ps.split_amount IN (50.00, 92.50, 13.80)
ORDER BY pt.last_name, ps.split_amount DESC;

-- =============================================================================
-- Query 13d: Raw payment table — same patient / amount search (pre-staging)
-- raw.payment uses OpenDental column names (quoted identifiers).
-- =============================================================================
SELECT
    "PayNum" AS payment_id,
    "PayType" AS payment_type_id,
    "PayAmt" AS payment_amount,
    "PayDate" AS payment_date,
    "PatNum" AS patient_id
FROM raw.payment
WHERE "PayDate"::date = DATE '2026-06-24'
  AND (
      "PayAmt" IN (50.00, 106.30, 92.50, 13.80)
      OR "PayAmt" BETWEEN 106.00 AND 107.00
  )
ORDER BY "PayAmt" DESC;

-- =============================================================================
-- Query 13e: Payments for golden-gap patients (Skinner 33168, Teller 33856)
-- 13a found patients; 13b–13d found no $106.30 / Skinner $50 on 2026-06-24.
-- =============================================================================
SELECT
    p.payment_id,
    p.payment_type_id,
    d.item_name AS pay_type_name,
    p.payment_amount,
    p.payment_date,
    p.check_number,
    pt.last_name || ', ' || pt.first_name AS patient_name
FROM staging.stg_opendental__payment p
LEFT JOIN staging.stg_opendental__definition d
    ON d.definition_id = p.payment_type_id
INNER JOIN staging.stg_opendental__patient pt
    ON p.patient_id = pt.patient_id
WHERE p.patient_id IN (33168, 33856)
  AND p.payment_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
ORDER BY p.payment_date, p.payment_amount DESC;

-- =============================================================================
-- Query 13f: Paysplits for Teller / Skinner around validation date
-- Teller OD CC: $92.50 + $13.80; Skinner OD CC: $50.00
-- =============================================================================
SELECT
    ps.paysplit_id,
    ps.payment_id,
    p.payment_type_id,
    d.item_name AS pay_type_name,
    ps.split_amount,
    ps.payment_date,
    pt.last_name || ', ' || pt.first_name AS patient_name
FROM staging.stg_opendental__paysplit ps
INNER JOIN staging.stg_opendental__payment p
    ON ps.payment_id = p.payment_id
LEFT JOIN staging.stg_opendental__definition d
    ON d.definition_id = p.payment_type_id
INNER JOIN staging.stg_opendental__patient pt
    ON ps.patient_id = pt.patient_id
WHERE ps.patient_id IN (33168, 33856)
  AND ps.payment_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
ORDER BY ps.payment_date, ps.split_amount DESC;

-- =============================================================================
-- Query 14a: Post-ETL check — golden-gap PayNums in raw (OpenDental columns)
-- Source OD: PayNum 931419 (Teller $106.30), 931420 (Skinner $50.00)
-- =============================================================================
SELECT
    "PayNum",
    "PayType",
    "PayAmt",
    "PayDate",
    "PatNum"
FROM raw.payment
WHERE "PayNum" IN (931419, 931420);

-- =============================================================================
-- Query 14b: Post-ETL check — same rows in staging (dbt column names)
-- Run dbt for payment/paysplit models after ETL if 14a has rows but 14b is empty.
-- =============================================================================
SELECT
    payment_id,
    payment_type_id,
    payment_amount,
    payment_date,
    patient_id
FROM staging.stg_opendental__payment
WHERE payment_id IN (931419, 931420);
