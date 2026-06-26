-- Daily Payments: staging line items for row-level match against golden CSV (PHI)
-- Run on clinic warehouse. Export result to CSV and diff against od_daily_payments_*.csv
-- in Excel / Beyond Compare, or eyeball mismatches in DBeaver.
--
-- Golden CSV columns → staging:
--   Patient sections: Paying Patient ≈ patient name, Provider, Check#, Amount
--   Insurance sections: Carrier, Patient Name, Provider, Check#, Amount (claimpayment path)

WITH params AS (
    SELECT DATE '2025-10-07' AS payment_date  -- change per validation
),
patient_lines AS (
    SELECT
        'patient' AS source,
        p.payment_date AS line_date,
        pt.last_name || ', ' || pt.first_name AS patient_name,
        NULL::text AS carrier,
        coalesce(pr.provider_abbreviation, pr.provider_id::text) AS provider,
        p.check_number,
        d.item_name AS pay_type,
        p.payment_amount AS amount,
        p.payment_id,
        NULL::bigint AS claim_payment_id
    FROM staging.stg_opendental__payment p
    CROSS JOIN params x
    LEFT JOIN staging.stg_opendental__patient pt ON pt.patient_id = p.patient_id
    LEFT JOIN staging.stg_opendental__provider pr ON pr.provider_id = p.provider_id
    LEFT JOIN staging.stg_opendental__definition d ON d.definition_id = p.payment_type_id
    WHERE p.payment_date = x.payment_date
      AND p.payment_amount <> 0
),
insurance_lines AS (
    SELECT
        'insurance' AS source,
        c.check_date AS line_date,
        pt.last_name || ', ' || pt.first_name AS patient_name,
        c.carrier_name AS carrier,
        coalesce(pr.provider_abbreviation, pr.provider_id::text) AS provider,
        c.check_number,
        d.item_name AS pay_type,
        c.check_amount::numeric(18, 2) AS amount,
        NULL::bigint AS payment_id,
        c.claim_payment_id
    FROM staging.stg_opendental__claimpayment c
    CROSS JOIN params x
    LEFT JOIN staging.stg_opendental__patient pt ON pt.patient_id = c.patient_id
    LEFT JOIN staging.stg_opendental__provider pr ON pr.provider_id = c.provider_id
    LEFT JOIN staging.stg_opendental__definition d ON d.definition_id = c.payment_type_id
    WHERE c.check_date = x.payment_date
),
paysplit_lines AS (
    SELECT
        'paysplit' AS source,
        ps.payment_date AS line_date,
        pt.last_name || ', ' || pt.first_name AS patient_name,
        NULL::text AS carrier,
        coalesce(pr.provider_abbreviation, pr.provider_id::text) AS provider,
        p.check_number,
        d.item_name AS pay_type,
        ps.split_amount AS amount,
        ps.payment_id,
        NULL::bigint AS claim_payment_id
    FROM staging.stg_opendental__paysplit ps
    INNER JOIN staging.stg_opendental__payment p ON ps.payment_id = p.payment_id
    CROSS JOIN params x
    LEFT JOIN staging.stg_opendental__patient pt ON pt.patient_id = ps.patient_id
    LEFT JOIN staging.stg_opendental__provider pr ON pr.provider_id = ps.provider_id
    LEFT JOIN staging.stg_opendental__definition d ON d.definition_id = p.payment_type_id
    WHERE p.payment_date = x.payment_date
      AND p.payment_amount <> 0
)
SELECT * FROM patient_lines
UNION ALL
SELECT * FROM insurance_lines
ORDER BY source, pay_type, patient_name, amount DESC;

-- Paysplit grain (matches OD Daily Payments provider-split export):
-- SELECT * FROM paysplit_lines ORDER BY pay_type, patient_name, amount DESC;
