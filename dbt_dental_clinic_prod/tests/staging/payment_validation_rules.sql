{# {{ config(severity = 'warn') }} #}

/*
  PAYMENT VALIDATION RULES
  =======================
  
  Purpose:
  Validates payment data patterns and business rules based on PostgreSQL schema and business logic.
  
  Payment Types (2023-current):
  - Type 71: Regular payments (avg $293.25)
  - Type 0:  Administrative entries (must be $0)
  - Type 69: Higher value payments (avg $760.20, flag if > $5,000)
  - Type 72: Refunds (negative amounts only)
  - Type 574: Very high value payments (avg $16,661.66, flag if > $50,000)
  - Type 412: Newer payment type (added 2023)
  - Type 634: Newest payment type (added Sept 2024)

  Key Validation Rules:
  1. Payment Type Rules:
     - Type 0: Must have $0 amount
     - Type 72: Must be negative
  2. Amount Thresholds:
     - Type 69: Flag if > $5,000
     - Type 574: Flag if > $50,000
*/

WITH PaymentData AS (
    SELECT * FROM {{ ref('stg_opendental__payment') }}
    WHERE payment_date >= '2023-01-01'
),

ValidationFailures AS (
    -- 1. Required Fields Validation
    SELECT 
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        'Missing required field' AS failure_type,
        CASE 
            WHEN patient_id IS NULL THEN 'patient_id'
            WHEN payment_date IS NULL THEN 'payment_date'
            WHEN payment_type_id IS NULL THEN 'payment_type_id'
        END AS failed_field
    FROM PaymentData
    WHERE patient_id IS NULL 
       OR payment_date IS NULL 
       OR payment_type_id IS NULL

    UNION ALL

    -- 2. Payment Type Rules
    SELECT 
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        CASE 
            WHEN payment_type_id = 0 AND payment_amount != 0 
                THEN 'Type 0 with non-zero amount'
            WHEN payment_type_id = 72 AND payment_amount >= 0 
                THEN 'Type 72 with non-negative amount'
        END AS failure_type,
        'payment_type_rule' AS failed_field
    FROM PaymentData
    WHERE (payment_type_id = 0 AND payment_amount != 0)
       OR (payment_type_id = 72 AND payment_amount >= 0)

    UNION ALL

    -- 3. Date Range Validation
    SELECT 
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        'Invalid date range' AS failure_type,
        'payment_date' AS failed_field
    FROM PaymentData
    WHERE payment_date > CURRENT_DATE
       OR payment_date < '2000-01-01'

    UNION ALL

    -- 4. Payment Amount Validation (with tiered warnings)
    SELECT 
        payment_id,
        payment_type_id,
        payment_amount,
        payment_date,
        CASE 
            WHEN payment_type_id = 69 AND payment_amount > 50000 
                THEN 'CRITICAL: Type 69 payment exceeds $50,000'
            WHEN payment_type_id = 69 AND payment_amount > 25000 
                THEN 'HIGH: Type 69 payment exceeds $25,000'
            WHEN payment_type_id = 69 AND payment_amount > 10000 
                THEN 'MEDIUM: Type 69 payment exceeds $10,000'
            WHEN payment_type_id = 69 AND payment_amount > 5000 
                THEN 'LOW: Type 69 payment exceeds $5,000'
            WHEN payment_type_id = 574 AND payment_amount > 50000 
                THEN 'Type 574 exceeds threshold'
        END AS failure_type,
        'payment_amount' AS failed_field
    FROM PaymentData
    WHERE (payment_type_id = 69 AND payment_amount > 5000)
       OR (payment_type_id = 574 AND payment_amount > 50000)
)

SELECT * FROM ValidationFailures
WHERE failure_type IS NOT NULL
ORDER BY 
    CASE 
        WHEN failure_type LIKE 'CRITICAL%' THEN 1
        WHEN failure_type LIKE 'HIGH%' THEN 2
        WHEN failure_type LIKE 'MEDIUM%' THEN 3
        WHEN failure_type LIKE 'LOW%' THEN 4
        ELSE 5
    END,
    payment_amount DESC