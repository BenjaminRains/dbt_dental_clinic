{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

WITH Source AS (
    SELECT * 
    FROM {{ source('opendental', 'payment') }}
    WHERE "PayDate" >= '2023-01-01'::date  
        AND "PayDate" <= CURRENT_DATE
        AND "PayDate" > '2000-01-01'::date  
    {% if is_incremental() %}
        AND "PayDate" > (SELECT max(payment_date) FROM {{ this }})
    {% endif %}
),

Renamed AS (
    SELECT
        -- Primary key (matches DDL primary key constraint)
        "PayNum" AS payment_id,

        -- Relationships (following DDL index order)
        "PatNum" AS patient_id,            -- Has index: payment_indexPatNum
        "ClinicNum" AS clinic_id,          -- Has index: payment_ClinicNum
        "PayType" AS payment_type_id,      -- Has index: payment_PayType
        "DepositNum" AS deposit_id,        -- Has index: payment_DepositNum
        "SecUserNumEntry" AS created_by_user_id,  -- Has index: payment_SecUserNumEntry

        -- Payment details
        "PayDate" AS payment_date,         -- Has index: payment_idx_ml_payment_window
        "PayAmt" AS payment_amount,        -- DDL default: 0
        COALESCE("MerchantFee", 0.0) AS merchant_fee,  -- Using 0.0 to match double precision
        NULLIF(TRIM("CheckNum"), '') AS check_number,   -- varchar(25)
        NULLIF(TRIM("BankBranch"), '') AS bank_branch,  -- varchar(25)
        NULLIF(TRIM("ExternalId"), '') AS external_id,  -- varchar(255)

        -- Status flags (all smallint in DDL, default 0)
        COALESCE("IsSplit", 0)::smallint::boolean AS is_split_flag,
        COALESCE("IsRecurringCC", 0)::smallint::boolean AS is_recurring_cc_flag,
        COALESCE("IsCcCompleted", 0)::smallint::boolean AS is_cc_completed_flag,
        COALESCE("PaymentStatus", 0)::smallint AS payment_status,
        COALESCE("ProcessStatus", 0)::smallint AS process_status,  -- Has index: payment_ProcessStatus
        COALESCE("PaymentSource", 0)::smallint AS payment_source,

        -- Dates
        NULLIF("RecurringChargeDate", '1900-01-01'::date) AS recurring_charge_date,
        NULLIF("DateEntry", '1900-01-01'::date) AS entry_date,
        COALESCE("SecDateTEdit", current_timestamp) AS updated_at,  -- DDL default: CURRENT_TIMESTAMP

        -- Text fields (handle empty strings)
        NULLIF(TRIM("PayNote"), '') AS payment_notes,    -- text
        NULLIF(TRIM("Receipt"), '') AS receipt_text,     -- text

        -- Metadata
        current_timestamp AS _loaded_at
    FROM Source
)

SELECT * FROM Renamed