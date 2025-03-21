with source as (
    select * from {{ source('opendental', 'adjustment') }}
    where AdjDate >= '2022-01-01'  
    and AdjDate <= CURRENT_DATE  -- Add upper bound to exclude invalid future dates
    and YEAR(AdjDate) > 1900     -- Exclude clearly invalid historical dates
),

renamed as (
    select
        -- Keys
        AdjNum as adjustment_id,  -- Primary key from source
        PatNum as patient_id,
        NULLIF(ProcNum, 0) as procedure_id,
        NULLIF(ProvNum, 0) as provider_id,
        NULLIF(ClinicNum, 0) as clinic_id,
        NULLIF(StatementNum, 0) as statement_id,
        AdjType as adjustment_type_id,
        NULLIF(TaxTransID, 0) as tax_transaction_id,
        
        -- Adjustment details
        AdjAmt as adjustment_amount,
        NULLIF(AdjNote, '') as adjustment_note,
        
        -- Dates
        AdjDate as adjustment_date,
        ProcDate as procedure_date,  -- Renamed for consistency
        DateEntry as entry_date,
        
        -- Calculated fields
        CASE 
            WHEN AdjAmt > 0 THEN 'positive'
            WHEN AdjAmt < 0 THEN 'negative'
            ELSE 'zero'
        END as adjustment_direction,
        
        CASE 
            WHEN ProcNum > 0 THEN 1
            ELSE 0
        END as is_procedure_adjustment,
        
        CASE
            WHEN ProcDate != AdjDate THEN 1
            ELSE 0
        END as is_retroactive_adjustment,
        
        -- Enhanced calculated fields
        CASE
            -- High volume insurance and discount adjustments
            WHEN AdjType = 188 THEN 'insurance_writeoff'        -- Most common, includes CT and other write-offs
            WHEN AdjType = 474 THEN 'provider_discount'         -- Dr. Kamp's discounts
            WHEN AdjType = 186 THEN 'senior_discount'          -- Usually smaller amounts
            WHEN AdjType = 235 THEN 'reallocation'            -- Positive adjustments
            WHEN AdjType = 472 THEN 'employee_discount'        -- MDC EDP
            WHEN AdjType = 475 THEN 'provider_discount'        -- Dr. Schneiss's discounts
            WHEN AdjType IN (9, 185) THEN 'cash_discount'     -- Cash/check discounts
            WHEN AdjType IN (18, 337) THEN 'patient_refund'   -- Always positive amounts
            WHEN AdjType = 483 THEN 'referral_credit'         -- $25-50 amounts
            WHEN AdjType = 537 THEN 'new_patient_discount'    -- New patient coupons
            WHEN AdjType = 485 THEN 'employee_discount'       -- MDC Employee
            WHEN AdjType = 549 THEN 'admin_correction'        -- Fixing refund checks
            WHEN AdjType = 550 THEN 'admin_adjustment'        -- Administrative adjustments
            WHEN EXISTS (
                SELECT 1 
                FROM paysplit ps 
                WHERE ps.AdjNum = source.AdjNum 
                AND ps.UnearnedType IN (288, 439)
            ) THEN 'unearned_income'
            ELSE 'other'
        END as adjustment_category,

        -- Additional flags based on data patterns
        CASE 
            WHEN LOWER(AdjNote) LIKE '%n/c%' 
              OR LOWER(AdjNote) LIKE '%nc %'
              OR LOWER(AdjNote) LIKE '%no charge%' THEN 1
            ELSE 0
        END as is_no_charge,

        CASE
            WHEN LOWER(AdjNote) LIKE '%military%' THEN 1
            ELSE 0
        END as is_military_discount,

        CASE
            WHEN LOWER(AdjNote) LIKE '%warranty%' 
              OR LOWER(AdjNote) LIKE '%courtesy%' THEN 1
            ELSE 0
        END as is_courtesy_adjustment,

        CASE
            WHEN AdjType IN (474, 475) 
              OR LOWER(AdjNote) LIKE '%per dr%'
              OR LOWER(AdjNote) LIKE '%dr.%' THEN 1
            ELSE 0
        END as is_provider_discretion,

        CASE
            WHEN ABS(AdjAmt) >= 1000 THEN 'large'
            WHEN ABS(AdjAmt) >= 500 THEN 'medium'
            WHEN ABS(AdjAmt) >= 100 THEN 'small'
            ELSE 'minimal'
        END as adjustment_size,

        -- Unearned income flag from paysplit
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM paysplit ps 
                WHERE ps.AdjNum = source.AdjNum 
                AND ps.UnearnedType = 288
            ) THEN 288
            WHEN EXISTS (
                SELECT 1 
                FROM paysplit ps 
                WHERE ps.AdjNum = source.AdjNum 
                AND ps.UnearnedType = 439
            ) THEN 439
            ELSE NULL
        END as unearned_type_id,
        
        -- Additional flags
        CASE 
            WHEN AdjType IN (472, 485, 655) THEN 1
            ELSE 0
        END as is_employee_discount,
        
        CASE
            WHEN AdjType IN (482, 486) THEN 1
            ELSE 0
        END as is_family_discount,
        
        CASE
            WHEN AdjType IN (474, 475, 601) THEN 1
            ELSE 0
        END as is_provider_discount,
        
        -- Additional flags for financial analysis
        CASE 
            WHEN AdjType IN (486, 474) AND AdjAmt < -1000 THEN 1
            ELSE 0
        END as is_large_adjustment,
        
        CASE
            WHEN AdjType IN (186, 9) AND AdjAmt > -50 THEN 1
            ELSE 0
        END as is_minor_adjustment,
        
        CASE 
            WHEN AdjType IN (288, 439) THEN 1
            ELSE 0
        END as is_unearned_income,
        
        -- Metadata and system fields
        NULLIF(SecUserNumEntry, 0) as created_by_user_id,
        CASE 
            WHEN SecDateTEdit > CURRENT_TIMESTAMP THEN NULL  -- Changed to TIMESTAMP to match DDL
            ELSE SecDateTEdit 
        END as last_modified_at  -- Renamed for clarity
    from source
)

select * from renamed
