{{ config(
    materialized='incremental',
    unique_key='adjustment_id',
    schema='staging'
) }}

with source as (
    select * 
    from {{ source('opendental', 'adjustment') }}
    where "AdjDate" >= '2023-01-01'::date
        and "AdjDate" <= '{{ var("max_valid_date") }}'::date
    {% if is_incremental() %}
        and "AdjDate" > (select max(adjustment_date) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Keys (matching PostgreSQL data types)
        "AdjNum"::integer as adjustment_id,  
        "PatNum"::bigint as patient_id,      
        -- Preserve 0 values for procedure_id as they represent general account adjustments
        "ProcNum"::bigint as procedure_id,  
        NULLIF("ProvNum", 0)::bigint as provider_id,   
        NULLIF("ClinicNum", 0)::bigint as clinic_id,   
        NULLIF("StatementNum", 0)::bigint as statement_id,  
        "AdjType"::bigint as adjustment_type_id,       
        NULLIF("TaxTransID", 0)::bigint as tax_transaction_id,  
        
        -- Adjustment details
        "AdjAmt"::double precision as adjustment_amount,  
        NULLIF("AdjNote", '')::text as adjustment_note,  
        
        -- Dates
        "AdjDate"::date as adjustment_date,    
        "ProcDate"::date as procedure_date,    
        "DateEntry"::date as entry_date,       
        
        -- Calculated fields
        CASE 
            WHEN "AdjAmt" > 0 THEN 'positive'
            WHEN "AdjAmt" < 0 THEN 'negative'
            ELSE 'zero'
        END as adjustment_direction,
        
        -- Modified to handle both 0 and NULL cases
        CASE 
            WHEN "ProcNum" > 0 THEN true
            ELSE false
        END::boolean as is_procedure_adjustment,
        
        CASE
            WHEN "ProcDate" != "AdjDate" THEN true
            ELSE false
        END::boolean as is_retroactive_adjustment,
        
        -- Enhanced calculated fields
        CASE
            WHEN "AdjType" = 188 THEN 'insurance_writeoff'
            WHEN "AdjType" = 474 THEN 'provider_discount'
            WHEN "AdjType" = 186 THEN 'senior_discount'
            WHEN "AdjType" = 235 THEN 'reallocation'
            WHEN "AdjType" = 472 THEN 'employee_discount'
            WHEN "AdjType" = 475 THEN 'provider_discount'
            WHEN "AdjType" IN (9, 185) THEN 'cash_discount'
            WHEN "AdjType" IN (18, 337) THEN 'patient_refund'
            WHEN "AdjType" = 483 THEN 'referral_credit'
            WHEN "AdjType" = 537 THEN 'new_patient_discount'
            WHEN "AdjType" = 485 THEN 'employee_discount'
            WHEN "AdjType" = 549 THEN 'admin_correction'
            WHEN "AdjType" = 550 THEN 'admin_adjustment'
            WHEN EXISTS (
                SELECT 1 
                FROM paysplit ps 
                WHERE ps."AdjNum" = source."AdjNum" 
                AND ps."UnearnedType" IN (288, 439)
            ) THEN 'unearned_income'
            ELSE 'other'
        END as adjustment_category,

        -- Additional flags based on data patterns
        CASE 
            WHEN LOWER("AdjNote") LIKE '%n/c%' 
              OR LOWER("AdjNote") LIKE '%nc %'
              OR LOWER("AdjNote") LIKE '%no charge%' THEN true
            ELSE false
        END::boolean as is_no_charge,

        CASE
            WHEN LOWER("AdjNote") LIKE '%military%' THEN true
            ELSE false
        END::boolean as is_military_discount,

        CASE
            WHEN LOWER("AdjNote") LIKE '%warranty%' 
              OR LOWER("AdjNote") LIKE '%courtesy%' THEN true
            ELSE false
        END::boolean as is_courtesy_adjustment,

        CASE
            WHEN "AdjType" IN (474, 475) 
              OR LOWER("AdjNote") LIKE '%per dr%'
              OR LOWER("AdjNote") LIKE '%dr.%' THEN true
            ELSE false
        END::boolean as is_provider_discretion,

        CASE
            WHEN ABS("AdjAmt") >= 1000 THEN 'large'
            WHEN ABS("AdjAmt") >= 500 THEN 'medium'
            WHEN ABS("AdjAmt") >= 100 THEN 'small'
            ELSE 'minimal'
        END as adjustment_size,

        -- Unearned income flag from paysplit
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM paysplit ps 
                WHERE ps."AdjNum" = source."AdjNum" 
                AND ps."UnearnedType" = 288
            ) THEN 288
            WHEN EXISTS (
                SELECT 1 
                FROM paysplit ps 
                WHERE ps."AdjNum" = source."AdjNum" 
                AND ps."UnearnedType" = 439
            ) THEN 439
            ELSE NULL
        END as unearned_type_id,
        
        -- Additional flags
        CASE 
            WHEN "AdjType" IN (472, 485, 655) THEN true
            ELSE false
        END::boolean as is_employee_discount,
        
        CASE
            WHEN "AdjType" IN (482, 486) THEN true
            ELSE false
        END::boolean as is_family_discount,
        
        CASE
            WHEN "AdjType" IN (474, 475, 601) THEN true
            ELSE false
        END::boolean as is_provider_discount,
        
        -- Additional flags for financial analysis
        CASE 
            WHEN "AdjType" IN (486, 474) AND "AdjAmt" < -1000 THEN true
            ELSE false
        END::boolean as is_large_adjustment,
        
        CASE
            WHEN "AdjType" IN (186, 9) AND "AdjAmt" > -50 THEN true
            ELSE false
        END::boolean as is_minor_adjustment,
        
        CASE 
            WHEN "AdjType" IN (288, 439) THEN true
            ELSE false
        END::boolean as is_unearned_income,
        
        -- Metadata fields
        NULLIF("SecUserNumEntry", 0)::bigint as created_by_user_id,
        CASE 
            WHEN "SecDateTEdit" > current_timestamp THEN NULL
            ELSE "SecDateTEdit"
        END::timestamp without time zone as last_modified_at,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data into data warehouse
        "DateEntry"::timestamp without time zone as _created_at,  -- Rename source creation timestamp
        CASE 
            WHEN "SecDateTEdit" > current_timestamp THEN "DateEntry"
            ELSE "SecDateTEdit"
        END::timestamp without time zone as _updated_at  -- Rename source update timestamp

    from source
)

select * from renamed
