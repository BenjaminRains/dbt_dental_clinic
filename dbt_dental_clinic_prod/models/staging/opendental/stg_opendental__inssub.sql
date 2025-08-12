{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'inssub') }}
    where 
        -- Include records modified in 2023+
        "SecDateTEdit" >= '2023-01-01'
        -- OR include historical records that are still active
        OR (
            "SecDateTEdit" < '2023-01-01'
            AND (
                -- Only include non-terminated or future-terminated records
                "DateTerm" IS NULL 
                OR "DateTerm" >= CURRENT_DATE
            )
        )
),

renamed_columns as (
    select
        -- Primary Key
        "InsSubNum" as inssub_id,
        
        -- Foreign Keys
        "PlanNum" as insurance_plan_id,
        "Subscriber" as subscriber_id,
        
        -- String Fields
        "SubscriberID" as subscriber_external_id,
        "BenefitNotes" as benefit_notes,
        "SubscNote" as subscriber_notes,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"ReleaseInfo"') }} as is_release_info,
        {{ convert_opendental_boolean('"AssignBen"') }} as is_assign_benefits,
        
        -- Date Fields
        {{ clean_opendental_date('"DateEffective"') }} as effective_date,
        {{ clean_opendental_date('"DateTerm"') }} as termination_date,
        {{ clean_opendental_date('"SecDateEntry"') }} as date_created,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_updated,
        
        -- Derived Status Field
        CASE 
            WHEN "DateTerm" < CURRENT_DATE THEN 'TERMINATED'
            WHEN "DateTerm" IS NULL THEN 'ACTIVE'
            ELSE 'FUTURE_TERMINATED'
        END as subscriber_status,

        -- Standardized metadata columns
        {{ standardize_metadata_columns() }},
        
        -- User tracking
        "SecUserNumEntry" as sec_user_num_entry

    from source_data
)

select * from renamed_columns
