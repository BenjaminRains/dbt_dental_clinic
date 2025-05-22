{{ config(
    materialized='view'
) }}

with source as (
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

renamed as (
    select
        -- Primary Key
        "InsSubNum" as inssub_id,
        
        -- Foreign Keys
        "PlanNum" as insurance_plan_id,
        "Subscriber" as subscriber_id,
        "SecUserNumEntry" as user_entry_id,
        
        -- Dates
        "DateEffective" as effective_date,
        "DateTerm" as termination_date,
        "SecDateEntry" as entry_date,
        "SecDateTEdit" as last_modified_at,
        
        -- Attributes
        "ReleaseInfo" as is_release_info,
        "AssignBen" as is_assign_benefits,
        "SubscriberID" as subscriber_external_id,
        "BenefitNotes" as benefit_notes,
        "SubscNote" as subscriber_notes,
        
        -- Metadata
        CASE 
            WHEN "DateTerm" < CURRENT_DATE THEN 'TERMINATED'
            WHEN "DateTerm" IS NULL THEN 'ACTIVE'
            ELSE 'FUTURE_TERMINATED'
        END as subscriber_status,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        "SecDateEntry" as _created_at,
        "SecDateTEdit" as _updated_at

    from source
)

select * from renamed
