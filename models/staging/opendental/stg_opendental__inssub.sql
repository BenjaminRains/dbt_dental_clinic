{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'inssub') }}
    where "SecDateTEdit" >= '2023-01-01'
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
        "SubscNote" as subscriber_notes

    from source
)

select * from renamed
