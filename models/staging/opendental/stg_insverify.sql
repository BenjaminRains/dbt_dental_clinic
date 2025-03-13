with source as (
    select * from {{ source('opendental', 'insverify') }}
),

renamed as (
    select
        -- Primary key
        InsVerifyNum as insurance_verification_id,

        -- Relationships
        UserNum as user_id,
        FKey as foreign_key_id, -- References different tables based on VerifyType
        DefNum as definition_id,

        -- Verification details
        VerifyType as verification_type,
        HoursAvailableForVerification as hours_available,

        -- Dates and timing
        DateLastVerified as last_verified_date,
        DateLastAssigned as last_assigned_date,
        DateTimeEntry as entry_datetime,

        -- Metadata
        SecDateTEdit as updated_at,
        Note -- (likely contains specific verification details that could include PHI)
    from source
)

select * from renamed