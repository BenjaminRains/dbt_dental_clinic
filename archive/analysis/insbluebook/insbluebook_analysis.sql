with source as (
    select * from {{ source('opendental', 'insbluebook') }}
),

renamed as (
    select
        -- Primary key
        InsBlueBookNum as insurance_bluebook_id,

        -- Relationships to other entities
        ProcCodeNum as procedure_code_id,
        CarrierNum as carrier_id,
        PlanNum as insurance_plan_id,
        ProcNum as procedure_id,
        ClaimNum as claim_id,

        -- Insurance details
        GroupNum as group_number,
        ClaimType as claim_type,

        -- Financial information
        InsPayAmt as insurance_payment_amount,
        AllowedOverride as allowed_amount_override,

        -- Dates
        DateTEntry as entry_datetime,
        ProcDate as procedure_date
    from source
)

select * from renamed