with source as (
    select * from {{ source('opendental', 'insbluebooklog') }}
),

renamed as (
    select
        -- Primary key
        InsBlueBookLogNum as insurance_bluebook_log_id,
        
        -- Relationships
        ClaimProcNum as claim_procedure_id,
        
        -- Financial information
        AllowedFee as allowed_fee_amount,
        
        -- Log details
        Description as description, -- Note: May contain PHI, consider filtering or anonymizing in downstream models
        
        -- Metadata
        DateTEntry as entry_datetime
    from source
)

select * from renamed