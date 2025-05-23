with source as (
    select * 
    from {{ source('opendental', 'referral') }}
    where "DateTStamp" >= '2023-01-01'
),

renamed as (
    select
        -- Primary Key
        "ReferralNum" as referral_id,
        
        -- Names and Identifiers
        "LName" as last_name,
        "FName" as first_name,
        "MName" as middle_name,
        "SSN" as ssn,
        "UsingTIN" as using_tin,
        "NationalProvID" as national_provider_id,
        "BusinessName" as business_name,
        
        -- Contact Information
        "Telephone" as telephone,
        "Phone2" as phone2,
        "EMail" as email,
        
        -- Address Details
        "Address" as address_line1,
        "Address2" as address_line2,
        "City" as city,
        "ST" as state,
        "Zip" as zip_code,
        
        -- Professional Details
        "Specialty" as specialty,
        "Title" as title,
        "IsDoctor" as is_doctor,
        "IsPreferred" as is_preferred,
        "IsTrustedDirect" as is_trusted_direct,
        
        -- Additional Attributes
        "Note" as note,
        "DisplayNote" as display_note,
        "IsHidden" as is_hidden,
        "NotPerson" as not_person,
        "PatNum" as patient_id,
        "Slip" as slip,
        
        -- Metadata
        current_timestamp as _loaded_at,
        "DateTStamp" as _created_at,
        "DateTStamp" as _updated_at
    
    from Source
)

select * from renamed
