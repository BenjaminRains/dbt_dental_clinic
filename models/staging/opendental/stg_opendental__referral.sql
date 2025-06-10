with source_data as (
    select * 
    from {{ source('opendental', 'referral') }}
    where "DateTStamp" >= '2023-01-01'
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"ReferralNum"', 'target': 'referral_id'},
            {'source': 'NULLIF("PatNum", 0)', 'target': 'patient_id'},
            {'source': '"Specialty"', 'target': 'specialty_id'}
        ]) }},
        
        -- Names and Identifiers
        nullif(trim("LName"), '') as last_name,
        nullif(trim("FName"), '') as first_name,
        nullif(trim("MName"), '') as middle_name,
        nullif(trim("SSN"), '') as ssn,
        nullif(trim("BusinessName"), '') as business_name,
        
        -- Professional Identifiers
        nullif(trim("NationalProvID"), '') as national_provider_id,
        nullif(trim("Title"), '') as title,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"UsingTIN"') }} as is_using_tin,
        {{ convert_opendental_boolean('"IsDoctor"') }} as is_doctor,
        {{ convert_opendental_boolean('"IsPreferred"') }} as is_preferred,
        {{ convert_opendental_boolean('"IsTrustedDirect"') }} as is_trusted_direct,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"NotPerson"') }} as not_person,
        
        -- Contact Information
        nullif(trim("Telephone"), '') as telephone,
        nullif(trim("Phone2"), '') as phone2,
        nullif(trim("EMail"), '') as email,
        
        -- Address Details
        nullif(trim("Address"), '') as address_line1,
        nullif(trim("Address2"), '') as address_line2,
        nullif(trim("City"), '') as city,
        nullif(trim("ST"), '') as state,
        nullif(trim("Zip"), '') as zip_code,
        
        -- Additional Attributes
        nullif(trim("Note"), '') as note,
        nullif(trim("DisplayNote"), '') as display_note,
        "Slip"::integer as slip,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
    
    from source_data
)

select * from renamed_columns
