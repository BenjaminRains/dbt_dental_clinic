with source as (
    select * from {{ source('opendental', 'referral') }}
),

renamed as (
    select
        -- Primary key
        ReferralNum as referral_id,

        -- Personal identification
        LName as last_name,
        FName as first_name,
        MName as middle_name,
        Title as title,
        SSN as social_security_number, -- Sensitive personal information
        NationalProvID as npi_number,

        -- Business information
        BusinessName as business_name,
        Specialty as specialty_id,
        UsingTIN as using_tin_flag,

        -- Contact information
        Address as address_line_1,
        Address2 as address_line_2,
        City as city,
        ST as state,
        Zip as postal_code,
        Telephone as primary_phone,
        Phone2 as secondary_phone,
        EMail as email_address,

        -- Status and type flags
        IsHidden as is_hidden_flag,
        NotPerson as is_not_person_flag,
        IsDoctor as is_doctor_flag,
        IsTrustedDirect as is_trusted_direct_flag,
        IsPreferred as is_preferred_flag,

        -- Related entities
        PatNum as patient_id,
        Slip as slip_id,

        -- Notes (may contain PHI)
        Note as referral_notes,
        DisplayNote as display_notes,

        -- Metadata
        DateTStamp as updated_at
    from source
)

select * from renamed