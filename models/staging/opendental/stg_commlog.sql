with source as (
    select * from {{ source('opendental', 'commlog') }}
),

renamed as (
    select
        -- Keys and relationships
        CommlogNum as communication_id,
        PatNum as patient_id,
        CommType as communication_type_id,
        UserNum as user_id,
        ProgramNum as program_id,
        ReferralNum as referral_id,

        -- Communication timing
        CommDateTime as communication_datetime,
        DateTimeEnd as communication_end_datetime,
        TIMESTAMPDIFF(
            MINUTE, 
            CommDateTime,
            CASE
                WHEN DateTimeEnd > '0001-01-01 00:00:00' THEN DateTimeEnd
                ELSE NULL
            END
        ) as communication_duration_minutes,

        -- Communication details
        Mode_ as communication_mode,
        SentOrReceived as sent_or_received,
        CommSource as communication_source,
        CommReferralBehavior as referral_behavior,

        -- Derived fields
        CASE
            WHEN SentOrReceived = 0 THEN 'Received'
            WHEN SentOrReceived = 1 THEN 'Sent'
            ELSE 'Unknown'
        END as direction,

        CASE
            WHEN Mode_ = 0 THEN 'None'
            WHEN Mode_ = 1 THEN 'Email'
            WHEN Mode_ = 2 THEN 'Voice'
            WHEN Mode_ = 3 THEN 'Mail'
            WHEN Mode_ = 4 THEN 'InPerson'
            WHEN Mode_ = 5 THEN 'Text'
            ELSE 'Other'
        END as communication_mode_description,

        -- Metadata
        DateTStamp as updated_at,
        DateTEntry as entry_datetime,
        SigIsTopaz as is_topaz_signature

        -- Excluded fields with potential PHI:
        Note, -- (typically contains PHI details about the communication)
        Signature -- (contains PHI - patient signature)
    from source
)

select * from renamed