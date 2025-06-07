{{
    config(
        materialized='table',
        schema='marts',
        unique_key='communication_id'
    )
}}

/*
Fact table for patient communications and messaging.
This model captures all communication activities including appointment reminders,
confirmations, marketing outreach, and patient messaging for comprehensive
communication effectiveness analysis.

Key features:
- Complete communication lifecycle tracking
- Multi-channel communication analysis
- Response and engagement tracking
- Communication timing and frequency
- Patient preference adherence
*/

with CommunicationBase as (
    select * from {{ ref('stg_opendental__commlog') }}
),

ConfirmationRequests as (
    select 
        cr.confirmation_request_id as communication_id,
        cr.appointment_id,
        cr.patient_id,
        cr.date_time_entry as communication_datetime,
        cr.date_time_confirmed as response_datetime,
        'Confirmation Request' as communication_type,
        case cr.confirmation_status
            when 0 then 'None'
            when 1 then 'Confirmed'
            when 2 then 'Call'
            when 3 then 'Rescheduled'
            when 4 then 'CancelledByPatient'
            when 5 then 'CancelledByOffice'
            else 'Unknown'
        end as communication_status,
        'Appointment' as communication_category,
        cr.confirmation_method as communication_method,
        null as provider_id,
        null as user_id,
        cr.note as communication_note,
        case when cr.date_time_confirmed is not null then true else false end as has_response,
        case when cr.confirmation_status = 1 then true else false end as is_successful
    from {{ ref('stg_opendental__confirmrequest') }} cr
),

EmailMessages as (
    select 
        em.email_message_id as communication_id,
        null as appointment_id,
        em.patient_id,
        em.msg_date_time as communication_datetime,
        em.date_read as response_datetime,
        'Email' as communication_type,
        case em.sent_or_received
            when 0 then 'Received'
            when 1 then 'Sent'
            when 2 then 'Sent Encrypted'
            else 'Unknown'
        end as communication_status,
        case 
            when em.subject like '%appointment%' or em.subject like '%reminder%' then 'Appointment'
            when em.subject like '%payment%' or em.subject like '%balance%' then 'Financial'
            when em.subject like '%treatment%' or em.subject like '%care%' then 'Clinical'
            else 'General'
        end as communication_category,
        'Email' as communication_method,
        em.provider_id,
        em.user_id,
        em.subject || ' - ' || left(em.body_text, 100) as communication_note,
        case when em.date_read is not null then true else false end as has_response,
        case when em.sent_or_received = 1 and em.date_read is not null then true else false end as is_successful
    from {{ ref('stg_opendental__emailmessage') }} em
),

TextMessages as (
    select 
        sm.sms_message_id as communication_id,
        null as appointment_id,
        sm.patient_id,
        sm.date_time_sent as communication_datetime,
        sm.date_time_delivered as response_datetime,
        'SMS' as communication_type,
        case sm.sms_status
            when 0 then 'Pending'
            when 1 then 'Sent'
            when 2 then 'Delivered'
            when 3 then 'Failed'
            when 4 then 'Received'
            else 'Unknown'
        end as communication_status,
        case 
            when sm.message_text like '%appointment%' or sm.message_text like '%reminder%' then 'Appointment'
            when sm.message_text like '%payment%' or sm.message_text like '%balance%' then 'Financial'
            when sm.message_text like '%confirm%' then 'Confirmation'
            else 'General'
        end as communication_category,
        'SMS' as communication_method,
        null as provider_id,
        sm.user_id,
        sm.message_text as communication_note,
        case when sm.sms_status in (2, 4) then true else false end as has_response,
        case when sm.sms_status = 2 then true else false end as is_successful
    from {{ ref('stg_opendental__smsmessage') }} sm
),

Final as (
    select
        -- Primary Key
        cb.communication_id,

        -- Foreign Keys
        cb.patient_id,
        cb.provider_id,
        cb.appointment_id,
        cb.user_id,

        -- Date and Time
        cb.communication_datetime,
        cb.response_datetime,
        extract(year from cb.communication_datetime) as communication_year,
        extract(month from cb.communication_datetime) as communication_month,
        extract(quarter from cb.communication_datetime) as communication_quarter,
        extract(dow from cb.communication_datetime) as communication_day_of_week,
        extract(hour from cb.communication_datetime) as communication_hour,

        -- Communication Details
        cb.communication_type,
        cb.communication_status,
        cb.communication_category,
        cb.communication_method,
        cb.communication_note,

        -- Response Metrics
        cb.has_response,
        cb.is_successful,
        case 
            when cb.response_datetime is not null and cb.communication_datetime is not null
            then extract(epoch from (cb.response_datetime - cb.communication_datetime))/3600
        end as response_time_hours,

        -- Timing Analysis
        case extract(dow from cb.communication_datetime)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as communication_day_name,

        case 
            when extract(hour from cb.communication_datetime) between 6 and 11 then 'Morning'
            when extract(hour from cb.communication_datetime) between 12 and 17 then 'Afternoon'
            when extract(hour from cb.communication_datetime) between 18 and 21 then 'Evening'
            else 'Night'
        end as communication_time_period,

        -- Communication Direction
        case 
            when cb.communication_type in ('Email') and cb.communication_status = 'Received' then 'Inbound'
            when cb.communication_type in ('SMS') and cb.communication_status = 'Received' then 'Inbound'
            else 'Outbound'
        end as communication_direction,

        -- Effectiveness Indicators
        case 
            when cb.communication_method = 'Email' and cb.has_response then 'Engaged'
            when cb.communication_method = 'SMS' and cb.is_successful then 'Delivered'
            when cb.communication_method = 'Phone' and cb.has_response then 'Connected'
            when cb.communication_type = 'Confirmation Request' and cb.is_successful then 'Confirmed'
            else 'Sent'
        end as engagement_level,

        -- Boolean Flags
        case when extract(dow from cb.communication_datetime) in (0, 6) then true else false end as sent_on_weekend,
        case when extract(hour from cb.communication_datetime) between 9 and 17 then true else false end as sent_during_business_hours,
        case when cb.communication_category = 'Appointment' then true else false end as is_appointment_related,
        case when cb.communication_category = 'Financial' then true else false end as is_financial_related,
        case when cb.communication_direction = 'Inbound' then true else false end as is_patient_initiated,

        -- Metadata
        current_timestamp as _loaded_at

    from (
        -- Union all communication sources
        select * from CommunicationBase
        where communication_datetime is not null
        
        union all
        
        select * from ConfirmationRequests
        
        union all
        
        select * from EmailMessages
        where communication_datetime is not null
        
        union all
        
        select * from TextMessages
        where communication_datetime is not null
        
    ) cb
)

select * from Final
