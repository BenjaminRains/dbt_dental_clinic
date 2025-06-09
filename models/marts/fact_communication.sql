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

TODO - APPOINTMENT CONFIRMATION ENHANCEMENT:
When stg_opendental__confirmrequest staging model is created, add back:
1. ConfirmationRequests CTE to transform confirmation request data
2. Add 'union all select * from ConfirmationRequests' to main query
3. This will include confirmation requests as a communication type alongside
   emails, SMS, and commlog entries for complete communication tracking
*/

with CommunicationBase as (
    select * from {{ ref('stg_opendental__commlog') }}
),

/*
TODO - EMAIL MESSAGE INTEGRATION:
The EmailMessages CTE has been temporarily removed because stg_opendental__emailmessage
staging model does not exist. A new approach needs to be developed for incorporating
email message information into the communication fact table.

Potential approaches:
1. Create stg_opendental__emailmessage staging model from emailmessage source table
2. Integrate email data through alternative communication tracking mechanisms
3. Use commlog entries that may contain email communication records

When implementing email message integration, restore the following structure:
- communication_id from email_message_id
- Email communication type classification
- Sent/Received/Encrypted status mapping
- Subject-based categorization (Appointment/Financial/Clinical/General)
- Response tracking via date_read
- Success metrics for sent encrypted emails
*/

/*
TODO - SMS MESSAGE INTEGRATION:
The TextMessages CTE has been temporarily removed because stg_opendental__smsmessage
staging model does not exist. A new approach needs to be developed for incorporating
SMS message information into the communication fact table.

Potential approaches:
1. Create stg_opendental__smsmessage staging model from smsmessage source table
2. Integrate SMS data through alternative communication tracking mechanisms  
3. Use commlog entries that may contain SMS communication records
4. Consider external SMS service integration logs

When implementing SMS message integration, restore the following structure:
- communication_id from sms_message_id
- SMS communication type classification
- Status mapping (Pending/Sent/Delivered/Failed/Received)
- Message text categorization (Appointment/Financial/Confirmation/General)
- Response tracking via delivery status
- Success metrics for delivered messages
- User tracking for sent messages
*/

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
        
        -- TODO: Add back when stg_opendental__confirmrequest is available:
        -- union all
        -- select * from ConfirmationRequests
        
        -- TODO: Add back when stg_opendental__emailmessage is available:
        -- union all
        -- select * from EmailMessages
        -- where communication_datetime is not null
        
        -- TODO: Add back when stg_opendental__smsmessage is available:
        -- union all
        -- select * from TextMessages
        -- where communication_datetime is not null
        
    ) cb
)

select * from Final
