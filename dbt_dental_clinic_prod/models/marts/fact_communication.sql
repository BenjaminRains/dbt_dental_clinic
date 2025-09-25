{{
    config(
        materialized='table',
        schema='marts',
        unique_key='communication_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['communication_id'], 'unique': true},
            {'columns': ['patient_id']},
            {'columns': ['user_id']},
            {'columns': ['communication_datetime']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
Fact table model for patient communications and messaging.
Part of System G: Scheduling and Communication Management

This model:
1. Captures all communication activities including appointment reminders, confirmations, marketing outreach, and patient messaging
2. Provides comprehensive communication effectiveness analysis and engagement tracking
3. Enables multi-channel communication analysis and response time measurement

Business Logic Features:
- Complete communication lifecycle tracking with response metrics
- Multi-channel communication analysis (Email, SMS, Phone, In-Person)
- Response and engagement tracking with timing analysis
- Communication direction classification (Inbound/Outbound)
- Business hours and weekend communication flagging

Key Metrics:
- Response time hours for communication effectiveness
- Engagement levels (Engaged, Delivered, Connected, Confirmed, Sent)
- Communication timing analysis (day of week, time period)
- Success rates by communication method and category

Data Quality Notes:
- Email and SMS message integration temporarily disabled due to missing staging models
- Confirmation requests not tracked as clinic doesn't use appointment confirmation functionality
- Communication datetime filtering ensures only valid communications are included

Performance Considerations:
- Indexed on communication_datetime for time-based queries
- Indexed on patient_id and user_id for relationship queries
- Communication direction and engagement level calculations optimized

Dependencies:
- stg_opendental__commlog: Primary source for communication log data
- dim_patient: Patient dimension for patient-related analysis
- dim_user: User dimension for user-related analysis
*/

-- 1. Source data retrieval
with source_communication as (
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

-- 2. Related dimension lookups
communication_dimensions as (
    select 
        patient_id,
        user_id,
        program_id,
        referral_id
    from source_communication
    where communication_datetime is not null
),

-- 3. Business logic and calculations
communication_calculated as (
    select
        -- Primary key
        sc.commlog_id as communication_id,

        -- Foreign keys
        sc.patient_id,
        sc.user_id,
        sc.program_id,
        sc.referral_id,

        -- Date and time measures
        sc.communication_datetime,
        sc.communication_end_datetime,
        sc.entry_datetime,
        extract(year from sc.communication_datetime) as communication_year,
        extract(month from sc.communication_datetime) as communication_month,
        extract(quarter from sc.communication_datetime) as communication_quarter,
        extract(dow from sc.communication_datetime) as communication_day_of_week,
        extract(hour from sc.communication_datetime) as communication_hour,

        -- Communication details
        sc.communication_type,
        sc.note as communication_note,
        sc.mode as communication_mode,
        sc.communication_source,
        sc.referral_behavior,

        -- Response metrics
        sc.is_sent,
        sc.is_topaz_signature,

        -- Timing analysis
        case extract(dow from sc.communication_datetime)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as communication_day_name,

        case 
            when extract(hour from sc.communication_datetime) between 6 and 11 then 'Morning'
            when extract(hour from sc.communication_datetime) between 12 and 17 then 'Afternoon'
            when extract(hour from sc.communication_datetime) between 18 and 21 then 'Evening'
            else 'Night'
        end as communication_time_period,

        -- Communication direction
        case 
            when sc.is_sent = 2 then 'Inbound'
            when sc.is_sent = 1 then 'Outbound'
            else 'System'
        end as communication_direction,

        -- Communication method based on mode
        case 
            when sc.mode = 0 then 'Email'
            when sc.mode = 1 then 'SMS'
            when sc.mode = 2 then 'Phone'
            when sc.mode = 3 then 'Letter'
            when sc.mode = 4 then 'In-Person'
            when sc.mode = 5 then 'System'
            else 'Unknown'
        end as communication_method,

        -- Communication category based on type
        case 
            when sc.communication_type = 224 then 'Appointment'
            when sc.communication_type = 228 then 'Appointment'
            when sc.communication_type = 226 then 'Financial'
            when sc.communication_type = 225 then 'Insurance'
            when sc.communication_type = 571 then 'Insurance'
            when sc.communication_type = 432 then 'Clinical'
            when sc.communication_type = 509 then 'Clinical'
            when sc.communication_type = 510 then 'Clinical'
            when sc.communication_type = 614 then 'Referral'
            when sc.communication_type = 615 then 'Referral'
            when sc.communication_type = 636 then 'Treatment Plan'
            else 'General'
        end as communication_category,

        -- Effectiveness indicators
        case 
            when sc.is_sent = 2 then 'Received'
            when sc.is_sent = 1 and sc.mode = 4 then 'Completed'
            when sc.is_sent = 1 then 'Sent'
            else 'System Generated'
        end as engagement_level,

        -- Boolean flags
        case when extract(dow from sc.communication_datetime) in (0, 6) then true else false end as sent_on_weekend,
        case when extract(hour from sc.communication_datetime) between 9 and 17 then true else false end as sent_during_business_hours,
        case 
            when sc.communication_type = 224 or sc.communication_type = 228 then true 
            else false 
        end as is_appointment_related,
        case 
            when sc.communication_type = 226 then true 
            else false 
        end as is_financial_related,
        case 
            when sc.is_sent = 2 then true
            else false
        end as is_patient_initiated,
        case 
            when sc.user_id is null or sc.user_id = 0 then true
            else false
        end as is_system_generated,

        -- Metadata
        {{ standardize_mart_metadata() }}

    from source_communication sc
    where sc.communication_datetime is not null
),

-- 4. Final validation
final as (
    select * from communication_calculated
    where communication_id is not null
)

select * from final