with source as (
    select * from {{ source('opendental', 'recall') }}
),

renamed as (
    select
        -- Primary key
        RecallNum as recall_id,

        -- Relationships
        PatNum as patient_id,
        RecallTypeNum as recall_type_id,

        -- Dates
        DateDueCalc as calculated_due_date,
        DateDue as due_date,
        DatePrevious as previous_date,
        DateScheduled as scheduled_date,
        DisableUntilDate as disable_until_date,

        -- Recall configuration
        RecallInterval as interval_days,
        RecallStatus as status_id,
        Priority as priority,
        TimePatternOverride as time_pattern_override,

        -- Disable conditions
        IsDisabled as is_disabled_flag,
        DisableUntilBalance as disable_until_balance,

        -- Additional information
        Note as recall_notes,

        -- Metadata
        DateTStamp as updated_at
    from source
)

select * from renamed