{{ config(
    materialized='incremental',
    unique_key='treatplan_attach_num'
) }}

with source as (
    select * 
    from {{ source('opendental', 'treatplanattach') }}
    {% if is_incremental() %}
        -- Note: Since treatplanattach doesn't appear to have a last_edit_timestamp field,
        -- we'll need to join with treatplan in downstream models for proper incremental logic
        -- This is a placeholder that will be improved when we understand the full data flow
        where 1=1
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "TreatPlanAttachNum" as treatplan_attach_num,
        
        -- Foreign keys
        "TreatPlanNum" as treatplan_num,
        "ProcNum" as proc_num,
        
        -- Other fields
        "Priority" as priority
    from source
)

select * from renamed
