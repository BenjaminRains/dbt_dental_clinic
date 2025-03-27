{{ config(
    materialized='incremental',
    unique_key='treatplan_param_id'
) }}

with source as (
    select * 
    from {{ source('opendental', 'treatplanparam') }}
    {% if is_incremental() %}
        -- Note: Since treatplanparam doesn't appear to have a direct timestamp field,
        -- we'll need to join with treatplan in downstream models for proper incremental logic
        where 1=1
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "TreatPlanParamNum" as treatplan_param_id,
        
        -- Foreign keys
        "PatNum" as patient_id,
        "TreatPlanNum" as treatplan_id,
        
        -- Display parameters
        "ShowDiscount" as show_discount,
        "ShowMaxDed" as show_max_deductible,
        "ShowSubTotals" as show_subtotals,
        "ShowTotals" as show_totals,
        "ShowCompleted" as show_completed,
        "ShowFees" as show_fees,
        "ShowIns" as show_insurance
    from source
)

select * from renamed
