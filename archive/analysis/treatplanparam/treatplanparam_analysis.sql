with source as (
    select * from {{ source('opendental', 'treatplanparam') }}
),

renamed as (
    select
        -- Primary key
        TreatPlanParamNum as treatment_plan_parameter_id,

        -- Relationships
        PatNum as patient_id,
        TreatPlanNum as treatment_plan_id,

        -- Display settings
        ShowFees as show_fees_flag,
        ShowIns as show_insurance_flag,
        ShowDiscount as show_discount_flag,
        ShowMaxDed as show_maximum_deductible_flag,

        -- Total display options
        ShowSubTotals as show_subtotals_flag,
        ShowTotals as show_totals_flag,

        -- Additional options
        ShowCompleted as show_completed_procedures_flag
    from source
)

select * from renamed