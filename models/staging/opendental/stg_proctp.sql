with source as (
    select * from {{ source('opendental', 'proctp') }}
),

renamed as (
    select
        -- Primary key
        ProcTPNum as treatment_plan_procedure_id,

        -- Relationships
        TreatPlanNum as treatment_plan_id,
        PatNum as patient_id,
        ProcNumOrig as original_procedure_id,
        ProvNum as provider_id,
        ClinicNum as clinic_id,
        SecUserNumEntry as created_by_user_id,

        -- Procedure details
        ProcCode as procedure_code,
        Descript as description,
        ProcAbbr as procedure_abbreviation,
        ToothNumTP as tooth_number,
        Surf as surface,

        -- Clinical information
        Prognosis as prognosis,
        Dx as diagnosis,

        -- Ordering
        ItemOrder as display_order,
        Priority as priority,

        -- Financial amounts
        FeeAmt as fee_amount,
        FeeAllowed as allowed_fee_amount,
        PriInsAmt as primary_insurance_amount,
        SecInsAmt as secondary_insurance_amount,
        PatAmt as patient_amount,
        Discount as discount_amount,
        TaxAmt as tax_amount,
        CatPercUCR as category_percent_ucr,

        -- Dates
        DateTP as treatment_plan_date,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed