with Source as (
    select * from {{ ref('stg_opendental__procedurecode') }}
),

FeeSchedules as (
    select
        fee_schedule_id,
        fee_schedule_description,
        fee_schedule_type_id,
        is_hidden
    from {{ ref('stg_opendental__feesched') }}
),

Definitions as (
    select
        definition_id,
        category_id,
        item_name,
        item_value,
        item_order,
        item_color
    from {{ ref('stg_opendental__definition') }}
),

-- Standard fees with ranking to get the most relevant fee per procedure code
StandardFees as (
    select
        fee_id,
        procedure_code_id,
        fee_schedule_id,
        clinic_id,
        provider_id,
        fee_amount as standard_fee,
        created_at,
        row_number() over (
            partition by procedure_code_id, clinic_id
            order by created_at desc
        ) as fee_rank
    from {{ ref('stg_opendental__fee') }}
),

-- Create fee statistics
FeeStats as (
    select
        procedure_code_id,
        count(distinct fee_id) as available_fee_options,
        min(fee_amount) as min_available_fee,
        max(fee_amount) as max_available_fee,
        avg(fee_amount) as avg_fee_amount
    from {{ ref('stg_opendental__fee') }}
    group by procedure_code_id
),

ProcedureCategories as (
    select distinct
        procedure_code_id,
        procedure_code,
        description,
        abbreviated_description,
        procedure_category_id,
        treatment_area,
        is_hygiene_flag,
        is_prosthetic_flag,
        is_radiology_flag,
        is_multi_visit_flag,
        base_units,
        default_provider_id,
        default_revenue_code,
        default_claim_note,
        default_treatment_plan_note,
        layman_term,
        medical_code,
        diagnostic_codes,
        _created_at,
        _updated_at
    from Source
),

ProcedureCategorization as (
    select
        *,
        -- Enhanced categorization for BI
        case
            when procedure_code like 'D1%' then 'Preventive'
            when procedure_code like 'D2%' then 'Restorative'
            when procedure_code like 'D3%' then 'Endodontics'
            when procedure_code like 'D4%' then 'Periodontics'
            when procedure_code like 'D5%' then 'Prosthodontics'
            when procedure_code like 'D6%' then 'Oral Surgery'
            when procedure_code like 'D7%' then 'Orthodontics'
            when procedure_code like 'D8%' then 'Other'
            when is_hygiene_flag = 1 then 'Preventive'
            when is_prosthetic_flag = 1 then 'Prosthodontics'
            when is_radiology_flag = 1 then 'Diagnostic'
            else 'Other'
        end as procedure_category,
        
        -- Complexity level based on procedure characteristics
        case
            when is_multi_visit_flag = 1 then 'Complex'
            when base_units >= 3 then 'Complex'
            when base_units >= 2 then 'Moderate'
            else 'Simple'
        end as complexity_level,
        
        -- Revenue tier based on procedure type and complexity
        case
            when procedure_category in ('Prosthodontics', 'Oral Surgery', 'Orthodontics') then 'High'
            when procedure_category in ('Endodontics', 'Periodontics') then 'Medium'
            when procedure_category in ('Preventive', 'Restorative', 'Diagnostic') then 'Low'
            else 'Low'
        end as revenue_tier,
        
        -- Clinical urgency level
        case
            when procedure_category in ('Oral Surgery', 'Endodontics') then 'High'
            when procedure_category in ('Periodontics', 'Restorative') then 'Medium'
            when procedure_category in ('Preventive', 'Prosthodontics', 'Orthodontics') then 'Low'
            else 'Low'
        end as clinical_urgency,
        
        -- Insurance billing complexity
        case
            when is_multi_visit_flag = 1 or base_units >= 3 then 'High'
            when medical_code is not null or diagnostic_codes is not null then 'Medium'
            else 'Low'
        end as insurance_complexity,
        
        -- Treatment planning category
        case
            when procedure_category in ('Prosthodontics', 'Orthodontics') then 'Long-term'
            when procedure_category in ('Endodontics', 'Periodontics') then 'Medium-term'
            when procedure_category in ('Preventive', 'Restorative', 'Diagnostic') then 'Short-term'
            else 'Short-term'
        end as treatment_planning_category
    from ProcedureCategories
),

Final as (
    select
        -- Primary key
        pc.procedure_code_id,
        
        -- Core attributes
        pc.procedure_code,
        pc.description,
        pc.abbreviated_description,
        pc.procedure_category_id,
        pc.treatment_area,
        def_treatment.item_name as treatment_area_desc,
        
        -- Enhanced categorizations
        pc.procedure_category,
        pc.complexity_level,
        pc.revenue_tier,
        pc.clinical_urgency,
        pc.insurance_complexity,
        pc.treatment_planning_category,
        
        -- Clinical flags
        pc.is_hygiene_flag,
        pc.is_prosthetic_flag,
        pc.is_radiology_flag,
        pc.is_multi_visit_flag,
        
        -- Clinical metrics
        pc.base_units,
        pc.default_provider_id,
        pc.default_revenue_code,
        
        -- Fee information
        sf.fee_id as standard_fee_id,
        sf.fee_schedule_id,
        sf.standard_fee,
        fs.fee_schedule_description,
        fs.fee_schedule_type_id,
        def_fee_type.item_name as fee_schedule_type_desc,
        
        -- Fee statistics
        fstat.available_fee_options,
        fstat.min_available_fee,
        fstat.max_available_fee,
        fstat.avg_fee_amount,
        
        -- Fee validation flags
        case 
            when sf.standard_fee is null then false
            else true 
        end as has_standard_fee,
        
        -- Documentation
        pc.default_claim_note,
        pc.default_treatment_plan_note,
        pc.layman_term,
        pc.medical_code,
        pc.diagnostic_codes,
        
        -- Metadata
        pc._created_at,
        pc._updated_at
    from ProcedureCategorization pc
    -- Join with fee information
    left join StandardFees sf
        on pc.procedure_code_id = sf.procedure_code_id
        and sf.fee_rank = 1  -- Only get the most recent fee
    left join FeeStats fstat
        on pc.procedure_code_id = fstat.procedure_code_id
    left join FeeSchedules fs
        on sf.fee_schedule_id = fs.fee_schedule_id
    -- Join with definitions for various coded values
    left join Definitions def_treatment
        on def_treatment.category_id = 5  -- Assuming category_id 5 is for treatment areas
        and def_treatment.item_value = pc.treatment_area::text
    left join Definitions def_fee_type
        on def_fee_type.category_id = 6  -- Assuming category_id 6 is for fee schedule types
        and def_fee_type.item_value = fs.fee_schedule_type_id::text
)

select * from Final
