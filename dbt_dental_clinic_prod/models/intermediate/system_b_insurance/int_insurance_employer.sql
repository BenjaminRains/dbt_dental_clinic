{{ config(        materialized='table',
        
        unique_key='employer_id') }}

/*
This model integrates employer data with insurance plans. Employers are entities
that sponsor insurance plans for patients. This model supports employer-based
insurance plan analysis and reporting.

Key relationships:
- Employers are linked to insurance plans through the employer_id field
- Insurance plans may have one employer but an employer can sponsor multiple plans
*/

with Employer as (
    select
        -- Primary Key
        employer_id,
        
        -- Attributes
        employer_name,
        address,
        address2,
        city,
        state,
        zip,
        phone
    from {{ ref('stg_opendental__employer') }}
),

InsurancePlan as (
    select
        insurance_plan_id,
        employer_id,
        carrier_id,
        group_name,
        group_number
    from {{ ref('stg_opendental__insplan') }}
    where employer_id is not null
),

-- Get counts of how many plans each employer has
EmployerPlanCount as (
    select
        employer_id,
        count(insurance_plan_id) as plan_count
    from InsurancePlan
    group by employer_id
),

-- Get plan information for reporting
EmployerPlans as (
    select
        employer_id,
        json_agg(
            json_build_object(
                'insurance_plan_id', insurance_plan_id,
                'carrier_id', carrier_id,
                'group_name', group_name,
                'group_number', group_number
            )
        ) as insurance_plans
    from InsurancePlan
    group by employer_id
),

Final as (
    select
        -- Primary Key
        e.employer_id,
        
        -- Employer Details
        e.employer_name,
        e.address,
        e.address2,
        e.city,
        e.state,
        e.zip,
        e.phone,
        
        -- Insurance Plan Information
        coalesce(epc.plan_count, 0) as plan_count,
        coalesce(ep.insurance_plans, '[]') as insurance_plans
        
    from Employer e
    left join EmployerPlanCount epc
        on e.employer_id = epc.employer_id
    left join EmployerPlans ep
        on e.employer_id = ep.employer_id
)

select * from Final
