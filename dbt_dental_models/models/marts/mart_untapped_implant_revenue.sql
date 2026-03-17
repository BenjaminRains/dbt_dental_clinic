{{ config(
    materialized='table',
    schema='marts',
    unique_key='patient_id',
    on_schema_change='fail',
    indexes=[
        {'columns': ['patient_id']},
        {'columns': ['first_extraction_bone_graft_date']},
        {'columns': ['provider_id']}
    ]
) }}

/*
Untapped Implant Revenue: patients who had extraction + bone graft on same day but no implant placement at any time.

Business question: How many people got a tooth extracted and a bone graft placed (same day)
but did NOT get D6010 (implant placement)? These are potential implant candidates / untapped revenue.

Procedure code logic:
- Extraction codes (any of): D7140 (erupted), D7210 (surgical partial bony), D7250 (surgical complete bony).
- Bone graft: D7953 (bone replacement graft).
- Implant placement (exclusion): D6010.

Same-day = same claim_date (service date) for both extraction and bone graft.
Exclusion = patient has no claim line with procedure_code = 'D6010' (ever).

Grain: One row per patient (first qualifying date and provider captured).
Dependencies: fact_claim.
*/

-- Procedure code sets (CDT)
{% set extraction_codes = ['D7140', 'D7210', 'D7250'] %}
{% set bone_graft_code = 'D7953' %}
{% set implant_placement_code = 'D6010' %}

with claim_procedures as (
    select
        patient_id,
        claim_date,
        procedure_code,
        provider_id,
        claim_id,
        procedure_id
    from {{ ref('fact_claim') }}
    where claim_date is not null
      and patient_id is not null
      and procedure_code is not null
),

-- Patients who have had implant placement (D6010) at any time
patients_with_implant as (
    select distinct patient_id
    from claim_procedures
    where procedure_code = '{{ implant_placement_code }}'
),

-- Per patient per day: flag extraction and bone graft
daily_codes as (
    select
        patient_id,
        claim_date,
        max(case when procedure_code in ({% for c in extraction_codes %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}) then 1 else 0 end) as has_extraction,
        max(case when procedure_code = '{{ bone_graft_code }}' then 1 else 0 end) as has_bone_graft,
        max(provider_id) as provider_id
    from claim_procedures
    group by patient_id, claim_date
),

-- Days where patient had both extraction and bone graft
same_day_extraction_and_bone_graft as (
    select
        patient_id,
        claim_date as extraction_bone_graft_date,
        provider_id
    from daily_codes
    where has_extraction = 1 and has_bone_graft = 1
),

-- One row per patient: first qualifying date, exclude anyone with D6010
untapped as (
    select
        s.patient_id,
        min(s.extraction_bone_graft_date) as first_extraction_bone_graft_date,
        max(s.extraction_bone_graft_date) as last_extraction_bone_graft_date,
        count(distinct s.extraction_bone_graft_date) as qualifying_visit_count,
        max(s.provider_id) as provider_id
    from same_day_extraction_and_bone_graft s
    left join patients_with_implant i on s.patient_id = i.patient_id
    where i.patient_id is null
    group by s.patient_id
),

patient_names as (
    select patient_id, first_name, last_name
    from {{ ref('dim_patient') }}
)

select
    u.patient_id,
    pn.first_name,
    pn.last_name,
    u.first_extraction_bone_graft_date,
    u.last_extraction_bone_graft_date,
    u.qualifying_visit_count,
    u.provider_id,
    {{ standardize_mart_metadata(preserve_source_metadata=false) }}
from untapped u
left join patient_names pn on u.patient_id = pn.patient_id
