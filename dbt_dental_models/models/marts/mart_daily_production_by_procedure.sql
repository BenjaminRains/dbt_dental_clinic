/*
Daily production by procedure — aligned with OpenDental Daily → Production by Procedure.

Grain:
- One row per production_date × procedure_code.

Rules (validated 2026-07-16 vs OD golden exports):
- Report date = DateComplete (date_complete), NOT ProcDate.
- Include ProcStatus = 2 (Complete) only — exclude status 4 (Existing Prior).
- Quantity = count of procedure rows; Total Fees = sum(procedure_fee);
  Average Fee = Total Fees / Quantity.

Sources:
- stg_opendental__procedurelog
- stg_opendental__procedurecode
- stg_opendental__definition (OD procedure category ItemName via ProcCat)

Config: models/marts/_mart_daily_production_by_procedure.yml
*/

with complete_procedures as (
    select
        pl.date_complete::date as production_date,
        pl.procedure_code_id,
        pl.procedure_fee::numeric(18, 2) as procedure_fee
    from {{ ref('stg_opendental__procedurelog') }} pl
    where pl.date_complete is not null
      and pl.procedure_status = 2
),

with_codes as (
    select
        cp.production_date,
        trim(pc.procedure_code) as procedure_code,
        pc.description as procedure_description,
        pc.procedure_category_id,
        cp.procedure_fee
    from complete_procedures cp
    inner join {{ ref('stg_opendental__procedurecode') }} pc
        on cp.procedure_code_id = pc.procedure_code_id
    where trim(pc.procedure_code) <> ''
),

daily_by_code as (
    select
        wc.production_date,
        wc.procedure_code,
        max(wc.procedure_description) as procedure_description,
        max(def.item_name) as procedure_category,
        count(*) as procedure_quantity,
        round(sum(wc.procedure_fee)::numeric, 2) as total_fees,
        round(
            (sum(wc.procedure_fee)::numeric / nullif(count(*), 0)),
            2
        ) as average_fee
    from with_codes wc
    left join {{ ref('stg_opendental__definition') }} def
        on wc.procedure_category_id = def.definition_id
    group by wc.production_date, wc.procedure_code
),

final as (
    select
        production_date,
        procedure_code,
        procedure_description,
        procedure_category,
        procedure_quantity,
        total_fees,
        average_fee,

        {{ standardize_mart_metadata(preserve_source_metadata=false) }}

    from daily_by_code
)

select *
from final
