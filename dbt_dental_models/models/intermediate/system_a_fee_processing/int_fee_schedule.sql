{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='fee_schedule_id',
    indexes=[
        {'columns': ['fee_schedule_id'], 'unique': true},
        {'columns': ['is_active_schedule']},
        {'columns': ['fee_schedule_type_id']}
    ],
    tags=['fee_processing']
) }}

/*
    Intermediate model for fee schedule information
    Part of System A: Fee Processing
    
    This model:
    1. Provides clean fee schedule data with business logic
    2. Applies status and categorization rules
    3. Creates derived fields for fee schedule management
    4. Establishes foundation for fee schedule analysis
    
    Business Logic Features:
    - Fee Schedule Status Classification: Active, hidden, global
    - Type Categorization: Standard vs global fee schedules
    - Display Management: Ordering for UI presentation
    
    Data Sources:
    - stg_opendental__feesched: Primary fee schedule data
    
    Data Quality Notes:
    - Hidden fee schedules retained for historical fee lookups
    - Global flag indicates organization-wide vs clinic-specific schedules
    
    Performance Considerations:
    - Table materialization for fast lookups
    - Indexed on key lookup columns
*/

with fee_schedule_base as (
    select
        fee_schedule_id,
        fee_schedule_type_id,
        fee_schedule_description,
        display_order,
        is_hidden,
        is_global_flag,
        date_created,
        date_updated,
        _loaded_at,
        _created_at,
        _updated_at
    from {{ ref('stg_opendental__feesched') }}
    where fee_schedule_id is not null
),

fee_schedule_enhanced as (
    select
        -- Core fields
        fee_schedule_id,
        fee_schedule_type_id,
        fee_schedule_description,
        display_order,
        is_hidden,
        is_global_flag,
        date_created,
        date_updated,
        
        -- Business logic flags
        case 
            when is_hidden = false then true
            else false
        end as is_active_schedule,
        
        -- Status categorization
        case 
            when is_hidden = true then 'Hidden'
            when is_global_flag = true then 'Global'
            else 'Standard'
        end as fee_schedule_status,
        
        -- Type categorization
        case 
            when is_global_flag = true then 'Global Fee Schedule'
            else 'Local Fee Schedule'
        end as fee_schedule_category
        
    from fee_schedule_base
),

final as (
    select
        fse.*,
        -- Standardized intermediate metadata
        {{ standardize_intermediate_metadata(
            primary_source_alias='fsb',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
    from fee_schedule_enhanced fse
    inner join fee_schedule_base fsb
        on fse.fee_schedule_id = fsb.fee_schedule_id
)

select * from final

