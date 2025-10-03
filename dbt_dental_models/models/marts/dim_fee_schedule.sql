{{ config(
    materialized='table',
    indexes=[
        {'columns': ['fee_schedule_id'], 'unique': true},
        {'columns': ['fee_schedule_description']},
        {'columns': ['is_hidden']}
    ]
) }}

/*
    Dimension table for fee schedule information
    Part of the marts layer - provides business-ready fee schedule dimensions
    
    This model:
    1. Creates a comprehensive dimension table for fee schedule analysis
    2. Standardizes fee schedule information across the organization
    3. Provides fee schedule hierarchy and configuration details
    4. Includes operational flags and type information
    
    Business Logic Features:
    - Fee Schedule Identification: Unique fee schedule identifiers with descriptions
    - Type Classification: Fee schedule types and categories
    - Configuration Flags: Operational settings and visibility controls
    - Display Management: Ordering and display preferences
    
    Data Sources:
    - stg_opendental__feesched: Staged fee schedule data from OpenDental
    
    Performance Considerations:
    - Table materialization for optimal query performance
    - Indexed on key lookup columns
    - Includes only essential attributes for analysis
*/

with fee_schedule_data as (
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

-- Add business logic and derived fields
final as (
    select
        -- Primary key
        fee_schedule_id,
        
        -- Basic Information
        fee_schedule_description,
        display_order,
        
        -- Type Information
        fee_schedule_type_id,
        
        -- Configuration Flags
        is_hidden,
        is_global_flag,
        
        -- Dates
        date_created,
        date_updated,
        
        -- Derived Fields
        case 
            when is_hidden = true then 'Hidden'
            when is_global_flag = true then 'Global'
            else 'Standard'
        end as fee_schedule_status,
        
        case 
            when is_global_flag = true then 'Global Fee Schedule'
            else 'Local Fee Schedule'
        end as fee_schedule_category,
        
        -- Metadata
        _loaded_at,
        _created_at,
        _updated_at
        
    from fee_schedule_data
)

select * from final
