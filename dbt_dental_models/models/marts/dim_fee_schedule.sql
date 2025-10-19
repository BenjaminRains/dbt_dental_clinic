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
    - int_fee_schedule: Intermediate model with fee schedule data and business logic
    
    Performance Considerations:
    - Table materialization for optimal query performance
    - Indexed on key lookup columns
    - Business logic centralized in intermediate layer
    - Includes comprehensive fee schedule attributes
*/

with fee_schedule_data as (
    select * from {{ ref('int_fee_schedule') }}
),

-- Final mart selection (business logic already in intermediate)
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
        
        -- Business Logic Flags (from intermediate)
        is_active_schedule,
        
        -- Derived Fields (from intermediate)
        fee_schedule_status,
        fee_schedule_category,
        
        -- Metadata
        {{ standardize_mart_metadata(
            primary_source_alias='fsd',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_transformed_at']
        ) }}
        
    from fee_schedule_data fsd
)

select * from final
