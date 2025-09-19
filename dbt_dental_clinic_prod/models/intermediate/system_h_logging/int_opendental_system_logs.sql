{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='entry_log_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['entry_log_id'], 'unique': true},
            {'columns': ['user_id']},
            {'columns': ['log_source']},
            {'columns': ['entry_datetime']},
            {'columns': ['_transformed_at']}
        ],
        tags=['intermediate', 'system_logging']
    )
}}

/*
    Intermediate model for OpenDental System Logs
    Part of System H: System Logging & Monitoring
    
    This model:
    1. Consolidates system entry logs with user context for comprehensive audit trails
    2. Enriches raw log data with user information for meaningful analysis
    3. Provides standardized system activity data for monitoring and analytics
    
    Business Logic Features:
    - User Context Enrichment: Joins entry logs with user master data
    - Log Source Classification: Categorizes different system modules and components
    - System Entry Validation: Handles system-generated vs user-generated entries
    
    Data Quality Notes:
    - All records have foreign_key_type = 0 (system entries only)
    - User group defaults to 0 for system entries when user data is missing
    - Log source distribution: 98.15% main system, 1.48% specific modules, 0.35% other modules
    
    Performance Considerations:
    - Materialized as table for complex user joins and frequent querying
    - Indexed on key fields for optimal query performance
    - Handles large volume of system logs efficiently
*/

-- 1. Source data retrieval
with source_entry_logs as (
    select
        entry_log_id,
        user_id,
        foreign_key_type,
        foreign_key,
        log_source,
        entry_datetime,
        _loaded_at,
        _transformed_at,
        _created_at
    from {{ ref('stg_opendental__entrylog') }}
),

-- 2. Lookup/reference data
user_lookup as (
    select
        user_id,
        username,
        user_group_id
    from {{ ref('stg_opendental__userod') }}
),

-- 3. Business logic transformation
system_logs_enhanced as (
    select
        -- Primary identification
        el.entry_log_id,
        el.user_id,
        
        -- System log information
        el.foreign_key_type,
        el.foreign_key,
        el.log_source,
        el.entry_datetime,
        
        -- User context enrichment
        u.username,
        COALESCE(u.user_group_id, 0) as user_group_id,  -- Default to 0 for system entries
        
        -- Business logic categorization
        case 
            when el.log_source = 0 then 'Main System'
            when el.log_source = 7 then 'Specific Module A'
            when el.log_source = 16 then 'Rare Events'
            when el.log_source = 19 then 'Specific Module B'
            else 'Unknown Module'
        end as log_source_description,
        
        -- System entry classification
        case 
            when el.foreign_key_type::integer = 0 then 'System Entry'
            else 'Entity Entry'
        end as entry_type,
        
        -- Metadata
        {{ standardize_intermediate_metadata(
            primary_source_alias='el',
            source_metadata_fields=['_loaded_at', '_created_at']
        ) }}
        
    from source_entry_logs el
    left join user_lookup u
        on el.user_id = u.user_id
)

select * from system_logs_enhanced 
