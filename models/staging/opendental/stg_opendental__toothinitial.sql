{{ config(
    materialized='incremental',
    unique_key='tooth_initial_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'toothinitial') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"ToothInitialNum"', 'target': 'tooth_initial_id'},
            {'source': 'NULLIF("PatNum", 0)', 'target': 'patient_id'}
        ]) }},
        
        -- Business attributes
        "ToothNum" as tooth_num,
        "InitialType" as initial_type,
        "Movement" as movement,
        "DrawingSegment" as drawing_segment,
        "ColorDraw" as color_draw,
        "DrawText" as draw_text,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"SecDateTEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
