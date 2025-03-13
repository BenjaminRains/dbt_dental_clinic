with source as (
    select * from {{ source('opendental', 'toothinitial') }}
),

renamed as (
    select
        -- Primary key
        ToothInitialNum as tooth_initial_id,

        -- Relationships
        PatNum as patient_id,

        -- Tooth information
        ToothNum as tooth_number,
        InitialType as initial_condition_type,
        Movement as movement_value,

        -- Drawing details
        DrawingSegment as drawing_segment,
        ColorDraw as drawing_color,
        DrawText as drawing_text,

        -- Metadata
        SecDateTEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed