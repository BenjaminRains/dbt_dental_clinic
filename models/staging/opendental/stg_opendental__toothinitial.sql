with source as (
    select * from {{ source('opendental', 'toothinitial') }}
),

renamed as (
    select
        "ToothInitialNum" as tooth_initial_id,
        "PatNum" as patient_id,
        "ToothNum" as tooth_id,
        "InitialType" as initial_type,
        "Movement" as movement,
        "DrawingSegment" as drawing_segment,
        "ColorDraw" as color_draw,
        "DrawText" as draw_text,
        current_timestamp as _loaded_at,
        "SecDateTEntry" as _created_at,
        "SecDateTEdit" as _updated_at
    from source
)

select * from renamed
