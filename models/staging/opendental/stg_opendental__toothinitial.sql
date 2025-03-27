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
        "SecDateTEntry" as created_at,
        "SecDateTEdit" as updated_at,
        "DrawText" as draw_text
    from source
)

select * from renamed
