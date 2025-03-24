with source as (
    select * from {{ source('opendental', 'toothinitial') }}
),

renamed as (
    select
        "ToothInitialNum" as tooth_initial_num,
        "PatNum" as patient_num,
        "ToothNum" as tooth_num,
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
