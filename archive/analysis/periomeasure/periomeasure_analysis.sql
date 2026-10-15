with source as (
    select * from {{ source('opendental', 'periomeasure') }}
),

renamed as (
    select
        -- Primary key
        PerioMeasureNum as periodontal_measure_id,

        -- Relationships
        PerioExamNum as periodontal_exam_id,

        -- Tooth information
        IntTooth as tooth_number,
        ToothValue as tooth_value,
        SequenceType as sequence_type,

        -- Measurement values
        MBvalue as mesial_buccal_value,
        Bvalue as buccal_value,
        DBvalue as distal_buccal_value,
        MLvalue as mesial_lingual_value,
        Lvalue as lingual_value,
        DLvalue as distal_lingual_value,

        -- Metadata
        SecDateTEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed