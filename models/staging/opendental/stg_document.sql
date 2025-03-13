with source as (
    select * from {{ source('opendental', 'document') }}
),

renamed as (
    select
        -- Primary key
        DocNum as document_id,

        -- Relationships and categories
        PatNum as patient_id,
        DocCategory as document_category_id,
        ProvNum as provider_id,
        MountItemNum as mount_item_id,

        -- Document metadata (excluding PHI content)
        ImgType as image_type,
        DateCreated as created_datetime,
        ToothNumbers as tooth_numbers,

        -- Document properties
        IsFlipped as is_flipped,
        DegreesRotated as degrees_rotated,
        PrintHeading as print_heading,

        -- Document source information
        ExternalSource as external_source,
        ImageCaptureType as image_capture_type,

        -- Image processing properties
        CropX as crop_x,
        CropY as crop_y,
        CropW as crop_width,
        CropH as crop_height,
        WindowingMin as windowing_min,
        WindowingMax as windowing_max,
        IsCropOld as is_crop_old,

        -- Metadata
        DateTStamp as updated_at

        -- fields that likely contain PHI:
        Description, 
        FileName, 
        Note, 
        Signature, 
        RawBase64, 
        Thumbnail
        SigIsTopaz -- it's related to the excluded Signature field)
        ExternalGUID -- (could be an identifier)
        OcrResponseData -- (could contain extracted text with PHI)
    from source
)

select * from renamed