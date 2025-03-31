{{ config(materialized='view') }}

with source as (

    select * from {{ source('opendental', 'document') }}

),

renamed as (

    select
        -- Primary Key
        "DocNum" as document_id,
        
        -- Foreign Keys
        "PatNum" as patient_id,
        "DocCategory" as document_category_id,
        "MountItemNum" as mount_item_id,
        NULLIF("ProvNum", 0) as provider_id,
        
        -- String Fields
        "Description" as description,
        "FileName" as file_name,
        "ToothNumbers" as tooth_numbers,
        "Note" as note,
        "Signature" as signature,
        "ExternalGUID" as external_guid,
        "ExternalSource" as external_source,
        "RawBase64" as raw_base64,
        "Thumbnail" as thumbnail,
        "OcrResponseData" as ocr_response_data,
        
        -- Numeric Fields
        "ImgType" as image_type,
        "IsFlipped" as is_flipped,
        "DegreesRotated" as degrees_rotated,
        "SigIsTopaz" as is_signature_topaz,
        "CropX" as crop_x,
        "CropY" as crop_y,
        "CropW" as crop_width,
        "CropH" as crop_height,
        "WindowingMin" as windowing_min,
        "WindowingMax" as windowing_max,
        "IsCropOld" as is_crop_old,
        "ImageCaptureType" as image_capture_type,
        "PrintHeading" as print_heading,
        
        -- Timestamps
        "DateCreated" as created_at,
        "DateTStamp" as updated_at

    from source

)

select * from renamed
