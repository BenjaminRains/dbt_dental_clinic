{{ config(
    materialized='incremental',
    unique_key='document_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'document') }}
),

renamed_columns as (
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
        "DegreesRotated" as degrees_rotated,
        "CropX" as crop_x,
        "CropY" as crop_y,
        "CropW" as crop_width,
        "CropH" as crop_height,
        "WindowingMin" as windowing_min,
        "WindowingMax" as windowing_max,
        "ImageCaptureType" as image_capture_type,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"IsFlipped"') }} as is_flipped,
        {{ convert_opendental_boolean('"SigIsTopaz"') }} as is_signature_topaz,
        {{ convert_opendental_boolean('"IsCropOld"') }} as is_crop_old,
        {{ convert_opendental_boolean('"PrintHeading"') }} as print_heading,
        
        -- Date Fields
        {{ clean_opendental_date('"DateCreated"') }} as date_created,
        {{ clean_opendental_date('"DateTStamp"') }} as date_timestamp,

        -- Standardized metadata columns
        {{ standardize_metadata_columns() }}

    from source_data
)

select * from renamed_columns
