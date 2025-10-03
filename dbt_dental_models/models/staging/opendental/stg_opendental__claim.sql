{{ config(
    materialized='incremental',
    unique_key='claim_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'claim') }}
    where {{ clean_opendental_date('"DateService"') }} >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"SecDateTEdit"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"ClaimNum"', 'target': 'claim_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"PlanNum"', 'target': 'plan_id'},
            {'source': '"PlanNum2"', 'target': 'secondary_plan_id'},
            {'source': '"ProvTreat"', 'target': 'treating_provider_id'},
            {'source': '"ProvBill"', 'target': 'billing_provider_id'},
            {'source': '"ReferringProv"', 'target': 'referring_provider_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"ClaimForm"', 'target': 'claim_form_id'},
            {'source': '"InsSubNum"', 'target': 'insurance_subscriber_id'},
            {'source': '"InsSubNum2"', 'target': 'secondary_insurance_subscriber_id'},
            {'source': '"OrderingReferralNum"', 'target': 'ordering_referral_id'},
            {'source': '"ProvOrderOverride"', 'target': 'provider_order_override_id'},
            {'source': '"CustomTracking"', 'target': 'custom_tracking_id'}
        ]) }},
        
        -- Date Fields
        {{ clean_opendental_date('"DateService"') }} as service_date,
        {{ clean_opendental_date('"DateSent"') }} as sent_date,
        {{ clean_opendental_date('"DateReceived"') }} as received_date,
        {{ clean_opendental_date('"PriorDate"') }} as prior_date,
        {{ clean_opendental_date('"AccidentDate"') }} as accident_date,
        {{ clean_opendental_date('"OrthoDate"') }} as ortho_date,
        {{ clean_opendental_date('"DateResent"') }} as resent_date,
        {{ clean_opendental_date('"DateSentOrig"') }} as original_sent_date,
        {{ clean_opendental_date('"DateIllnessInjuryPreg"') }} as illness_injury_pregnancy_date,
        {{ clean_opendental_date('"DateOther"') }} as other_date,
        
        -- Numerical Values
        "ClaimFee" as claim_fee,
        "InsPayEst" as insurance_payment_estimate,
        "InsPayAmt" as insurance_payment_amount,
        "DedApplied" as deductible_applied,
        "WriteOff" as write_off,
        "ShareOfCost" as share_of_cost,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"EmployRelated"') }} as is_employment_related,
        {{ convert_opendental_boolean('"IsOrtho"') }} as is_ortho,
        {{ convert_opendental_boolean('"IsOutsideLab"') }} as is_outside_lab,
        {{ convert_opendental_boolean('"IsProsthesis"') }} as is_prosthesis,
        
        -- Integer/Status Fields (using transform_id_columns for proper type conversion)
        {{ transform_id_columns([
            {'source': '"PlaceService"', 'target': 'place_of_service'},
            {'source': '"OrthoRemainM"', 'target': 'ortho_remaining_months'},
            {'source': '"PatRelat"', 'target': 'patient_relation'},
            {'source': '"PatRelat2"', 'target': 'secondary_patient_relation'},
            {'source': '"Radiographs"', 'target': 'radiographs'},
            {'source': '"AttachedImages"', 'target': 'attached_images'},
            {'source': '"AttachedModels"', 'target': 'attached_models'},
            {'source': '"SpecialProgramCode"', 'target': 'special_program_code'},
            {'source': '"MedType"', 'target': 'med_type'},
            {'source': '"CorrectionType"', 'target': 'correction_type'},
            {'source': '"OrthoTotalM"', 'target': 'ortho_total_months'},
            {'source': '"DateIllnessInjuryPregQualifier"', 'target': 'illness_injury_pregnancy_date_qualifier'},
            {'source': '"DateOtherQualifier"', 'target': 'other_date_qualifier'}
        ]) }},
        
        -- Character Fields
        "ClaimStatus" as claim_status,
        "PreAuthString" as pre_auth_string,
        "ReasonUnderPaid" as reason_under_paid,
        "ClaimNote" as claim_note,
        "ClaimType" as claim_type,
        "RefNumString" as reference_number_string,
        "AccidentRelated" as accident_related,
        "AccidentST" as accident_state,
        "AttachedFlags" as attached_flags,
        "AttachmentID" as attachment_id,
        "PriorAuthorizationNumber" as prior_authorization_number,
        "UniformBillType" as uniform_bill_type,
        "AdmissionTypeCode" as admission_type_code,
        "AdmissionSourceCode" as admission_source_code,
        "PatientStatusCode" as patient_status_code,
        "ClaimIdentifier" as claim_identifier,
        "OrigRefNum" as original_reference_number,
        "SecurityHash" as security_hash,
        "Narrative" as narrative,
        
        -- Raw metadata columns (preserved from source)
        {{ clean_opendental_date('"SecDateEntry"') }} as sec_date_entry,
        {{ clean_opendental_date('"SecDateTEdit"') }} as sec_date_t_edit,
        
        -- User ID column (using transform_id_columns for proper type conversion)
        {{ transform_id_columns([
            {'source': '"SecUserNumEntry"', 'target': 'sec_user_num_entry'}
        ]) }},
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"'
        ) }}
    
    from source_data
)

select * from renamed_columns
