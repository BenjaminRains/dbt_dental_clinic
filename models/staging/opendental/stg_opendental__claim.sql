{{ config(
    materialized='incremental',
    unique_key='claim_id'
) }}

with source as (
    select * from {{ source('opendental', 'claim') }}
    where "DateService" >= '2023-01-01'
    {% if is_incremental() %}
        and "SecDateTEdit" > (select max(secure_edit_timestamp) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "ClaimNum" as claim_id,
        
        -- Foreign keys
        "PatNum" as patient_id,
        "PlanNum" as plan_id,
        "PlanNum2" as secondary_plan_id,
        "ProvTreat" as treating_provider_id,
        "ProvBill" as billing_provider_id,
        "ReferringProv" as referring_provider_id,
        "ClinicNum" as clinic_id,
        "ClaimForm" as claim_form_id,
        "InsSubNum" as insurance_subscriber_id,
        "InsSubNum2" as secondary_insurance_subscriber_id,
        "OrderingReferralNum" as ordering_referral_id,
        "ProvOrderOverride" as provider_order_override_id,
        "CustomTracking" as custom_tracking_id,
        "SecUserNumEntry" as secure_user_entry_id,
        
        -- Date fields
        "DateService" as service_date,
        "DateSent" as sent_date,
        "DateReceived" as received_date,
        "PriorDate" as prior_date,
        "AccidentDate" as accident_date,
        "OrthoDate" as ortho_date,
        "DateResent" as resent_date,
        "DateSentOrig" as original_sent_date,
        "DateIllnessInjuryPreg" as illness_injury_pregnancy_date,
        "DateOther" as other_date,
        "SecDateEntry" as secure_entry_date,
        "SecDateTEdit" as secure_edit_timestamp,
        
        -- Numerical values
        "ClaimFee" as claim_fee,
        "InsPayEst" as insurance_payment_estimate,
        "InsPayAmt" as insurance_payment_amount,
        "DedApplied" as deductible_applied,
        "WriteOff" as write_off,
        "ShareOfCost" as share_of_cost,
        
        -- Integer/status fields
        "PlaceService" as place_of_service,
        "EmployRelated" as is_employment_related,
        "IsOrtho" as is_ortho,
        "OrthoRemainM" as ortho_remaining_months,
        "PatRelat" as patient_relation,
        "PatRelat2" as secondary_patient_relation,
        "Radiographs" as radiographs,
        "AttachedImages" as attached_images,
        "AttachedModels" as attached_models,
        "SpecialProgramCode" as special_program_code,
        "MedType" as med_type,
        "CorrectionType" as correction_type,
        "OrthoTotalM" as ortho_total_months,
        "DateIllnessInjuryPregQualifier" as illness_injury_pregnancy_date_qualifier,
        "DateOtherQualifier" as other_date_qualifier,
        "IsOutsideLab" as is_outside_lab,
        
        -- Character fields
        "ClaimStatus" as claim_status,
        "PreAuthString" as pre_auth_string,
        "IsProsthesis" as is_prosthesis,
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
        "Narrative" as narrative
    
    from source
)

select * from renamed
