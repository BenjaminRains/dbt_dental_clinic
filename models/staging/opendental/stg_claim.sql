with source as (
    select * from {{ source('opendental', 'claim') }}
),

renamed as (
    select
        -- Primary keys and relationships
        ClaimNum as claim_id,
        PatNum as patient_id,
        PlanNum as insurance_plan_id,
        PlanNum2 as secondary_insurance_plan_id,
        InsSubNum as insurance_subscriber_id,
        InsSubNum2 as secondary_insurance_subscriber_id,
        ProvTreat as treating_provider_id,
        ProvBill as billing_provider_id,
        ReferringProv as referring_provider_id,
        OrderingReferralNum as ordering_referral_id,
        ClinicNum as clinic_id,
        ClaimForm as claim_form_id,
        CustomTracking as custom_tracking_id,
        ProvOrderOverride as provider_order_override_id,
        
        -- Claim status and timing
        ClaimStatus as claim_status,
        ClaimType as claim_type,
        DateService as service_date,
        DateSent as sent_date,
        DateSentOrig as original_sent_date,
        DateResent as resent_date,
        DateReceived as received_date,
        
        -- Claim identifiers
        PreAuthString as preauthorization_number,
        PriorAuthorizationNumber as prior_authorization_number,
        RefNumString as reference_number,
        ClaimIdentifier as claim_identifier,
        OrigRefNum as original_reference_number,
        
        -- Financial details
        ClaimFee as claim_fee,
        InsPayEst as insurance_payment_estimate,
        InsPayAmt as insurance_payment_amount,
        DedApplied as deductible_applied,
        WriteOff as write_off_amount,
        ShareOfCost as share_of_cost,
        
        -- Service details
        PlaceService as place_of_service,
        ReasonUnderPaid as underpayment_reason,
        PatRelat as patient_relationship,
        PatRelat2 as secondary_patient_relationship,
        CorrectionType as correction_type,
        
        -- Accident and employment info
        AccidentRelated as accident_related,
        AccidentDate as accident_date,
        AccidentST as accident_state,
        EmployRelated as employment_related,
        DateIllnessInjuryPreg as illness_injury_pregnancy_date,
        DateIllnessInjuryPregQualifier as illness_injury_pregnancy_qualifier,
        DateOther as other_date,
        DateOtherQualifier as other_date_qualifier,
        
        -- Orthodontic information
        IsOrtho as is_orthodontic,
        OrthoDate as orthodontic_date,
        OrthoRemainM as orthodontic_remaining_months,
        OrthoTotalM as orthodontic_total_months,
        
        -- Prosthesis information
        IsProsthesis as is_prosthesis,
        PriorDate as prior_prosthesis_date,
        IsOutsideLab as is_outside_lab,
        
        -- Attachment information
        Radiographs as radiographs_count,
        AttachedImages as attached_images_count,
        AttachedModels as attached_models_count,
        AttachedFlags as attachment_flags,
        AttachmentID as attachment_id,
        
        -- Special program codes
        SpecialProgramCode as special_program_code,
        UniformBillType as uniform_bill_type,
        MedType as medical_type,
        AdmissionTypeCode as admission_type_code,
        AdmissionSourceCode as admission_source_code,
        PatientStatusCode as patient_status_code,
        
        -- Canadian-specific fields
        CanadianMaterialsForwarded as canadian_materials_forwarded,
        CanadianReferralProviderNum as canadian_referral_provider_number,
        CanadianReferralReason as canadian_referral_reason,
        CanadianIsInitialLower as canadian_is_initial_lower,
        CanadianDateInitialLower as canadian_date_initial_lower,
        CanadianMandProsthMaterial as canadian_mand_prosth_material,
        CanadianIsInitialUpper as canadian_is_initial_upper,
        CanadianDateInitialUpper as canadian_date_initial_upper,
        CanadianMaxProsthMaterial as canadian_max_prosth_material,
        CanadaTransRefNum as canada_transaction_reference_number,
        CanadaEstTreatStartDate as canada_est_treatment_start_date,
        CanadaInitialPayment as canada_initial_payment,
        CanadaPaymentMode as canada_payment_mode,
        CanadaTreatDuration as canada_treatment_duration,
        CanadaNumAnticipatedPayments as canada_num_anticipated_payments,
        CanadaAnticipatedPayAmount as canada_anticipated_payment_amount,
        
        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at,
        
        -- Excluded PHI fields:
        ClaimNote as claim_note,
        Narrative as narrative
    from source
)

select * from renamed

