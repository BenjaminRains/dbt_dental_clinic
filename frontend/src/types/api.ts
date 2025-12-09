// TypeScript interfaces for API responses
// These match the Pydantic models in the backend

// DBT Lineage and Metadata Types
export interface MetricLineageInfo {
    metric_name: string;
    source_model: string;
    source_schema: string;
    calculation_description: string;
    data_freshness: string;
    business_definition: string;
    dependencies: string[];
    last_updated: string;
}

export interface DBTModelMetadata {
    model_name: string;
    model_type: string;
    schema_name: string;
    description?: string;
    business_context?: string;
    technical_specs?: string;
    dependencies?: string[];
    downstream_models?: string[];
    data_quality_notes?: string;
    refresh_frequency?: string;
    grain_definition?: string;
    source_tables?: string[];
    created_at?: string;
    updated_at?: string;
    is_active: boolean;
}

// Patient interface matching the API Patient model
export interface Patient {
    // Primary identification
    patient_id: number;

    // Demographics
    middle_initial?: string;
    preferred_name?: string;
    gender?: string; // M=Male, F=Female, O=Other, U=Unknown
    language?: string;
    birth_date?: string; // ISO date string
    // age removed (PII) - replaced with numeric age_category
    age_category?: number; // 1=Minor (0-17), 2=Young Adult (18-34), 3=Middle Aged (35-54), 4=Older Adult (55+)

    // Status and Classification
    patient_status?: string; // Patient, NonPatient, Inactive, Archived, Deceased, Deleted
    position_code?: string; // Default, House, Staff, VIP, Other
    student_status?: string;
    urgency?: string; // Normal, High
    premedication_required?: boolean;

    // Contact Preferences
    preferred_confirmation_method?: string; // None, Email, Text, Phone
    preferred_contact_method?: string; // None, Email, Mail, Phone, Text, Other, Portal
    preferred_recall_method?: string; // None, Email, Text, Phone
    text_messaging_consent?: boolean;
    prefer_confidential_contact?: boolean;

    // Financial Information
    estimated_balance?: number;
    total_balance?: number;
    balance_0_30_days?: number;
    balance_31_60_days?: number;
    balance_61_90_days?: number;
    balance_over_90_days?: number;
    insurance_estimate?: number;
    payment_plan_due?: number;
    has_insurance_flag?: boolean;
    billing_cycle_day?: number;
    balance_status?: string; // No Balance, Outstanding Balance, Credit Balance

    // Important Dates
    first_visit_date?: string; // ISO date string
    deceased_datetime?: string; // ISO date string
    admit_date?: string; // ISO date string

    // Relationships
    guarantor_id?: number;
    primary_provider_id?: number;
    secondary_provider_id?: number;
    clinic_id?: number;
    fee_schedule_id?: number;

    // Patient Notes
    medical_notes?: string;
    treatment_notes?: string;
    financial_notes?: string;
    emergency_contact_name?: string;
    emergency_contact_phone?: string;

    // Patient Links (arrays)
    linked_patient_ids?: string[];
    link_types?: string[];

    // Patient Diseases
    disease_count?: number;
    disease_ids?: string[];
    disease_statuses?: string[];

    // Patient Documents
    document_count?: number;
    document_categories?: string[];

    // Metadata
    _loaded_at?: string; // ISO date string
    _created_at?: string; // ISO date string
    _updated_at?: string; // ISO date string
    _transformed_at?: string; // ISO date string
    _mart_refreshed_at?: string; // ISO date string
}

export interface RevenueTrend {
    date: string;
    revenue_lost: number;
    recovery_potential: number;
    opportunity_count: number;
}

export interface RevenueKPISummary {
    total_revenue_lost: number;
    total_recovery_potential: number;
    avg_recovery_potential: number;
    total_opportunities: number;
    affected_patients: number;
    affected_providers: number;
}

export interface RevenueLostSummary {
    appointments_lost_amount: number;  // Appmts Lost $ (Failed or Cancelled $$$)
    recovered_amount: number;  // Failed Re-Appnt $ (Recovered)
    lost_appointments_percent: number;  // Lost Appmts %
}

export interface RevenueLostOpportunity {
    failed_percent: number;
    cancelled_percent: number;
    failed_reappnt_percent: number;
    cancelled_reappnt_percent: number;
    failed_count: number;
    cancelled_count: number;
    failed_reappnt_count: number;
    cancelled_reappnt_count: number;
}

export interface LostAppointmentDetail {
    appointment_id: number;
    patient_id: number;
    patient_name?: string;
    original_date: string;
    status: string;  // "Failed" or "Cancelled"
    procedure_codes?: string[];
    production_amount: number;
    appointment_type?: string;
    next_date?: string;  // If rescheduled
    is_rescheduled: boolean;
}

export interface ProviderPerformance {
    provider_name: string;
    provider_specialty: string;
    date: string;
    production_amount: number;
    collection_amount: number;
    collection_rate: number;
    patient_count: number;
    appointment_count: number;
    no_show_count: number;
    no_show_rate: number;
    avg_production_per_patient: number;
    avg_production_per_appointment: number;
}

export interface ProviderSummary {
    provider_id: number;
    total_appointments: number;
    completed_appointments: number;
    no_show_appointments: number;
    broken_appointments: number;
    unique_patients: number;
    completion_rate: number;
    no_show_rate: number;
    cancellation_rate: number;
    total_scheduled_production: number;
    completed_production: number;
    avg_production_per_appointment: number;
}

// Comprehensive provider interface matching the backend Provider model - pseudonymized for public access
export interface Provider {
    // Primary identification
    provider_id: number;

    // Provider identifiers (non-PII only)
    provider_custom_id?: string;

    // Provider classifications
    fee_schedule_id?: number;
    specialty_id?: number;
    specialty_description?: string;
    provider_status?: number;
    provider_status_description?: string;
    anesthesia_provider_type?: number;
    anesthesia_provider_type_description?: string;

    // Provider flags
    is_secondary?: boolean;
    is_hidden?: boolean;
    is_using_tin?: boolean;
    has_signature_on_file?: boolean;
    is_cdanet?: boolean;
    is_not_person?: boolean;
    is_instructor?: boolean;
    is_hidden_report?: boolean;
    is_erx_enabled?: boolean;

    // Provider display properties
    provider_color?: string;
    outline_color?: string;
    schedule_note?: string;
    web_schedule_description?: string;
    web_schedule_image_location?: string;

    // Financial and goals
    hourly_production_goal_amount?: number;

    // Availability metrics
    scheduled_days?: number;
    total_available_minutes?: number;
    avg_daily_available_minutes?: number;
    days_off_count?: number;
    avg_minutes_per_scheduled_day?: number;
    availability_percentage?: number;

    // Provider categorizations
    provider_status_category?: string; // Active, Inactive, Terminated, Unknown
    provider_type_category?: string; // Primary, Secondary, Instructor, Non-Person
    provider_specialty_category?: string; // General Practice, Specialist, Hygiene, Other
    availability_performance_tier?: string; // Excellent, Good, Fair, Poor, No Data

    // Metadata
    _loaded_at?: string; // ISO date string
    _created_at?: string; // ISO date string
    _updated_at?: string; // ISO date string
    _transformed_at?: string; // ISO date string
    _mart_refreshed_at?: string; // ISO date string
}

export interface ARSummary {
    date: string;
    total_ar_balance: number;
    current_balance: number;
    overdue_balance: number;
    overdue_30_days: number;
    overdue_60_days: number;
    overdue_90_days: number;
    overdue_120_plus_days: number;
    collection_rate: number;
    avg_days_to_payment: number;
    patient_count_with_ar: number;
    insurance_ar_balance: number;
    patient_ar_balance: number;
}

// AR Aging Dashboard Types
export interface ARKPISummary {
    total_ar_outstanding: number;
    current_amount: number;
    current_percentage: number;
    over_90_amount: number;
    over_90_percentage: number;
    patient_ar?: number;  // Patient responsibility portion
    insurance_ar?: number;  // Insurance estimate portion
    dso_days: number;  // Legacy DSO calculation
    pbn_ar_days: number;  // Practice by Numbers AR Days = (Total AR × 360) ÷ billed_last_year
    collection_rate: number;  // Collection rate (last 365 days): Collections / Production
    ar_ratio?: number;  // AR Ratio (PBN style, current month): Collections / Production
    high_risk_count: number;
    high_risk_amount: number;
}

export interface ARAgingSummary {
    aging_bucket: string;
    amount: number;
    percentage: number;
    patient_count: number;
}

export interface ARPriorityQueueItem {
    patient_id: number;
    provider_id: number;
    patient_name: string;
    provider_name: string;
    total_balance: number;
    balance_0_30_days: number;
    balance_31_60_days: number;
    balance_61_90_days: number;
    balance_over_90_days: number;
    aging_risk_category: string;
    collection_priority_score: number;
    days_since_last_payment: number | null;
    payment_recency: string;
    collection_rate: number | null;
}

export interface ARRiskDistribution {
    risk_category: string;
    patient_count: number;
    total_amount: number;
    percentage: number;
}

export interface ARAgingTrend {
    date: string;
    current_amount: number;
    over_30_amount: number;
    over_60_amount: number;
    over_90_amount: number;
    total_amount: number;
}

export interface RevenueKPIs {
    total_revenue_lost: number;
    total_recovery_potential: number;
}

export interface RevenueOpportunity {
    // Primary identification
    date_id: number;
    opportunity_id: number;
    appointment_date: string;

    // Dimension keys
    provider_id?: number;
    clinic_id?: number;
    patient_id: number;
    appointment_id: number;

    // Provider information (pseudonymized)
    provider_type_category?: string;
    provider_specialty_category?: string;

    // Patient information
    patient_age?: number;
    patient_gender?: string;
    has_insurance_flag?: boolean;
    patient_specific: boolean;

    // Date information
    year: number;
    month: number;
    quarter: number;
    day_name: string;
    is_weekend: boolean;
    is_holiday: boolean;

    // Opportunity details
    opportunity_type: string;
    opportunity_subtype: string;
    lost_revenue?: number;
    lost_time_minutes?: number;
    missed_procedures?: string[];
    opportunity_datetime: string;
    recovery_potential: string;

    // Enhanced business logic
    opportunity_hour: number;
    time_period: string;
    revenue_impact_category: string;
    time_impact_category: string;
    recovery_timeline: string;
    recovery_priority_score: number;
    preventability: string;

    // Boolean flags
    has_revenue_impact: boolean;
    has_time_impact: boolean;
    recoverable: boolean;
    recent_opportunity: boolean;
    appointment_related: boolean;

    // Time analysis
    days_since_opportunity: number;
    estimated_recoverable_amount?: number;

    // Metadata
    _loaded_at?: string;
    _created_at?: string;
    _updated_at?: string;
    _transformed_at?: string;
    _mart_refreshed_at?: string;
}

export interface RevenueOpportunitySummary {
    opportunity_type: string;
    opportunity_subtype: string;
    total_opportunities: number;
    total_revenue_lost: number;
    total_recovery_potential: number;
    avg_priority_score: number;
    recent_opportunities: number;
    high_priority_opportunities: number;
}

export interface RevenueRecoveryPlan {
    opportunity_id: number;
    provider_id?: number;
    patient_id: number;
    opportunity_type: string;
    lost_revenue?: number;
    recovery_potential: string;
    priority_score: number;
    recommended_actions: string[];
    estimated_recovery_amount?: number;
    recovery_timeline: string;
}

export interface ProviderKPIs {
    active_providers: number;
    total_production: number;
    total_collection: number;
    avg_collection_rate: number;
}

export interface DashboardKPIs {
    revenue: RevenueKPIs;
    providers: ProviderKPIs;
}

// API Query Parameters
export interface DateRange {
    start_date?: string;
    end_date?: string;
}

export interface ProviderFilter extends DateRange {
    provider_id?: number;
}

// Appointment interfaces matching the backend models
export interface AppointmentSummary {
    date: string;
    provider_id: number;
    total_appointments: number;
    completed_appointments: number;
    no_show_appointments: number;
    broken_appointments: number;
    new_patient_appointments: number;
    hygiene_appointments: number;
    unique_patients: number;
    completion_rate: number;
    no_show_rate: number;
    cancellation_rate: number;
    utilization_rate: number;
    scheduled_production: number;
    completed_production: number;
}

export interface AppointmentDetail {
    appointment_id: number;
    patient_id: number;
    provider_id: number;
    appointment_date: string;
    appointment_time: string;
    appointment_type: string;
    appointment_status: string;
    is_completed: boolean;
    is_no_show: boolean;
    is_broken: boolean;
    scheduled_production_amount: number;
    appointment_length_minutes: number;
}

export interface AppointmentCreate {
    patient_id: number;
    provider_id: number;
    appointment_date: string;
    appointment_time: string;
    appointment_type: string;
    appointment_length_minutes: number;
    scheduled_production_amount?: number;
}

export interface AppointmentUpdate {
    appointment_status?: string;
    is_completed?: boolean;
    is_no_show?: boolean;
    is_broken?: boolean;
    scheduled_production_amount?: number;
    appointment_length_minutes?: number;
}

// Treatment Acceptance Types
export interface TreatmentAcceptanceKPISummary {
    // Primary KPIs (matching PBN)
    tx_acceptance_rate: number | null;
    patient_acceptance_rate: number | null;
    diagnosis_rate: number | null;

    // Volume Metrics
    patients_seen: number;
    patients_with_exams: number;
    patients_with_exams_and_presented: number;
    patients_presented: number;
    patients_accepted: number;
    procedures_presented: number;
    procedures_accepted: number;

    // Financial Metrics
    tx_presented_amount: number;
    tx_accepted_amount: number;
    same_day_treatment_amount: number;

    // Additional Metrics
    same_day_treatment_rate: number | null;
    procedure_acceptance_rate: number | null;
    patients_with_exams_presented: number;
    patients_with_exams_accepted: number;
    patients_with_exams_completed: number;

    // Procedure Status Breakdown
    procedures_planned: number;
    procedures_ordered: number;
    procedures_completed: number;
    procedures_scheduled: number;
}

export interface TreatmentAcceptanceSummary {
    procedure_date: string;
    provider_id: number;
    clinic_id: number;
    patients_seen: number;
    patients_presented: number;
    patients_accepted: number;
    procedures_presented: number;
    procedures_accepted: number;
    tx_acceptance_rate: number | null;
    patient_acceptance_rate: number | null;
    diagnosis_rate: number | null;
    tx_presented_amount: number;
    tx_accepted_amount: number;
    same_day_treatment_amount: number;
    same_day_treatment_rate: number | null;
    procedure_acceptance_rate: number | null;
    patients_with_exams_presented: number;
    patients_with_exams_accepted: number;
    patients_with_exams_completed: number;
    procedures_planned: number;
    procedures_ordered: number;
    procedures_completed: number;
    procedures_scheduled: number;
}

export interface TreatmentAcceptanceTrend {
    date: string;
    tx_acceptance_rate: number | null;
    patient_acceptance_rate: number | null;
    diagnosis_rate: number | null;
    tx_presented_amount: number;
    tx_accepted_amount: number;
    patients_seen: number;
    patients_presented: number;
    patients_accepted: number;
}

export interface TreatmentAcceptanceProviderPerformance {
    provider_id: number;
    provider_name: string;
    tx_acceptance_rate: number | null;
    patient_acceptance_rate: number | null;
    diagnosis_rate: number | null;
    tx_presented_amount: number;
    tx_accepted_amount: number;
    patients_seen: number;
    patients_with_exams: number;
    patients_with_exams_and_presented: number;
    patients_presented: number;
    patients_accepted: number;
    procedures_presented: number;
    procedures_accepted: number;
    same_day_treatment_amount: number;
    same_day_treatment_rate: number | null;
}

// Hygiene Retention Types
export interface HygieneRetentionSummary {
    recall_current_percent: number;  // Recall Current %
    hyg_pre_appointment_percent: number;  // Hyg Pre-Appointment Any %
    hyg_patients_seen: number;  // Hyg Patients Seen
    hyg_pts_reappntd: number;  // Hyg Pts Re-appntd
    recall_overdue_percent: number;  // Recall Overdue %
    not_on_recall_percent: number;  // Not on Recall %
}

// API Response wrapper for error handling
export interface ApiResponse<T> {
    data?: T;
    error?: string;
    loading: boolean;
}
