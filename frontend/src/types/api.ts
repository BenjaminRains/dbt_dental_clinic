// TypeScript interfaces for API responses
// These match the Pydantic models in the backend

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
    age?: number;
    age_category?: string; // Minor, Adult, Senior

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
    provider_name: string;
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

// Comprehensive provider interface matching the backend Provider model
export interface Provider {
    // Primary identification
    provider_id: number;

    // Provider identifiers
    provider_abbreviation?: string;
    provider_last_name?: string;
    provider_first_name?: string;
    provider_middle_initial?: string;
    provider_suffix?: string;
    provider_preferred_name?: string;
    provider_custom_id?: string;

    // Provider classifications
    fee_schedule_id?: number;
    specialty_id?: number;
    specialty_description?: string;
    provider_status?: number;
    provider_status_description?: string;
    anesthesia_provider_type?: number;
    anesthesia_provider_type_description?: string;

    // Provider credentials
    state_license?: string;
    dea_number?: string;
    blue_cross_id?: string;
    medicaid_id?: string;
    national_provider_id?: string;
    state_rx_id?: string;
    state_where_licensed?: string;
    taxonomy_code_override?: string;

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

    // Provider information
    provider_last_name?: string;
    provider_first_name?: string;
    provider_preferred_name?: string;
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
    provider_name?: string;
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
    provider_name: string;
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
    provider_name: string;
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

// API Response wrapper for error handling
export interface ApiResponse<T> {
    data?: T;
    error?: string;
    loading: boolean;
}
