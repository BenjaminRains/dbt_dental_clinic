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
