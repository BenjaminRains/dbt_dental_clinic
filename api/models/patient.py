# api/models/patient.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class Patient(BaseModel):
    # Primary identification
    patient_id: int
    
    # Demographics (PII removed for portfolio)
    gender: Optional[str] = None  # M=Male, F=Female, O=Other
    language: Optional[str] = None
    age: Optional[int] = None
    age_category: Optional[str] = None  # Minor, Adult, Senior
    
    # Status and Classification
    patient_status: Optional[str] = None  # Patient, NonPatient, Inactive, Archived, Deceased, Deleted
    position_code: Optional[str] = None  # Default, House, Staff, VIP, Other
    student_status: Optional[str] = None
    urgency: Optional[str] = None  # Normal, High
    premedication_required: Optional[bool] = None
    
    # Contact Preferences
    preferred_confirmation_method: Optional[str] = None  # None, Email, Text, Phone
    preferred_contact_method: Optional[str] = None  # None, Email, Mail, Phone, Text, Other, Portal
    preferred_recall_method: Optional[str] = None  # None, Email, Text, Phone
    text_messaging_consent: Optional[bool] = None
    prefer_confidential_contact: Optional[bool] = None
    
    # Financial Information
    estimated_balance: Optional[float] = None
    total_balance: Optional[float] = None
    balance_0_30_days: Optional[float] = None
    balance_31_60_days: Optional[float] = None
    balance_61_90_days: Optional[float] = None
    balance_over_90_days: Optional[float] = None
    insurance_estimate: Optional[float] = None
    payment_plan_due: Optional[float] = None
    has_insurance_flag: Optional[bool] = None
    billing_cycle_day: Optional[int] = None
    balance_status: Optional[str] = None  # No Balance, Outstanding Balance, Credit Balance
    
    # Important Dates
    first_visit_date: Optional[datetime] = None
    deceased_datetime: Optional[datetime] = None
    admit_date: Optional[datetime] = None
    
    # Relationships
    guarantor_id: Optional[int] = None
    primary_provider_id: Optional[int] = None
    secondary_provider_id: Optional[int] = None
    clinic_id: Optional[int] = None
    fee_schedule_id: Optional[int] = None
    
    # Patient Notes (PII removed for portfolio)
    # medical_notes: Removed (PII)
    # treatment_notes: Removed (PII)
    # financial_notes: Removed (PII)
    
    # Patient Links (arrays)
    linked_patient_ids: Optional[List[str]] = None
    link_types: Optional[List[str]] = None
    
    # Patient Diseases
    disease_count: Optional[int] = None
    disease_ids: Optional[List[str]] = None
    disease_statuses: Optional[List[str]] = None
    
    # Patient Documents
    document_count: Optional[int] = None
    document_categories: Optional[List[str]] = None
    
    # Metadata
    _loaded_at: Optional[datetime] = None
    _created_at: Optional[datetime] = None
    _updated_at: Optional[datetime] = None
    _transformed_at: Optional[datetime] = None
    _mart_refreshed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class PaginatedPatients(BaseModel):
    patients: List[Patient]
    total: int