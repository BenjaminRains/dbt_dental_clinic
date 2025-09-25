# api/models/provider.py
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ProviderSummary(BaseModel):
    """Provider performance summary model matching the frontend ProviderSummary interface"""
    provider_name: str
    total_appointments: int
    completed_appointments: int
    no_show_appointments: int
    broken_appointments: int
    unique_patients: int
    completion_rate: float
    no_show_rate: float
    cancellation_rate: float
    total_scheduled_production: float
    completed_production: float
    avg_production_per_appointment: float
    
    class Config:
        from_attributes = True

class Provider(BaseModel):
    """Comprehensive provider model based on dim_provider mart"""
    # Primary identification
    provider_id: int
    
    # Provider identifiers
    provider_abbreviation: Optional[str] = None
    provider_last_name: Optional[str] = None
    provider_first_name: Optional[str] = None
    provider_middle_initial: Optional[str] = None
    provider_suffix: Optional[str] = None
    provider_preferred_name: Optional[str] = None
    provider_custom_id: Optional[str] = None
    
    # Provider classifications
    fee_schedule_id: Optional[int] = None
    specialty_id: Optional[int] = None
    specialty_description: Optional[str] = None
    provider_status: Optional[int] = None
    provider_status_description: Optional[str] = None
    anesthesia_provider_type: Optional[int] = None
    anesthesia_provider_type_description: Optional[str] = None
    
    # Provider credentials
    state_license: Optional[str] = None
    dea_number: Optional[str] = None
    blue_cross_id: Optional[str] = None
    medicaid_id: Optional[str] = None
    national_provider_id: Optional[str] = None
    state_rx_id: Optional[str] = None
    state_where_licensed: Optional[str] = None
    taxonomy_code_override: Optional[str] = None
    
    # Provider flags
    is_secondary: Optional[bool] = None
    is_hidden: Optional[bool] = None
    is_using_tin: Optional[bool] = None
    has_signature_on_file: Optional[bool] = None
    is_cdanet: Optional[bool] = None
    is_not_person: Optional[bool] = None
    is_instructor: Optional[bool] = None
    is_hidden_report: Optional[bool] = None
    is_erx_enabled: Optional[bool] = None
    
    # Provider display properties
    provider_color: Optional[str] = None
    outline_color: Optional[str] = None
    schedule_note: Optional[str] = None
    web_schedule_description: Optional[str] = None
    web_schedule_image_location: Optional[str] = None
    
    # Financial and goals
    hourly_production_goal_amount: Optional[float] = None
    
    # Availability metrics
    scheduled_days: Optional[int] = None
    total_available_minutes: Optional[int] = None
    avg_daily_available_minutes: Optional[float] = None
    days_off_count: Optional[int] = None
    avg_minutes_per_scheduled_day: Optional[float] = None
    availability_percentage: Optional[float] = None
    
    # Provider categorizations
    provider_status_category: Optional[str] = None  # Active, Inactive, Terminated, Unknown
    provider_type_category: Optional[str] = None  # Primary, Secondary, Instructor, Non-Person
    provider_specialty_category: Optional[str] = None  # General Practice, Specialist, Hygiene, Other
    availability_performance_tier: Optional[str] = None  # Excellent, Good, Fair, Poor, No Data
    
    # Metadata
    _loaded_at: Optional[datetime] = None
    _created_at: Optional[datetime] = None
    _updated_at: Optional[datetime] = None
    _transformed_at: Optional[datetime] = None
    _mart_refreshed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True
