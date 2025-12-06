# api/models/revenue.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class RevenueTrend(BaseModel):
    """Revenue trend data for time-series analysis"""
    date: str
    revenue_lost: float
    recovery_potential: float
    opportunity_count: int
    
    class Config:
        from_attributes = True

class RevenueKPISummary(BaseModel):
    """Revenue KPI summary for dashboard display"""
    total_revenue_lost: float
    total_recovery_potential: float
    avg_recovery_potential: float
    total_opportunities: int
    affected_patients: int
    affected_providers: int
    
    class Config:
        from_attributes = True

class RevenueOpportunity(BaseModel):
    """Individual revenue opportunity from mart_revenue_lost"""
    # Primary identification
    date_id: int
    opportunity_id: int
    appointment_date: str
    
    # Dimension keys
    provider_id: Optional[int] = None
    clinic_id: Optional[int] = None
    patient_id: int
    appointment_id: int
    
    # Provider information (pseudonymized)
    provider_type_category: Optional[str] = None
    provider_specialty_category: Optional[str] = None
    
    # Patient information
    patient_age: Optional[int] = None
    patient_gender: Optional[str] = None
    has_insurance_flag: Optional[bool] = None
    patient_specific: bool
    
    # Date information
    year: int
    month: int
    quarter: int
    day_name: str
    is_weekend: bool
    is_holiday: bool
    
    # Opportunity details
    opportunity_type: str
    opportunity_subtype: str
    lost_revenue: Optional[float] = None
    lost_time_minutes: Optional[int] = None
    missed_procedures: Optional[List[str]] = None
    opportunity_datetime: str
    recovery_potential: str
    
    # Enhanced business logic
    opportunity_hour: Optional[int] = None  # Can be NULL for date-only fields (Treatment Plan Delay, Claim Rejection)
    time_period: str
    revenue_impact_category: str
    time_impact_category: str
    recovery_timeline: str
    recovery_priority_score: int
    preventability: str
    
    # Boolean flags
    has_revenue_impact: bool
    has_time_impact: bool
    recoverable: bool
    recent_opportunity: bool
    appointment_related: bool
    
    # Time analysis
    days_since_opportunity: int
    estimated_recoverable_amount: Optional[float] = None
    
    # Metadata
    _loaded_at: Optional[datetime] = None
    _updated_at: Optional[datetime] = None
    _created_by: Optional[int] = None
    
    class Config:
        from_attributes = True

class RevenueOpportunitySummary(BaseModel):
    """Summary of revenue opportunities by type and category"""
    opportunity_type: str
    opportunity_subtype: str
    total_opportunities: int
    total_revenue_lost: float
    total_recovery_potential: float
    avg_priority_score: float
    recent_opportunities: int
    high_priority_opportunities: int
    
    class Config:
        from_attributes = True

class RevenueRecoveryPlan(BaseModel):
    """Revenue recovery plan with actionable items - pseudonymized for public access"""
    opportunity_id: int
    provider_id: Optional[int] = None
    patient_id: int
    opportunity_type: str
    lost_revenue: Optional[float] = None
    recovery_potential: str
    priority_score: int
    recommended_actions: List[str]
    estimated_recovery_amount: Optional[float] = None
    recovery_timeline: str
    
    class Config:
        from_attributes = True

class RevenueLostSummary(BaseModel):
    """PBN-style Revenue Lost summary metrics"""
    appointments_lost_amount: float  # Appmts Lost $ (Failed or Cancelled $$$)
    recovered_amount: float  # Failed Re-Appnt $ (Recovered)
    lost_appointments_percent: float  # Lost Appmts %
    
    class Config:
        from_attributes = True

class RevenueLostOpportunity(BaseModel):
    """PBN-style Opportunity metrics (Failed %, Cancelled %, Re-appnt %)"""
    failed_percent: float
    cancelled_percent: float
    failed_reappnt_percent: float
    cancelled_reappnt_percent: float
    failed_count: int
    cancelled_count: int
    failed_reappnt_count: int
    cancelled_reappnt_count: int
    
    class Config:
        from_attributes = True

class LostAppointmentDetail(BaseModel):
    """Detailed information about a cancelled or failed appointment"""
    appointment_id: int
    patient_id: int
    # patient_name: Optional[str] = None (removed for portfolio)
    original_date: str
    status: str  # "Failed" or "Cancelled"
    procedure_codes: Optional[List[str]] = None
    production_amount: float
    appointment_type: Optional[str] = None
    next_date: Optional[str] = None  # If rescheduled
    is_rescheduled: bool
    
    class Config:
        from_attributes = True