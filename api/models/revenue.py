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
    
    # Provider information
    provider_last_name: Optional[str] = None
    provider_first_name: Optional[str] = None
    provider_preferred_name: Optional[str] = None
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
    opportunity_hour: int
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
    _created_at: Optional[datetime] = None
    _updated_at: Optional[datetime] = None
    _transformed_at: Optional[datetime] = None
    _mart_refreshed_at: Optional[datetime] = None
    
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
    """Revenue recovery plan with actionable items"""
    opportunity_id: int
    provider_name: Optional[str] = None
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
