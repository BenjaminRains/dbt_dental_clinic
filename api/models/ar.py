# api/models/ar.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import date

class ARKPISummary(BaseModel):
    """KPI summary for AR dashboard"""
    total_ar_outstanding: float
    current_amount: float  # 0-30 days
    current_percentage: float
    over_90_amount: float
    over_90_percentage: float
    patient_ar: float  # Patient responsibility portion
    insurance_ar: float  # Insurance estimate portion
    dso_days: float  # Days Sales Outstanding (legacy calculation)
    collection_rate: float  # Collection rate (last 365 days): Collections / Production
    ar_ratio: float  # AR Ratio (current month): Collections / Production
    high_risk_count: int
    high_risk_amount: float
    
    class Config:
        from_attributes = True

class ARAgingSummary(BaseModel):
    """Aging summary by bucket"""
    aging_bucket: str  # "0-30", "31-60", "61-90", "90+"
    amount: float
    percentage: float
    patient_count: int
    
    class Config:
        from_attributes = True

class ARPriorityQueueItem(BaseModel):
    """Individual AR item for priority queue (PII removed for portfolio)"""
    patient_id: int
    provider_id: int
    # patient_name: Removed (PII)
    # provider_name: str  # From dim_provider join
    total_balance: float
    balance_0_30_days: float
    balance_31_60_days: float
    balance_61_90_days: float
    balance_over_90_days: float
    aging_risk_category: str
    collection_priority_score: int
    days_since_last_payment: Optional[int]
    payment_recency: str
    collection_rate: Optional[float]
    
    class Config:
        from_attributes = True

class ARRiskDistribution(BaseModel):
    """Risk category distribution"""
    risk_category: str
    patient_count: int
    total_amount: float
    percentage: float
    
    class Config:
        from_attributes = True

class ARAgingTrend(BaseModel):
    """Aging trends over time"""
    date: date
    current_amount: float
    over_30_amount: float
    over_60_amount: float
    over_90_amount: float
    total_amount: float
    
    class Config:
        from_attributes = True

