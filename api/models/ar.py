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
    pbn_ar_days: float  # Practice by Numbers AR Days = (Total AR × 30) ÷ collections_30_days
    collection_rate: float  # Collection rate (last 365 days): Collections / Production
    ar_ratio: float  # AR Ratio (PBN style, current month): Collections / Production
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
    """Individual AR item for priority queue"""
    patient_id: int
    provider_id: int
    patient_name: str  # From dim_patient join
    provider_name: str  # From dim_provider join
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

class PBNARSummary(BaseModel):
    """Practice by Numbers AR summary"""
    total_ar_outstanding: float
    current_amount: float
    current_percentage: float
    amount_30_60: float
    percentage_30_60: float
    amount_60_90: float
    percentage_60_90: float
    amount_over_90: float
    percentage_over_90: float
    patient_ar: float
    insurance_ar: float
    
    class Config:
        from_attributes = True

class StandardKPISummary(BaseModel):
    """Standard KPI metrics for comparison"""
    total_ar_outstanding: float
    current_amount: float
    current_percentage: float
    over_90_amount: float
    over_90_percentage: float
    patient_ar: float
    insurance_ar: float
    dso_days: float
    pbn_ar_days: float
    collection_rate: float
    
    class Config:
        from_attributes = True

class PBNKPISummary(BaseModel):
    """PBN KPI metrics for comparison"""
    total_ar_outstanding: float
    current_amount: float
    current_percentage: float
    amount_30_60: float
    percentage_30_60: float
    amount_60_90: float
    percentage_60_90: float
    amount_over_90: float
    percentage_over_90: float
    patient_ar: float
    insurance_ar: float
    
    class Config:
        from_attributes = True

class ComparisonDifferences(BaseModel):
    """Differences between standard and PBN KPIs"""
    total_ar_difference: float
    current_amount_difference: float
    current_percentage_difference: float
    over_90_amount_difference: float
    over_90_percentage_difference: float
    
    class Config:
        from_attributes = True

class ComparisonMetadata(BaseModel):
    """Metadata for the comparison"""
    snapshot_date: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    comparison_type: str
    
    class Config:
        from_attributes = True

class ARComparison(BaseModel):
    """Complete AR comparison between standard and PBN"""
    comparison_metadata: ComparisonMetadata
    standard_kpi: StandardKPISummary
    pbn_kpi: PBNKPISummary
    differences: ComparisonDifferences
    
    class Config:
        from_attributes = True

