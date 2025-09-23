# Type definitions for API responses
from pydantic import BaseModel
from typing import List, Optional
from datetime import date

# Revenue Analytics Types
class RevenueTrend(BaseModel):
    date: str
    revenue_lost: float
    recovery_potential: float
    opportunity_count: int

class RevenueKPISummary(BaseModel):
    total_revenue_lost: float
    total_recovery_potential: float
    avg_recovery_potential: float
    total_opportunities: int
    affected_patients: int
    affected_providers: int

# Provider Performance Types
class ProviderPerformance(BaseModel):
    provider_name: str
    provider_specialty: str
    date: str
    production_amount: float
    collection_amount: float
    collection_rate: float
    patient_count: int
    appointment_count: int
    no_show_count: int
    no_show_rate: float
    avg_production_per_patient: float
    avg_production_per_appointment: float

class ProviderSummary(BaseModel):
    provider_name: str
    provider_specialty: str
    total_production: float
    total_collection: float
    avg_collection_rate: float
    total_patients: int
    total_appointments: int
    total_no_shows: int
    avg_no_show_rate: float
    avg_production_per_patient: float
    avg_production_per_appointment: float

# Accounts Receivable Types
class ARSummary(BaseModel):
    date: str
    total_ar_balance: float
    current_balance: float
    overdue_balance: float
    overdue_30_days: float
    overdue_60_days: float
    overdue_90_days: float
    overdue_120_plus_days: float
    collection_rate: float
    avg_days_to_payment: float
    patient_count_with_ar: int
    insurance_ar_balance: float
    patient_ar_balance: float

# Dashboard KPI Types
class RevenueKPIs(BaseModel):
    total_revenue_lost: float
    total_recovery_potential: float

class ProviderKPIs(BaseModel):
    active_providers: int
    total_production: float
    total_collection: float
    avg_collection_rate: float

class DashboardKPIs(BaseModel):
    revenue: RevenueKPIs
    providers: ProviderKPIs
