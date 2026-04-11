# Type definitions for API responses
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import date, datetime

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
    provider_id: int
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
    provider_id: int
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


# Referral source KPIs (mart_referral_source_kpis)
class ReferralSourceKPIRow(BaseModel):
    reporting_month: str
    reporting_year: int
    reporting_month_number: int
    reporting_year_month: str
    referral_id: int
    referral_display_name: str
    referral_last_name: Optional[str] = None
    referral_first_name: Optional[str] = None
    referral_middle_name: Optional[str] = None
    referral_business_name: Optional[str] = None
    referral_title: Optional[str] = None
    referral_national_provider_id: Optional[str] = None
    referral_is_doctor: bool
    referral_not_person: bool
    referral_source_segment: str
    period_basis: str
    period_basis_sort_order: int
    period_basis_description: str
    distinct_patient_count: int
    production_value_in_period: float
    net_collections_in_period: float


class ReferralSourceMonthlySummary(BaseModel):
    """Aggregated by month + period_basis. Money sums are additive. summed_distinct_patient_count is SUM of mart row counts and is not deduplicated across referral_id (same patient can appear under multiple referrers)."""

    reporting_month: str
    reporting_year_month: str
    period_basis: str
    period_basis_sort_order: int
    total_production_value: float
    total_net_collections: float
    summed_distinct_patient_count: int
    source_row_count: int
    patient_counts_are_trustworthy_for_unique_patients: bool = False


class ReferralSourceSummaryResponse(BaseModel):
    """Time series summary for charts; includes a single note on patient count semantics."""

    rows: List[ReferralSourceMonthlySummary]
    patient_count_note: str


# DBT Lineage and Metadata Types
class DBTModelMetadata(BaseModel):
    model_name: str
    model_type: str
    schema_name: str
    description: Optional[str] = None
    business_context: Optional[str] = None
    technical_specs: Optional[str] = None
    dependencies: Optional[List[str]] = None
    downstream_models: Optional[List[str]] = None
    data_quality_notes: Optional[str] = None
    refresh_frequency: Optional[str] = None
    grain_definition: Optional[str] = None
    source_tables: Optional[List[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    is_active: bool = True

class DBTMetricLineage(BaseModel):
    metric_name: str
    metric_display_name: str
    source_model: str
    source_column: Optional[str] = None
    calculation_logic: Optional[str] = None
    business_definition: Optional[str] = None
    data_freshness: Optional[str] = None
    last_updated: Optional[datetime] = None
    is_active: bool = True

class MetricLineageInfo(BaseModel):
    """Lineage information for a specific metric displayed in UI"""
    metric_name: str
    source_model: str
    source_schema: str
    calculation_description: str
    data_freshness: str
    business_definition: str
    dependencies: List[str]
    last_updated: datetime
