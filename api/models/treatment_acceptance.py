# api/models/treatment_acceptance.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import date

class TreatmentAcceptanceKPISummary(BaseModel):
    """KPI summary for Treatment Acceptance dashboard"""
    # Primary KPIs (matching PBN)
    tx_acceptance_rate: Optional[float]  # Tx Accept %
    patient_acceptance_rate: Optional[float]  # Pt Accept %
    diagnosis_rate: Optional[float]  # Diag %
    
    # Volume Metrics
    patients_seen: int
    patients_with_exams: int
    patients_with_exams_and_presented: int
    patients_presented: int
    patients_accepted: int
    procedures_presented: int
    procedures_accepted: int
    
    # Financial Metrics
    tx_presented_amount: float
    tx_accepted_amount: float
    same_day_treatment_amount: float
    
    # Additional Metrics
    same_day_treatment_rate: Optional[float]
    procedure_acceptance_rate: Optional[float]
    patients_with_exams_presented: int
    patients_with_exams_accepted: int
    patients_with_exams_completed: int
    
    # Procedure Status Breakdown
    procedures_planned: int
    procedures_ordered: int
    procedures_completed: int
    procedures_scheduled: int
    
    class Config:
        from_attributes = True

class TreatmentAcceptanceSummary(BaseModel):
    """Daily grain summary for Treatment Acceptance"""
    procedure_date: date
    provider_id: int
    clinic_id: int
    
    # Volume Metrics
    patients_seen: int
    patients_presented: int
    patients_accepted: int
    procedures_presented: int
    procedures_accepted: int
    
    # Exam-specific metrics
    patients_with_exams_presented: int
    patients_with_exams_accepted: int
    patients_with_exams_completed: int
    
    # Financial Metrics
    tx_presented_amount: float
    tx_accepted_amount: float
    same_day_treatment_amount: float
    
    # Procedure Status Breakdown
    procedures_planned: int
    procedures_ordered: int
    procedures_completed: int
    procedures_scheduled: int
    
    # Percentage Metrics
    tx_acceptance_rate: Optional[float]
    patient_acceptance_rate: Optional[float]
    diagnosis_rate: Optional[float]
    same_day_treatment_rate: Optional[float]
    procedure_acceptance_rate: Optional[float]
    
    class Config:
        from_attributes = True

class TreatmentAcceptanceTrend(BaseModel):
    """Time series data for trending charts"""
    date: date
    tx_acceptance_rate: Optional[float]
    patient_acceptance_rate: Optional[float]
    diagnosis_rate: Optional[float]
    tx_presented_amount: float
    tx_accepted_amount: float
    patients_seen: int
    patients_presented: int
    patients_accepted: int
    
    class Config:
        from_attributes = True

class TreatmentAcceptanceProviderPerformance(BaseModel):
    """Provider-level performance breakdown"""
    provider_id: int
    # provider_name: str (removed for portfolio)
    tx_acceptance_rate: Optional[float]
    patient_acceptance_rate: Optional[float]
    diagnosis_rate: Optional[float]
    tx_presented_amount: float
    tx_accepted_amount: float
    patients_seen: int
    patients_with_exams: int
    patients_with_exams_and_presented: int
    patients_presented: int
    patients_accepted: int
    procedures_presented: int
    procedures_accepted: int
    same_day_treatment_amount: float
    same_day_treatment_rate: Optional[float]
    
    class Config:
        from_attributes = True

