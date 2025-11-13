# api/services/treatment_acceptance_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)

def get_treatment_acceptance_kpi_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None,
    clinic_id: Optional[int] = None
) -> dict:
    """
    Get aggregated KPI summary for Treatment Acceptance dashboard
    - Aggregates daily mart data to date range
    - Supports provider and clinic filtering
    - Returns summary metrics matching PBN design
    """
    query = """
    SELECT 
        -- Volume Metrics
        SUM(patients_seen) as patients_seen,
        SUM(patients_with_exams) as patients_with_exams,
        SUM(patients_with_exams_and_presented) as patients_with_exams_and_presented,
        SUM(patients_presented) as patients_presented,
        SUM(patients_accepted) as patients_accepted,
        SUM(procedures_presented) as procedures_presented,
        SUM(procedures_accepted) as procedures_accepted,
        
        -- Exam-specific metrics
        SUM(patients_with_exams_presented) as patients_with_exams_presented,
        SUM(patients_with_exams_accepted) as patients_with_exams_accepted,
        SUM(patients_with_exams_completed) as patients_with_exams_completed,
        
        -- Financial Metrics
        SUM(tx_presented_amount) as tx_presented_amount,
        SUM(tx_accepted_amount) as tx_accepted_amount,
        SUM(same_day_treatment_amount) as same_day_treatment_amount,
        
        -- Procedure Status Breakdown
        SUM(procedures_planned) as procedures_planned,
        SUM(procedures_ordered) as procedures_ordered,
        SUM(procedures_completed) as procedures_completed,
        SUM(procedures_scheduled) as procedures_scheduled,
        
        -- Percentage Metrics (calculated from aggregated totals)
        CASE 
            WHEN SUM(tx_presented_amount) > 0 
            THEN ROUND((SUM(tx_accepted_amount)::numeric / NULLIF(SUM(tx_presented_amount), 0) * 100)::numeric, 2)
            WHEN SUM(tx_accepted_amount) > 0 
            THEN NULL
            ELSE 0
        END as tx_acceptance_rate,
        
        CASE 
            WHEN SUM(patients_presented) > 0 
            THEN ROUND((SUM(patients_accepted)::numeric / NULLIF(SUM(patients_presented), 0) * 100)::numeric, 2)
            WHEN SUM(patients_accepted) > 0 
            THEN NULL
            ELSE 0
        END as patient_acceptance_rate,
        
        -- Diagnosis Rate: (Patients with Exams AND Presented / Patients with Exams) * 100
        -- PBN Definition: (Patients with Treatment Plans / Patients Receiving Exams) * 100
        -- For procedure-based approach: (Patients with Exams AND Presented / Patients with Exams) * 100
        -- Exam must be within 30 days before treatment presentation (matching PBN requirement)
        CASE 
            WHEN SUM(patients_with_exams) > 0 
            THEN ROUND((SUM(patients_with_exams_and_presented)::numeric / NULLIF(SUM(patients_with_exams), 0) * 100)::numeric, 2)
            ELSE NULL
        END as diagnosis_rate,
        
        CASE 
            WHEN SUM(tx_presented_amount) > 0 
            THEN ROUND((SUM(same_day_treatment_amount)::numeric / NULLIF(SUM(tx_presented_amount), 0) * 100)::numeric, 2)
            WHEN SUM(same_day_treatment_amount) > 0 
            THEN NULL
            ELSE 0
        END as same_day_treatment_rate,
        
        CASE 
            WHEN SUM(procedures_presented) > 0 
            THEN ROUND((SUM(procedures_accepted)::numeric / NULLIF(SUM(procedures_presented), 0) * 100)::numeric, 2)
            WHEN SUM(procedures_accepted) > 0 
            THEN NULL
            ELSE 0
        END as procedure_acceptance_rate
        
    FROM raw_marts.mart_procedure_acceptance_summary
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND procedure_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND procedure_date <= :end_date"
        params['end_date'] = end_date
    if provider_id is not None:
        query += " AND provider_id = :provider_id"
        params['provider_id'] = provider_id
    if clinic_id is not None:
        query += " AND clinic_id = :clinic_id"
        params['clinic_id'] = clinic_id
    
    try:
        result = db.execute(text(query), params).fetchone()
        if result:
            return dict(result._mapping)
        else:
            # Return empty summary with zeros
            return {
                'patients_seen': 0,
                'patients_presented': 0,
                'patients_accepted': 0,
                'procedures_presented': 0,
                'procedures_accepted': 0,
                'patients_with_exams_presented': 0,
                'patients_with_exams_accepted': 0,
                'patients_with_exams_completed': 0,
                'tx_presented_amount': 0.0,
                'tx_accepted_amount': 0.0,
                'same_day_treatment_amount': 0.0,
                'procedures_planned': 0,
                'procedures_ordered': 0,
                'procedures_completed': 0,
                'procedures_scheduled': 0,
                'tx_acceptance_rate': None,
                'patient_acceptance_rate': None,
                'diagnosis_rate': None,
                'same_day_treatment_rate': None,
                'procedure_acceptance_rate': None
            }
    except SQLAlchemyError as e:
        logger.error(f"Error fetching treatment acceptance KPI summary: {str(e)}")
        raise

def get_treatment_acceptance_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None,
    clinic_id: Optional[int] = None
) -> List[dict]:
    """
    Get daily grain summary for Treatment Acceptance
    - Returns daily data for date range
    - Supports provider and clinic filtering
    - Used for trending charts and detailed analysis
    """
    query = """
    SELECT 
        procedure_date,
        provider_id,
        clinic_id,
        patients_seen,
        patients_presented,
        patients_accepted,
        procedures_presented,
        procedures_accepted,
        patients_with_exams_presented,
        patients_with_exams_accepted,
        patients_with_exams_completed,
        tx_presented_amount,
        tx_accepted_amount,
        same_day_treatment_amount,
        procedures_planned,
        procedures_ordered,
        procedures_completed,
        procedures_scheduled,
        tx_acceptance_rate,
        patient_acceptance_rate,
        diagnosis_rate,
        same_day_treatment_rate,
        procedure_acceptance_rate
    FROM raw_marts.mart_procedure_acceptance_summary
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND procedure_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND procedure_date <= :end_date"
        params['end_date'] = end_date
    if provider_id is not None:
        query += " AND provider_id = :provider_id"
        params['provider_id'] = provider_id
    if clinic_id is not None:
        query += " AND clinic_id = :clinic_id"
        params['clinic_id'] = clinic_id
    
    query += " ORDER BY procedure_date DESC, provider_id, clinic_id"
    
    try:
        result = db.execute(text(query), params).fetchall()
        return [dict(row._mapping) for row in result]
    except SQLAlchemyError as e:
        logger.error(f"Error fetching treatment acceptance summary: {str(e)}")
        raise

def get_treatment_acceptance_trends(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None,
    clinic_id: Optional[int] = None,
    group_by: str = "month"
) -> List[dict]:
    """
    Get time series trends for Treatment Acceptance
    - Aggregates daily data to specified group_by period
    - Default to monthly aggregation (matching PBN October 2025 view)
    - Used for trending charts
    """
    # Determine date truncation based on group_by
    date_trunc_map = {
        "day": "DAY",
        "week": "WEEK",
        "month": "MONTH",
        "year": "YEAR"
    }
    date_trunc = date_trunc_map.get(group_by.lower(), "MONTH")
    
    query = f"""
    SELECT 
        DATE_TRUNC('{date_trunc}', procedure_date)::date as date,
        -- Percentage Metrics (calculated from aggregated totals)
        CASE 
            WHEN SUM(tx_presented_amount) > 0 
            THEN ROUND((SUM(tx_accepted_amount)::numeric / NULLIF(SUM(tx_presented_amount), 0) * 100)::numeric, 2)
            WHEN SUM(tx_accepted_amount) > 0 
            THEN NULL
            ELSE 0
        END as tx_acceptance_rate,
        
        CASE 
            WHEN SUM(patients_presented) > 0 
            THEN ROUND((SUM(patients_accepted)::numeric / NULLIF(SUM(patients_presented), 0) * 100)::numeric, 2)
            WHEN SUM(patients_accepted) > 0 
            THEN NULL
            ELSE 0
        END as patient_acceptance_rate,
        
        CASE 
            WHEN SUM(patients_seen) > 0 
            THEN ROUND((SUM(patients_presented)::numeric / NULLIF(SUM(patients_seen), 0) * 100)::numeric, 2)
            ELSE NULL
        END as diagnosis_rate,
        
        -- Aggregated Metrics
        SUM(tx_presented_amount) as tx_presented_amount,
        SUM(tx_accepted_amount) as tx_accepted_amount,
        SUM(patients_seen) as patients_seen,
        SUM(patients_presented) as patients_presented,
        SUM(patients_accepted) as patients_accepted
        
    FROM raw_marts.mart_procedure_acceptance_summary
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND procedure_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND procedure_date <= :end_date"
        params['end_date'] = end_date
    if provider_id is not None:
        query += " AND provider_id = :provider_id"
        params['provider_id'] = provider_id
    if clinic_id is not None:
        query += " AND clinic_id = :clinic_id"
        params['clinic_id'] = clinic_id
    
    query += f" GROUP BY DATE_TRUNC('{date_trunc}', procedure_date) ORDER BY date DESC"
    
    try:
        result = db.execute(text(query), params).fetchall()
        return [dict(row._mapping) for row in result]
    except SQLAlchemyError as e:
        logger.error(f"Error fetching treatment acceptance trends: {str(e)}")
        raise

def get_treatment_acceptance_provider_performance(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    clinic_id: Optional[int] = None
) -> List[dict]:
    """
    Get provider-level performance breakdown
    - Aggregates daily data by provider for date range
    - Supports clinic filtering
    - Used for provider performance table
    """
    query = """
    SELECT 
        mas.provider_id,
        CONCAT(COALESCE(dp.provider_first_name, ''), ' ', COALESCE(dp.provider_last_name, '')) as provider_name,
        
        -- Percentage Metrics (calculated from aggregated totals)
        CASE 
            WHEN SUM(mas.tx_presented_amount) > 0 
            THEN ROUND((SUM(mas.tx_accepted_amount)::numeric / NULLIF(SUM(mas.tx_presented_amount), 0) * 100)::numeric, 2)
            WHEN SUM(mas.tx_accepted_amount) > 0 
            THEN NULL
            ELSE 0
        END as tx_acceptance_rate,
        
        CASE 
            WHEN SUM(mas.patients_presented) > 0 
            THEN ROUND((SUM(mas.patients_accepted)::numeric / NULLIF(SUM(mas.patients_presented), 0) * 100)::numeric, 2)
            WHEN SUM(mas.patients_accepted) > 0 
            THEN NULL
            ELSE 0
        END as patient_acceptance_rate,
        
        -- Diagnosis Rate: (Patients with Exams AND Presented / Patients with Exams) * 100
        -- PBN Definition: (Patients with Treatment Plans / Patients Receiving Exams) * 100
        -- For procedure-based approach: (Patients with Exams AND Presented / Patients with Exams) * 100
        -- Exam must be within 30 days before treatment presentation (matching PBN requirement)
        CASE 
            WHEN SUM(mas.patients_with_exams) > 0 
            THEN ROUND((SUM(mas.patients_with_exams_and_presented)::numeric / NULLIF(SUM(mas.patients_with_exams), 0) * 100)::numeric, 2)
            ELSE NULL
        END as diagnosis_rate,
        
        -- Aggregated Metrics
        SUM(mas.tx_presented_amount) as tx_presented_amount,
        SUM(mas.tx_accepted_amount) as tx_accepted_amount,
        SUM(mas.patients_seen) as patients_seen,
        SUM(mas.patients_with_exams) as patients_with_exams,
        SUM(mas.patients_with_exams_and_presented) as patients_with_exams_and_presented,
        SUM(mas.patients_presented) as patients_presented,
        SUM(mas.patients_accepted) as patients_accepted,
        SUM(mas.procedures_presented) as procedures_presented,
        SUM(mas.procedures_accepted) as procedures_accepted,
        SUM(mas.same_day_treatment_amount) as same_day_treatment_amount,
        
        -- Same-Day Treatment Rate
        CASE 
            WHEN SUM(mas.tx_presented_amount) > 0 
            THEN ROUND((SUM(mas.same_day_treatment_amount)::numeric / NULLIF(SUM(mas.tx_presented_amount), 0) * 100)::numeric, 2)
            WHEN SUM(mas.same_day_treatment_amount) > 0 
            THEN NULL
            ELSE 0
        END as same_day_treatment_rate
        
    FROM raw_marts.mart_procedure_acceptance_summary mas
    LEFT JOIN raw_marts.dim_provider dp ON mas.provider_id = dp.provider_id
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND mas.procedure_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND mas.procedure_date <= :end_date"
        params['end_date'] = end_date
    if clinic_id is not None:
        query += " AND mas.clinic_id = :clinic_id"
        params['clinic_id'] = clinic_id
    
    query += " GROUP BY mas.provider_id, dp.provider_first_name, dp.provider_last_name ORDER BY mas.provider_id"
    
    try:
        result = db.execute(text(query), params).fetchall()
        rows = []
        for row in result:
            row_dict = dict(row._mapping)
            # Handle empty provider_name
            if not row_dict.get('provider_name') or row_dict['provider_name'].strip() == '':
                row_dict['provider_name'] = f"Provider {row_dict['provider_id']}"
            else:
                row_dict['provider_name'] = row_dict['provider_name'].strip()
            rows.append(row_dict)
        return rows
    except SQLAlchemyError as e:
        logger.error(f"Error fetching treatment acceptance provider performance: {str(e)}")
        raise

