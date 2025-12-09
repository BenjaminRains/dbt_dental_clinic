# api/services/revenue_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)

def get_revenue_trends(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None
) -> List[dict]:
    """Get revenue trends over time from mart_revenue_lost"""
    
    query = """
    SELECT 
        appointment_date as date,
        SUM(lost_revenue) as revenue_lost,
        SUM(estimated_recoverable_amount) as recovery_potential,
        COUNT(*) as opportunity_count
    FROM raw_marts.mart_revenue_lost
    WHERE lost_revenue > 0
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND provider_id = :provider_id"
        params['provider_id'] = provider_id
    
    query += " GROUP BY appointment_date ORDER BY appointment_date"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "date": str(row.date),
            "revenue_lost": float(row.revenue_lost or 0),
            "recovery_potential": float(row.recovery_potential or 0),
            "opportunity_count": int(row.opportunity_count or 0)
        }
        for row in result
    ]

def get_revenue_kpi_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
    """Get revenue KPI summary from mart_revenue_lost"""
    
    query = """
    SELECT 
        SUM(lost_revenue) as total_revenue_lost,
        SUM(estimated_recoverable_amount) as total_recovery_potential,
        AVG(estimated_recoverable_amount) as avg_recovery_potential,
        COUNT(*) as total_opportunities,
        COUNT(DISTINCT patient_id) as affected_patients,
        COUNT(DISTINCT provider_id) as affected_providers
    FROM raw_marts.mart_revenue_lost
    WHERE lost_revenue > 0
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    
    result = db.execute(text(query), params).fetchone()
    
    if result:
        return {
            "total_revenue_lost": float(result.total_revenue_lost or 0),
            "total_recovery_potential": float(result.total_recovery_potential or 0),
            "avg_recovery_potential": float(result.avg_recovery_potential or 0),
            "total_opportunities": int(result.total_opportunities or 0),
            "affected_patients": int(result.affected_patients or 0),
            "affected_providers": int(result.affected_providers or 0)
        }
    else:
        return {
            "total_revenue_lost": 0.0,
            "total_recovery_potential": 0.0,
            "avg_recovery_potential": 0.0,
            "total_opportunities": 0,
            "affected_patients": 0,
            "affected_providers": 0
        }

def get_revenue_opportunities(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None,
    opportunity_type: Optional[str] = None,
    recovery_potential: Optional[str] = None,
    min_priority_score: Optional[int] = None
) -> List[dict]:
    """Get detailed revenue opportunities from mart_revenue_lost or source tables"""
    
    # If filtering by opportunity types that don't exist in mart, query source tables directly
    if opportunity_type in ('Missed Appointment', 'Claim Rejection', 'Treatment Plan Delay'):
        return get_revenue_opportunities_from_source(
            db, skip, limit, start_date, end_date, provider_id, 
            opportunity_type, recovery_potential, min_priority_score
        )
    
    # Otherwise, query from mart_revenue_lost (for Write Off and others)
    query = """
    SELECT 
        date_id,
        opportunity_id,
        appointment_date,
        provider_id,
        clinic_id,
        patient_id,
        appointment_id,
        provider_type_category,
        provider_specialty_category,
        -- patient_age removed (PII) - replaced with numeric age_category
        CASE 
            WHEN patient_age IS NULL THEN NULL
            WHEN patient_age <= 17 THEN 1  -- Minor (0-17)
            WHEN patient_age <= 34 THEN 2  -- Young Adult (18-34)
            WHEN patient_age <= 54 THEN 3  -- Middle Aged (35-54)
            ELSE 4  -- Older Adult (55+)
        END as patient_age_category,
        patient_gender,
        has_insurance_flag,
        patient_specific,
        year,
        month,
        quarter,
        day_name,
        is_weekend,
        is_holiday,
        opportunity_type,
        opportunity_subtype,
        lost_revenue,
        lost_time_minutes,
        missed_procedures,
        opportunity_datetime,
        recovery_potential,
        opportunity_hour,
        time_period,
        revenue_impact_category,
        time_impact_category,
        recovery_timeline,
        recovery_priority_score,
        preventability,
        has_revenue_impact,
        has_time_impact,
        recoverable,
        recent_opportunity,
        appointment_related,
        days_since_opportunity,
        estimated_recoverable_amount,
        _loaded_at,
        _updated_at,
        _created_by
    FROM raw_marts.mart_revenue_lost
    WHERE 1=1
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    if provider_id:
        query += " AND provider_id = :provider_id"
        params['provider_id'] = provider_id
    if opportunity_type:
        query += " AND opportunity_type = :opportunity_type"
        params['opportunity_type'] = opportunity_type
        query += " AND lost_revenue > 0"
    else:
        query += " AND lost_revenue > 0"
    if recovery_potential:
        query += " AND recovery_potential = :recovery_potential"
        params['recovery_potential'] = recovery_potential
    if min_priority_score is not None:
        query += " AND recovery_priority_score >= :min_priority_score"
        params['min_priority_score'] = min_priority_score
    
    query += " ORDER BY recovery_priority_score DESC, appointment_date DESC"
    query += f" LIMIT {limit} OFFSET {skip}"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            **{k: v for k, v in dict(row._mapping).items() if k not in ['date_id', 'appointment_date', 'opportunity_datetime']},
            # Convert date_id - dim_date.date_id is actually a date type, convert to YYYYMMDD integer
            'date_id': (
                int(row.date_id) if isinstance(row.date_id, (int, float)) 
                else int(f"{row.date_id.year}{row.date_id.month:02d}{row.date_id.day:02d}") if hasattr(row.date_id, 'year') and hasattr(row.date_id, 'month')
                else None
            ) if row.date_id else None,
            # Convert dates to strings for API response
            'appointment_date': str(row.appointment_date) if row.appointment_date else None,
            'opportunity_datetime': str(row.opportunity_datetime) if row.opportunity_datetime else None,
        }
        for row in result
    ]

def get_revenue_opportunities_from_source(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    provider_id: Optional[int] = None,
    opportunity_type: Optional[str] = None,
    recovery_potential: Optional[str] = None,
    min_priority_score: Optional[int] = None
) -> List[dict]:
    """Get revenue opportunities directly from source tables for types not in mart"""
    
    if opportunity_type == 'Missed Appointment':
        # Query from fact_appointment for missed appointments
        query = """
        WITH missed_appts AS (
            SELECT 
                fa.appointment_id,
                fa.appointment_date,
                fa.provider_id,
                COALESCE(fa.clinic_id, 0) as clinic_id,
                fa.patient_id,
                fa.is_broken,
                fa.is_no_show,
                CASE 
                    WHEN fa.is_no_show THEN 'No Show'
                    WHEN fa.is_broken THEN 'Cancellation'
                    ELSE 'Other'
                END as opportunity_subtype,
                COALESCE(SUM(pc.procedure_fee), 0) as lost_revenue,
                fa.appointment_length_minutes as lost_time_minutes,
                ARRAY_AGG(DISTINCT pc.procedure_code) FILTER (WHERE pc.procedure_code IS NOT NULL) as missed_procedures,
                fa.appointment_datetime as opportunity_datetime,
                CASE 
                    WHEN fa.is_no_show THEN 'High'
                    WHEN fa.is_broken THEN 'Medium'
                    ELSE 'Low'
                END as recovery_potential,
                EXTRACT(HOUR FROM fa.appointment_datetime)::int as opportunity_hour,
                CASE 
                    WHEN EXTRACT(HOUR FROM fa.appointment_datetime) BETWEEN 8 AND 12 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM fa.appointment_datetime) BETWEEN 13 AND 17 THEN 'Afternoon'
                    ELSE 'Evening'
                END as time_period,
                CASE 
                    WHEN COALESCE(SUM(pc.procedure_fee), 0) > 500 THEN 'High'
                    WHEN COALESCE(SUM(pc.procedure_fee), 0) > 200 THEN 'Medium'
                    ELSE 'Low'
                END as revenue_impact_category,
                CASE 
                    WHEN fa.appointment_length_minutes > 60 THEN 'High'
                    WHEN fa.appointment_length_minutes > 30 THEN 'Medium'
                    ELSE 'Low'
                END as time_impact_category,
                CASE 
                    WHEN fa.is_no_show THEN 'Immediate'
                    WHEN fa.is_broken THEN 'Short-term'
                    ELSE 'Medium-term'
                END as recovery_timeline,
                CASE 
                    WHEN fa.is_no_show THEN 80
                    WHEN fa.is_broken AND fa.appointment_datetime > CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 60
                    ELSE 40
                END as recovery_priority_score,
                CASE 
                    WHEN fa.is_no_show THEN 'Preventable'
                    WHEN fa.is_broken THEN 'Partially Preventable'
                    ELSE 'Unpreventable'
                END as preventability,
                CASE WHEN COALESCE(SUM(pc.procedure_fee), 0) > 0 THEN true ELSE false END as has_revenue_impact,
                CASE WHEN fa.appointment_length_minutes > 0 THEN true ELSE false END as has_time_impact,
                true as recoverable,
                CASE WHEN fa.appointment_date >= CURRENT_DATE - INTERVAL '30 days' THEN true ELSE false END as recent_opportunity,
                true as appointment_related,
                (CURRENT_DATE - fa.appointment_date)::int as days_since_opportunity,
                COALESCE(SUM(pc.procedure_fee), 0) * 0.5 as estimated_recoverable_amount
            FROM raw_marts.fact_appointment fa
            LEFT JOIN raw_intermediate.int_procedure_complete pc
                ON fa.appointment_id = pc.appointment_id
                AND pc.procedure_date = fa.appointment_date
            WHERE (fa.is_broken = true OR fa.is_no_show = true)
                AND fa.appointment_date IS NOT NULL
        """
        
        params = {}
        if start_date:
            query += " AND fa.appointment_date >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND fa.appointment_date <= :end_date"
            params['end_date'] = end_date
        if provider_id:
            query += " AND fa.provider_id = :provider_id"
            params['provider_id'] = provider_id
        
        query += """
            GROUP BY fa.appointment_id, fa.appointment_date, fa.provider_id, fa.clinic_id, 
                     fa.patient_id, fa.is_broken, fa.is_no_show, fa.appointment_length_minutes, 
                     fa.appointment_datetime
        ),
        with_dimensions AS (
            SELECT 
                ma.*,
                dd.date_id,
                dd.year,
                dd.month,
                dd.quarter,
                dd.day_name,
                dd.is_weekend,
                dd.is_holiday,
                dp.provider_type_category,
                dp.provider_specialty_category,
                -- Calculate numeric age_category from age (PII removal: age -> age_category)
                CASE 
                    WHEN pt.age IS NULL THEN NULL
                    WHEN pt.age <= 17 THEN 1  -- Minor (0-17)
                    WHEN pt.age <= 34 THEN 2  -- Young Adult (18-34)
                    WHEN pt.age <= 54 THEN 3  -- Middle Aged (35-54)
                    ELSE 4  -- Older Adult (55+)
                END as patient_age_category,
                pt.gender as patient_gender,
                pt.has_insurance_flag,
                CASE WHEN pt.patient_id IS NOT NULL THEN true ELSE false END as patient_specific
            FROM missed_appts ma
            LEFT JOIN raw_marts.dim_date dd ON ma.appointment_date = dd.date_day
            LEFT JOIN raw_marts.dim_provider dp ON ma.provider_id = dp.provider_id
            LEFT JOIN raw_marts.dim_patient pt ON ma.patient_id = pt.patient_id
        )
        SELECT 
            date_id,
            ROW_NUMBER() OVER (ORDER BY recovery_priority_score DESC, appointment_date DESC) as opportunity_id,
            appointment_date,
            provider_id,
            clinic_id,
            patient_id,
            appointment_id,
            provider_type_category,
            provider_specialty_category,
            patient_age_category,
            patient_gender,
            has_insurance_flag,
            patient_specific,
            year,
            month,
            quarter,
            day_name,
            is_weekend,
            is_holiday,
            'Missed Appointment' as opportunity_type,
            opportunity_subtype,
            lost_revenue,
            lost_time_minutes,
            missed_procedures,
            opportunity_datetime,
            recovery_potential,
            opportunity_hour,
            time_period,
            revenue_impact_category,
            time_impact_category,
            recovery_timeline,
            recovery_priority_score,
            preventability,
            has_revenue_impact,
            has_time_impact,
            recoverable,
            recent_opportunity,
            appointment_related,
            days_since_opportunity,
            estimated_recoverable_amount,
            NULL::timestamp as _loaded_at,
            NULL::timestamp as _updated_at,
            NULL::bigint as _created_by
        FROM with_dimensions
        WHERE 1=1
        """
        
        if recovery_potential:
            query += " AND recovery_potential = :recovery_potential"
            params['recovery_potential'] = recovery_potential
        if min_priority_score is not None:
            query += " AND recovery_priority_score >= :min_priority_score"
            params['min_priority_score'] = min_priority_score
        
        query += " ORDER BY recovery_priority_score DESC, appointment_date DESC"
        query += f" LIMIT {limit} OFFSET {skip}"
        
    elif opportunity_type == 'Claim Rejection':
        # Query from fact_claim for claim rejections
        query = """
        WITH claim_rejections AS (
            SELECT 
                fc.claim_id as appointment_id,
                fc.claim_date as appointment_date,
                NULL::integer as provider_id,
                0::integer as clinic_id,
                fc.patient_id,
                fc.claim_status,  -- Keep original claim_status code
                CASE 
                    WHEN fc.claim_status = 'H' THEN 'Insurance Denial'  -- H = Hold (likely denied)
                    WHEN fc.claim_status = 'S' THEN 'Processing Issue'  -- S = Submitted (Pending)
                    WHEN fc.claim_status = 'W' THEN 'Processing Issue'  -- W = Waiting (Pending)
                    ELSE 'Processing Issue'
                END as opportunity_subtype,
                (fc.billed_amount - COALESCE(fc.paid_amount, 0)) as lost_revenue,
                NULL::integer as lost_time_minutes,
                CASE 
                    WHEN fc.procedure_code IS NOT NULL THEN ARRAY[fc.procedure_code]::text[]
                    ELSE ARRAY[]::text[]
                END as missed_procedures,
                fc.claim_date as opportunity_datetime,
                CASE 
                    WHEN fc.claim_status = 'H' AND COALESCE(fc.patient_responsibility, 0) > 0 THEN 'High'  -- H = Hold (likely denied)
                    WHEN fc.claim_status = 'H' THEN 'High'  -- Hold claims are high priority
                    WHEN fc.claim_status IN ('S', 'W') THEN 'Medium'  -- S = Submitted, W = Waiting (Pending)
                    ELSE 'Medium'
                END as recovery_potential,
                NULL::integer as opportunity_hour,  -- claim_date is date type, no hour available
                'Business Hours' as time_period,
                CASE 
                    WHEN (fc.billed_amount - COALESCE(fc.paid_amount, 0)) > 500 THEN 'High'
                    WHEN (fc.billed_amount - COALESCE(fc.paid_amount, 0)) > 200 THEN 'Medium'
                    ELSE 'Low'
                END as revenue_impact_category,
                'N/A' as time_impact_category,
                'Medium-term' as recovery_timeline,
                CASE 
                    WHEN fc.claim_status = 'H' AND COALESCE(fc.patient_responsibility, 0) > 0 THEN 70  -- H = Hold (likely denied)
                    WHEN fc.claim_status = 'H' THEN 65  -- Hold claims
                    WHEN fc.claim_status IN ('S', 'W') THEN 50  -- S = Submitted, W = Waiting (Pending)
                    ELSE 50
                END as recovery_priority_score,
                'Partially Preventable' as preventability,
                CASE WHEN (fc.billed_amount - COALESCE(fc.paid_amount, 0)) > 0 THEN true ELSE false END as has_revenue_impact,
                false as has_time_impact,
                true as recoverable,
                CASE WHEN fc.claim_date >= CURRENT_DATE - INTERVAL '30 days' THEN true ELSE false END as recent_opportunity,
                false as appointment_related,
                (CURRENT_DATE - fc.claim_date)::int as days_since_opportunity,
                (fc.billed_amount - COALESCE(fc.paid_amount, 0)) * 0.3 as estimated_recoverable_amount
            FROM raw_marts.fact_claim fc
            WHERE fc.claim_status IN ('S', 'W', 'H')  -- S = Submitted (Pending), W = Waiting (Pending), H = Hold (Denied/Rejected)
                AND fc.claim_date IS NOT NULL
                AND fc.billed_amount > 0  -- Must have billed amount
                AND (fc.billed_amount - COALESCE(fc.paid_amount, 0)) > 0  -- Unpaid amount > 0
                -- Note: Excluding 'R' (Received/Paid) since those are paid claims
        """
        
        params = {}
        if start_date:
            query += " AND fc.claim_date >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND fc.claim_date <= :end_date"
            params['end_date'] = end_date
        
        query += """
        ),
        with_dimensions AS (
            SELECT 
                cr.*,
                dd.date_id,
                dd.year,
                dd.month,
                dd.quarter,
                dd.day_name,
                dd.is_weekend,
                dd.is_holiday,
                NULL::text as provider_type_category,
                NULL::text as provider_specialty_category,
                pt.age as patient_age,
                pt.gender as patient_gender,
                pt.has_insurance_flag,
                CASE WHEN pt.patient_id IS NOT NULL THEN true ELSE false END as patient_specific
            FROM claim_rejections cr
            LEFT JOIN raw_marts.dim_date dd ON cr.appointment_date = dd.date_day
            LEFT JOIN raw_marts.dim_patient pt ON cr.patient_id = pt.patient_id
        )
        SELECT 
            date_id,
            ROW_NUMBER() OVER (ORDER BY recovery_priority_score DESC, appointment_date DESC) as opportunity_id,
            appointment_date,
            provider_id,
            clinic_id,
            patient_id,
            appointment_id,
            provider_type_category,
            provider_specialty_category,
            patient_age_category,
            patient_gender,
            has_insurance_flag,
            patient_specific,
            year,
            month,
            quarter,
            day_name,
            is_weekend,
            is_holiday,
            'Claim Rejection' as opportunity_type,
            opportunity_subtype,
            lost_revenue,
            lost_time_minutes,
            missed_procedures,
            opportunity_datetime,
            recovery_potential,
            opportunity_hour,
            time_period,
            revenue_impact_category,
            time_impact_category,
            recovery_timeline,
            recovery_priority_score,
            preventability,
            has_revenue_impact,
            has_time_impact,
            recoverable,
            recent_opportunity,
            appointment_related,
            days_since_opportunity,
            estimated_recoverable_amount,
            NULL::timestamp as _loaded_at,
            NULL::timestamp as _updated_at,
            NULL::bigint as _created_by
        FROM with_dimensions
        WHERE 1=1
        """
        
        if recovery_potential:
            query += " AND recovery_potential = :recovery_potential"
            params['recovery_potential'] = recovery_potential
        if min_priority_score is not None:
            query += " AND recovery_priority_score >= :min_priority_score"
            params['min_priority_score'] = min_priority_score
        
        query += " ORDER BY recovery_priority_score DESC, appointment_date DESC"
        query += f" LIMIT {limit} OFFSET {skip}"
        
    elif opportunity_type == 'Treatment Plan Delay':
        # Query from int_treatment_plan for delayed treatment plans
        query = """
        WITH treatment_delays AS (
            SELECT 
                tp.treatment_plan_id as appointment_id,
                tp.treatment_plan_date as appointment_date,
                COALESCE(tp.primary_provider_id, 0) as provider_id,  -- Handle NULL provider_id
                COALESCE(tp.primary_clinic_id, 0) as clinic_id,
                tp.patient_id,
                tp.timeline_status as opportunity_subtype,
                COALESCE(tp.remaining_amount, 0) as lost_revenue,
                NULL::integer as lost_time_minutes,
                COALESCE(tp.procedure_codes, ARRAY[]::text[]) as missed_procedures,  -- Handle NULL procedure_codes
                tp.treatment_plan_date as opportunity_datetime,
                COALESCE(tp.recovery_potential, 'Medium') as recovery_potential,
                NULL::integer as opportunity_hour,  -- treatment_plan_date is date type, no hour available
                'Business Hours' as time_period,
                CASE 
                    WHEN COALESCE(tp.remaining_amount, 0) > 5000 THEN 'High'
                    WHEN COALESCE(tp.remaining_amount, 0) > 2000 THEN 'Medium'
                    ELSE 'Low'
                END as revenue_impact_category,
                'N/A' as time_impact_category,
                CASE 
                    WHEN COALESCE(tp.days_since_last_activity, 0) > 180 THEN 'Long-term'
                    WHEN COALESCE(tp.days_since_last_activity, 0) > 90 THEN 'Medium-term'
                    ELSE 'Short-term'
                END as recovery_timeline,
                CASE 
                    WHEN COALESCE(tp.days_since_last_activity, 0) > 180 THEN 75
                    WHEN COALESCE(tp.days_since_last_activity, 0) > 90 THEN 60
                    ELSE 45
                END as recovery_priority_score,
                'Preventable' as preventability,
                CASE WHEN COALESCE(tp.remaining_amount, 0) > 0 THEN true ELSE false END as has_revenue_impact,
                false as has_time_impact,
                true as recoverable,
                CASE WHEN tp.treatment_plan_date >= CURRENT_DATE - INTERVAL '90 days' THEN true ELSE false END as recent_opportunity,
                false as appointment_related,
                COALESCE(tp.days_since_last_activity, 0) as days_since_opportunity,
                COALESCE(tp.remaining_amount, 0) * 0.4 as estimated_recoverable_amount
            FROM raw_intermediate.int_treatment_plan tp
            WHERE tp.treatment_plan_status = 0  -- Active treatment plans only
                AND tp.treatment_plan_date >= CURRENT_DATE - INTERVAL '2 years'  -- Within last 2 years
                AND tp.days_since_last_activity > 90  -- Only include delayed treatment plans (>90 days since last activity)
                -- Matches the original definition in mart_revenue_lost.sql treatment_base CTE
        """
        
        params = {}
        if start_date:
            query += " AND tp.treatment_plan_date >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND tp.treatment_plan_date <= :end_date"
            params['end_date'] = end_date
        if provider_id:
            query += " AND tp.primary_provider_id = :provider_id"
            params['provider_id'] = provider_id
        
        query += """
        ),
        with_dimensions AS (
            SELECT 
                td.*,
                dd.date_id,
                dd.year,
                dd.month,
                dd.quarter,
                dd.day_name,
                dd.is_weekend,
                dd.is_holiday,
                dp.provider_type_category,
                dp.provider_specialty_category,
                -- Calculate numeric age_category from age (PII removal: age -> age_category)
                CASE 
                    WHEN pt.age IS NULL THEN NULL
                    WHEN pt.age <= 17 THEN 1  -- Minor (0-17)
                    WHEN pt.age <= 34 THEN 2  -- Young Adult (18-34)
                    WHEN pt.age <= 54 THEN 3  -- Middle Aged (35-54)
                    ELSE 4  -- Older Adult (55+)
                END as patient_age_category,
                pt.gender as patient_gender,
                pt.has_insurance_flag,
                CASE WHEN pt.patient_id IS NOT NULL THEN true ELSE false END as patient_specific
            FROM treatment_delays td
            LEFT JOIN raw_marts.dim_date dd ON td.appointment_date = dd.date_day
            LEFT JOIN raw_marts.dim_provider dp ON td.provider_id = dp.provider_id
            LEFT JOIN raw_marts.dim_patient pt ON td.patient_id = pt.patient_id
        )
        SELECT 
            date_id,
            ROW_NUMBER() OVER (ORDER BY recovery_priority_score DESC, appointment_date DESC) as opportunity_id,
            appointment_date,
            provider_id,
            clinic_id,
            patient_id,
            appointment_id,
            provider_type_category,
            provider_specialty_category,
            patient_age_category,
            patient_gender,
            has_insurance_flag,
            patient_specific,
            year,
            month,
            quarter,
            day_name,
            is_weekend,
            is_holiday,
            'Treatment Plan Delay' as opportunity_type,
            opportunity_subtype,
            lost_revenue,
            lost_time_minutes,
            missed_procedures,
            opportunity_datetime,
            recovery_potential,
            opportunity_hour,
            time_period,
            revenue_impact_category,
            time_impact_category,
            recovery_timeline,
            recovery_priority_score,
            preventability,
            has_revenue_impact,
            has_time_impact,
            recoverable,
            recent_opportunity,
            appointment_related,
            days_since_opportunity,
            estimated_recoverable_amount,
            NULL::timestamp as _loaded_at,
            NULL::timestamp as _updated_at,
            NULL::bigint as _created_by
        FROM with_dimensions
        WHERE 1=1
        """
        
        if recovery_potential:
            query += " AND recovery_potential = :recovery_potential"
            params['recovery_potential'] = recovery_potential
        if min_priority_score is not None:
            query += " AND recovery_priority_score >= :min_priority_score"
            params['min_priority_score'] = min_priority_score
        
        query += " ORDER BY recovery_priority_score DESC, appointment_date DESC"
        query += f" LIMIT {limit} OFFSET {skip}"
    else:
        return []
    
    try:
        print(f"[DEBUG] Executing query for opportunity_type: {opportunity_type}")
        logger.info(f"Executing query for opportunity_type: {opportunity_type}")
        print(f"[DEBUG] Query (first 1000 chars): {query[:1000]}...")
        logger.info(f"Query (first 1000 chars): {query[:1000]}...")  # Log first 1000 chars
        print(f"[DEBUG] Params: {params}")
        logger.info(f"Params: {params}")
        
        result = db.execute(text(query), params).fetchall()
        print(f"[DEBUG] Query returned {len(result)} rows")
        logger.info(f"Query returned {len(result)} rows")
        
        return [
            {
                **{k: v for k, v in dict(row._mapping).items() if k not in ['date_id', 'appointment_date', 'opportunity_datetime']},
                # Convert date_id - dim_date.date_id is actually a date type, convert to YYYYMMDD integer
                'date_id': (
                    int(row.date_id) if isinstance(row.date_id, (int, float)) 
                    else int(f"{row.date_id.year}{row.date_id.month:02d}{row.date_id.day:02d}") if hasattr(row.date_id, 'year') and hasattr(row.date_id, 'month')
                    else None
                ) if row.date_id else None,
                # Convert dates to strings for API response
                'appointment_date': str(row.appointment_date) if row.appointment_date else None,
                'opportunity_datetime': str(row.opportunity_datetime) if row.opportunity_datetime else None,
            }
            for row in result
        ]
    except Exception as e:
        error_msg = str(e)
        print(f"[ERROR] ERROR executing query for {opportunity_type}: {error_msg}")
        print(f"[ERROR] Full query: {query}")
        print(f"[ERROR] Params: {params}")
        logger.error(f"ERROR executing query for {opportunity_type}: {error_msg}")
        logger.error(f"Full query: {query}")
        logger.error(f"Params: {params}")
        import traceback
        traceback_str = traceback.format_exc()
        print(f"[ERROR] Traceback: {traceback_str}")
        logger.error(f"Traceback: {traceback_str}")
        raise

def get_revenue_opportunity_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """Get revenue opportunity summary by type and category"""
    
    query = """
    SELECT 
        opportunity_type,
        opportunity_subtype,
        COUNT(*) as total_opportunities,
        SUM(lost_revenue) as total_revenue_lost,
        SUM(estimated_recoverable_amount) as total_recovery_potential,
        AVG(recovery_priority_score) as avg_priority_score,
        SUM(CASE WHEN recent_opportunity THEN 1 ELSE 0 END) as recent_opportunities,
        SUM(CASE WHEN recovery_priority_score >= 70 THEN 1 ELSE 0 END) as high_priority_opportunities
    FROM raw_marts.mart_revenue_lost
    WHERE lost_revenue > 0
    """
    
    params = {}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += " GROUP BY opportunity_type, opportunity_subtype ORDER BY total_revenue_lost DESC"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "opportunity_type": str(row.opportunity_type),
            "opportunity_subtype": str(row.opportunity_subtype),
            "total_opportunities": int(row.total_opportunities or 0),
            "total_revenue_lost": float(row.total_revenue_lost or 0),
            "total_recovery_potential": float(row.total_recovery_potential or 0),
            "avg_priority_score": float(row.avg_priority_score or 0),
            "recent_opportunities": int(row.recent_opportunities or 0),
            "high_priority_opportunities": int(row.high_priority_opportunities or 0)
        }
        for row in result
    ]

def get_revenue_recovery_plan(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    min_priority_score: int = 50
) -> List[dict]:
    """Get revenue recovery plan with actionable items"""
    
    query = """
    SELECT 
        opportunity_id,
        provider_id,
        patient_id,
        opportunity_type,
        opportunity_subtype,
        lost_revenue,
        recovery_potential,
        recovery_priority_score as priority_score,
        estimated_recoverable_amount,
        recovery_timeline,
        CASE 
            WHEN opportunity_type = 'Missed Appointment' AND opportunity_subtype = 'No Show' THEN 
                ARRAY['Call patient to reschedule', 'Send follow-up reminder', 'Update patient contact preferences']
            WHEN opportunity_type = 'Missed Appointment' AND opportunity_subtype = 'Cancellation' THEN 
                ARRAY['Reschedule appointment', 'Review cancellation policy', 'Send confirmation reminder']
            WHEN opportunity_type = 'Claim Rejection' THEN 
                ARRAY['Review claim details', 'Resubmit with corrections', 'Contact insurance company']
            WHEN opportunity_type = 'Treatment Plan Delay' THEN 
                ARRAY['Contact patient about treatment plan', 'Schedule consultation', 'Review treatment options']
            WHEN opportunity_type = 'Write Off' THEN 
                ARRAY['Review adjustment reason', 'Verify patient eligibility', 'Consider payment plan']
            ELSE ARRAY['Review opportunity details', 'Determine appropriate action']
        END as recommended_actions
    FROM raw_marts.mart_revenue_lost
    WHERE recovery_priority_score >= :min_priority_score
    AND recoverable = true
    AND lost_revenue > 0
    """
    
    params = {"min_priority_score": min_priority_score}
    if start_date:
        query += " AND appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += " ORDER BY recovery_priority_score DESC, appointment_date DESC LIMIT 50"
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "opportunity_id": int(row.opportunity_id),
            "provider_id": int(row.provider_id) if row.provider_id else None,
            "patient_id": int(row.patient_id),
            "opportunity_type": str(row.opportunity_type),
            "lost_revenue": float(row.lost_revenue or 0),
            "recovery_potential": str(row.recovery_potential),
            "priority_score": int(row.priority_score),
            "recommended_actions": row.recommended_actions,
            "estimated_recovery_amount": float(row.estimated_recoverable_amount or 0),
            "recovery_timeline": str(row.recovery_timeline)
        }
        for row in result
    ]

def get_revenue_opportunity_by_id(
    db: Session,
    opportunity_id: int
) -> Optional[dict]:
    """Get specific revenue opportunity by ID"""
    
    query = """
    SELECT 
        date_id,
        opportunity_id,
        appointment_date,
        provider_id,
        clinic_id,
        patient_id,
        appointment_id,
        provider_type_category,
        provider_specialty_category,
        -- patient_age removed (PII) - replaced with numeric age_category
        CASE 
            WHEN patient_age IS NULL THEN NULL
            WHEN patient_age <= 17 THEN 1  -- Minor (0-17)
            WHEN patient_age <= 34 THEN 2  -- Young Adult (18-34)
            WHEN patient_age <= 54 THEN 3  -- Middle Aged (35-54)
            ELSE 4  -- Older Adult (55+)
        END as patient_age_category,
        patient_gender,
        has_insurance_flag,
        patient_specific,
        year,
        month,
        quarter,
        day_name,
        is_weekend,
        is_holiday,
        opportunity_type,
        opportunity_subtype,
        lost_revenue,
        lost_time_minutes,
        missed_procedures,
        opportunity_datetime,
        recovery_potential,
        opportunity_hour,
        time_period,
        revenue_impact_category,
        time_impact_category,
        recovery_timeline,
        recovery_priority_score,
        preventability,
        has_revenue_impact,
        has_time_impact,
        recoverable,
        recent_opportunity,
        appointment_related,
        days_since_opportunity,
        estimated_recoverable_amount,
        _loaded_at,
        _updated_at,
        _created_by
    FROM raw_marts.mart_revenue_lost
    WHERE opportunity_id = :opportunity_id
    """
    
    result = db.execute(text(query), {"opportunity_id": opportunity_id}).fetchone()
    return dict(result._mapping) if result else None

def get_revenue_lost_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
    """
    Get PBN-style Revenue Lost summary metrics:
    - Appmts Lost $ (Failed or Cancelled $$$)
    - Failed Re-Appnt $ (Recovered)
    - Lost Appmts %
    """
    
    # Base query to get cancelled/failed appointments with production amounts
    query = """
    WITH lost_appointments AS (
        SELECT 
            fa.appointment_id,
            fa.appointment_date,
            fa.patient_id,
            fa.is_broken,
            fa.is_no_show,
            CASE 
                WHEN fa.is_no_show THEN 'Failed'
                WHEN fa.is_broken THEN 'Cancelled'
                ELSE 'Other'
            END as status,
            COALESCE(SUM(pc.procedure_fee), 0) as production_amount
        FROM raw_marts.fact_appointment fa
        LEFT JOIN raw_intermediate.int_procedure_complete pc
            ON fa.appointment_id = pc.appointment_id
            AND pc.procedure_date = fa.appointment_date
        WHERE (fa.is_broken = true OR fa.is_no_show = true)
            AND fa.appointment_date IS NOT NULL
    """
    
    params = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += """
        GROUP BY fa.appointment_id, fa.appointment_date, fa.patient_id, 
                 fa.is_broken, fa.is_no_show
    ),
    rescheduled_appointments AS (
        SELECT 
            iad.appointment_id as original_appointment_id,
            COALESCE(SUM(pc.procedure_fee), 0) as rescheduled_production
        FROM raw_intermediate.int_appointment_details iad
        INNER JOIN lost_appointments la
            ON iad.appointment_id = la.appointment_id
        LEFT JOIN raw_marts.fact_appointment resched
            ON iad.rescheduled_appointment_id = resched.appointment_id
        LEFT JOIN raw_intermediate.int_procedure_complete pc
            ON resched.appointment_id = pc.appointment_id
            AND pc.procedure_date = resched.appointment_date
        WHERE iad.rescheduled_appointment_id IS NOT NULL
        GROUP BY iad.appointment_id
    ),
    total_appointments AS (
        SELECT COUNT(*) as total_count
        FROM raw_marts.fact_appointment
        WHERE appointment_date IS NOT NULL
    """
    
    if start_date:
        query += " AND appointment_date >= :start_date"
    if end_date:
        query += " AND appointment_date <= :end_date"
    
    query += """
    ),
    summary_metrics AS (
        SELECT 
            COALESCE(SUM(la.production_amount), 0) as appointments_lost_amount,
            COALESCE(SUM(ra.rescheduled_production), 0) as recovered_amount,
            COUNT(DISTINCT la.appointment_id) as lost_count
        FROM lost_appointments la
        LEFT JOIN rescheduled_appointments ra
            ON la.appointment_id = ra.original_appointment_id
    )
    SELECT 
        sm.appointments_lost_amount,
        sm.recovered_amount,
        CASE 
            WHEN ta.total_count > 0 
            THEN (sm.lost_count::numeric / ta.total_count::numeric * 100)
            ELSE 0
        END as lost_appointments_percent
    FROM summary_metrics sm
    CROSS JOIN total_appointments ta
    """
    
    result = db.execute(text(query), params).fetchone()
    
    if result:
        return {
            "appointments_lost_amount": float(result.appointments_lost_amount or 0),
            "recovered_amount": float(result.recovered_amount or 0),
            "lost_appointments_percent": float(result.lost_appointments_percent or 0)
        }
    else:
        return {
            "appointments_lost_amount": 0.0,
            "recovered_amount": 0.0,
            "lost_appointments_percent": 0.0
        }

def get_revenue_lost_opportunity(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
    """
    Get PBN-style Opportunity metrics:
    - Failed %
    - Cancelled %
    - Failed Re-appnt %
    - Cancelled Re-appnt %
    """
    
    query = """
    WITH appointment_status AS (
        SELECT 
            fa.appointment_id,
            fa.appointment_date,
            CASE 
                WHEN fa.is_no_show THEN 'Failed'
                WHEN fa.is_broken THEN 'Cancelled'
                ELSE NULL
            END as status
        FROM raw_marts.fact_appointment fa
        WHERE (fa.is_broken = true OR fa.is_no_show = true)
            AND fa.appointment_date IS NOT NULL
    """
    
    params = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    
    query += """
    ),
    rescheduled_tracking AS (
        SELECT 
            iad.appointment_id,
            iad.rescheduled_appointment_id,
            CASE 
                WHEN iad.rescheduled_appointment_id IS NOT NULL THEN true
                ELSE false
            END as is_rescheduled
        FROM raw_intermediate.int_appointment_details iad
        INNER JOIN appointment_status ast
            ON iad.appointment_id = ast.appointment_id
        WHERE iad.rescheduled_appointment_id IS NOT NULL
    ),
    total_appointments AS (
        SELECT COUNT(*) as total_count
        FROM raw_marts.fact_appointment
        WHERE appointment_date IS NOT NULL
    """
    
    if start_date:
        query += " AND appointment_date >= :start_date"
    if end_date:
        query += " AND appointment_date <= :end_date"
    
    query += """
    ),
    failed_appointments AS (
        SELECT COALESCE(COUNT(*), 0) as count
        FROM appointment_status
        WHERE status = 'Failed'
    ),
    cancelled_appointments AS (
        SELECT COALESCE(COUNT(*), 0) as count
        FROM appointment_status
        WHERE status = 'Cancelled'
    ),
    failed_rescheduled AS (
        SELECT COALESCE(COUNT(DISTINCT rt.appointment_id), 0) as count
        FROM rescheduled_tracking rt
        INNER JOIN appointment_status ast
            ON rt.appointment_id = ast.appointment_id
        WHERE ast.status = 'Failed'
            AND rt.is_rescheduled = true
    ),
    cancelled_rescheduled AS (
        SELECT COALESCE(COUNT(DISTINCT rt.appointment_id), 0) as count
        FROM rescheduled_tracking rt
        INNER JOIN appointment_status ast
            ON rt.appointment_id = ast.appointment_id
        WHERE ast.status = 'Cancelled'
            AND rt.is_rescheduled = true
    )
    SELECT 
        COALESCE(ta.total_count, 0) as total_count,
        COALESCE(fa.count, 0) as failed_count,
        COALESCE(ca.count, 0) as cancelled_count,
        COALESCE(fr.count, 0) as failed_reappnt_count,
        COALESCE(cr.count, 0) as cancelled_reappnt_count,
        CASE 
            WHEN COALESCE(ta.total_count, 0) > 0 
            THEN (COALESCE(fa.count, 0)::numeric / ta.total_count::numeric * 100)
            ELSE 0
        END as failed_percent,
        CASE 
            WHEN COALESCE(ta.total_count, 0) > 0 
            THEN (COALESCE(ca.count, 0)::numeric / ta.total_count::numeric * 100)
            ELSE 0
        END as cancelled_percent,
        CASE 
            WHEN COALESCE(fa.count, 0) > 0 
            THEN (COALESCE(fr.count, 0)::numeric / fa.count::numeric * 100)
            ELSE 0
        END as failed_reappnt_percent,
        CASE 
            WHEN COALESCE(ca.count, 0) > 0 
            THEN (COALESCE(cr.count, 0)::numeric / ca.count::numeric * 100)
            ELSE 0
        END as cancelled_reappnt_percent
    FROM total_appointments ta
    LEFT JOIN failed_appointments fa ON true
    LEFT JOIN cancelled_appointments ca ON true
    LEFT JOIN failed_rescheduled fr ON true
    LEFT JOIN cancelled_rescheduled cr ON true
    """
    
    result = db.execute(text(query), params).fetchone()
    
    if result:
        return {
            "failed_percent": float(result.failed_percent or 0),
            "cancelled_percent": float(result.cancelled_percent or 0),
            "failed_reappnt_percent": float(result.failed_reappnt_percent or 0),
            "cancelled_reappnt_percent": float(result.cancelled_reappnt_percent or 0),
            "failed_count": int(result.failed_count or 0),
            "cancelled_count": int(result.cancelled_count or 0),
            "failed_reappnt_count": int(result.failed_reappnt_count or 0),
            "cancelled_reappnt_count": int(result.cancelled_reappnt_count or 0)
        }
    else:
        return {
            "failed_percent": 0.0,
            "cancelled_percent": 0.0,
            "failed_reappnt_percent": 0.0,
            "cancelled_reappnt_percent": 0.0,
            "failed_count": 0,
            "cancelled_count": 0,
            "failed_reappnt_count": 0,
            "cancelled_reappnt_count": 0
        }

def get_lost_appointments_detail(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    status_filter: Optional[str] = None
) -> List[dict]:
    """
    Get detailed list of cancelled/failed appointments with patient info, codes, amounts
    """
    
    query = """
    WITH lost_appointments AS (
        SELECT 
            fa.appointment_id,
            fa.appointment_date,
            fa.patient_id,
            fa.is_broken,
            fa.is_no_show,
            CASE 
                WHEN fa.is_no_show THEN 'Failed'
                WHEN fa.is_broken THEN 'Cancelled'
                ELSE 'Other'
            END as status,
            COALESCE(SUM(pc.procedure_fee), 0) as production_amount,
            ARRAY_AGG(DISTINCT pc.procedure_code) FILTER (WHERE pc.procedure_code IS NOT NULL) as procedure_codes,
            MAX(at.appointment_type_name) as appointment_type
        FROM raw_marts.fact_appointment fa
        LEFT JOIN raw_intermediate.int_procedure_complete pc
            ON fa.appointment_id = pc.appointment_id
            AND pc.procedure_date = fa.appointment_date
        LEFT JOIN raw_intermediate.int_appointment_details iad
            ON fa.appointment_id = iad.appointment_id
        LEFT JOIN raw_staging.stg_opendental__appointmenttype at
            ON iad.appointment_type_id = at.appointment_type_id
        WHERE (fa.is_broken = true OR fa.is_no_show = true)
            AND fa.appointment_date IS NOT NULL
    """
    
    params = {}
    if start_date:
        query += " AND fa.appointment_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND fa.appointment_date <= :end_date"
        params['end_date'] = end_date
    if status_filter:
        if status_filter == 'Failed':
            query += " AND fa.is_no_show = true"
        elif status_filter == 'Cancelled':
            query += " AND fa.is_broken = true"
    
    query += """
        GROUP BY fa.appointment_id, fa.appointment_date, fa.patient_id, 
                 fa.is_broken, fa.is_no_show
    ),
    rescheduled_info AS (
        SELECT 
            iad.appointment_id,
            iad.rescheduled_appointment_id,
            resched.appointment_date as next_date
        FROM raw_intermediate.int_appointment_details iad
        INNER JOIN lost_appointments la
            ON iad.appointment_id = la.appointment_id
        LEFT JOIN raw_marts.fact_appointment resched
            ON iad.rescheduled_appointment_id = resched.appointment_id
        WHERE iad.rescheduled_appointment_id IS NOT NULL
    )
    SELECT 
        la.appointment_id,
        la.patient_id,
        -- patient_name removed (PII) - only patient_id is returned
        la.appointment_date::text as original_date,
        la.status,
        la.procedure_codes,
        la.production_amount,
        la.appointment_type,
        ri.next_date::text as next_date,
        CASE 
            WHEN ri.rescheduled_appointment_id IS NOT NULL THEN true
            ELSE false
        END as is_rescheduled
    FROM lost_appointments la
    LEFT JOIN rescheduled_info ri
        ON la.appointment_id = ri.appointment_id
    ORDER BY la.appointment_date DESC, la.production_amount DESC
    LIMIT :limit OFFSET :skip
    """
    
    params['limit'] = limit
    params['skip'] = skip
    
    result = db.execute(text(query), params).fetchall()
    
    return [
        {
            "appointment_id": int(row.appointment_id),
            "patient_id": int(row.patient_id),
            # patient_name removed (PII) - only patient_id is returned
            "original_date": row.original_date,
            "status": row.status,
            "procedure_codes": row.procedure_codes if row.procedure_codes else [],
            "production_amount": float(row.production_amount or 0),
            "appointment_type": row.appointment_type,
            "next_date": row.next_date,
            "is_rescheduled": bool(row.is_rescheduled)
        }
        for row in result
    ]