# api/services/ar_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)

def get_ar_kpi_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
    """
    Get AR KPI summary from mart_ar_summary
    
    Returns:
    - Total AR outstanding
    - Current (0-30) amount and percentage
    - Over 90 days amount and percentage
    - DSO (Days Sales Outstanding) - simplified calculation
    - Collection rate (calculated from ALL procedures and ALL payments for direct-pay practice)
    - High risk counts and amounts
    
    Collection Rate Calculation:
    - Production: All procedures from fact_procedure (last 365 days) - includes both insurance and direct-pay
    - Collections: All payments from fact_payment (last 365 days) - includes both insurance and patient payments, excludes refunds
    - Rate = (Total Collections / Total Production) × 100
    
    Now queries mart_ar_summary directly - aging buckets and insurance estimates are calculated in the mart model
    """
    query = """
    WITH latest_snapshots AS (
        SELECT DISTINCT ON (patient_id, provider_id)
            patient_id,
            provider_id,
            total_balance,
            balance_0_30_days,
            balance_31_60_days,
            balance_61_90_days,
            balance_over_90_days,
            patient_responsibility,
            insurance_estimate,
            aging_risk_category,
            collection_rate_last_year,
            billed_last_year,
            total_payments_last_year
        FROM raw_marts.mart_ar_summary
        WHERE (:start_date IS NULL OR snapshot_date >= :start_date)
          AND (:end_date IS NULL OR snapshot_date <= :end_date)
          AND total_balance > 0
        ORDER BY patient_id, provider_id, snapshot_date DESC
    ),
    ar_totals AS (
        SELECT 
            SUM(total_balance) as total_ar,
            SUM(balance_0_30_days) as balance_0_30,
            SUM(balance_31_60_days) as balance_31_60,
            SUM(balance_61_90_days) as balance_61_90,
            SUM(balance_over_90_days) as balance_over_90,
            SUM(patient_responsibility) as patient_ar_total,
            SUM(insurance_estimate) as insurance_ar_total,
            SUM(billed_last_year) as total_billed,
            SUM(total_payments_last_year) as total_payments
        FROM latest_snapshots
    ),
    risk_metrics AS (
        SELECT 
            COUNT(DISTINCT CASE WHEN aging_risk_category = 'High Risk' THEN patient_id END) as high_risk_count,
            COALESCE(SUM(CASE WHEN aging_risk_category = 'High Risk' THEN total_balance ELSE 0 END), 0) as high_risk_amount
        FROM latest_snapshots
    ),
    -- Calculate collection rate using ALL procedures and ALL payments (correct for direct-pay practice)
    -- Production: All procedures from fact_procedure (both insurance and direct-pay)
    -- Collections: All payments from fact_payment (both insurance and patient, excluding refunds)
    collection_rate_calc AS (
        SELECT 
            -- Production: All procedures in last 365 days
            (SELECT COALESCE(SUM(fp.actual_fee), 0)
             FROM raw_marts.fact_procedure fp
             INNER JOIN raw_marts.dim_date dd ON fp.date_id = dd.date_id
             WHERE dd.date_day >= CURRENT_DATE - INTERVAL '365 days') as total_production,
            -- Collections: All payments in last 365 days (excluding refunds)
            (SELECT COALESCE(SUM(payment_amount), 0)
             FROM raw_marts.fact_payment
             WHERE payment_date >= CURRENT_DATE - INTERVAL '365 days'
               AND payment_direction = 'Income') as total_collections
    ),
    -- Calculate AR Ratio: Collections (current month) / Production (current month)
    ar_ratio_calc AS (
        SELECT 
            -- Production: All procedures in current month
            (SELECT COALESCE(SUM(fp.actual_fee), 0)
             FROM raw_marts.fact_procedure fp
             INNER JOIN raw_marts.dim_date dd ON fp.date_id = dd.date_id
             WHERE dd.date_day >= DATE_TRUNC('month', CURRENT_DATE)) as monthly_production,
            -- Collections: All payments in current month (excluding refunds)
            (SELECT COALESCE(SUM(payment_amount), 0)
             FROM raw_marts.fact_payment
             WHERE payment_date >= DATE_TRUNC('month', CURRENT_DATE)
               AND payment_direction = 'Income') as monthly_collections
    )
    SELECT 
        at.total_ar as total_ar_outstanding,
        at.balance_0_30 as current_amount,
        CASE 
            WHEN at.total_ar > 0 
            THEN (at.balance_0_30 / NULLIF(at.total_ar, 0)) * 100
            ELSE 0
        END as current_percentage,
        at.balance_over_90 as over_90_amount,
        CASE 
            WHEN at.total_ar > 0 
            THEN (at.balance_over_90 / NULLIF(at.total_ar, 0)) * 100
            ELSE 0
        END as over_90_percentage,
        at.patient_ar_total as patient_ar,
        at.insurance_ar_total as insurance_ar,
        -- Collection rate: Collections / Production (using all procedures and all payments, last 365 days)
        CASE 
            WHEN crc.total_production > 0 
            THEN (crc.total_collections / NULLIF(crc.total_production, 0)) * 100
            ELSE 0
        END as collection_rate,
        -- AR Ratio: Collections (current month) / Production (current month)
        CASE 
            WHEN arc.monthly_production > 0 
            THEN (arc.monthly_collections / NULLIF(arc.monthly_production, 0)) * 100
            ELSE 0
        END as ar_ratio,
        rm.high_risk_count as high_risk_count,
        rm.high_risk_amount as high_risk_amount,
        CASE 
            WHEN at.total_ar > 0 
            THEN ((at.balance_over_90 / NULLIF(at.total_ar, 0)) * 90) + 30
            ELSE 0
        END as dso_days
    FROM ar_totals at
    CROSS JOIN risk_metrics rm
    CROSS JOIN collection_rate_calc crc
    CROSS JOIN ar_ratio_calc arc
    """
    
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    try:
        result = db.execute(text(query), params).fetchone()
    except Exception as e:
        logger.error(f"Error executing AR KPI query: {e}", exc_info=True)
        # Return zeros on error instead of crashing
        return {
            "total_ar_outstanding": 0.0,
            "current_amount": 0.0,
            "current_percentage": 0.0,
            "over_90_amount": 0.0,
            "over_90_percentage": 0.0,
            "patient_ar": 0.0,
            "insurance_ar": 0.0,
            "dso_days": 0.0,
            "collection_rate": 0.0,
            "ar_ratio": 0.0,
            "high_risk_count": 0,
            "high_risk_amount": 0.0
        }
    
    if result:
        return {
            "total_ar_outstanding": float(result.total_ar_outstanding or 0),
            "current_amount": float(result.current_amount or 0),
            "current_percentage": float(result.current_percentage or 0),
            "over_90_amount": float(result.over_90_amount or 0),
            "over_90_percentage": float(result.over_90_percentage or 0),
            "patient_ar": float(getattr(result, 'patient_ar', 0) or 0),
            "insurance_ar": float(getattr(result, 'insurance_ar', 0) or 0),
            "dso_days": float(result.dso_days or 0),
            "collection_rate": float(result.collection_rate or 0),
            "ar_ratio": float(getattr(result, 'ar_ratio', 0) or 0),
            "high_risk_count": int(result.high_risk_count or 0),
            "high_risk_amount": float(result.high_risk_amount or 0)
        }
    else:
        return {
            "total_ar_outstanding": 0.0,
            "current_amount": 0.0,
            "current_percentage": 0.0,
            "over_90_amount": 0.0,
            "over_90_percentage": 0.0,
            "patient_ar": 0.0,
            "insurance_ar": 0.0,
            "dso_days": 0.0,
            "collection_rate": 0.0,
            "ar_ratio": 0.0,
            "high_risk_count": 0,
            "high_risk_amount": 0.0
        }

def get_ar_aging_summary(
    db: Session,
    snapshot_date: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """
    Get AR aging summary by bucket from mart_ar_summary
    
    Returns list of aging buckets with amounts, percentages, patient counts
    
    Now queries mart_ar_summary directly - aging buckets are calculated in the mart model
    
    If snapshot_date is provided, uses that specific date.
    If start_date/end_date are provided, filters by date range.
    Otherwise, uses latest snapshot.
    """
    params = {}
    date_filter = ""
    
    if snapshot_date:
        params["snapshot_date"] = snapshot_date
        date_filter = "AND snapshot_date = :snapshot_date"
    elif start_date or end_date:
        if start_date:
            date_filter += " AND snapshot_date >= :start_date"
            params["start_date"] = start_date
        if end_date:
            date_filter += " AND snapshot_date <= :end_date"
            params["end_date"] = end_date
    else:
        # Get latest snapshot date
        latest_date_query = """
        SELECT MAX(snapshot_date) as latest_date
        FROM raw_marts.mart_ar_summary
        """
        latest_result = db.execute(text(latest_date_query)).fetchone()
        if latest_result and latest_result.latest_date:
            date_filter = "AND snapshot_date = :snapshot_date"
            params["snapshot_date"] = latest_result.latest_date
    
    query = f"""
    WITH latest_snapshots AS (
        SELECT DISTINCT ON (patient_id, provider_id)
            patient_id,
            provider_id,
            total_balance,
            balance_0_30_days,
            balance_31_60_days,
            balance_61_90_days,
            balance_over_90_days
        FROM raw_marts.mart_ar_summary
        WHERE total_balance > 0 {date_filter}
        ORDER BY patient_id, provider_id, snapshot_date DESC
    ),
    bucket_totals AS (
        SELECT 
            SUM(balance_0_30_days) as bucket_0_30,
            SUM(balance_31_60_days) as bucket_31_60,
            SUM(balance_61_90_days) as bucket_61_90,
            SUM(balance_over_90_days) as bucket_over_90,
            SUM(total_balance) as total,
            COUNT(DISTINCT patient_id) as patient_count_total,
            COUNT(DISTINCT CASE WHEN balance_0_30_days > 0 THEN patient_id END) as patient_count_0_30,
            COUNT(DISTINCT CASE WHEN balance_31_60_days > 0 THEN patient_id END) as patient_count_31_60,
            COUNT(DISTINCT CASE WHEN balance_61_90_days > 0 THEN patient_id END) as patient_count_61_90,
            COUNT(DISTINCT CASE WHEN balance_over_90_days > 0 THEN patient_id END) as patient_count_over_90
        FROM latest_snapshots
    ),
    bucket_results AS (
        SELECT * FROM bucket_totals
        UNION ALL
        SELECT 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        WHERE NOT EXISTS (SELECT 1 FROM bucket_totals)
        LIMIT 1
    ),
    aging_summary AS (
        SELECT 
            'Current' as aging_bucket,
            COALESCE(bucket_0_30, 0) as amount,
            CASE 
                WHEN COALESCE(total, 0) > 0 
                THEN (COALESCE(bucket_0_30, 0) / NULLIF(total, 0)) * 100 
                ELSE 0 
            END as percentage,
            COALESCE(patient_count_0_30, 0) as patient_count
        FROM bucket_results
        UNION ALL
        SELECT 
            '30 Day' as aging_bucket,
            COALESCE(bucket_31_60, 0) as amount,
            CASE 
                WHEN COALESCE(total, 0) > 0 
                THEN (COALESCE(bucket_31_60, 0) / NULLIF(total, 0)) * 100 
                ELSE 0 
            END as percentage,
            COALESCE(patient_count_31_60, 0) as patient_count
        FROM bucket_results
        UNION ALL
        SELECT 
            '60 Day' as aging_bucket,
            COALESCE(bucket_61_90, 0) as amount,
            CASE 
                WHEN COALESCE(total, 0) > 0 
                THEN (COALESCE(bucket_61_90, 0) / NULLIF(total, 0)) * 100 
                ELSE 0 
            END as percentage,
            COALESCE(patient_count_61_90, 0) as patient_count
        FROM bucket_results
        UNION ALL
        SELECT 
            '90 Day' as aging_bucket,
            COALESCE(bucket_over_90, 0) as amount,
            CASE 
                WHEN COALESCE(total, 0) > 0 
                THEN (COALESCE(bucket_over_90, 0) / NULLIF(total, 0)) * 100 
                ELSE 0 
            END as percentage,
            COALESCE(patient_count_over_90, 0) as patient_count
        FROM bucket_results
    )
    SELECT 
        aging_bucket,
        amount,
        percentage,
        patient_count
    FROM aging_summary
    ORDER BY 
        CASE 
            WHEN aging_bucket = 'Current' THEN 1
            WHEN aging_bucket = '30 Day' THEN 2
            WHEN aging_bucket = '60 Day' THEN 3
            WHEN aging_bucket = '90 Day' THEN 4
            ELSE 5
        END
    """
    
    try:
        result = db.execute(text(query), params).fetchall()
        logger.info(f"AR aging summary query returned {len(result)} rows")
        if result:
            logger.info(f"Sample row: {dict(result[0]._mapping) if hasattr(result[0], '_mapping') else result[0]}")
        
        # If query returns no rows, return empty buckets
        if not result or len(result) == 0:
            logger.warning("AR aging summary query returned no rows, returning empty buckets")
            return [
                {"aging_bucket": "Current", "amount": 0.0, "percentage": 0.0, "patient_count": 0},
                {"aging_bucket": "30 Day", "amount": 0.0, "percentage": 0.0, "patient_count": 0},
                {"aging_bucket": "60 Day", "amount": 0.0, "percentage": 0.0, "patient_count": 0},
                {"aging_bucket": "90 Day", "amount": 0.0, "percentage": 0.0, "patient_count": 0}
            ]
        
        formatted_result = [
            {
                "aging_bucket": str(row.aging_bucket),
                "amount": float(row.amount or 0),
                "percentage": float(row.percentage or 0),
                "patient_count": int(row.patient_count or 0)
            }
            for row in result
        ]
        logger.info(f"Formatted AR aging summary: {formatted_result}")
        return formatted_result
    except Exception as e:
        logger.error(f"Error executing AR aging query: {e}", exc_info=True)
        return [
            {"aging_bucket": "Current", "amount": 0.0, "percentage": 0.0, "patient_count": 0},
            {"aging_bucket": "30 Day", "amount": 0.0, "percentage": 0.0, "patient_count": 0},
            {"aging_bucket": "60 Day", "amount": 0.0, "percentage": 0.0, "patient_count": 0},
            {"aging_bucket": "90 Day", "amount": 0.0, "percentage": 0.0, "patient_count": 0}
        ]

def get_ar_priority_queue(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    min_priority_score: Optional[int] = None,
    risk_category: Optional[str] = None,
    min_balance: Optional[float] = None,
    provider_id: Optional[int] = None
) -> List[dict]:
    """
    Get AR priority queue sorted by collection_priority_score
    
    Joins:
    # - dim_provider for provider_name (removed for portfolio)
    Note: patient_name removed (PII) for portfolio use
    
    Filters:
    - min_priority_score: Minimum priority score threshold
    - risk_category: Filter by aging_risk_category
    - min_balance: Minimum total_balance threshold
    - provider_id: Filter by specific provider
    """
    # Build WHERE clause dynamically
    where_clauses = []
    params = {"skip": skip, "limit": limit}
    
    if min_priority_score is not None:
        where_clauses.append("mas.collection_priority_score >= :min_priority_score")
        params["min_priority_score"] = min_priority_score
    
    if risk_category:
        where_clauses.append("mas.aging_risk_category = :risk_category")
        params["risk_category"] = risk_category
    
    if min_balance is not None:
        where_clauses.append("mas.total_balance >= :min_balance")
        params["min_balance"] = min_balance
    
    if provider_id is not None:
        where_clauses.append("mas.provider_id = :provider_id")
        params["provider_id"] = provider_id
    
    where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    WITH latest_snapshots AS (
        SELECT DISTINCT ON (patient_id, provider_id)
            mas.patient_id,
            mas.provider_id,
            mas.total_balance,
            mas.balance_0_30_days,
            mas.balance_31_60_days,
            mas.balance_61_90_days,
            mas.balance_over_90_days,
            mas.aging_risk_category,
            mas.collection_priority_score,
            mas.days_since_last_payment,
            mas.payment_recency,
            mas.collection_rate_last_year,
            mas.snapshot_date
        FROM raw_marts.mart_ar_summary mas
        WHERE mas.total_balance > 0 {where_clause}
        ORDER BY mas.patient_id, mas.provider_id, mas.snapshot_date DESC
    )
    SELECT 
        ls.patient_id,
        ls.provider_id,
        -- patient_name and provider_name removed (PII)
        ls.total_balance,
        ls.balance_0_30_days,
        ls.balance_31_60_days,
        ls.balance_61_90_days,
        ls.balance_over_90_days,
        ls.aging_risk_category,
        ls.collection_priority_score,
        ls.days_since_last_payment,
        ls.payment_recency,
        ls.collection_rate_last_year as collection_rate
    FROM latest_snapshots ls
    ORDER BY ls.collection_priority_score DESC, ls.total_balance DESC
    LIMIT :limit OFFSET :skip
    """
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "patient_id": int(row.patient_id),
            "provider_id": int(row.provider_id),
            # "patient_name": Removed (PII)
            # "provider_name": str(row.provider_name or "").strip(),
            "total_balance": float(row.total_balance or 0),
            "balance_0_30_days": float(row.balance_0_30_days or 0),
            "balance_31_60_days": float(row.balance_31_60_days or 0),
            "balance_61_90_days": float(row.balance_61_90_days or 0),
            "balance_over_90_days": float(row.balance_over_90_days or 0),
            "aging_risk_category": str(row.aging_risk_category or ""),
            "collection_priority_score": int(row.collection_priority_score or 0),
            "days_since_last_payment": int(row.days_since_last_payment) if row.days_since_last_payment is not None else None,
            "payment_recency": str(row.payment_recency or ""),
            "collection_rate": float(row.collection_rate) if row.collection_rate is not None else None
        }
        for row in result
    ]

def get_ar_risk_distribution(
    db: Session,
    snapshot_date: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """
    Get risk category distribution
    
    Groups by aging_risk_category, counts patients, sums amounts
    
    If snapshot_date is provided, uses that specific date.
    If start_date/end_date are provided, filters by date range.
    Otherwise, uses latest snapshot.
    """
    date_filter = ""
    params = {}
    
    if snapshot_date:
        date_filter = "AND snapshot_date = :snapshot_date"
        params["snapshot_date"] = snapshot_date
    elif start_date or end_date:
        if start_date:
            date_filter += " AND snapshot_date >= :start_date"
            params["start_date"] = start_date
        if end_date:
            date_filter += " AND snapshot_date <= :end_date"
            params["end_date"] = end_date
    else:
        # Get latest snapshot date
        latest_date_query = """
        SELECT MAX(snapshot_date) as latest_date
        FROM raw_marts.mart_ar_summary
        """
        latest_result = db.execute(text(latest_date_query)).fetchone()
        if latest_result and latest_result.latest_date:
            date_filter = "AND snapshot_date = :snapshot_date"
            params["snapshot_date"] = latest_result.latest_date
    
    query = f"""
    WITH latest_snapshots AS (
        SELECT DISTINCT ON (patient_id, provider_id)
            patient_id,
            provider_id,
            aging_risk_category,
            total_balance
        FROM raw_marts.mart_ar_summary
        WHERE total_balance > 0 {date_filter}
        ORDER BY patient_id, provider_id, snapshot_date DESC
    ),
    category_totals AS (
        SELECT 
            COALESCE(aging_risk_category, 'Unknown') as risk_category,
            COUNT(DISTINCT patient_id) as patient_count,
            SUM(total_balance) as total_amount
        FROM latest_snapshots
        GROUP BY aging_risk_category
    ),
    grand_total AS (
        SELECT SUM(total_amount) as grand_total
        FROM category_totals
    )
    SELECT 
        ct.risk_category,
        ct.patient_count,
        COALESCE(ct.total_amount, 0) as total_amount,
        CASE 
            WHEN gt.grand_total > 0 
            THEN (ct.total_amount / gt.grand_total * 100)
            ELSE 0
        END as percentage
    FROM category_totals ct
    CROSS JOIN grand_total gt
    ORDER BY ct.total_amount DESC
    """
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "risk_category": str(row.risk_category or "Unknown"),
            "patient_count": int(row.patient_count or 0),
            "total_amount": float(row.total_amount or 0),
            "percentage": float(row.percentage or 0)
        }
        for row in result
    ]

def get_available_snapshot_dates(
    db: Session
) -> List[dict]:
    """
    Get list of available snapshot dates from mart_ar_summary
    
    Returns list of distinct snapshot dates ordered by date (most recent first)
    """
    query = """
    SELECT DISTINCT snapshot_date
    FROM raw_marts.mart_ar_summary
    WHERE snapshot_date IS NOT NULL
    ORDER BY snapshot_date DESC
    """
    
    try:
        result = db.execute(text(query)).fetchall()
        return [
            {"snapshot_date": row.snapshot_date.isoformat() if row.snapshot_date else None}
            for row in result
        ]
    except Exception as e:
        logger.error(f"Error fetching available snapshot dates: {e}", exc_info=True)
        return []

def get_ar_aging_trends(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """
    Get AR aging trends over time
    
    Groups by snapshot_date, aggregates by aging buckets
    """
    date_filter = ""
    params = {}
    
    if start_date:
        date_filter += " AND snapshot_date >= :start_date"
        params["start_date"] = start_date
    
    if end_date:
        date_filter += " AND snapshot_date <= :end_date"
        params["end_date"] = end_date
    
    query = f"""
    WITH daily_totals AS (
        SELECT 
            snapshot_date,
            SUM(balance_0_30_days) as current_amount,
            SUM(balance_31_60_days) + SUM(balance_61_90_days) as over_30_amount,
            SUM(balance_61_90_days) as over_60_amount,
            SUM(balance_over_90_days) as over_90_amount,
            SUM(total_balance) as total_amount
        FROM raw_marts.mart_ar_summary
        WHERE total_balance > 0 {date_filter}
        GROUP BY snapshot_date
    )
    SELECT 
        snapshot_date as date,
        COALESCE(current_amount, 0) as current_amount,
        COALESCE(over_30_amount, 0) as over_30_amount,
        COALESCE(over_60_amount, 0) as over_60_amount,
        COALESCE(over_90_amount, 0) as over_90_amount,
        COALESCE(total_amount, 0) as total_amount
    FROM daily_totals
    ORDER BY snapshot_date
    """
    
    result = db.execute(text(query), params).fetchall()
    return [
        {
            "date": row.date,
            "current_amount": float(row.current_amount or 0),
            "over_30_amount": float(row.over_30_amount or 0),
            "over_60_amount": float(row.over_60_amount or 0),
            "over_90_amount": float(row.over_90_amount or 0),
            "total_amount": float(row.total_amount or 0)
        }
        for row in result
    ]

