"""
Test different interpretations of PBN logic to match their dashboard values

Key insights from PBN documentation:
1. "scheduled another appointment within the time range" - ambiguous phrase
2. Recall Current % - maybe "current" means "has recall program" not "up to date"
3. Date ranges might be different (PBN shows 180 patients, we have 884 for 12 months)
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import date, timedelta
import sys
import os

api_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, api_dir)

from config import get_config

def get_db_session():
    """Get database session"""
    config = get_config()
    DATABASE_URL = config.get_database_url()
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

def test_pre_appointment_interpretations():
    """Test different interpretations of 'scheduled another appointment within the time range'"""
    db = get_db_session()
    try:
        print("\n" + "="*70)
        print("TESTING: Hyg Pre-Appointment % (PBN: 81.1%)")
        print("="*70)
        
        # Try different date ranges
        test_ranges = [
            ("Last 30 days", 30),
            ("Last 60 days", 60),
            ("Last 90 days", 90),
        ]
        
        for label, days in test_ranges:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            
            print(f"\n{label} ({start_date} to {end_date}):")
            print("-" * 70)
            
            # Interpretation 1: Appointment DATE falls within time range (after hygiene)
            query1 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_appt_in_range AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.appointment_date >= :start_date
                        AND fa2.appointment_date <= :end_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_appt_in_range pwa ON hp.patient_id = pwa.patient_id
            """
            result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"1. Appt DATE in range (after hygiene): {result1.percent:.2f}% ({result1.with_appt}/{result1.total_hygiene})")
            
            # Interpretation 2: Any future appointment (current logic)
            query2 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_future_appt AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.appointment_date > CURRENT_DATE
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_future_appt pwa ON hp.patient_id = pwa.patient_id
            """
            result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"2. Any future appt (after hygiene): {result2.percent:.2f}% ({result2.with_appt}/{result2.total_hygiene})")
            
            # Interpretation 3: Appointment scheduled BEFORE hygiene (pre-appointment)
            query3 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_pre_appt AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date < hp.hygiene_date
                        AND fa2.appointment_date >= :start_date
                        AND fa2.appointment_date <= :end_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_pre_appt pwa ON hp.patient_id = pwa.patient_id
            """
            result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"3. Appt BEFORE hygiene (in range): {result3.percent:.2f}% ({result3.with_appt}/{result3.total_hygiene})")
            
            # Interpretation 4: Any appointment (past or future) after hygiene date
            query4 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_any_appt_after AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_any_appt_after pwa ON hp.patient_id = pwa.patient_id
            """
            result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"4. ANY appt after hygiene (past or future): {result4.percent:.2f}% ({result4.with_appt}/{result4.total_hygiene})")
            
            # Interpretation 5: Appointment SCHEDULED (created) within time range, after hygiene date
            query5 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_appt_scheduled_in_range AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.date_scheduled >= :start_date
                        AND fa2.date_scheduled <= :end_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_appt_scheduled_in_range pwa ON hp.patient_id = pwa.patient_id
            """
            result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"5. Appt SCHEDULED in range (after hygiene): {result5.percent:.2f}% ({result5.with_appt}/{result5.total_hygiene})")
            
            # Interpretation 6: Appointment scheduled on SAME DAY as hygiene or within 7 days after
            query6 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_appt_same_day_or_soon AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.date_scheduled >= hp.hygiene_date
                        AND fa2.date_scheduled <= hp.hygiene_date + INTERVAL '7 days'
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_appt_same_day_or_soon pwa ON hp.patient_id = pwa.patient_id
            """
            result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"6. Appt scheduled same day or within 7 days: {result6.percent:.2f}% ({result6.with_appt}/{result6.total_hygiene})")
            
            # Interpretation 7: Appointment scheduled BEFORE hygiene visit (pre-appointment strategy)
            query7 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date,
                    fa.date_scheduled as hygiene_scheduled
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_pre_scheduled_appt AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.date_scheduled < hp.hygiene_date
                        AND fa2.date_scheduled >= :start_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_pre_scheduled_appt pwa ON hp.patient_id = pwa.patient_id
            """
            result7 = db.execute(text(query7), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"7. Appt scheduled BEFORE hygiene (pre-appt): {result7.percent:.2f}% ({result7.with_appt}/{result7.total_hygiene})")
            
            # Interpretation 8: Appointment EXISTS at time of hygiene (any future appointment, regardless of when scheduled)
            # This checks if patient had ANY future appointment scheduled when they came for hygiene
            query8 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_future_appt_at_hygiene AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.date_scheduled <= hp.hygiene_date + INTERVAL '1 day'
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_future_appt_at_hygiene pwa ON hp.patient_id = pwa.patient_id
            """
            result8 = db.execute(text(query8), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"8. Future appt EXISTS at hygiene time: {result8.percent:.2f}% ({result8.with_appt}/{result8.total_hygiene})")
            
            # Interpretation 9: Check if maybe we need to look at a rolling window (e.g., last 30 days from TODAY, not from end_date)
            # This would match PBN's "current" view
            today = date.today()
            query9 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_future_appt AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.appointment_date > CURRENT_DATE
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_future_appt pwa ON hp.patient_id = pwa.patient_id
            """
            result9 = db.execute(text(query9), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"9. Future appt (current logic, for comparison): {result9.percent:.2f}% ({result9.with_appt}/{result9.total_hygiene})")
            
            # Interpretation 10: "scheduled another appointment within the time range" 
            # Maybe means: appointment was SCHEDULED on same day as hygiene or within extended window
            # This would indicate it was scheduled during/after the hygiene visit
            print(f"\n10. Testing extended scheduling windows:")
            for days_window in [0, 1, 3, 7, 14, 30, 60, 90]:
                query10 = f"""
                WITH hygiene_appointments AS (
                    SELECT DISTINCT
                        fa.patient_id,
                        fa.appointment_date as hygiene_date
                    FROM raw_marts.fact_appointment fa
                    WHERE (fa.is_hygiene_appointment = true 
                           OR fa.hygienist_id IS NOT NULL)
                        AND fa.appointment_date >= :start_date
                        AND fa.appointment_date <= :end_date
                ),
                patients_with_appt_scheduled_near_hygiene AS (
                    SELECT DISTINCT hp.patient_id
                    FROM hygiene_appointments hp
                    WHERE EXISTS (
                        SELECT 1 
                        FROM raw_marts.fact_appointment fa2
                        WHERE fa2.patient_id = hp.patient_id
                            AND fa2.appointment_date > hp.hygiene_date
                            AND fa2.date_scheduled >= hp.hygiene_date
                            AND fa2.date_scheduled <= hp.hygiene_date + INTERVAL '{days_window} days'
                    )
                )
                SELECT 
                    COUNT(DISTINCT hp.patient_id) as total_hygiene,
                    COUNT(DISTINCT pwa.patient_id) as with_appt,
                    CASE 
                        WHEN COUNT(DISTINCT hp.patient_id) > 0 
                        THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                        ELSE 0
                    END as percent
                FROM hygiene_appointments hp
                LEFT JOIN patients_with_appt_scheduled_near_hygiene pwa ON hp.patient_id = pwa.patient_id
                """
                result10 = db.execute(text(query10), {"start_date": start_date, "end_date": end_date}).fetchone()
                if days_window == 0:
                    print(f"    Same day: {result10.percent:.2f}% ({result10.with_appt}/{result10.total_hygiene})")
                elif days_window in [30, 60, 90]:
                    print(f"    Within {days_window} days: {result10.percent:.2f}% ({result10.with_appt}/{result10.total_hygiene})")
            
            # Interpretation 11: "scheduled another appointment within the time range"
            # Maybe means: appointment was SCHEDULED (created) during the time range
            # AND appointment date is after hygiene date (but appointment date doesn't need to be in range)
            query11 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_appt_scheduled_in_range AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.date_scheduled >= :start_date
                        AND fa2.date_scheduled <= :end_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_appt_scheduled_in_range pwa ON hp.patient_id = pwa.patient_id
            """
            result11 = db.execute(text(query11), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"11. Appt SCHEDULED in time range (date after hygiene): {result11.percent:.2f}% ({result11.with_appt}/{result11.total_hygiene})")
            
            # Interpretation 12: Maybe "within the time range" means the appointment DATE (not when scheduled)
            # falls within the time range AND is after hygiene
            query12 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
            ),
            patients_with_appt_date_in_range AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                        AND fa2.appointment_date >= :start_date
                        AND fa2.appointment_date <= :end_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_appt_date_in_range pwa ON hp.patient_id = pwa.patient_id
            """
            result12 = db.execute(text(query12), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"12. Appt DATE in time range (after hygiene): {result12.percent:.2f}% ({result12.with_appt}/{result12.total_hygiene})")
            
            # Interpretation 13: Only COMPLETED hygiene appointments count
            # Maybe PBN only counts patients who actually completed their hygiene visit
            query13 = """
            WITH hygiene_appointments AS (
                SELECT DISTINCT
                    fa.patient_id,
                    fa.appointment_date as hygiene_date
                FROM raw_marts.fact_appointment fa
                WHERE (fa.is_hygiene_appointment = true 
                       OR fa.hygienist_id IS NOT NULL)
                    AND fa.appointment_date >= :start_date
                    AND fa.appointment_date <= :end_date
                    AND fa.is_completed = true
            ),
            patients_with_future_appt AS (
                SELECT DISTINCT hp.patient_id
                FROM hygiene_appointments hp
                WHERE EXISTS (
                    SELECT 1 
                    FROM raw_marts.fact_appointment fa2
                    WHERE fa2.patient_id = hp.patient_id
                        AND fa2.appointment_date > hp.hygiene_date
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as total_hygiene,
                COUNT(DISTINCT pwa.patient_id) as with_appt,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_future_appt pwa ON hp.patient_id = pwa.patient_id
            """
            result13 = db.execute(text(query13), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"\n13. COMPLETED hygiene only + ANY appt after: {result13.percent:.2f}% ({result13.with_appt}/{result13.total_hygiene})")
            print(f"    (Total completed hygiene patients: {result13.total_hygiene})")
            
    finally:
        db.close()

def test_recall_current_interpretations():
    """Test different interpretations of 'current on recall'"""
    db = get_db_session()
    try:
        print("\n" + "="*70)
        print("TESTING: Recall Current % (PBN: 53.4%)")
        print("="*70)
        
        # Interpretation 1: Simply "has recall program"
        query1 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT irm.patient_id) as with_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT irm.patient_id)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN raw_intermediate.int_recall_management irm 
            ON ap.patient_id = irm.patient_id
            AND irm.is_disabled = false
            AND irm.is_valid_recall = true
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"\n1. Has recall program: {result1.percent:.2f}% ({result1.with_recall}/{result1.total_active})")
        
        # Interpretation 2: Has recall AND not overdue
        query2 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT irm.patient_id) as with_recall_not_overdue,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT irm.patient_id)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN raw_intermediate.int_recall_management irm 
            ON ap.patient_id = irm.patient_id
            AND irm.is_disabled = false
            AND irm.is_valid_recall = true
            AND irm.is_overdue = false
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"2. Has recall AND not overdue: {result2.percent:.2f}% ({result2.with_recall_not_overdue}/{result2.total_active})")
        
        # Interpretation 3: Has recall AND due date within reasonable window (not too far overdue)
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT irm.patient_id) as with_recall_reasonable,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT irm.patient_id)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN raw_intermediate.int_recall_management irm 
            ON ap.patient_id = irm.patient_id
            AND irm.is_disabled = false
            AND irm.is_valid_recall = true
            AND (irm.date_due >= CURRENT_DATE - INTERVAL '6 months' OR irm.is_overdue = false)
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"3. Has recall AND (due within 6mo OR not overdue): {result3.percent:.2f}% ({result3.with_recall_reasonable}/{result3.total_active})")
        
        # Interpretation 4: "Attended their scheduled follow-up within recommended time frame"
        # This means: Patient has completed an appointment that satisfies their recall requirement
        # Check if patient has a completed appointment within their recall interval
        query4 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT 
                ap.patient_id,
                irm.recall_interval_months,
                irm.date_due
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm 
                ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        patients_current_on_recall AS (
            SELECT DISTINCT pwr.patient_id
            FROM patients_with_recall pwr
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = pwr.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= CURRENT_DATE - (INTERVAL '1 month' * COALESCE(pwr.recall_interval_months, 6))
                    AND fa.appointment_date <= CURRENT_DATE
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT pcor.patient_id) as current_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT pcor.patient_id)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN patients_current_on_recall pcor ON ap.patient_id = pcor.patient_id
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"4. Completed appt within recall interval: {result4.percent:.2f}% ({result4.current_on_recall}/{result4.total_active})")
        
        # Interpretation 5: Similar but check if appointment was completed AFTER the recall due date (within window)
        query5 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT 
                ap.patient_id,
                irm.recall_interval_months,
                irm.date_due,
                irm.date_scheduled
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm 
                ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        patients_current_on_recall AS (
            SELECT DISTINCT pwr.patient_id
            FROM patients_with_recall pwr
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = pwr.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= COALESCE(pwr.date_due, CURRENT_DATE - INTERVAL '6 months')
                    AND fa.appointment_date <= CURRENT_DATE
                    AND fa.appointment_date >= pwr.date_due - INTERVAL '3 months'  -- Allow 3 months grace period before due
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT pcor.patient_id) as current_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT pcor.patient_id)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN patients_current_on_recall pcor ON ap.patient_id = pcor.patient_id
        """
        result5 = db.execute(text(query5)).fetchone()
        print(f"5. Completed appt after recall due (within window): {result5.percent:.2f}% ({result5.current_on_recall}/{result5.total_active})")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_pre_appointment_interpretations()
    test_recall_current_interpretations()

