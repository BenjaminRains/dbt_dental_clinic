"""
Test hygiene metrics with full year 2025 to match PBN's numbers
PBN Actual Numbers:
- Hyg Pre-appmt: 50.7%
- Hyg Pts Seen: 2073
- Hyg Pts Re-appntd: 1051
- Recall Current: 53.4%
- Recall Overdue: 25.6%
- Not on Recall: 20%
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import sys
import os
from datetime import date

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

def test_hygiene_definitions():
    """Test different definitions of hygiene appointments"""
    db = get_db_session()
    try:
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        
        print("="*70)
        print(f"TESTING HYGIENE DEFINITIONS (Full Year 2025: {start_date} to {end_date})")
        print("="*70)
        print(f"PBN Expected: Hyg Pts Seen: 2073, Hyg Pts Re-appntd: 1051, Pre-appmt: 50.7%")
        print()
        
        # Test 1: Only completed hygiene appointments (current logic)
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
                AND fa.is_completed = true
        ),
        patients_with_next_appt AS (
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
            COUNT(DISTINCT pwna.patient_id) as reappointed,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0 
                THEN (COUNT(DISTINCT pwna.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM hygiene_appointments hp
        LEFT JOIN patients_with_next_appt pwna ON hp.patient_id = pwna.patient_id
        """
        result1 = db.execute(text(query1), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"1. Completed hygiene only:")
        print(f"   Hyg Pts Seen: {result1.total_hygiene} (PBN: 2073)")
        print(f"   Hyg Pts Re-appntd: {result1.reappointed} (PBN: 1051)")
        print(f"   Pre-appmt %: {result1.percent:.2f}% (PBN: 50.7%)")
        print()
        
        # Test 2: ALL hygiene appointments (not just completed)
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
        patients_with_next_appt AS (
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
            COUNT(DISTINCT pwna.patient_id) as reappointed,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0 
                THEN (COUNT(DISTINCT pwna.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM hygiene_appointments hp
        LEFT JOIN patients_with_next_appt pwna ON hp.patient_id = pwna.patient_id
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"2. ALL hygiene appointments (completed + scheduled):")
        print(f"   Hyg Pts Seen: {result2.total_hygiene} (PBN: 2073)")
        print(f"   Hyg Pts Re-appntd: {result2.reappointed} (PBN: 1051)")
        print(f"   Pre-appmt %: {result2.percent:.2f}% (PBN: 50.7%)")
        print()
        
        # Test 3: Include procedures from int_procedure_complete
        query3 = """
        WITH hygiene_procedures AS (
            SELECT DISTINCT
                pc.patient_id,
                pc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete pc
            WHERE pc.is_hygiene = true
                AND pc.procedure_date >= :start_date
                AND pc.procedure_date <= :end_date
        ),
        hygiene_appointments AS (
            SELECT DISTINCT
                fa.patient_id,
                fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.is_hygiene_appointment = true 
                   OR fa.hygienist_id IS NOT NULL)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id, hygiene_date
            FROM hygiene_procedures
            UNION
            SELECT DISTINCT patient_id, hygiene_date
            FROM hygiene_appointments
        ),
        patients_with_next_appt AS (
            SELECT DISTINCT hp.patient_id
            FROM hygiene_patients hp
            WHERE EXISTS (
                SELECT 1 
                FROM raw_marts.fact_appointment fa2
                WHERE fa2.patient_id = hp.patient_id
                    AND fa2.appointment_date > hp.hygiene_date
            )
        )
        SELECT 
            COUNT(DISTINCT hp.patient_id) as total_hygiene,
            COUNT(DISTINCT pwna.patient_id) as reappointed,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0 
                THEN (COUNT(DISTINCT pwna.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM hygiene_patients hp
        LEFT JOIN patients_with_next_appt pwna ON hp.patient_id = pwna.patient_id
        """
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"3. Procedures + Appointments (current service logic):")
        print(f"   Hyg Pts Seen: {result3.total_hygiene} (PBN: 2073)")
        print(f"   Hyg Pts Re-appntd: {result3.reappointed} (PBN: 1051)")
        print(f"   Pre-appmt %: {result3.percent:.2f}% (PBN: 50.7%)")
        print()
        
        # Test 4: ALL appointments (no is_completed filter) + procedures
        query4 = """
        WITH hygiene_procedures AS (
            SELECT DISTINCT
                pc.patient_id,
                pc.procedure_date as hygiene_date
            FROM raw_intermediate.int_procedure_complete pc
            WHERE pc.is_hygiene = true
                AND pc.procedure_date >= :start_date
                AND pc.procedure_date <= :end_date
        ),
        hygiene_appointments AS (
            SELECT DISTINCT
                fa.patient_id,
                fa.appointment_date as hygiene_date
            FROM raw_marts.fact_appointment fa
            WHERE (fa.is_hygiene_appointment = true 
                   OR fa.hygienist_id IS NOT NULL)
                AND fa.appointment_date >= :start_date
                AND fa.appointment_date <= :end_date
                -- NO is_completed filter
        ),
        hygiene_patients AS (
            SELECT DISTINCT patient_id, hygiene_date
            FROM hygiene_procedures
            UNION
            SELECT DISTINCT patient_id, hygiene_date
            FROM hygiene_appointments
        ),
        patients_with_next_appt AS (
            SELECT DISTINCT hp.patient_id
            FROM hygiene_patients hp
            WHERE EXISTS (
                SELECT 1 
                FROM raw_marts.fact_appointment fa2
                WHERE fa2.patient_id = hp.patient_id
                    AND fa2.appointment_date > hp.hygiene_date
            )
        )
        SELECT 
            COUNT(DISTINCT hp.patient_id) as total_hygiene,
            COUNT(DISTINCT pwna.patient_id) as reappointed,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0 
                THEN (COUNT(DISTINCT pwna.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM hygiene_patients hp
        LEFT JOIN patients_with_next_appt pwna ON hp.patient_id = pwna.patient_id
        """
        result4 = db.execute(text(query4), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"4. Procedures + ALL Appointments (no is_completed filter):")
        print(f"   Hyg Pts Seen: {result4.total_hygiene} (PBN: 2073)")
        print(f"   Hyg Pts Re-appntd: {result4.reappointed} (PBN: 1051)")
        print(f"   Pre-appmt %: {result4.percent:.2f}% (PBN: 50.7%)")
        print()
        
        # Test 5: ALL appointments + only FUTURE appointments count as reappointed
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
            COUNT(DISTINCT pwfa.patient_id) as reappointed,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0 
                THEN (COUNT(DISTINCT pwfa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM hygiene_appointments hp
        LEFT JOIN patients_with_future_appt pwfa ON hp.patient_id = pwfa.patient_id
        """
        result5 = db.execute(text(query5), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"5. ALL Appointments + only FUTURE appointments count as reappointed:")
        print(f"   Hyg Pts Seen: {result5.total_hygiene} (PBN: 2073)")
        print(f"   Hyg Pts Re-appntd: {result5.reappointed} (PBN: 1051)")
        print(f"   Pre-appmt %: {result5.percent:.2f}% (PBN: 50.7%)")
        print()
        
        # Test 6: Check if maybe PBN counts appointments scheduled within a window after hygiene
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
        patients_with_appt_scheduled_after AS (
            SELECT DISTINCT hp.patient_id
            FROM hygiene_appointments hp
            WHERE EXISTS (
                SELECT 1 
                FROM raw_marts.fact_appointment fa2
                WHERE fa2.patient_id = hp.patient_id
                    AND fa2.appointment_date > hp.hygiene_date
                    AND fa2.date_scheduled >= hp.hygiene_date
                    AND fa2.date_scheduled <= hp.hygiene_date + INTERVAL '90 days'
            )
        )
        SELECT 
            COUNT(DISTINCT hp.patient_id) as total_hygiene,
            COUNT(DISTINCT pwasa.patient_id) as reappointed,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0 
                THEN (COUNT(DISTINCT pwasa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM hygiene_appointments hp
        LEFT JOIN patients_with_appt_scheduled_after pwasa ON hp.patient_id = pwasa.patient_id
        """
        result6 = db.execute(text(query6), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"6. ALL Appointments + appt scheduled within 90 days after hygiene:")
        print(f"   Hyg Pts Seen: {result6.total_hygiene} (PBN: 2073)")
        print(f"   Hyg Pts Re-appntd: {result6.reappointed} (PBN: 1051)")
        print(f"   Pre-appmt %: {result6.percent:.2f}% (PBN: 50.7%)")
        print()
        
    finally:
        db.close()

if __name__ == "__main__":
    test_hygiene_definitions()

