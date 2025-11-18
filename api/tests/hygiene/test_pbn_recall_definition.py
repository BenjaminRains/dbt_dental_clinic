"""
Test if PBN considers patients "on recall" based on recent appointments or other criteria
"""

import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config import get_config

def get_db_session():
    config = get_config()
    DATABASE_URL = config.get_database_url()
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

def test_pbn_recall_definition():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING PBN'S DEFINITION OF 'ON RECALL'")
        print("="*80)
        print("PBN shows: 20% not on recall (80% ARE on recall)")
        print("We have: 2,923 patients with recall records out of 30,456 total")
        print("Maybe PBN uses a different definition?")
        print()
        
        # Test 1: Patients with recent appointments (past 18 months)
        query1 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_recent_appointments AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active', 'Inactive')
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwa.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwa.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_recent_appointments pwa ON ap.patient_id = pwa.patient_id
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"Test 1 - Patients with recent appointments (18 months) = 'on recall':")
        print(f"  Not on recall: {result1.not_on_recall_count}")
        print(f"  Total patients: {result1.total_patients}")
        print(f"  Not on Recall %: {result1.not_on_recall_percent:.2f}%")
        print()
        
        # Test 2: Patients with scheduled appointments
        query2 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_scheduled_appointments AS (
            SELECT DISTINCT fa.patient_id
            FROM raw_marts.fact_appointment fa
            WHERE fa.appointment_date > CURRENT_DATE
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pws.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_scheduled_appointments pws ON ap.patient_id = pws.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"Test 2 - Patients with scheduled appointments = 'on recall':")
        print(f"  Not on recall: {result2.not_on_recall_count}")
        print(f"  Total patients: {result2.total_patients}")
        print(f"  Not on Recall %: {result2.not_on_recall_percent:.2f}%")
        print()
        
        # Test 3: Patients with recall OR recent appointments
        query3 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_recall AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_disabled = false
                AND is_valid_recall = true
        ),
        patients_with_recent_appointments AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_on_recall AS (
            SELECT patient_id FROM patients_with_recall
            UNION
            SELECT patient_id FROM patients_with_recent_appointments
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN por.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN por.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_on_recall por ON ap.patient_id = por.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"Test 3 - Recall records OR recent appointments = 'on recall':")
        print(f"  Not on recall: {result3.not_on_recall_count}")
        print(f"  Total patients: {result3.total_patients}")
        print(f"  Not on Recall %: {result3.not_on_recall_percent:.2f}%")
        print()
        
        # Test 4: Breakdown
        query4 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_recall AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_disabled = false
                AND is_valid_recall = true
        ),
        patients_with_recent_appointments AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active', 'Inactive')
        )
        SELECT 
            (SELECT COUNT(DISTINCT patient_id) FROM all_patients) as total_patients,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_recall) as with_recall_record,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_recent_appointments) as with_recent_appt,
            (SELECT COUNT(DISTINCT patient_id) FROM patients_with_recall 
             WHERE patient_id IN (SELECT patient_id FROM patients_with_recent_appointments)) as both
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"Breakdown:")
        print(f"  Total patients: {result4.total_patients}")
        print(f"  With recall record: {result4.with_recall_record}")
        print(f"  With recent appointment (18 months): {result4.with_recent_appt}")
        print(f"  Both: {result4.both}")
        print(f"  Only recall record: {result4.with_recall_record - result4.both}")
        print(f"  Only recent appointment: {result4.with_recent_appt - result4.both}")
        print(f"  Neither: {result4.total_patients - result4.with_recall_record - result4.with_recent_appt + result4.both}")
        print()
        
        print("="*80)
        print("CLOSEST TO PBN'S 20%:")
        print("="*80)
        results = [
            ("Test 1 (recent appointments)", result1.not_on_recall_percent),
            ("Test 2 (scheduled appointments)", result2.not_on_recall_percent),
            ("Test 3 (recall OR recent)", result3.not_on_recall_percent),
        ]
        results.sort(key=lambda x: abs(x[1] - 20))
        for name, percent in results[:3]:
            print(f"  {name}: {percent:.2f}% (diff: {abs(percent - 20):.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_pbn_recall_definition()

