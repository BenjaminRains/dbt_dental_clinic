"""
Test different definitions of "on recall" to match PBN's 20%
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

def test_not_on_recall_logic():
    db = get_db_session()
    try:
        print("="*80)
        print("TESTING 'NOT ON RECALL %' LOGIC")
        print("="*80)
        print("PBN Target: 20% not on recall (meaning 80% ARE on recall)")
        print("Current: 90.42% not on recall (meaning 9.58% ARE on recall)")
        print()
        
        # Test 1: Current logic (is_disabled = false AND is_valid_recall = true)
        query1 = """
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
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"Test 1 - Current logic (is_disabled=false AND is_valid_recall=true):")
        print(f"  Not on recall: {result1.not_on_recall_count}")
        print(f"  Total patients: {result1.total_patients}")
        print(f"  Not on Recall %: {result1.not_on_recall_percent:.2f}%")
        print()
        
        # Test 2: Any recall record (no filters)
        query2 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_recall AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"Test 2 - Any recall record (no filters):")
        print(f"  Not on recall: {result2.not_on_recall_count}")
        print(f"  Total patients: {result2.total_patients}")
        print(f"  Not on Recall %: {result2.not_on_recall_percent:.2f}%")
        print()
        
        # Test 3: Only is_valid_recall = true (ignore is_disabled)
        query3 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_with_recall AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"Test 3 - Only is_valid_recall = true (ignore is_disabled):")
        print(f"  Not on recall: {result3.not_on_recall_count}")
        print(f"  Total patients: {result3.total_patients}")
        print(f"  Not on Recall %: {result3.not_on_recall_percent:.2f}%")
        print()
        
        # Test 4: Only is_disabled = false (ignore is_valid_recall)
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
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"Test 4 - Only is_disabled = false (ignore is_valid_recall):")
        print(f"  Not on recall: {result4.not_on_recall_count}")
        print(f"  Total patients: {result4.total_patients}")
        print(f"  Not on Recall %: {result4.not_on_recall_percent:.2f}%")
        print()
        
        # Test 5: Breakdown of recall records
        query5 = """
        SELECT 
            COUNT(DISTINCT patient_id) as total_patients_with_recall,
            COUNT(DISTINCT CASE WHEN is_disabled = false AND is_valid_recall = true THEN patient_id END) as valid_active_recall,
            COUNT(DISTINCT CASE WHEN is_disabled = false THEN patient_id END) as active_recall,
            COUNT(DISTINCT CASE WHEN is_valid_recall = true THEN patient_id END) as valid_recall,
            COUNT(DISTINCT patient_id) as any_recall
        FROM raw_intermediate.int_recall_management
        """
        result5 = db.execute(text(query5)).fetchone()
        print(f"Breakdown of recall records:")
        print(f"  Total patients with ANY recall record: {result5.any_recall}")
        print(f"  Valid + Active (is_disabled=false AND is_valid_recall=true): {result5.valid_active_recall}")
        print(f"  Active (is_disabled=false): {result5.active_recall}")
        print(f"  Valid (is_valid_recall=true): {result5.valid_recall}")
        print()
        
        # Test 6: What if we only count active patients?
        query6 = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT patient_id
            FROM raw_intermediate.int_recall_management
            WHERE is_disabled = false
                AND is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric as not_on_recall_count,
            COUNT(DISTINCT ap.patient_id)::numeric as total_patients,
            (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric / 
             NULLIF(COUNT(DISTINCT ap.patient_id)::numeric, 0)) * 100 as not_on_recall_percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result6 = db.execute(text(query6)).fetchone()
        print(f"Test 6 - All patients (same as Test 1, for comparison):")
        print(f"  Not on recall: {result6.not_on_recall_count}")
        print(f"  Total patients: {result6.total_patients}")
        print(f"  Not on Recall %: {result6.not_on_recall_percent:.2f}%")
        print()
        
        print("="*80)
        print("CLOSEST TO PBN'S 20%:")
        print("="*80)
        results = [
            ("Test 1 (current)", result1.not_on_recall_percent),
            ("Test 2 (any recall)", result2.not_on_recall_percent),
            ("Test 3 (valid only)", result3.not_on_recall_percent),
            ("Test 4 (active only)", result4.not_on_recall_percent),
        ]
        results.sort(key=lambda x: abs(x[1] - 20))
        for name, percent in results[:3]:
            print(f"  {name}: {percent:.2f}% (diff: {abs(percent - 20):.2f}%)")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_not_on_recall_logic()

