"""
Compare our calculations with PBN dashboard values

PBN Dashboard Values:
- Recall Current: 53.4%
- Hyg Pre-appmt: 81.1%
- Hyg Pts Seen: 180
- Hyg Pts Re-appntd: 146
- Recall Overdue: 25.6%
- Not on Recall: 20%

Our Current Values:
- Recall Current %: 0.0%
- Hyg Pre-Appointment %: 34.5%
- Hyg Patients Seen: 884
- Hyg Pts Re-appntd: 305
- Recall Overdue %: 99.7%
- Not on Recall %: 90.4%
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

def analyze_recall_current():
    """Analyze why Recall Current % is 0.0% instead of 53.4%"""
    db = get_db_session()
    try:
        print("\n" + "="*60)
        print("ANALYZING: Recall Current % (Expected: 53.4%, Got: 0.0%)")
        print("="*60)
        
        # Check active patients
        query1 = """
        SELECT COUNT(DISTINCT p.patient_id) as active_count
        FROM raw_marts.dim_patient p
        INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
        WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
            AND p.patient_status IN ('Patient', 'Active')
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"\n1. Active Patients: {result1.active_count}")
        
        # Check recall records
        query2 = """
        SELECT 
            COUNT(*) as total_recalls,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(CASE WHEN is_disabled = false AND is_valid_recall = true THEN 1 END) as active_valid_recalls,
            COUNT(CASE WHEN date_due >= CURRENT_DATE THEN 1 END) as future_due_dates,
            COUNT(CASE WHEN date_due < CURRENT_DATE THEN 1 END) as past_due_dates,
            COUNT(CASE WHEN recall_status_description = 'Completed' THEN 1 END) as completed,
            COUNT(CASE WHEN recall_status_description = 'Scheduled' THEN 1 END) as scheduled,
            COUNT(CASE WHEN compliance_status = 'Compliant' THEN 1 END) as compliant
        FROM raw_intermediate.int_recall_management
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"\n2. Recall Records:")
        print(f"   Total: {result2.total_recalls}")
        print(f"   Unique Patients: {result2.unique_patients}")
        print(f"   Active & Valid: {result2.active_valid_recalls}")
        print(f"   Future Due Dates: {result2.future_due_dates}")
        print(f"   Past Due Dates: {result2.past_due_dates}")
        print(f"   Completed: {result2.completed}")
        print(f"   Scheduled: {result2.scheduled}")
        print(f"   Compliant: {result2.compliant}")
        
        # Check current on recall calculation
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT 
                irm.patient_id,
                CASE 
                    WHEN irm.date_due IS NULL THEN false
                    WHEN irm.date_due >= CURRENT_DATE THEN true
                    WHEN irm.recall_status_description = 'Scheduled' 
                        AND irm.date_scheduled <= irm.date_due THEN true
                    WHEN irm.recall_status_description = 'Completed' THEN true
                    WHEN irm.compliance_status = 'Compliant' THEN true
                    ELSE false
                END as is_current_on_recall
            FROM raw_intermediate.int_recall_management irm
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT pwr.patient_id) as patients_with_recall,
            COUNT(DISTINCT CASE WHEN pwr.is_current_on_recall THEN ap.patient_id END) as current_count,
            COUNT(DISTINCT CASE WHEN pwr.is_current_on_recall = false THEN ap.patient_id END) as not_current_count,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END) as no_recall_record
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"\n3. Current on Recall Calculation:")
        print(f"   Total Active: {result3.total_active}")
        print(f"   Patients with Recall Record: {result3.patients_with_recall}")
        print(f"   Current Count: {result3.current_count}")
        print(f"   Not Current Count: {result3.not_current_count}")
        print(f"   No Recall Record: {result3.no_recall_record}")
        if result3.total_active > 0:
            pct = (result3.current_count / result3.total_active) * 100
            print(f"   Calculated %: {pct:.2f}%")
        
        # Check if maybe PBN uses a different definition - maybe NOT overdue = current?
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
                irm.patient_id,
                CASE 
                    WHEN irm.is_overdue = false THEN true
                    ELSE false
                END as is_current_on_recall
            FROM raw_intermediate.int_recall_management irm
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT CASE WHEN pwr.is_current_on_recall THEN ap.patient_id END) as current_count
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result4 = db.execute(text(query4)).fetchone()
        print(f"\n4. Alternative: Using is_overdue = false as 'current':")
        print(f"   Total Active: {result4.total_active}")
        print(f"   Current Count: {result4.current_count}")
        if result4.total_active > 0:
            pct = (result4.current_count / result4.total_active) * 100
            print(f"   Calculated %: {pct:.2f}%")
        
        # Check if PBN simply counts "has recall program" as "current"
        query5 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT irm.patient_id) as patients_with_recall
        FROM active_patients ap
        LEFT JOIN raw_intermediate.int_recall_management irm 
            ON ap.patient_id = irm.patient_id
            AND irm.is_disabled = false
            AND irm.is_valid_recall = true
        """
        result5 = db.execute(text(query5)).fetchone()
        print(f"\n5. Alternative: Simply 'has recall program' = 'current':")
        print(f"   Total Active: {result5.total_active}")
        print(f"   Patients with Recall: {result5.patients_with_recall}")
        if result5.total_active > 0:
            pct = (result5.patients_with_recall / result5.total_active) * 100
            print(f"   Calculated %: {pct:.2f}%")
            print(f"   PBN Expected: 53.4% (would need {int(result5.total_active * 0.534)} patients)")
            
    finally:
        db.close()

def analyze_hygiene_metrics():
    """Analyze hygiene metrics - check date ranges and pre-appointment logic"""
    db = get_db_session()
    try:
        print("\n" + "="*60)
        print("ANALYZING: Hygiene Metrics (PBN: 180 patients, 81.1% pre-appt)")
        print("="*60)
        
        # Check different date ranges
        date_ranges = [
            ("Last 30 days", 30),
            ("Last 90 days", 90),
            ("Last 180 days", 180),
            ("Last 365 days", 365),
        ]
        
        for label, days in date_ranges:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            
            query = """
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
                        AND fa2.appointment_date > CURRENT_DATE
                )
            )
            SELECT 
                COUNT(DISTINCT hp.patient_id) as unique_patients,
                COUNT(DISTINCT pwna.patient_id) as reappointed_count,
                CASE 
                    WHEN COUNT(DISTINCT hp.patient_id) > 0 
                    THEN (COUNT(DISTINCT pwna.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                    ELSE 0
                END as pre_appt_percent
            FROM hygiene_appointments hp
            LEFT JOIN patients_with_next_appt pwna ON hp.patient_id = pwna.patient_id
            """
            result = db.execute(text(query), {"start_date": start_date, "end_date": end_date}).fetchone()
            print(f"\n{label} ({start_date} to {end_date}):")
            print(f"   Unique Patients: {result.unique_patients}")
            print(f"   Re-appointed: {result.reappointed_count}")
            print(f"   Pre-Appointment %: {result.pre_appt_percent:.2f}%")
            
        # Check if PBN counts appointments scheduled BEFORE hygiene (pre-appointment)
        print(f"\n" + "="*60)
        print("Checking: Maybe 'pre-appointment' means scheduled BEFORE hygiene?")
        print("="*60)
        end_date = date.today()
        start_date = end_date - timedelta(days=90)  # Try 90 days
        
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
        patients_with_pre_appt AS (
            SELECT DISTINCT hp.patient_id
            FROM hygiene_appointments hp
            WHERE EXISTS (
                SELECT 1 
                FROM raw_marts.fact_appointment fa2
                WHERE fa2.patient_id = hp.patient_id
                    AND fa2.appointment_date < hp.hygiene_date
                    AND fa2.appointment_date >= hp.hygiene_date - INTERVAL '30 days'
            )
        )
        SELECT 
            COUNT(DISTINCT hp.patient_id) as unique_patients,
            COUNT(DISTINCT pwa.patient_id) as pre_appt_count,
            CASE 
                WHEN COUNT(DISTINCT hp.patient_id) > 0 
                THEN (COUNT(DISTINCT pwa.patient_id)::numeric / COUNT(DISTINCT hp.patient_id)::numeric) * 100
                ELSE 0
            END as pre_appt_percent
        FROM hygiene_appointments hp
        LEFT JOIN patients_with_pre_appt pwa ON hp.patient_id = pwa.patient_id
        """
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"   Unique Patients: {result2.unique_patients}")
        print(f"   Pre-appt Count: {result2.pre_appt_count}")
        print(f"   Pre-Appointment %: {result2.pre_appt_percent:.2f}%")
            
    finally:
        db.close()

if __name__ == "__main__":
    analyze_recall_current()
    analyze_hygiene_metrics()

