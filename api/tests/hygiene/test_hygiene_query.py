"""
Test the hygiene retention query directly to debug why it's returning zeros
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import date, timedelta
import sys
import os

# Add api directory to path
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

def test_hygiene_appointments():
    """Test if hygiene appointments CTE works"""
    db = get_db_session()
    try:
        end_date = date.today()
        start_date = end_date - timedelta(days=365)
        
        query = """
        SELECT 
            COUNT(*) as appointment_count,
            COUNT(DISTINCT patient_id) as unique_patients,
            MIN(appointment_date) as earliest,
            MAX(appointment_date) as latest
        FROM raw_marts.fact_appointment fa
        WHERE (fa.is_hygiene_appointment = true 
               OR fa.hygienist_id IS NOT NULL)
            AND fa.appointment_date >= :start_date
            AND fa.appointment_date <= :end_date
        """
        
        result = db.execute(text(query), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nHygiene Appointments Test:")
        print(f"  Count: {result.appointment_count}")
        print(f"  Unique Patients: {result.unique_patients}")
        print(f"  Date Range: {result.earliest} to {result.latest}")
        
        # Test hygiene_patients CTE
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
        )
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT patient_id) as unique_patients
        FROM hygiene_appointments
        """
        
        result2 = db.execute(text(query2), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nHygiene Patients CTE Test:")
        print(f"  Total Rows: {result2.total_rows}")
        print(f"  Unique Patients: {result2.unique_patients}")
        
        # Test the full hygiene_patients_seen_calc
        query3 = """
        WITH hygiene_procedures AS (
            SELECT DISTINCT
                pc.patient_id,
                pc.procedure_date as hygiene_date,
                pc.appointment_id
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
            SELECT DISTINCT
                COALESCE(hp.patient_id, ha.patient_id) as patient_id,
                COALESCE(hp.hygiene_date, ha.hygiene_date) as hygiene_date
            FROM hygiene_procedures hp
            FULL OUTER JOIN hygiene_appointments ha 
                ON hp.patient_id = ha.patient_id
        ),
        hyg_patients_seen_calc AS (
            SELECT COALESCE(COUNT(DISTINCT patient_id)::integer, 0) as unique_patients
            FROM hygiene_patients
            UNION ALL
            SELECT 0
            WHERE NOT EXISTS (SELECT 1 FROM hygiene_patients)
            LIMIT 1
        )
        SELECT * FROM hyg_patients_seen_calc
        """
        
        result3 = db.execute(text(query3), {"start_date": start_date, "end_date": end_date}).fetchone()
        print(f"\nFull Hygiene Patients Seen Calc Test:")
        print(f"  Unique Patients: {result3.unique_patients}")
        
        # Test the FULL query from the service
        print(f"\n{'='*60}")
        print("Testing FULL Service Query")
        print(f"{'='*60}")
        
        # Read the actual query from the service file
        import inspect
        from services.hygiene_service import get_hygiene_retention_summary
        
        # Get the query from the function (we'll need to extract it)
        # For now, let's just call the service function
        result4 = get_hygiene_retention_summary(db, start_date, end_date)
        print(f"Service Function Result:")
        print(f"  Recall Current %: {result4.get('recall_current_percent', 0)}")
        print(f"  Hyg Pre-Appointment %: {result4.get('hyg_pre_appointment_percent', 0)}")
        print(f"  Hyg Patients Seen: {result4.get('hyg_patients_seen', 0)}")
        print(f"  Hyg Pts Re-appntd: {result4.get('hyg_pts_reappntd', 0)}")
        print(f"  Recall Overdue %: {result4.get('recall_overdue_percent', 0)}")
        print(f"  Not on Recall %: {result4.get('not_on_recall_percent', 0)}")
        
    finally:
        db.close()

if __name__ == "__main__":
    test_hygiene_appointments()

