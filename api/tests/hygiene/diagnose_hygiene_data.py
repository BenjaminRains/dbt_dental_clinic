"""
Diagnostic script to check if data exists for hygiene retention queries

This script helps identify why the hygiene retention endpoint might be returning zeros.
It checks:
1. If active patients exist
2. If hygiene procedures exist
3. If recall data exists
4. If appointments exist

Usage:
    python diagnose_hygiene_data.py
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

def run_diagnostic_query(name: str, query: str, db):
    """Run a diagnostic query and print results"""
    print(f"\n{'='*60}")
    print(f"Diagnostic: {name}")
    print(f"{'='*60}")
    try:
        result = db.execute(text(query)).fetchone()
        if result:
            print(f"✅ Query executed successfully")
            print(f"Result: {dict(result._mapping)}")
            return result
        else:
            print(f"⚠️  Query returned no rows")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

def main():
    """Run all diagnostic queries"""
    print("="*60)
    print("Hygiene Retention Data Diagnostic")
    print("="*60)
    
    db = get_db_session()
    
    try:
        # 1. Check if active patients exist (visited in past 18 months)
        query1 = """
        SELECT 
            COUNT(DISTINCT p.patient_id) as active_patient_count
        FROM raw_marts.dim_patient p
        INNER JOIN raw_marts.fact_appointment fa
            ON p.patient_id = fa.patient_id
        WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
            AND p.patient_status IN ('Patient', 'Active')
        """
        run_diagnostic_query("Active Patients (past 18 months)", query1, db)
        
        # 2. Check if hygiene procedures exist
        query2 = """
        SELECT 
            COUNT(*) as hygiene_procedure_count,
            COUNT(DISTINCT patient_id) as unique_patients,
            MIN(procedure_date) as earliest_date,
            MAX(procedure_date) as latest_date
        FROM raw_intermediate.int_procedure_complete
        WHERE is_hygiene = true
        """
        run_diagnostic_query("Hygiene Procedures (all time)", query2, db)
        
        # 3. Check hygiene procedures in last 12 months
        query3 = """
        SELECT 
            COUNT(*) as hygiene_procedure_count,
            COUNT(DISTINCT patient_id) as unique_patients,
            MIN(procedure_date) as earliest_date,
            MAX(procedure_date) as latest_date
        FROM raw_intermediate.int_procedure_complete
        WHERE is_hygiene = true
            AND procedure_date >= CURRENT_DATE - INTERVAL '12 months'
            AND procedure_date <= CURRENT_DATE
        """
        run_diagnostic_query("Hygiene Procedures (last 12 months)", query3, db)
        
        # 4. Check hygiene appointments
        query4 = """
        SELECT 
            COUNT(*) as hygiene_appointment_count,
            COUNT(DISTINCT patient_id) as unique_patients,
            MIN(appointment_date) as earliest_date,
            MAX(appointment_date) as latest_date
        FROM raw_marts.fact_appointment
        WHERE (is_hygiene_appointment = true 
               OR hygienist_id IS NOT NULL)
            AND appointment_date >= CURRENT_DATE - INTERVAL '12 months'
            AND appointment_date <= CURRENT_DATE
        """
        run_diagnostic_query("Hygiene Appointments (last 12 months)", query4, db)
        
        # 5. Check recall management data
        query5 = """
        SELECT 
            COUNT(*) as recall_count,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(CASE WHEN is_disabled = false AND is_valid_recall = true THEN 1 END) as active_recall_count,
            COUNT(CASE WHEN is_overdue = true THEN 1 END) as overdue_count
        FROM raw_intermediate.int_recall_management
        """
        run_diagnostic_query("Recall Management Data", query5, db)
        
        # 6. Check if patients have future appointments
        query6 = """
        SELECT 
            COUNT(DISTINCT patient_id) as patients_with_future_appts
        FROM raw_marts.fact_appointment
        WHERE appointment_date > CURRENT_DATE
        """
        run_diagnostic_query("Patients with Future Appointments", query6, db)
        
        # 7. Check all patients count
        query7 = """
        SELECT 
            COUNT(DISTINCT patient_id) as total_patients
        FROM raw_marts.dim_patient
        WHERE patient_status IN ('Patient', 'Active', 'Inactive')
        """
        run_diagnostic_query("Total Patients (all statuses)", query7, db)
        
        # 8. Check schema/table existence
        query8 = """
        SELECT 
            table_schema,
            table_name
        FROM information_schema.tables
        WHERE table_schema IN ('raw_intermediate', 'raw_marts')
            AND table_name IN ('int_procedure_complete', 'int_recall_management', 'fact_appointment', 'dim_patient')
        ORDER BY table_schema, table_name
        """
        print(f"\n{'='*60}")
        print(f"Diagnostic: Table Existence Check")
        print(f"{'='*60}")
        try:
            result = db.execute(text(query8)).fetchall()
            if result:
                print(f"✅ Found {len(result)} tables:")
                for row in result:
                    print(f"  - {row.table_schema}.{row.table_name}")
            else:
                print(f"⚠️  No tables found - check schema names")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print(f"\n{'='*60}")
        print("Diagnostic Complete")
        print(f"{'='*60}")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()

