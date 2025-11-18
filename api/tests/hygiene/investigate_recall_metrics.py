"""
Deep dive into recall metrics to match PBN calculations
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
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

def investigate_recall_overdue():
    """Investigate Recall Overdue % calculation"""
    db = get_db_session()
    try:
        print("="*70)
        print("INVESTIGATING: Recall Overdue % (PBN: 25.6%, Ours: 99.70%)")
        print("="*70)
        
        # Check compliance_status distribution for active recall patients
        query1 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        active_recall_patients AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        )
        SELECT 
            irm.compliance_status,
            COUNT(DISTINCT arp.patient_id) as patient_count,
            COUNT(*) as recall_record_count
        FROM active_recall_patients arp
        INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
        WHERE irm.is_disabled = false
            AND irm.is_valid_recall = true
        GROUP BY irm.compliance_status
        ORDER BY patient_count DESC
        """
        result1 = db.execute(text(query1)).fetchall()
        print("\n1. Compliance Status Distribution for Active Recall Patients:")
        total = 0
        for row in result1:
            print(f"   {row.compliance_status}: {row.patient_count} patients ({row.recall_record_count} records)")
            total += row.patient_count
        print(f"   Total: {total} patients")
        
        # Check if PBN uses a grace period (e.g., not overdue if within 30 days)
        query2 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        active_recall_patients AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        overdue_with_grace AS (
            SELECT DISTINCT arp.patient_id
            FROM active_recall_patients arp
            INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
                AND irm.date_due < CURRENT_DATE
                AND irm.date_due < CURRENT_DATE - INTERVAL '30 days'  -- Grace period: only overdue if 30+ days past due
                AND irm.recall_status_description != 'Scheduled'
                AND irm.recall_status_description != 'Completed'
        )
        SELECT 
            COUNT(DISTINCT arp.patient_id) as total_recall,
            COUNT(DISTINCT owg.patient_id) as overdue_30day_grace,
            CASE 
                WHEN COUNT(DISTINCT arp.patient_id) > 0 
                THEN (COUNT(DISTINCT owg.patient_id)::numeric / COUNT(DISTINCT arp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_recall_patients arp
        LEFT JOIN overdue_with_grace owg ON arp.patient_id = owg.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"\n2. Overdue with 30-day grace period: {result2.percent:.2f}% ({result2.overdue_30day_grace}/{result2.total_recall})")
        
        # Check days overdue distribution
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        active_recall_patients AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        recall_with_category AS (
            SELECT 
                arp.patient_id,
                CASE 
                    WHEN irm.days_overdue IS NULL THEN 'No due date'
                    WHEN irm.days_overdue < 0 THEN 'Future due date'
                    WHEN irm.days_overdue = 0 THEN 'Due today'
                    WHEN irm.days_overdue <= 30 THEN '1-30 days overdue'
                    WHEN irm.days_overdue <= 60 THEN '31-60 days overdue'
                    WHEN irm.days_overdue <= 90 THEN '61-90 days overdue'
                    WHEN irm.days_overdue <= 180 THEN '91-180 days overdue'
                    ELSE '180+ days overdue'
                END as overdue_category,
                CASE 
                    WHEN irm.days_overdue IS NULL THEN 8
                    WHEN irm.days_overdue < 0 THEN 1
                    WHEN irm.days_overdue = 0 THEN 2
                    WHEN irm.days_overdue <= 30 THEN 3
                    WHEN irm.days_overdue <= 60 THEN 4
                    WHEN irm.days_overdue <= 90 THEN 5
                    WHEN irm.days_overdue <= 180 THEN 6
                    ELSE 7
                END as sort_order
            FROM active_recall_patients arp
            INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        )
        SELECT 
            overdue_category,
            COUNT(DISTINCT patient_id) as patient_count
        FROM recall_with_category
        GROUP BY overdue_category, sort_order
        ORDER BY sort_order
        """
        result3 = db.execute(text(query3)).fetchall()
        print(f"\n3. Days Overdue Distribution:")
        for row in result3:
            print(f"   {row.overdue_category}: {row.patient_count} patients")
            
    finally:
        db.close()

def investigate_not_on_recall():
    """Investigate Not on Recall % calculation"""
    db = get_db_session()
    try:
        print("\n" + "="*70)
        print("INVESTIGATING: Not on Recall % (PBN: 20%, Ours: 90.42%)")
        print("="*70)
        
        # Check if PBN uses only active patients as denominator
        query1 = """
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
            COUNT(DISTINCT ap_all.patient_id) as all_patients,
            COUNT(DISTINCT ap_active.patient_id) as active_patients,
            COUNT(DISTINCT pwr.patient_id) as patients_with_recall,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap_all.patient_id END) as not_on_recall_all,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap_active.patient_id END) as not_on_recall_active,
            CASE 
                WHEN COUNT(DISTINCT ap_all.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap_all.patient_id END)::numeric / COUNT(DISTINCT ap_all.patient_id)::numeric) * 100
                ELSE 0
            END as percent_all,
            CASE 
                WHEN COUNT(DISTINCT ap_active.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap_active.patient_id END)::numeric / COUNT(DISTINCT ap_active.patient_id)::numeric) * 100
                ELSE 0
            END as percent_active
        FROM all_patients ap_all
        LEFT JOIN active_patients ap_active ON ap_all.patient_id = ap_active.patient_id
        LEFT JOIN patients_with_recall pwr ON ap_all.patient_id = pwr.patient_id
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"\n1. Not on Recall % by denominator:")
        print(f"   All patients: {result1.percent_all:.2f}% ({result1.not_on_recall_all}/{result1.all_patients})")
        print(f"   Active patients only: {result1.percent_active:.2f}% ({result1.not_on_recall_active}/{result1.active_patients})")
        
        # Check if PBN excludes inactive patients from denominator
        query2 = """
        WITH active_patients_only AS (
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
            COUNT(DISTINCT apo.patient_id) as total_active,
            COUNT(DISTINCT pwr.patient_id) as with_recall,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN apo.patient_id END) as not_on_recall,
            CASE 
                WHEN COUNT(DISTINCT apo.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN apo.patient_id END)::numeric / COUNT(DISTINCT apo.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients_only apo
        LEFT JOIN patients_with_recall pwr ON apo.patient_id = pwr.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"\n2. Not on Recall % (active patients only, simpler query):")
        print(f"   {result2.percent:.2f}% ({result2.not_on_recall}/{result2.total_active})")
        
        # Test if PBN includes patients with recent appointments as "on recall"
        print(f"\n3. Testing if 'on recall' includes patients with recent appointments:")
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
        patients_with_recent_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= CURRENT_DATE - INTERVAL '6 months'
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_patients,
            COUNT(DISTINCT pwr.patient_id) as with_recall,
            COUNT(DISTINCT pwa.patient_id) as with_recent_appt,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pwa.patient_id IS NOT NULL THEN ap.patient_id END) as on_recall_or_recent,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pwa.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pwa.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        LEFT JOIN patients_with_recent_appt pwa ON ap.patient_id = pwa.patient_id
        """
        result3 = db.execute(text(query3)).fetchone()
        print(f"   Total Patients: {result3.total_patients}")
        print(f"   With Recall: {result3.with_recall}")
        print(f"   With Recent Appt (6mo): {result3.with_recent_appt}")
        print(f"   On Recall OR Recent: {result3.on_recall_or_recent}")
        print(f"   Not on Recall: {result3.not_on_recall}")
        print(f"   Not on Recall %: {result3.percent:.2f}%")
        
        # Test with different time windows for recent appointments
        for months in [3, 6, 12, 18]:
            query4 = f"""
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
            patients_with_recent_appt AS (
                SELECT DISTINCT ap.patient_id
                FROM all_patients ap
                WHERE EXISTS (
                    SELECT 1
                    FROM raw_marts.fact_appointment fa
                    WHERE fa.patient_id = ap.patient_id
                        AND fa.is_completed = true
                        AND fa.appointment_date >= CURRENT_DATE - INTERVAL '{months} months'
                )
            )
            SELECT 
                COUNT(DISTINCT ap.patient_id) as total_patients,
                COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pwa.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
                CASE 
                    WHEN COUNT(DISTINCT ap.patient_id) > 0 
                    THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pwa.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM all_patients ap
            LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
            LEFT JOIN patients_with_recent_appt pwa ON ap.patient_id = pwa.patient_id
            """
            result4 = db.execute(text(query4)).fetchone()
            print(f"   Not on Recall % (recall OR {months}mo recent appt): {result4.percent:.2f}% ({result4.not_on_recall}/{result4.total_patients})")
        
        # Test if including scheduled appointments helps
        print(f"\n4. Testing if 'on recall' includes patients with scheduled appointments:")
        query5 = """
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
        patients_with_scheduled_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.appointment_date > CURRENT_DATE
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_patients,
            COUNT(DISTINCT pwr.patient_id) as with_recall,
            COUNT(DISTINCT pws.patient_id) as with_scheduled,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pws.patient_id IS NOT NULL THEN ap.patient_id END) as on_recall_or_scheduled,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        LEFT JOIN patients_with_scheduled_appt pws ON ap.patient_id = pws.patient_id
        """
        result5 = db.execute(text(query5)).fetchone()
        print(f"   Total Patients: {result5.total_patients}")
        print(f"   With Recall: {result5.with_recall}")
        print(f"   With Scheduled Appt: {result5.with_scheduled}")
        print(f"   On Recall OR Scheduled: {result5.on_recall_or_scheduled}")
        print(f"   Not on Recall: {result5.not_on_recall}")
        print(f"   Not on Recall %: {result5.percent:.2f}%")
        
        # Test combination: recall OR scheduled OR recent appointment
        print(f"\n5. Testing 'on recall' = recall OR scheduled OR recent (6mo):")
        query6 = """
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
        patients_with_scheduled_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.appointment_date > CURRENT_DATE
            )
        ),
        patients_with_recent_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= CURRENT_DATE - INTERVAL '6 months'
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_patients,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pws.patient_id IS NOT NULL OR pwa.patient_id IS NOT NULL THEN ap.patient_id END) as on_recall,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL AND pwa.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL AND pwa.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        LEFT JOIN patients_with_scheduled_appt pws ON ap.patient_id = pws.patient_id
        LEFT JOIN patients_with_recent_appt pwa ON ap.patient_id = pwa.patient_id
        """
        result6 = db.execute(text(query6)).fetchone()
        print(f"   Total Patients: {result6.total_patients}")
        print(f"   On Recall (recall OR scheduled OR recent): {result6.on_recall}")
        print(f"   Not on Recall: {result6.not_on_recall}")
        print(f"   Not on Recall %: {result6.percent:.2f}%")
        
        # Test if PBN considers patients "on recall" if seen within recall interval (6, 12, 18 months)
        print(f"\n6. Testing if 'on recall' = seen within recall interval (no formal recall needed):")
        for months in [6, 12, 18, 24]:
            query7 = f"""
            WITH all_patients AS (
                SELECT DISTINCT p.patient_id
                FROM raw_marts.dim_patient p
                WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
            ),
            patients_seen_within_interval AS (
                SELECT DISTINCT ap.patient_id
                FROM all_patients ap
                WHERE EXISTS (
                    SELECT 1
                    FROM raw_marts.fact_appointment fa
                    WHERE fa.patient_id = ap.patient_id
                        AND fa.is_completed = true
                        AND fa.appointment_date >= CURRENT_DATE - INTERVAL '{months} months'
                )
            )
            SELECT 
                COUNT(DISTINCT ap.patient_id) as total_patients,
                COUNT(DISTINCT pswi.patient_id) as seen_within_interval,
                COUNT(DISTINCT CASE WHEN pswi.patient_id IS NULL THEN ap.patient_id END) as not_seen_within_interval,
                CASE 
                    WHEN COUNT(DISTINCT ap.patient_id) > 0 
                    THEN (COUNT(DISTINCT CASE WHEN pswi.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM all_patients ap
            LEFT JOIN patients_seen_within_interval pswi ON ap.patient_id = pswi.patient_id
            """
            result7 = db.execute(text(query7)).fetchone()
            print(f"   Not seen within {months} months: {result7.percent:.2f}% ({result7.not_seen_within_interval}/{result7.total_patients})")
        
        # Test if PBN uses "active patients" definition (seen in 18 months) as "on recall"
        print(f"\n7. Testing if 'on recall' = seen within 18 months (active patients definition):")
        
        # Test with is_completed = true
        query8a = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_seen_18mo AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_patients,
            COUNT(DISTINCT ps18.patient_id) as seen_18mo,
            COUNT(DISTINCT CASE WHEN ps18.patient_id IS NULL THEN ap.patient_id END) as not_seen_18mo,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN ps18.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM all_patients ap
        LEFT JOIN patients_seen_18mo ps18 ON ap.patient_id = ps18.patient_id
        """
        result8a = db.execute(text(query8a)).fetchone()
        print(f"   a) With is_completed = true:")
        print(f"      Total Patients: {result8a.total_patients}")
        print(f"      Seen within 18 months: {result8a.seen_18mo}")
        print(f"      Not on Recall %: {result8a.percent:.2f}%")
        
        # Test without is_completed filter (any appointment)
        query8b = """
        WITH all_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            WHERE p.patient_status IN ('Patient', 'Active', 'Inactive')
        ),
        patients_seen_18mo AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_patients,
            COUNT(DISTINCT ps18.patient_id) as seen_18mo,
            COUNT(DISTINCT CASE WHEN ps18.patient_id IS NULL THEN ap.patient_id END) as not_seen_18mo,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN ps18.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM all_patients ap
        LEFT JOIN patients_seen_18mo ps18 ON ap.patient_id = ps18.patient_id
        """
        result8b = db.execute(text(query8b)).fetchone()
        print(f"   b) Without is_completed filter (any appointment):")
        print(f"      Total Patients: {result8b.total_patients}")
        print(f"      Seen within 18 months: {result8b.seen_18mo}")
        print(f"      Not on Recall %: {result8b.percent:.2f}%")
        print(f"      PBN Expected: 20% (would need {int(result8b.total_patients * 0.20)} patients)")
        
        # Test if PBN uses ACTIVE patients only as denominator (not all patients)
        print(f"\n8. Testing if PBN uses ACTIVE patients only as denominator:")
        query9 = """
        WITH active_patients AS (
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
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT pwr.patient_id) as with_recall,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        """
        result9 = db.execute(text(query9)).fetchone()
        print(f"   Total Active Patients: {result9.total_active}")
        print(f"   With Recall: {result9.with_recall}")
        print(f"   Not on Recall: {result9.not_on_recall}")
        print(f"   Not on Recall % (active patients only): {result9.percent:.2f}%")
        print(f"   PBN Expected: 20% (would need {int(result9.total_active * 0.20)} patients)")
        
        # Test if PBN considers patients "on recall" if seen within recall interval
        print(f"\n9. Testing if 'on recall' = recall program OR seen within interval (active patients only):")
        for months in [6, 12, 18]:
            query10 = f"""
            WITH active_patients AS (
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
            ),
            patients_seen_within_interval AS (
                SELECT DISTINCT ap.patient_id
                FROM active_patients ap
                WHERE EXISTS (
                    SELECT 1
                    FROM raw_marts.fact_appointment fa
                    WHERE fa.patient_id = ap.patient_id
                        AND fa.is_completed = true
                        AND fa.appointment_date >= CURRENT_DATE - INTERVAL '{months} months'
                )
            )
            SELECT 
                COUNT(DISTINCT ap.patient_id) as total_active,
                COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR psi.patient_id IS NOT NULL THEN ap.patient_id END) as on_recall,
                COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND psi.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
                CASE 
                    WHEN COUNT(DISTINCT ap.patient_id) > 0 
                    THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND psi.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM active_patients ap
            LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
            LEFT JOIN patients_seen_within_interval psi ON ap.patient_id = psi.patient_id
            """
            result10 = db.execute(text(query10)).fetchone()
            print(f"   Recall OR seen within {months}mo: {result10.percent:.2f}% not on recall ({result10.not_on_recall}/{result10.total_active})")
            print(f"      -> {100 - result10.percent:.2f}% on recall ({result10.on_recall}/{result10.total_active})")
        
        # Test if PBN includes scheduled appointments as "on recall"
        print(f"\n10. Testing if 'on recall' = recall OR scheduled OR seen (active patients only):")
        query11 = """
        WITH active_patients AS (
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
        ),
        patients_with_scheduled AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.appointment_date > CURRENT_DATE
            )
        ),
        patients_seen_6mo AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= CURRENT_DATE - INTERVAL '6 months'
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pws.patient_id IS NOT NULL OR ps6.patient_id IS NOT NULL THEN ap.patient_id END) as on_recall,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL AND ps6.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL AND ps6.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        LEFT JOIN patients_with_scheduled pws ON ap.patient_id = pws.patient_id
        LEFT JOIN patients_seen_6mo ps6 ON ap.patient_id = ps6.patient_id
        """
        result11 = db.execute(text(query11)).fetchone()
        print(f"   Total Active: {result11.total_active}")
        print(f"   On Recall (recall OR scheduled OR seen 6mo): {result11.on_recall}")
        print(f"   Not on Recall: {result11.not_on_recall}")
        print(f"   Not on Recall %: {result11.percent:.2f}%")
        print(f"   PBN Expected: 20% (would need {int(result11.total_active * 0.20)} patients)")
        
        # Test if PBN uses ALL patients as denominator (not just active)
        print(f"\n11. Testing if PBN uses ALL patients as denominator (recall OR scheduled OR seen 6mo):")
        query12 = """
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
        patients_with_scheduled AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.appointment_date > CURRENT_DATE
            )
        ),
        patients_seen_6mo AS (
            SELECT DISTINCT ap.patient_id
            FROM all_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= CURRENT_DATE - INTERVAL '6 months'
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_patients,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pws.patient_id IS NOT NULL OR ps6.patient_id IS NOT NULL THEN ap.patient_id END) as on_recall,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL AND ps6.patient_id IS NULL THEN ap.patient_id END) as not_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NULL AND pws.patient_id IS NULL AND ps6.patient_id IS NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM all_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        LEFT JOIN patients_with_scheduled pws ON ap.patient_id = pws.patient_id
        LEFT JOIN patients_seen_6mo ps6 ON ap.patient_id = ps6.patient_id
        """
        result12 = db.execute(text(query12)).fetchone()
        print(f"   Total Patients (ALL): {result12.total_patients}")
        print(f"   On Recall (recall OR scheduled OR seen 6mo): {result12.on_recall}")
        print(f"   Not on Recall: {result12.not_on_recall}")
        print(f"   Not on Recall %: {result12.percent:.2f}%")
        print(f"   PBN Expected: 20% (would need {int(result12.total_patients * 0.20)} patients)")
        
    finally:
        db.close()

def investigate_recall_current():
    """Investigate Recall Current % calculation"""
    db = get_db_session()
    try:
        print("\n" + "="*70)
        print("INVESTIGATING: Recall Current % (PBN: 53.4%, Ours: 42.97%)")
        print("="*70)
        
        # Check if including patients with recent completed appointments helps
        query1 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT irm.patient_id
            FROM raw_intermediate.int_recall_management irm
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        patients_with_recent_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.is_completed = true
                    AND fa.appointment_date >= CURRENT_DATE - INTERVAL '6 months'
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT pwr.patient_id) as with_recall,
            COUNT(DISTINCT pwa.patient_id) as with_recent_appt,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pwa.patient_id IS NOT NULL THEN ap.patient_id END) as current_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pwa.patient_id IS NOT NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        LEFT JOIN patients_with_recent_appt pwa ON ap.patient_id = pwa.patient_id
        """
        result1 = db.execute(text(query1)).fetchone()
        print(f"\n1. Recall Current % with recent appointments:")
        print(f"   Total Active: {result1.total_active}")
        print(f"   With Recall: {result1.with_recent_appt}")
        print(f"   With Recent Appt (6mo): {result1.with_recent_appt}")
        print(f"   Current (recall OR recent appt): {result1.current_on_recall}")
        print(f"   Percent: {result1.percent:.2f}%")
        
        # Test if including scheduled appointments helps
        query2 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        patients_with_recall AS (
            SELECT DISTINCT irm.patient_id
            FROM raw_intermediate.int_recall_management irm
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        patients_with_scheduled_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.appointment_date > CURRENT_DATE
            )
        )
        SELECT 
            COUNT(DISTINCT ap.patient_id) as total_active,
            COUNT(DISTINCT pwr.patient_id) as with_recall,
            COUNT(DISTINCT pws.patient_id) as with_scheduled,
            COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pws.patient_id IS NOT NULL THEN ap.patient_id END) as current_on_recall,
            CASE 
                WHEN COUNT(DISTINCT ap.patient_id) > 0 
                THEN (COUNT(DISTINCT CASE WHEN pwr.patient_id IS NOT NULL OR pws.patient_id IS NOT NULL THEN ap.patient_id END)::numeric / COUNT(DISTINCT ap.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_patients ap
        LEFT JOIN patients_with_recall pwr ON ap.patient_id = pwr.patient_id
        LEFT JOIN patients_with_scheduled_appt pws ON ap.patient_id = pws.patient_id
        """
        result2 = db.execute(text(query2)).fetchone()
        print(f"\n2. Recall Current % (recall OR scheduled appt):")
        print(f"   Total Active: {result2.total_active}")
        print(f"   With Recall: {result2.with_recall}")
        print(f"   With Scheduled Appt: {result2.with_scheduled}")
        print(f"   Current (recall OR scheduled): {result2.current_on_recall}")
        print(f"   Percent: {result2.percent:.2f}%")
        
        # Test Recall Overdue with different thresholds
        print(f"\n3. Testing Recall Overdue % with different thresholds:")
        query3 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        active_recall_patients AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        )
        SELECT 
            COUNT(DISTINCT arp.patient_id) as total_recall
        FROM active_recall_patients arp
        """
        result3 = db.execute(text(query3)).fetchone()
        total_recall = result3.total_recall
        
        for threshold in [0, 30, 60, 90]:
            query4 = f"""
            WITH active_patients AS (
                SELECT DISTINCT p.patient_id
                FROM raw_marts.dim_patient p
                INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
                WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                    AND p.patient_status IN ('Patient', 'Active')
            ),
            active_recall_patients AS (
                SELECT DISTINCT ap.patient_id
                FROM active_patients ap
                INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
                WHERE irm.is_disabled = false
                    AND irm.is_valid_recall = true
            ),
            overdue_patients AS (
                SELECT DISTINCT arp.patient_id
                FROM active_recall_patients arp
                INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
                WHERE irm.is_disabled = false
                    AND irm.is_valid_recall = true
                    AND irm.compliance_status = 'Overdue'
                    AND irm.days_overdue >= {threshold}
            )
            SELECT 
                COUNT(DISTINCT op.patient_id) as overdue_count
            FROM active_recall_patients arp
            LEFT JOIN overdue_patients op ON arp.patient_id = op.patient_id
            """
            result4 = db.execute(text(query4)).fetchone()
            pct = (result4.overdue_count / total_recall) * 100 if total_recall > 0 else 0
            print(f"   {threshold}+ days overdue: {pct:.2f}% ({result4.overdue_count}/{total_recall})")
        
        # Test if excluding patients seen recently helps
        print(f"\n4. Testing Recall Overdue % excluding patients seen recently:")
        for months in [3, 6, 12]:
            query5 = f"""
            WITH active_patients AS (
                SELECT DISTINCT p.patient_id
                FROM raw_marts.dim_patient p
                INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
                WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                    AND p.patient_status IN ('Patient', 'Active')
            ),
            active_recall_patients AS (
                SELECT DISTINCT ap.patient_id
                FROM active_patients ap
                INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
                WHERE irm.is_disabled = false
                    AND irm.is_valid_recall = true
            ),
            patients_seen_recently AS (
                SELECT DISTINCT ap.patient_id
                FROM active_patients ap
                WHERE EXISTS (
                    SELECT 1
                    FROM raw_marts.fact_appointment fa
                    WHERE fa.patient_id = ap.patient_id
                        AND fa.is_completed = true
                        AND fa.appointment_date >= CURRENT_DATE - INTERVAL '{months} months'
                )
            ),
            overdue_excluding_recent AS (
                SELECT DISTINCT arp.patient_id
                FROM active_recall_patients arp
                INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
                WHERE irm.is_disabled = false
                    AND irm.is_valid_recall = true
                    AND irm.compliance_status = 'Overdue'
                    AND NOT EXISTS (
                        SELECT 1 FROM patients_seen_recently psr 
                        WHERE psr.patient_id = arp.patient_id
                    )
            )
            SELECT 
                COUNT(DISTINCT arp.patient_id) as total_recall,
                COUNT(DISTINCT oer.patient_id) as overdue_excluding_recent,
                CASE 
                    WHEN COUNT(DISTINCT arp.patient_id) > 0 
                    THEN (COUNT(DISTINCT oer.patient_id)::numeric / COUNT(DISTINCT arp.patient_id)::numeric) * 100
                    ELSE 0
                END as percent
            FROM active_recall_patients arp
            LEFT JOIN overdue_excluding_recent oer ON arp.patient_id = oer.patient_id
            """
            result5 = db.execute(text(query5)).fetchone()
            print(f"   Excluding patients seen in last {months} months: {result5.percent:.2f}% ({result5.overdue_excluding_recent}/{result5.total_recall})")
        
        # Test if excluding patients with scheduled appointments helps
        print(f"\n5. Testing Recall Overdue % excluding patients with scheduled appointments:")
        query6 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        active_recall_patients AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        patients_with_scheduled_appt AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            WHERE EXISTS (
                SELECT 1
                FROM raw_marts.fact_appointment fa
                WHERE fa.patient_id = ap.patient_id
                    AND fa.appointment_date > CURRENT_DATE
            )
        ),
        overdue_excluding_scheduled AS (
            SELECT DISTINCT arp.patient_id
            FROM active_recall_patients arp
            INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
                AND irm.compliance_status = 'Overdue'
                AND NOT EXISTS (
                    SELECT 1 FROM patients_with_scheduled_appt psa 
                    WHERE psa.patient_id = arp.patient_id
                )
        )
        SELECT 
            COUNT(DISTINCT arp.patient_id) as total_recall,
            COUNT(DISTINCT oes.patient_id) as overdue_excluding_scheduled,
            CASE 
                WHEN COUNT(DISTINCT arp.patient_id) > 0 
                THEN (COUNT(DISTINCT oes.patient_id)::numeric / COUNT(DISTINCT arp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_recall_patients arp
        LEFT JOIN overdue_excluding_scheduled oes ON arp.patient_id = oes.patient_id
        """
        result6 = db.execute(text(query6)).fetchone()
        print(f"   Excluding patients with scheduled appointments: {result6.percent:.2f}% ({result6.overdue_excluding_scheduled}/{result6.total_recall})")
        
        # Test combination: overdue AND not scheduled AND not seen recently
        print(f"\n6. Testing Recall Overdue % (overdue AND not scheduled AND not seen in 6mo):")
        query7 = """
        WITH active_patients AS (
            SELECT DISTINCT p.patient_id
            FROM raw_marts.dim_patient p
            INNER JOIN raw_marts.fact_appointment fa ON p.patient_id = fa.patient_id
            WHERE fa.appointment_date >= CURRENT_DATE - INTERVAL '18 months'
                AND p.patient_status IN ('Patient', 'Active')
        ),
        active_recall_patients AS (
            SELECT DISTINCT ap.patient_id
            FROM active_patients ap
            INNER JOIN raw_intermediate.int_recall_management irm ON ap.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
        ),
        overdue_excluding_scheduled_and_recent AS (
            SELECT DISTINCT arp.patient_id
            FROM active_recall_patients arp
            INNER JOIN raw_intermediate.int_recall_management irm ON arp.patient_id = irm.patient_id
            WHERE irm.is_disabled = false
                AND irm.is_valid_recall = true
                AND irm.compliance_status = 'Overdue'
                AND NOT EXISTS (
                    SELECT 1
                    FROM raw_marts.fact_appointment fa
                    WHERE fa.patient_id = arp.patient_id
                        AND (fa.appointment_date > CURRENT_DATE  -- No scheduled appointments
                             OR (fa.is_completed = true AND fa.appointment_date >= CURRENT_DATE - INTERVAL '6 months'))  -- Not seen recently
                )
        )
        SELECT 
            COUNT(DISTINCT arp.patient_id) as total_recall,
            COUNT(DISTINCT oesr.patient_id) as overdue_count,
            CASE 
                WHEN COUNT(DISTINCT arp.patient_id) > 0 
                THEN (COUNT(DISTINCT oesr.patient_id)::numeric / COUNT(DISTINCT arp.patient_id)::numeric) * 100
                ELSE 0
            END as percent
        FROM active_recall_patients arp
        LEFT JOIN overdue_excluding_scheduled_and_recent oesr ON arp.patient_id = oesr.patient_id
        """
        result7 = db.execute(text(query7)).fetchone()
        print(f"   Overdue AND not scheduled AND not seen in 6mo: {result7.percent:.2f}% ({result7.overdue_count}/{result7.total_recall})")
        
    finally:
        db.close()

if __name__ == "__main__":
    investigate_recall_overdue()
    investigate_not_on_recall()
    investigate_recall_current()

