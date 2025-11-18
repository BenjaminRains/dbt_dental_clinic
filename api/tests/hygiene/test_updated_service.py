"""
Test the updated hygiene service with PBN logic
"""

import sys
import os
from datetime import date, timedelta

api_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, api_dir)

from services.hygiene_service import get_hygiene_retention_summary
from database import get_db

def test_updated_service():
    """Test the updated service with different date ranges"""
    db = next(get_db())
    
    print("="*70)
    print("TESTING UPDATED HYGIENE SERVICE (PBN Logic)")
    print("="*70)
    
    # Test different date ranges - focusing on full year 2025
    date_ranges = [
        ("Full Year 2025 (PBN Dashboard)", (date(2025, 1, 1), date(2025, 12, 31))),
        ("Year to Date 2025", (date(2025, 1, 1), date.today())),
        ("Last 12 months", (date.today() - timedelta(days=365), date.today())),
    ]
    
    for label, (start_date, end_date) in date_ranges:
        
        result = get_hygiene_retention_summary(db, start_date, end_date)
        
        print(f"\n{label} ({start_date} to {end_date}):")
        print(f"  Recall Current %: {result['recall_current_percent']:.2f}% (PBN: 53.4%)")
        print(f"  Hyg Pre-Appointment %: {result['hyg_pre_appointment_percent']:.2f}% (PBN: 50.7%)")
        print(f"  Hyg Patients Seen: {result['hyg_patients_seen']} (PBN: 2073)")
        print(f"  Hyg Pts Re-appntd: {result['hyg_pts_reappntd']} (PBN: 1051)")
        print(f"  Recall Overdue %: {result['recall_overdue_percent']:.2f}% (PBN: 25.6%)")
        print(f"  Not on Recall %: {result['not_on_recall_percent']:.2f}% (PBN: 20%)")

if __name__ == "__main__":
    test_updated_service()

