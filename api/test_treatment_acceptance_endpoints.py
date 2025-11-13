# api/test_treatment_acceptance_endpoints.py
"""
Test script for Treatment Acceptance API endpoints

Usage:
    - API server must be running (run 'api-run' in another terminal)
    - requests library must be installed (run: pip install requests)
    - Run: python test_treatment_acceptance_endpoints.py
"""

import requests
import json
from datetime import date, timedelta
from typing import Optional

BASE_URL = "http://localhost:8000"

def test_api_health():
    """Test if API is running"""
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200:
            print("✅ API is running")
            return True
        else:
            print(f"❌ API returned status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ API server is not running!")
        print("   Start it with: api-run")
        return False

def test_kpi_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    provider_id: Optional[int] = None,
    clinic_id: Optional[int] = None
):
    """Test /treatment-acceptance/kpi-summary endpoint"""
    print("\n" + "="*60)
    print("Testing /treatment-acceptance/kpi-summary")
    print("="*60)
    
    params = {}
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date
    if provider_id is not None:
        params['provider_id'] = provider_id
    if clinic_id is not None:
        params['clinic_id'] = clinic_id
    
    try:
        response = requests.get(f"{BASE_URL}/treatment-acceptance/kpi-summary", params=params)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("\n✅ KPI Summary:")
            print(f"  - Patients Seen: {data.get('patients_seen', 0)}")
            print(f"  - Patients Presented: {data.get('patients_presented', 0)}")
            print(f"  - Patients Accepted: {data.get('patients_accepted', 0)}")
            print(f"  - Tx Acceptance Rate: {data.get('tx_acceptance_rate')}%")
            print(f"  - Patient Acceptance Rate: {data.get('patient_acceptance_rate')}%")
            print(f"  - Diagnosis Rate: {data.get('diagnosis_rate')}%")
            print(f"  - Tx Presented Amount: ${data.get('tx_presented_amount', 0):,.2f}")
            print(f"  - Tx Accepted Amount: ${data.get('tx_accepted_amount', 0):,.2f}")
            print(f"  - Same Day Treatment: ${data.get('same_day_treatment_amount', 0):,.2f}")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return False

def test_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    provider_id: Optional[int] = None,
    clinic_id: Optional[int] = None
):
    """Test /treatment-acceptance/summary endpoint"""
    print("\n" + "="*60)
    print("Testing /treatment-acceptance/summary")
    print("="*60)
    
    params = {}
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date
    if provider_id is not None:
        params['provider_id'] = provider_id
    if clinic_id is not None:
        params['clinic_id'] = clinic_id
    
    try:
        response = requests.get(f"{BASE_URL}/treatment-acceptance/summary", params=params)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Summary: {len(data)} records returned")
            if len(data) > 0:
                print(f"\nFirst record:")
                first = data[0]
                print(f"  - Date: {first.get('procedure_date')}")
                print(f"  - Provider ID: {first.get('provider_id')}")
                print(f"  - Clinic ID: {first.get('clinic_id')}")
                print(f"  - Patients Seen: {first.get('patients_seen', 0)}")
                print(f"  - Patients Presented: {first.get('patients_presented', 0)}")
                print(f"  - Tx Acceptance Rate: {first.get('tx_acceptance_rate')}%")
                
                if len(data) > 1:
                    print(f"\nLast record:")
                    last = data[-1]
                    print(f"  - Date: {last.get('procedure_date')}")
                    print(f"  - Patients Seen: {last.get('patients_seen', 0)}")
                    print(f"  - Patients Presented: {last.get('patients_presented', 0)}")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return False

def test_trends(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    provider_id: Optional[int] = None,
    clinic_id: Optional[int] = None,
    group_by: str = "month"
):
    """Test /treatment-acceptance/trends endpoint"""
    print("\n" + "="*60)
    print(f"Testing /treatment-acceptance/trends (group_by={group_by})")
    print("="*60)
    
    params = {'group_by': group_by}
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date
    if provider_id is not None:
        params['provider_id'] = provider_id
    if clinic_id is not None:
        params['clinic_id'] = clinic_id
    
    try:
        response = requests.get(f"{BASE_URL}/treatment-acceptance/trends", params=params)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Trends: {len(data)} records returned")
            if len(data) > 0:
                print(f"\nFirst {min(5, len(data))} records:")
                for i, record in enumerate(data[:5]):
                    print(f"  {i+1}. Date: {record.get('date')}")
                    print(f"     Tx Acceptance Rate: {record.get('tx_acceptance_rate')}%")
                    print(f"     Patient Acceptance Rate: {record.get('patient_acceptance_rate')}%")
                    print(f"     Tx Presented: ${record.get('tx_presented_amount', 0):,.2f}")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return False

def test_provider_performance(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    clinic_id: Optional[int] = None
):
    """Test /treatment-acceptance/provider-performance endpoint"""
    print("\n" + "="*60)
    print("Testing /treatment-acceptance/provider-performance")
    print("="*60)
    
    params = {}
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date
    if clinic_id is not None:
        params['clinic_id'] = clinic_id
    
    try:
        response = requests.get(f"{BASE_URL}/treatment-acceptance/provider-performance", params=params)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Provider Performance: {len(data)} providers returned")
            if len(data) > 0:
                print(f"\nFirst {min(5, len(data))} providers:")
                for i, provider in enumerate(data[:5]):
                    print(f"  {i+1}. {provider.get('provider_name')} (ID: {provider.get('provider_id')})")
                    print(f"     Tx Acceptance Rate: {provider.get('tx_acceptance_rate')}%")
                    print(f"     Patient Acceptance Rate: {provider.get('patient_acceptance_rate')}%")
                    print(f"     Tx Presented: ${provider.get('tx_presented_amount', 0):,.2f}")
                    print(f"     Patients Seen: {provider.get('patients_seen', 0)}")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return False

def run_all_tests():
    """Run all endpoint tests"""
    print("="*60)
    print("Treatment Acceptance API Endpoint Tests")
    print("="*60)
    
    # Test API health
    if not test_api_health():
        return
    
    # Get current month date range
    today = date.today()
    first_day_of_month = today.replace(day=1)
    last_day_of_month = (first_day_of_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    # Test 1: KPI Summary (current month)
    print(f"\n📅 Testing with date range: {first_day_of_month} to {last_day_of_month}")
    test_kpi_summary(
        start_date=str(first_day_of_month),
        end_date=str(last_day_of_month)
    )
    
    # Test 2: KPI Summary (all time, no filters)
    test_kpi_summary()
    
    # Test 3: Summary (current month)
    test_summary(
        start_date=str(first_day_of_month),
        end_date=str(last_day_of_month)
    )
    
    # Test 4: Trends (monthly, current month)
    test_trends(
        start_date=str(first_day_of_month),
        end_date=str(last_day_of_month),
        group_by="month"
    )
    
    # Test 5: Trends (daily, last 7 days)
    last_7_days = today - timedelta(days=7)
    test_trends(
        start_date=str(last_7_days),
        end_date=str(today),
        group_by="day"
    )
    
    # Test 6: Provider Performance (current month)
    test_provider_performance(
        start_date=str(first_day_of_month),
        end_date=str(last_day_of_month)
    )
    
    print("\n" + "="*60)
    print("✅ All tests completed!")
    print("="*60)

if __name__ == "__main__":
    run_all_tests()

