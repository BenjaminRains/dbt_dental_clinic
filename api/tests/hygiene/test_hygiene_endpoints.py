"""
Quick test script for Hygiene Retention Dashboard endpoints

Prerequisites:
    - API server must be running (run 'api-run' in another terminal)
    - requests library must be installed (run: pip install requests)

Usage:
    python test_hygiene_endpoints.py
"""

import requests
import json
from datetime import date, timedelta
from typing import Optional

BASE_URL = "http://localhost:8000"

def test_endpoint(name: str, url: str, params: Optional[dict] = None):
    """Test an endpoint and print results"""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    if params:
        print(f"Params: {params}")
    print(f"{'='*60}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✅ Status: {response.status_code}")
        
        # Pretty print response
        if isinstance(data, list):
            print(f"Response: Array with {len(data)} items")
            if len(data) > 0:
                print(f"First item: {json.dumps(data[0], indent=2, default=str)}")
        else:
            print(f"Response: {json.dumps(data, indent=2, default=str)}")
        
        return data
    except requests.exceptions.ConnectionError:
        print(f"❌ Error: Could not connect to {url}")
        print("   Make sure the API server is running (api-run)")
        return None
    except requests.exceptions.Timeout:
        print(f"❌ Error: Request timed out")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        try:
            error_detail = response.json()
            print(f"   Details: {json.dumps(error_detail, indent=2)}")
        except:
            print(f"   Response: {response.text}")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return None

def main():
    """Run all endpoint tests"""
    print("="*60)
    print("Hygiene Retention Dashboard Endpoint Tests")
    print("="*60)
    
    results = {}
    
    # 1. Test root endpoint (health check)
    print("\n🔍 Health Check...")
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        if response.status_code == 200:
            print(f"✅ API is running: {response.json()}")
        else:
            print(f"⚠️  API returned status {response.status_code}")
    except:
        print("❌ API server is not running!")
        print("   Start it with: api-run")
        return
    
    # 2. Test retention summary with default dates (last 12 months)
    results['retention_summary_default'] = test_endpoint(
        "Hygiene Retention Summary (Default - Last 12 months)",
        f"{BASE_URL}/hygiene/retention-summary"
    )
    
    # 3. Test retention summary with custom date range (last 6 months)
    results['retention_summary_6months'] = test_endpoint(
        "Hygiene Retention Summary (Last 6 months)",
        f"{BASE_URL}/hygiene/retention-summary",
        params={
            "start_date": (date.today() - timedelta(days=180)).isoformat(),
            "end_date": date.today().isoformat()
        }
    )
    
    # 4. Test retention summary with custom date range (last 3 months)
    results['retention_summary_3months'] = test_endpoint(
        "Hygiene Retention Summary (Last 3 months)",
        f"{BASE_URL}/hygiene/retention-summary",
        params={
            "start_date": (date.today() - timedelta(days=90)).isoformat(),
            "end_date": date.today().isoformat()
        }
    )
    
    # 5. Test retention summary with specific year
    results['retention_summary_2024'] = test_endpoint(
        "Hygiene Retention Summary (2024)",
        f"{BASE_URL}/hygiene/retention-summary",
        params={
            "start_date": "2024-01-01",
            "end_date": "2024-12-31"
        }
    )
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    
    success_count = sum(1 for v in results.values() if v is not None)
    total_count = len(results)
    
    print(f"✅ Successful: {success_count}/{total_count}")
    print(f"❌ Failed: {total_count - success_count}/{total_count}")
    
    if success_count == total_count:
        print("\n🎉 All tests passed!")
        
        # Print KPI summary from first successful test
        if results.get('retention_summary_default'):
            print("\n" + "="*60)
            print("KPI Summary (Default - Last 12 months)")
            print("="*60)
            data = results['retention_summary_default']
            print(f"  Recall Current %:           {data.get('recall_current_percent', 0):.2f}%")
            print(f"  Hyg Pre-Appointment %:     {data.get('hyg_pre_appointment_percent', 0):.2f}%")
            print(f"  Hyg Patients Seen:         {data.get('hyg_patients_seen', 0)}")
            print(f"  Hyg Pts Re-appntd:         {data.get('hyg_pts_reappntd', 0)}")
            print(f"  Recall Overdue %:           {data.get('recall_overdue_percent', 0):.2f}%")
            print(f"  Not on Recall %:           {data.get('not_on_recall_percent', 0):.2f}%")
    else:
        print("\n⚠️  Some tests failed. Check the errors above.")
    
    return results

if __name__ == "__main__":
    main()

