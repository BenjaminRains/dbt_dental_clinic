"""
Quick test script for AR Aging Dashboard endpoints

Prerequisites:
    - API server must be running (run 'api-run' in another terminal)
    - requests library must be installed (run: pip install requests)

Usage:
    python test_ar_endpoints.py
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
        response = requests.get(url, params=params, timeout=10)
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
        return None

def main():
    """Run all endpoint tests"""
    print("="*60)
    print("AR Aging Dashboard Endpoint Tests")
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
    
    # 2. KPI Summary
    results['kpi_summary'] = test_endpoint(
        "AR KPI Summary",
        f"{BASE_URL}/ar/kpi-summary"
    )
    
    # 3. Aging Summary
    results['aging_summary'] = test_endpoint(
        "AR Aging Summary",
        f"{BASE_URL}/ar/aging-summary"
    )
    
    # 4. Priority Queue
    results['priority_queue'] = test_endpoint(
        "AR Priority Queue",
        f"{BASE_URL}/ar/priority-queue",
        params={"limit": 5, "min_priority_score": 50}
    )
    
    # 5. Risk Distribution
    results['risk_distribution'] = test_endpoint(
        "AR Risk Distribution",
        f"{BASE_URL}/ar/risk-distribution"
    )
    
    # 6. Aging Trends
    results['aging_trends'] = test_endpoint(
        "AR Aging Trends",
        f"{BASE_URL}/ar/aging-trends",
        params={
            "start_date": (date.today() - timedelta(days=90)).isoformat(),
            "end_date": date.today().isoformat()
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
    else:
        print("\n⚠️  Some tests failed. Check the errors above.")
    
    return results

if __name__ == "__main__":
    main()

