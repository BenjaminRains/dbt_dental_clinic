"""
Test script for Request Logging Middleware

This script makes various requests to test that logging is working correctly:
- Public endpoints (no auth)
- Business endpoints (with auth)
- Business endpoints (without auth - should get 401)
- Different HTTP methods

Prerequisites:
    - API server must be running (run 'api-run' in another terminal)
    - requests library must be installed
    - .ssh/dbt-dental-clinic-api-key.pem file must exist in project root

Usage:
    python api/tests/test_request_logging.py
"""

import requests
import os
import sys
import time

BASE_URL = "http://localhost:8000"


def get_api_key():
    """Get API key from .ssh/dbt-dental-clinic-api-key.pem file"""
    script_path = os.path.abspath(__file__)
    api_tests_dir = os.path.dirname(script_path)
    api_dir = os.path.dirname(api_tests_dir)
    project_root = os.path.dirname(api_dir)
    pem_file = os.path.join(project_root, ".ssh", "dbt-dental-clinic-api-key.pem")
    
    if os.path.exists(pem_file):
        try:
            with open(pem_file, 'r') as f:
                key_content = f.read().strip()
                lines = key_content.split('\n')
                key_lines = [line for line in lines 
                            if line and 
                            not line.startswith('-----BEGIN') and 
                            not line.startswith('-----END')]
                if key_lines:
                    return ''.join(key_lines).strip()
        except Exception as e:
            print(f"   ⚠️  Could not read {pem_file}: {e}")
    
    return None


def test_request(endpoint, method="GET", api_key=None, description=""):
    """Make a test request and return status"""
    url = f"{BASE_URL}{endpoint}"
    headers = {}
    
    if api_key:
        headers["X-API-Key"] = api_key
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        else:
            response = requests.request(method, url, headers=headers, timeout=10)
        
        return response.status_code, response
    except requests.exceptions.ConnectionError:
        return None, None
    except Exception as e:
        return None, None


def main():
    """Run logging tests"""
    print("=" * 70)
    print("REQUEST LOGGING TEST")
    print("=" * 70)
    
    # Check if API is running
    print("\n🔍 Checking if API server is running...")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print(f"✅ API server is running at {BASE_URL}")
        else:
            print(f"⚠️  API returned status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to {BASE_URL}")
        print("   Make sure the API server is running:")
        print("   - Run 'api-run' in another terminal")
        sys.exit(1)
    
    # Get API key
    api_key = get_api_key()
    if not api_key:
        print("\n⚠️  WARNING: API key not found. Some tests will fail.")
        print("   Tests will still run to check logging for unauthenticated requests.")
    
    print("\n" + "=" * 70)
    print("MAKING TEST REQUESTS")
    print("=" * 70)
    print("\n📝 Watch the API server console/logs for request logging output.")
    print("   Expected log format:")
    print("   [IP] [METHOD] [PATH] [AUTH_STATUS] [STATUS_CODE] [RESPONSE_TIME_MS]")
    print("\n" + "-" * 70)
    
    # Test 1: Public endpoints (no auth)
    print("\n1️⃣  Testing PUBLIC endpoints (no authentication):")
    print("-" * 70)
    test_request("/", description="Root endpoint")
    print("   ✅ GET /")
    time.sleep(0.5)
    
    test_request("/health", description="Health check")
    print("   ✅ GET /health")
    time.sleep(0.5)
    
    # Test 2: Business endpoints without API key (should get 401)
    print("\n2️⃣  Testing BUSINESS endpoints WITHOUT API key (should return 401):")
    print("-" * 70)
    status, _ = test_request("/patients/", description="Patient list")
    if status == 401:
        print("   ✅ GET /patients/ → 401 (expected)")
    else:
        print(f"   ⚠️  GET /patients/ → {status} (expected 401)")
    time.sleep(0.5)
    
    status, _ = test_request("/providers/", description="Provider list")
    if status == 401:
        print("   ✅ GET /providers/ → 401 (expected)")
    else:
        print(f"   ⚠️  GET /providers/ → {status} (expected 401)")
    time.sleep(0.5)
    
    # Test 3: Business endpoints with API key (should get 200)
    if api_key:
        print("\n3️⃣  Testing BUSINESS endpoints WITH API key (should return 200):")
        print("-" * 70)
        status, _ = test_request("/patients/", api_key=api_key, description="Patient list")
        if status == 200:
            print("   ✅ GET /patients/ → 200 (expected)")
        else:
            print(f"   ⚠️  GET /patients/ → {status} (expected 200)")
        time.sleep(0.5)
        
        status, _ = test_request("/providers/", api_key=api_key, description="Provider list")
        if status == 200:
            print("   ✅ GET /providers/ → 200 (expected)")
        else:
            print(f"   ⚠️  GET /providers/ → {status} (expected 200)")
        time.sleep(0.5)
        
        status, _ = test_request("/revenue/kpi-summary", api_key=api_key, description="Revenue KPI")
        if status == 200:
            print("   ✅ GET /revenue/kpi-summary → 200 (expected)")
        else:
            print(f"   ⚠️  GET /revenue/kpi-summary → {status} (expected 200)")
        time.sleep(0.5)
    else:
        print("\n3️⃣  Skipping authenticated tests (no API key available)")
    
    # Test 4: Invalid endpoint (should get 404)
    print("\n4️⃣  Testing INVALID endpoint (should return 404):")
    print("-" * 70)
    status, _ = test_request("/nonexistent", description="Invalid endpoint")
    if status == 404:
        print("   ✅ GET /nonexistent → 404 (expected)")
    else:
        print(f"   ⚠️  GET /nonexistent → {status} (expected 404)")
    time.sleep(0.5)
    
    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)
    print("\n📋 Check the API server console/logs above for request logging output.")
    print("\n✅ Expected log entries should show:")
    print("   - Client IP address")
    print("   - HTTP method (GET)")
    print("   - Endpoint path")
    print("   - Authentication status (authenticated/unauthenticated)")
    print("   - Response status code (200, 401, 404, etc.)")
    print("   - Response time in milliseconds")
    print("\nExample log line:")
    print("   [127.0.0.1] [GET] [/patients/] [authenticated] [200] [45.23ms]")
    print()


if __name__ == "__main__":
    main()

