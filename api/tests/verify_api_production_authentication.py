"""
Verification script for DEMO API Key Authentication

⚠️  THIS SCRIPT TESTS THE DEMO API (https://api.dbtdentalclinic.com) ⚠️

This script verifies:
1. All business endpoints return 401 without API key
2. All business endpoints return 200 with valid API key
3. /health endpoint works without API key
4. Invalid API keys are rejected

Prerequisites:
    - Demo API must be running and accessible
    - requests library must be installed (run: pip install requests)
    - deployment_credentials.json must exist in project root with demo API key

Usage:
    python api/tests/verify_api_production_authentication.py

For local API testing, use:
    python api/tests/verify_api_local_authentication.py

Note: File name kept as verify_api_production_authentication.py for backward compatibility.
The script now tests the demo API (previously called "production").
"""

import requests
import json
import os
import sys
from typing import Dict, List, Tuple, Optional

BASE_URL = "https://api.dbtdentalclinic.com"

# Get API key from deployment_credentials.json
def get_api_key():
    """
    Get DEMO API key from deployment_credentials.json file in project root.
    
    Returns None if not found (test will show appropriate error).
    """
    # Get absolute path to project root
    script_path = os.path.abspath(__file__)  # Full path to verify_api_production_authentication.py
    # Go up: api/tests/ -> api/ -> project root
    api_tests_dir = os.path.dirname(script_path)  # api/tests/
    api_dir = os.path.dirname(api_tests_dir)  # api/
    project_root = os.path.dirname(api_dir)  # project root
    creds_file = os.path.join(project_root, "deployment_credentials.json")
    
    # Debug output
    print(f"   🔍 Script path: {script_path}")
    print(f"   🔍 Project root: {project_root}")
    print(f"   🔍 Looking for credentials file at: {creds_file}")
    
    if os.path.exists(creds_file):
        try:
            with open(creds_file, 'r') as f:
                creds = json.load(f)
                api_key = creds.get("backend_api", {}).get("api_key")
                if api_key:
                    return api_key
                else:
                    print(f"   ⚠️  'backend_api.api_key' not found in {creds_file}")
        except json.JSONDecodeError as e:
            print(f"   ⚠️  Could not parse JSON from {creds_file}: {e}")
        except Exception as e:
            print(f"   ⚠️  Could not read {creds_file}: {e}")
    else:
        print(f"   🔍 Looking for credentials file at: {creds_file}")
    
    # Return None if not found (test will handle this)
    return None

API_KEY = get_api_key()

# Sample business endpoints to test (representative of all routers)
BUSINESS_ENDPOINTS = [
    # Patient endpoints
    ("/patients/", "GET", "Patient list"),
    # Revenue endpoints
    ("/revenue/kpi-summary", "GET", "Revenue KPI Summary"),
    ("/revenue/trends", "GET", "Revenue Trends"),
    # AR endpoints
    ("/ar/kpi-summary", "GET", "AR KPI Summary"),
    ("/ar/aging-summary", "GET", "AR Aging Summary"),
    # Provider endpoints
    ("/providers/", "GET", "Provider list"),
    ("/providers/summary", "GET", "Provider Summary"),
    # Appointment endpoints
    ("/appointments/summary", "GET", "Appointment Summary"),
    ("/appointments/today", "GET", "Today's Appointments"),
    # Reports endpoints
    ("/reports/dashboard/kpis", "GET", "Dashboard KPIs"),
    # Treatment Acceptance endpoints
    ("/treatment-acceptance/kpi-summary", "GET", "Treatment Acceptance KPI"),
    # Hygiene endpoints
    ("/hygiene/retention-summary", "GET", "Hygiene Retention Summary"),
    # DBT Metadata endpoints
    ("/dbt/metric-lineage", "GET", "DBT Metric Lineage"),
]

# Public endpoints (should work without API key)
PUBLIC_ENDPOINTS = [
    ("/", "GET", "Root endpoint"),
    ("/health", "GET", "Health check"),
]

# Test results storage
test_results: Dict[str, List[Tuple[str, bool, str]]] = {
    "business_without_key": [],
    "business_with_key": [],
    "public_endpoints": [],
    "invalid_key": [],
}


def test_endpoint(
    endpoint: str,
    method: str = "GET",
    description: str = "",
    api_key: Optional[str] = None,
    expected_status: Optional[int] = None,
) -> Tuple[bool, str, int]:
    """
    Test an endpoint with optional API key
    
    Returns: (success, message, status_code)
    """
    url = f"{BASE_URL}{endpoint}"
    headers = {}
    
    if api_key:
        headers["X-API-Key"] = api_key
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=30)
        else:
            response = requests.request(method, url, headers=headers, timeout=30)
        
        status_code = response.status_code
        
        # Check if status matches expected
        if expected_status:
            if status_code == expected_status:
                # Status matches expected - this is a success
                # Try to get detail message for better reporting
                try:
                    data = response.json()
                    detail = data.get("detail", "") if isinstance(data, dict) else ""
                    if detail:
                        return (True, f"Status {status_code}: {detail[:80]}", status_code)
                except:
                    pass
                return (True, f"Status {status_code}", status_code)
            else:
                # Status doesn't match expected - this is a failure
                return (
                    False,
                    f"Expected {expected_status}, got {status_code}",
                    status_code,
                )
        
        # No expected status specified - treat 4xx/5xx as errors
        if status_code >= 400:
            # Try to parse response for error details
            try:
                data = response.json()
                detail = data.get("detail", "") if isinstance(data, dict) else ""
                if detail:
                    return (False, f"Status {status_code}: {detail[:80]}", status_code)
            except:
                detail = response.text[:100]
                return (False, f"Status {status_code}: {detail}", status_code)
        
        return (True, f"Status {status_code}", status_code)
        
    except requests.exceptions.ConnectionError:
        return (False, "Connection error - Demo API not accessible", 0)
    except requests.exceptions.Timeout:
        return (False, "Request timeout (30s)", 0)
    except requests.exceptions.SSLError as e:
        return (False, f"SSL error: {str(e)[:80]}", 0)
    except Exception as e:
        return (False, f"Error: {str(e)}", 0)


def verify_business_endpoints_without_key():
    """Verify all business endpoints return 401 without API key"""
    print("\n" + "=" * 70)
    print("TEST 1: Business Endpoints WITHOUT API Key (Should return 401)")
    print("=" * 70)
    
    all_passed = True
    for endpoint, method, description in BUSINESS_ENDPOINTS:
        success, message, status = test_endpoint(
            endpoint, method, description, api_key=None, expected_status=401
        )
        
        result = (endpoint, success, message)
        test_results["business_without_key"].append(result)
        
        status_icon = "✅" if success else "❌"
        print(f"{status_icon} {description:40} {endpoint:35} {message}")
        
        if not success:
            all_passed = False
    
    return all_passed


def verify_business_endpoints_with_key():
    """Verify all business endpoints return 200 with valid API key"""
    print("\n" + "=" * 70)
    print("TEST 2: Business Endpoints WITH Valid API Key (Should return 200)")
    print("=" * 70)
    print(f"Using API Key: {API_KEY[:20]}...")
    
    # First, test one endpoint to see if the key works
    test_endpoint_url = BUSINESS_ENDPOINTS[0]
    test_success, test_message, test_status = test_endpoint(
        test_endpoint_url[0], test_endpoint_url[1], test_endpoint_url[2], 
        api_key=API_KEY, expected_status=None
    )
    
    if test_status == 401:
        print(f"\n⚠️  WARNING: API key authentication failed!")
        print(f"   Status: {test_status}")
        print(f"   The API key from deployment_credentials.json doesn't match demo API_KEY")
        print(f"\n💡 To fix this:")
        print(f"   1. Verify the API key in deployment_credentials.json matches demo API key")
        print(f"   2. Check demo API logs or EC2 .env file for DEMO_API_KEY")
        print(f"   3. Update deployment_credentials.json if needed")
        print(f"\n   Current test key: {API_KEY[:30]}...")
        print(f"   This test will likely fail - fix the API key first!")
        print()
    
    all_passed = True
    for endpoint, method, description in BUSINESS_ENDPOINTS:
        success, message, status = test_endpoint(
            endpoint, method, description, api_key=API_KEY, expected_status=200
        )
        
        result = (endpoint, success, message)
        test_results["business_with_key"].append(result)
        
        status_icon = "✅" if success else "❌"
        # Show error message if test failed
        if not success and status != 200:
            print(f"{status_icon} {description:40} {endpoint:35} Status {status} - {message}")
        else:
            print(f"{status_icon} {description:40} {endpoint:35} Status {status}")
        
        if not success and status != 200:
            all_passed = False
    
    return all_passed


def verify_public_endpoints():
    """Verify public endpoints work without API key"""
    print("\n" + "=" * 70)
    print("TEST 3: Public Endpoints WITHOUT API Key (Should return 200)")
    print("=" * 70)
    
    all_passed = True
    for endpoint, method, description in PUBLIC_ENDPOINTS:
        success, message, status = test_endpoint(
            endpoint, method, description, api_key=None, expected_status=200
        )
        
        result = (endpoint, success, message)
        test_results["public_endpoints"].append(result)
        
        status_icon = "✅" if success else "❌"
        print(f"{status_icon} {description:40} {endpoint:35} {message}")
        
        if not success:
            all_passed = False
    
    return all_passed


def verify_invalid_api_key():
    """Verify endpoints reject invalid API keys"""
    print("\n" + "=" * 70)
    print("TEST 4: Business Endpoints WITH Invalid API Key (Should return 401)")
    print("=" * 70)
    
    invalid_key = "invalid-api-key-12345"
    all_passed = True
    
    # Test a few representative endpoints
    test_endpoints = BUSINESS_ENDPOINTS[:5]  # Test first 5 endpoints
    
    for endpoint, method, description in test_endpoints:
        success, message, status = test_endpoint(
            endpoint, method, description, api_key=invalid_key, expected_status=401
        )
        
        result = (endpoint, success, message)
        test_results["invalid_key"].append(result)
        
        status_icon = "✅" if success else "❌"
        print(f"{status_icon} {description:40} {endpoint:35} {message}")
        
        if not success:
            all_passed = False
    
    return all_passed


def print_summary():
    """Print test summary"""
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)
    
    # Test 1: Business endpoints without key
    passed_1 = sum(1 for _, success, _ in test_results["business_without_key"] if success)
    total_1 = len(test_results["business_without_key"])
    print(f"\n✅ Test 1: Business Endpoints WITHOUT API Key (401 expected)")
    print(f"   Passed: {passed_1}/{total_1}")
    
    # Test 2: Business endpoints with key
    passed_2 = sum(1 for _, success, _ in test_results["business_with_key"] if success)
    total_2 = len(test_results["business_with_key"])
    print(f"\n✅ Test 2: Business Endpoints WITH Valid API Key (200 expected)")
    print(f"   Passed: {passed_2}/{total_2}")
    
    # Test 3: Public endpoints
    passed_3 = sum(1 for _, success, _ in test_results["public_endpoints"] if success)
    total_3 = len(test_results["public_endpoints"])
    print(f"\n✅ Test 3: Public Endpoints WITHOUT API Key (200 expected)")
    print(f"   Passed: {passed_3}/{total_3}")
    
    # Test 4: Invalid API key
    passed_4 = sum(1 for _, success, _ in test_results["invalid_key"] if success)
    total_4 = len(test_results["invalid_key"])
    print(f"\n✅ Test 4: Business Endpoints WITH Invalid API Key (401 expected)")
    print(f"   Passed: {passed_4}/{total_4}")
    
    # Overall
    total_passed = passed_1 + passed_2 + passed_3 + passed_4
    total_tests = total_1 + total_2 + total_3 + total_4
    
    print("\n" + "=" * 70)
    print(f"OVERALL: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("🎉 ALL DEMO API VERIFICATION TESTS PASSED!")
        print("\n✅ Checklist Items Verified:")
        print("   [✓] All business endpoints return 401 without API key")
        print("   [✓] All business endpoints return 200 with valid API key")
        print("   [✓] /health endpoint works without API key")
        print("   [✓] Invalid API keys are rejected")
        return True
    else:
        print("❌ SOME TESTS FAILED - Review output above")
        return False


def main():
    """Run all verification tests"""
    print("=" * 70)
    print("DEMO API KEY AUTHENTICATION VERIFICATION")
    print("Testing: https://api.dbtdentalclinic.com")
    print("=" * 70)
    
    # Check if API is running
    print("\n🔍 Checking if DEMO API server is accessible...")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        if response.status_code == 200:
            print(f"✅ DEMO API server is accessible at {BASE_URL}")
        else:
            print(f"⚠️  API returned status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to {BASE_URL}")
        print("   Check your internet connection and verify the demo API is running")
        sys.exit(1)
    except requests.exceptions.SSLError as e:
        print(f"❌ SSL error connecting to {BASE_URL}: {e}")
        print("   Verify SSL certificate is valid")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error connecting to {BASE_URL}: {e}")
        sys.exit(1)
    
    # Check API key
    if API_KEY is None:
        print(f"\n❌ ERROR: DEMO API key not found!")
        print("   💡 To fix:")
        print("      Ensure deployment_credentials.json exists in project root")
        print("      and contains 'backend_api.api_key' property")
        print("   The test cannot proceed without an API key.")
        sys.exit(1)
    
    print(f"\n🔑 Using DEMO API Key: {API_KEY[:20]}...")
    print("   ✅ API key loaded from deployment_credentials.json")
    
    # Run all tests
    results = []
    results.append(("Business endpoints without key", verify_business_endpoints_without_key()))
    results.append(("Business endpoints with key", verify_business_endpoints_with_key()))
    results.append(("Public endpoints", verify_public_endpoints()))
    results.append(("Invalid API key", verify_invalid_api_key()))
    
    # Print summary
    all_passed = print_summary()
    
    # Exit code
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()

