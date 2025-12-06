"""
Test script for CORS Configuration

This script tests CORS headers and preflight requests to verify:
- CORS preflight (OPTIONS) requests work correctly
- CORS headers are present in responses
- Only allowed headers are accepted
- Unauthorized headers are rejected
- Different origins are handled correctly

Prerequisites:
    - API server must be running (run 'api-run' in another terminal)
    - requests library must be installed

Usage:
    python api/tests/test_cors.py
"""

import requests
import sys

BASE_URL = "http://localhost:8000"

# Common frontend origins
FRONTEND_ORIGINS = [
    "http://localhost:3000",  # Vite dev server
    "https://dbtdentalclinic.com",
    "https://www.dbtdentalclinic.com",
]


def test_cors_preflight(origin, endpoint="/patients/", headers=None):
    """Test CORS preflight OPTIONS request"""
    if headers is None:
        headers = {}
    
    headers.update({
        "Origin": origin,
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "Content-Type, X-API-Key, Accept",
    })
    
    try:
        response = requests.options(f"{BASE_URL}{endpoint}", headers=headers, timeout=10)
        if response.status_code >= 400:
            print(f"      Status: {response.status_code}")
            print(f"      Response: {response.text[:200]}")
        return response
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None


def test_cors_request(origin, endpoint="/", method="GET", api_key=None):
    """Test actual CORS request"""
    headers = {
        "Origin": origin,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    if api_key:
        headers["X-API-Key"] = api_key
    
    try:
        if method.upper() == "GET":
            response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, timeout=10)
        else:
            response = requests.request(method, f"{BASE_URL}{endpoint}", headers=headers, timeout=10)
        return response
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None


def check_cors_headers(response, expected_origin=None):
    """Check if CORS headers are present and correct"""
    if not response:
        return False, "No response"
    
    cors_headers = {
        "Access-Control-Allow-Origin": response.headers.get("Access-Control-Allow-Origin"),
        "Access-Control-Allow-Methods": response.headers.get("Access-Control-Allow-Methods"),
        "Access-Control-Allow-Headers": response.headers.get("Access-Control-Allow-Headers"),
        "Access-Control-Allow-Credentials": response.headers.get("Access-Control-Allow-Credentials"),
        "Access-Control-Expose-Headers": response.headers.get("Access-Control-Expose-Headers"),
    }
    
    issues = []
    
    # Check if origin is allowed
    if expected_origin:
        allowed_origin = cors_headers["Access-Control-Allow-Origin"]
        if allowed_origin != expected_origin and allowed_origin != "*":
            issues.append(f"Origin mismatch: expected {expected_origin}, got {allowed_origin}")
    
    # Check if methods are present
    if not cors_headers["Access-Control-Allow-Methods"]:
        issues.append("Missing Access-Control-Allow-Methods")
    
    # Check if headers are present
    if not cors_headers["Access-Control-Allow-Headers"]:
        issues.append("Missing Access-Control-Allow-Headers")
    
    # Check if credentials are allowed
    if cors_headers["Access-Control-Allow-Credentials"] != "true":
        issues.append("Access-Control-Allow-Credentials should be 'true'")
    
    return len(issues) == 0, issues, cors_headers


def main():
    """Run CORS tests"""
    print("=" * 70)
    print("CORS CONFIGURATION TEST")
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
    
    print("\n" + "=" * 70)
    print("TEST 1: CORS Preflight (OPTIONS) Requests")
    print("=" * 70)
    
    for origin in FRONTEND_ORIGINS:
        print(f"\n📍 Testing origin: {origin}")
        print("-" * 70)
        
        # Test preflight for public endpoint
        print("   Testing OPTIONS /health (public endpoint)...")
        response = test_cors_preflight(origin, "/health")
        if response:
            success, issues, cors_headers = check_cors_headers(response, expected_origin=origin)
            if success:
                print("   ✅ Preflight successful")
                print(f"      Access-Control-Allow-Origin: {cors_headers['Access-Control-Allow-Origin']}")
                print(f"      Access-Control-Allow-Methods: {cors_headers['Access-Control-Allow-Methods']}")
                print(f"      Access-Control-Allow-Headers: {cors_headers['Access-Control-Allow-Headers']}")
            else:
                print(f"   ⚠️  Preflight issues: {', '.join(issues)}")
        else:
            print("   ❌ Preflight request failed")
        
        # Test preflight for business endpoint
        print("   Testing OPTIONS /patients/ (business endpoint)...")
        response = test_cors_preflight(origin, "/patients/")
        if response:
            success, issues, cors_headers = check_cors_headers(response, expected_origin=origin)
            if success:
                print("   ✅ Preflight successful")
                print(f"      Access-Control-Allow-Origin: {cors_headers['Access-Control-Allow-Origin']}")
            else:
                print(f"   ⚠️  Preflight issues: {', '.join(issues)}")
        else:
            print("   ❌ Preflight request failed")
    
    print("\n" + "=" * 70)
    print("TEST 2: Actual CORS Requests")
    print("=" * 70)
    
    test_origin = "http://localhost:3000"  # Common dev origin
    
    print(f"\n📍 Testing origin: {test_origin}")
    print("-" * 70)
    
    # Test public endpoint
    print("   Testing GET /health (public, no API key)...")
    response = test_cors_request(test_origin, "/health")
    if response:
        success, issues, cors_headers = check_cors_headers(response, expected_origin=test_origin)
        print(f"      Status: {response.status_code}")
        print(f"      All headers: {dict(response.headers)}")
        if success:
            print(f"   ✅ Request successful (Status: {response.status_code})")
            print(f"      Access-Control-Allow-Origin: {cors_headers['Access-Control-Allow-Origin']}")
        else:
            print(f"   ⚠️  CORS issues: {', '.join(issues)}")
    else:
        print("   ❌ Request failed")
    
    # Test business endpoint without API key (should get 401)
    print("   Testing GET /patients/ (business, no API key - should get 401)...")
    response = test_cors_request(test_origin, "/patients/")
    if response:
        success, issues, cors_headers = check_cors_headers(response, expected_origin=test_origin)
        if response.status_code == 401:
            print(f"   ✅ Request successful (Status: {response.status_code} - expected)")
            print(f"      Access-Control-Allow-Origin: {cors_headers['Access-Control-Allow-Origin']}")
        else:
            print(f"   ⚠️  Unexpected status: {response.status_code} (expected 401)")
    else:
        print("   ❌ Request failed")
    
    print("\n" + "=" * 70)
    print("TEST 3: Unauthorized Headers (Should be Rejected)")
    print("=" * 70)
    
    print(f"\n📍 Testing with unauthorized header: X-Custom-Header")
    print("-" * 70)
    
    # Test preflight with unauthorized header
    headers = {
        "Origin": test_origin,
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "Content-Type, X-API-Key, Accept, X-Custom-Header",  # Unauthorized header
    }
    
    print("   Testing OPTIONS with unauthorized header...")
    response = test_cors_preflight(test_origin, "/health", headers=headers)
    if response:
        allowed_headers = response.headers.get("Access-Control-Allow-Headers", "")
        if "X-Custom-Header" not in allowed_headers:
            print("   ✅ Unauthorized header correctly rejected")
            print(f"      Allowed headers: {allowed_headers}")
        else:
            print("   ⚠️  Unauthorized header was allowed (should be rejected)")
    else:
        print("   ❌ Preflight request failed")
    
    print("\n" + "=" * 70)
    print("TEST 4: Exposed Headers (Rate Limit Headers)")
    print("=" * 70)
    
    print(f"\n📍 Testing exposed headers in response")
    print("-" * 70)
    
    response = test_cors_request(test_origin, "/health")
    if response:
        exposed_headers = response.headers.get("Access-Control-Expose-Headers", "")
        expected_headers = ["X-RateLimit-Limit-Minute", "X-RateLimit-Remaining-Minute", 
                           "X-RateLimit-Limit-Hour", "X-RateLimit-Remaining-Hour"]
        
        print(f"   Exposed headers: {exposed_headers}")
        missing = [h for h in expected_headers if h not in exposed_headers]
        if not missing:
            print("   ✅ All rate limit headers are exposed")
        else:
            print(f"   ⚠️  Missing exposed headers: {', '.join(missing)}")
    else:
        print("   ❌ Request failed")
    
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print("\n✅ CORS tests completed!")
    print("\n📋 Expected CORS behavior:")
    print("   - Preflight (OPTIONS) requests should return 200 with CORS headers")
    print("   - Actual requests should include Access-Control-Allow-Origin header")
    print("   - Only allowed headers (Content-Type, X-API-Key, Accept) should be accepted")
    print("   - Rate limit headers should be exposed to frontend")
    print("   - Credentials should be allowed")
    print()


if __name__ == "__main__":
    main()

