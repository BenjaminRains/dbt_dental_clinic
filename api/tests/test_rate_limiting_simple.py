"""
Simple Rate Limiting Test

This test makes rapid requests to verify rate limiting works correctly.
"""

import requests
import time
import os
import sys

BASE_URL = "http://localhost:8000"


def get_api_key():
    """Get API key from .ssh/dbt-dental-clinic-api-key.pem file"""
    # Get project root (go up from api/tests/)
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


def main():
    print("=" * 70)
    print("SIMPLE RATE LIMITING TEST")
    print("=" * 70)
    
    # Check if API is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print(f"❌ API returned status {response.status_code}")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to {BASE_URL}")
        sys.exit(1)
    
    api_key = get_api_key()
    if not api_key:
        print("❌ API key not found")
        sys.exit(1)
    
    # Use /test/rate-limit endpoint which is fast and rate-limited
    # This ensures requests are truly rapid and we can hit the limit
    endpoint = "/test/rate-limit"
    url = f"{BASE_URL}{endpoint}"
    headers = {}  # Test endpoint doesn't require API key
    
    # First, make one request to get the actual rate limit from headers
    try:
        test_response = requests.get(url, headers=headers, timeout=5)
        rate_limit = int(test_response.headers.get("X-RateLimit-Limit-Minute", "60"))
    except:
        rate_limit = 60  # Default to 60 if we can't detect it
    
    # Make enough requests to test the limit (limit + 5 to ensure we hit it)
    num_requests = rate_limit + 5
    
    print(f"\n📍 Testing rate limit: {endpoint}")
    print(f"   Limit: {rate_limit} requests/minute (detected from headers)")
    print(f"   Making {num_requests} rapid requests (no delay)...")
    print("   Using fast test endpoint to properly test rate limiting\n")
    
    results = []
    start_time = time.time()
    
    # Make requests as fast as possible (no delay)
    for i in range(num_requests):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            status = response.status_code
            remaining = response.headers.get("X-RateLimit-Remaining-Minute", "?")
            
            results.append({
                "request": i + 1,
                "status": status,
                "remaining": remaining
            })
            
            # Show all requests
            print(f"   Request {i+1:3d}: Status {status:3d}, Remaining: {remaining:>3s}")
            
            if status == 429:
                print(f"\n   ✅ Rate limit triggered on request {i+1}")
                break
        except Exception as e:
            print(f"   ❌ Request {i+1}: Error - {e}")
            break
    
    elapsed = time.time() - start_time
    print(f"\n   Made {len(results)} requests in {elapsed:.2f} seconds")
    
    # Check rate limiter state via debug endpoint
    try:
        debug_response = requests.get(f"{BASE_URL}/debug/rate-limit", headers=headers, timeout=5)
        if debug_response.status_code == 200:
            debug_data = debug_response.json()
            print(f"\n   📊 Rate Limiter State:")
            print(f"      Total stored: {debug_data.get('total_stored_minute', '?')}")
            print(f"      Recent count (in 60s): {debug_data.get('recent_minute_count', '?')}")
            print(f"      Oldest request age: {debug_data.get('oldest_request_age', '?')}s")
            print(f"      Newest request age: {debug_data.get('newest_request_age', '?')}s")
    except:
        pass
    
    # Analyze results
    status_200 = sum(1 for r in results if r["status"] == 200)
    status_429 = sum(1 for r in results if r["status"] == 429)
    
    print(f"\n   Results: {status_200} successful (200), {status_429} rate limited (429)")
    
    if status_429 > 0:
        print("   ✅ Rate limiting is working! Got 429 response as expected")
        # Find which request got 429
        first_429 = next((r for r in results if r["status"] == 429), None)
        if first_429:
            expected_first_429 = rate_limit + 1
            print(f"   ✅ First 429 at request {first_429['request']} (expected: {expected_first_429})")
            if first_429['request'] <= expected_first_429:
                print("   ✅ Rate limit triggered at correct request number!")
        return True
    elif status_200 == rate_limit:
        print(f"   ⚠️  Got exactly {rate_limit} requests - rate limiter may be working but not blocking")
        print("   ⚠️  This might mean requests are too slow to hit the limit")
        print("   ⚠️  Check elapsed time - if > 60 seconds, requests are too slow")
        if elapsed > 60:
            print(f"   ⚠️  Test took {elapsed:.1f}s - requests are too slow to hit {rate_limit}/min limit")
            print("   ⚠️  Try using a faster endpoint or reducing the limit for testing")
        return False  # Should have gotten 429 on request (limit + 1)
    else:
        print(f"   ❌ Rate limiting may not be working (got {status_200} requests without 429)")
        if elapsed > 60:
            print(f"   ⚠️  Test took {elapsed:.1f}s - requests are too slow")
            print(f"   ⚠️  With slow requests, you may not hit the {rate_limit}/minute limit")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

