"""
Test script for debugging /patients/ and /providers/ endpoints

This script tests the problematic endpoints and shows detailed error information.
Output is written to both console and a log file in api/logs/
"""

import requests
import json
import os
import sys
from datetime import datetime

BASE_URL = "http://localhost:8000"

# Setup logging to file
def setup_logging():
    """Setup log file for test output"""
    # Get script directory and navigate to api/logs
    script_path = os.path.abspath(__file__)
    api_tests_dir = os.path.dirname(script_path)
    api_dir = os.path.dirname(api_tests_dir)
    logs_dir = os.path.join(api_dir, "logs")
    
    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f"test_problem_endpoints_{timestamp}.log")
    
    return log_file

# Initialize log file
LOG_FILE = setup_logging()

def log_print(message: str, end: str = "\n"):
    """Print to both console and log file"""
    print(message, end=end)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + end)

def get_api_key():
    """Get API key from .ssh/dbt-dental-clinic-api-key.pem file in project root."""
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
                    api_key = ''.join(key_lines).strip()
                    if api_key:
                        return api_key
        except Exception as e:
            print(f"Error reading API key: {e}")
    
    return None

def test_endpoint_detailed(endpoint: str, api_key: str = None):
    """Test an endpoint and show detailed response information."""
    url = f"{BASE_URL}{endpoint}"
    headers = {}
    
    if api_key:
        headers["X-API-Key"] = api_key
    
    log_print(f"\n{'='*70}")
    log_print(f"Testing: {endpoint}")
    log_print(f"{'='*70}")
    log_print(f"URL: {url}")
    # Don't expose full API key in output
    if api_key and "X-API-Key" in headers:
        safe_headers = {k: (v[:20] + "..." if k == "X-API-Key" else v) for k, v in headers.items()}
        log_print(f"Headers: {safe_headers}")
    else:
        log_print(f"Headers: {headers}")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        log_print(f"\nStatus Code: {response.status_code}")
        log_print(f"Status Text: {response.reason}")
        
        # Try to parse JSON response
        try:
            data = response.json()
            log_print(f"\nResponse JSON:")
            json_str = json.dumps(data, indent=2, default=str)
            log_print(json_str)
            
            # Show error detail if present
            if "detail" in data:
                log_print(f"\n⚠️  Error Detail: {data['detail']}")
            
        except json.JSONDecodeError:
            log_print(f"\nResponse Text (first 500 chars):")
            log_print(response.text[:500])
        
        # Show response headers
        log_print(f"\nResponse Headers:")
        for key, value in response.headers.items():
            log_print(f"  {key}: {value}")
        
        return response.status_code == 200
        
    except requests.exceptions.ConnectionError:
        log_print(f"\n❌ Connection error - API server not running")
        return False
    except requests.exceptions.Timeout:
        log_print(f"\n❌ Request timeout")
        return False
    except Exception as e:
        log_print(f"\n❌ Exception: {str(e)}")
        import traceback
        traceback_str = traceback.format_exc()
        log_print(traceback_str)
        return False

def main():
    """Run detailed tests on problem endpoints."""
    log_print("="*70)
    log_print("DETAILED TEST: Problem Endpoints")
    log_print("="*70)
    log_print(f"Log file: {LOG_FILE}")
    log_print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if API is running
    log_print("\n🔍 Checking if API server is running...")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            log_print(f"✅ API server is running at {BASE_URL}")
        else:
            log_print(f"⚠️  API returned status {response.status_code}")
    except requests.exceptions.ConnectionError:
        log_print(f"❌ Cannot connect to {BASE_URL}")
        log_print("   Make sure the API server is running")
        sys.exit(1)
    
    # Get API key
    api_key = get_api_key()
    if not api_key:
        log_print("\n❌ ERROR: API key not found!")
        log_print("   Ensure .ssh/dbt-dental-clinic-api-key.pem file exists in project root")
        sys.exit(1)
    
    log_print(f"\n🔑 Using API Key: {api_key[:30]}... (truncated)")
    
    # Test endpoints
    problem_endpoints = [
        "/patients/",
        "/providers/",
    ]
    
    log_print("\n" + "="*70)
    log_print("TESTING WITH VALID API KEY")
    log_print("="*70)
    
    for endpoint in problem_endpoints:
        test_endpoint_detailed(endpoint, api_key=api_key)
    
    log_print("\n" + "="*70)
    log_print("TESTING WITHOUT API KEY (should return 401)")
    log_print("="*70)
    
    for endpoint in problem_endpoints:
        test_endpoint_detailed(endpoint, api_key=None)
    
    log_print("\n" + "="*70)
    log_print(f"Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_print(f"Full log saved to: {LOG_FILE}")
    log_print("="*70)

if __name__ == "__main__":
    main()

