# api/auth/api_key.py
"""
Simple API Key Authentication
For portfolio use - demonstrates authentication patterns
"""
from fastapi import Security, HTTPException, status, Depends
from fastapi.security import APIKeyHeader
from pathlib import Path
from typing import Optional
import os

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


def load_api_key() -> str:
    """
    Load API key with priority:
    1. DEMO_API_KEY environment variable (when API_ENVIRONMENT=demo - public portfolio site)
    2. .ssh/dbt-dental-clinic-api-key.pem file (for test/demo environments - localhost only)
    
    Raises ValueError if no key is found (fail-fast).
    """
    # Check if we're in demo environment (public portfolio site) and DEMO_API_KEY is set
    api_environment = os.getenv("API_ENVIRONMENT", "").lower()
    if api_environment == "demo":
        demo_api_key = os.getenv("DEMO_API_KEY")
        if demo_api_key and demo_api_key.strip():
            return demo_api_key.strip()
        # If DEMO_API_KEY not set, fall through to file-based loading
    
    # Read from .ssh/dbt-dental-clinic-api-key.pem file
    # Find project root (go up from api/auth/ directory)
    # __file__ is at api/auth/api_key.py
    # parent = api/auth/
    # parent.parent = api/
    # parent.parent.parent = project root
    current_dir = Path(__file__).parent  # api/auth directory
    project_root = current_dir.parent.parent  # project root (go up 2 levels: auth -> api -> root)
    pem_file = project_root / ".ssh" / "dbt-dental-clinic-api-key.pem"
    
    if pem_file.exists():
        # Check if file is readable before attempting to open
        if not pem_file.is_file():
            raise ValueError(
                f"Path exists but is not a file: {pem_file}\n"
                "Ensure .ssh/api-key.pem is a regular file."
            )
        
        try:
            # Try to open the file
            with open(pem_file, 'r') as f:
                key_content = f.read().strip()
                # Remove PEM headers/footers if present, or just use the content
                # Handle both cases: raw key or PEM formatted
                lines = key_content.split('\n')
                # Filter out PEM headers/footers
                key_lines = [line for line in lines 
                            if line and 
                            not line.startswith('-----BEGIN') and 
                            not line.startswith('-----END')]
                if key_lines:
                    # Join the key lines (might be base64 encoded or plain text)
                    api_key = ''.join(key_lines).strip()
                    if api_key:
                        return api_key
        except PermissionError as e:
            raise ValueError(
                f"Permission denied reading API key from {pem_file}\n"
                f"Error: {e}\n"
                "The file may be locked by another process or you don't have read permissions.\n"
                "Try closing any programs that might be accessing the file."
            )
        except IOError as e:
            raise ValueError(
                f"IO error reading API key from {pem_file}\n"
                f"Error: {e}\n"
                "The file may be locked or inaccessible. Try:\n"
                "  1. Close any programs accessing the file\n"
                "  2. Check file permissions\n"
                "  3. Ensure the file is not read-only"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to read API key from {pem_file}: {e}\n"
                "Ensure .ssh/api-key.pem exists and is readable."
            )
    
    # Fail fast if no key found
    if api_environment == "demo":
        raise ValueError(
            f"DEMO_API_KEY not found. Set DEMO_API_KEY environment variable or create .ssh/api-key.pem file at: {pem_file}\n"
            "The API will not start without a valid API key."
        )
    else:
        raise ValueError(
            f"API key not found. Create .ssh/api-key.pem file at: {pem_file}\n"
            "The API will not start without a valid API key."
        )


# Generate or load API key
API_KEY = load_api_key()


def verify_api_key(api_key: Optional[str] = Security(api_key_header)) -> dict:
    """
    Verify API key
    
    For portfolio:
    - If valid key provided: full access with normal rate limits
    - If no key provided: allow access but with stricter rate limits
    - If invalid key provided: reject
    
    Returns dict with authentication status and rate limit info
    """
    if not api_key:
        # No API key provided - allow but with stricter limits
        return {
            "authenticated": False,
            "rate_limit_per_minute": 30,  # Stricter limit
            "rate_limit_per_hour": 500
        }
    
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    # Valid API key
    return {
        "authenticated": True,
        "rate_limit_per_minute": 100,  # Higher limit for authenticated
        "rate_limit_per_hour": 2000
    }


# Dependency for optional API key (doesn't block if missing)
def get_api_key_info(api_key: Optional[str] = Security(api_key_header)) -> dict:
    """Get API key info without blocking"""
    return verify_api_key(api_key)


# Dependency for required API key (blocks if missing)
def require_api_key(api_key: Optional[str] = Security(api_key_header)) -> dict:
    """Require valid API key"""
    result = verify_api_key(api_key)
    if not result["authenticated"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key required",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return result

