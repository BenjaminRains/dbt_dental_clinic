# api/main.py
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError, HTTPException
import logging
import os
import time
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Ensure logs go to console
    ]
)

from routers import patient, reports, appointment, provider, revenue, dbt_metadata, ar, treatment_acceptance, hygiene
from config import APIConfig
from middleware.rate_limit import rate_limit_middleware
from middleware.request_logger import RequestLoggingMiddleware

# Initialize config (this will load .env file and set API_ENVIRONMENT)
# But we need to check environment BEFORE config loads, or handle it differently
try:
    config = APIConfig()
    # Config will load .env file which might set API_ENVIRONMENT
    # But we want to detect if we're running locally (not production)
except ValueError as e:
    # If API_ENVIRONMENT is not set, config will fail
    # This is fine for local development - we'll default to test
    logger = logging.getLogger(__name__)
    logger.warning(f"Config initialization failed (likely API_ENVIRONMENT not set): {e}")
    logger.info("Defaulting to test environment for local development")
    config = None

app = FastAPI(
    title="Dental Practice API",
    description="API for accessing OpenDental data transformed by DBT",
    version="0.1.0"
)

# Get CORS origins from environment or config
# IMPORTANT: Check environment AFTER config loads (config may set it from .env file)
# But we want to ensure localhost origins are always available for local development
api_environment = os.getenv("API_ENVIRONMENT", "test")  # Default to test if not set

# Detect if we're running locally (not on EC2/production server)
# Check multiple indicators: hostname, computer name, API host, or database host
def is_running_locally() -> bool:
    """Detect if API is running on local development machine (not production server)."""
    # Check hostname
    hostname = os.getenv("HOSTNAME", "").lower()
    if hostname.startswith("localhost") or "localhost" in hostname:
        return True
    
    # Check computer name (Windows)
    computername = os.getenv("COMPUTERNAME", "").lower()
    if computername in ["localhost", "127.0.0.1"]:
        return True
    
    # Check API host - if it's 0.0.0.0 or localhost, we're likely running locally
    api_host = os.getenv("API_HOST", "0.0.0.0").lower()
    if "localhost" in api_host or api_host in ["0.0.0.0", "127.0.0.1"]:
        return True
    
    # Check database host - if connecting to localhost, we're running locally
    db_host = os.getenv("POSTGRES_ANALYTICS_HOST") or os.getenv("TEST_POSTGRES_ANALYTICS_HOST", "")
    if db_host.lower() in ["localhost", "127.0.0.1", "0.0.0.0"]:
        return True
    
    # If no environment is set, assume local dev
    if os.getenv("API_ENVIRONMENT") is None:
        return True
    
    return False

is_local_dev = is_running_locally()

if is_local_dev and api_environment == "production":
    logger = logging.getLogger(__name__)
    logger.warning("API_ENVIRONMENT=production detected but running locally - allowing localhost origins for development")
    # Keep production mode but allow localhost origins

cors_origins_str = os.getenv("API_CORS_ORIGINS", None)

# Start with default development origins (always safe to include)
cors_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]

if cors_origins_str:
    # Parse from environment variable (comma-separated string) and add to defaults
    env_origins = [origin.strip() for origin in cors_origins_str.split(",")]
    for origin in env_origins:
        if origin not in cors_origins:
            cors_origins.append(origin)
elif config and hasattr(config, 'environment'):
    # Use from config (returns a list) and add to defaults
    try:
        config_origins = config.get_cors_origins()
        if isinstance(config_origins, list):
            for origin in config_origins:
                if origin and origin not in cors_origins:
                    cors_origins.append(origin)
        elif config_origins and config_origins not in cors_origins:
            cors_origins.append(config_origins)
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"Could not load CORS origins from config: {e}")

# In production, remove localhost origins UNLESS we're running locally
# (This allows local testing even if API_ENVIRONMENT=production is set)
# Use the same detection function we defined above
if api_environment == "production" and not is_local_dev:
    # Remove localhost origins for production (but only if not running locally)
    cors_origins = [origin for origin in cors_origins if not origin.startswith("http://localhost") and not origin.startswith("http://127.0.0.1")]
    
    # If no origins left, use safe defaults
    if not cors_origins:
        cors_origins = ["https://dbtdentalclinic.com", "https://www.dbtdentalclinic.com"]
        logging.warning("WARNING: No CORS origins configured in production! Using safe defaults.")
    elif "*" in cors_origins:
        logging.warning("WARNING: CORS wildcard (*) detected in production! Using safe defaults.")
        cors_origins = ["https://dbtdentalclinic.com", "https://www.dbtdentalclinic.com"]
elif api_environment == "production" and is_local_dev:
    # Production mode but running locally - keep localhost origins for development
    logger = logging.getLogger(__name__)
    logger.warning("API_ENVIRONMENT=production but running locally - keeping localhost origins for local development")

# Clean and deduplicate origins list (remove empty strings, trim whitespace, remove duplicates)
cors_origins = sorted(list(set([origin.strip() for origin in cors_origins if origin and origin.strip()])))

# Log configured origins for debugging
logger = logging.getLogger(__name__)
logger.info(f"CORS allowed origins ({len(cors_origins)}): {cors_origins}")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,  # Use environment-specific origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],  # Restrict methods
    allow_headers=["Content-Type", "X-API-Key", "Accept"],  # Explicit headers only
    expose_headers=["X-RateLimit-Limit-Minute", "X-RateLimit-Remaining-Minute", 
                    "X-RateLimit-Limit-Hour", "X-RateLimit-Remaining-Hour"],  # Rate limit headers
    max_age=3600,  # Cache preflight requests
)

# Add request logging middleware (should be first to log all requests)
app.add_middleware(RequestLoggingMiddleware)

# Add rate limiting middleware
app.middleware("http")(rate_limit_middleware)

# Include routers
app.include_router(patient.router)
app.include_router(reports.router)
app.include_router(appointment.router)
app.include_router(provider.router)
app.include_router(revenue.router)
app.include_router(dbt_metadata.router)
app.include_router(ar.router)
app.include_router(treatment_acceptance.router)
app.include_router(hygiene.router)

@app.get("/")
def read_root():
    """Root endpoint - public access for API discovery"""
    return {"message": "Welcome to the Dental Practice API"}

@app.get("/health")
def health_check():
    """Health check endpoint - public access for ALB health checks"""
    return {"status": "healthy", "service": "dental-practice-api"}

@app.get("/test/rate-limit")
def test_rate_limit_endpoint():
    """
    Fast test endpoint for rate limiting tests
    This endpoint is rate-limited (unlike /health) but returns immediately
    """
    return {"message": "Rate limit test endpoint", "timestamp": time.time()}

@app.get("/debug/cors")
def debug_cors(request: Request):
    """Debug endpoint to check CORS configuration (development only)"""
    # Allow in test environment or when API_ENVIRONMENT is not set (local dev)
    current_env = os.getenv("API_ENVIRONMENT", "test")
    if current_env == "production":
        return {"error": "Debug endpoint disabled in production"}
    
    origin = request.headers.get("Origin", "No Origin header")
    return {
        "origin_received": origin,
        "allowed_origins": cors_origins,
        "origin_in_allowed_list": origin in cors_origins,
        "environment": api_environment,
        "api_environment_env_var": current_env,
        "cors_origins_count": len(cors_origins)
    }

@app.get("/debug/rate-limit")
def debug_rate_limit(request: Request):
    """Debug endpoint to inspect rate limiter state (development only)"""
    from middleware.rate_limit import rate_limiter
    import time
    
    # Only allow in test/development
    current_env = os.getenv("API_ENVIRONMENT", "test")
    if current_env == "production":
        return {"error": "Debug endpoint disabled in production"}
    
    # Get client IP
    client_ip = request.client.host if request.client else "unknown"
    if "x-forwarded-for" in request.headers:
        client_ip = request.headers["x-forwarded-for"].split(",")[0].strip()
    
    now = time.time()
    minute_requests = rate_limiter.minute_requests.get(client_ip, [])
    recent_minute = [t for t in minute_requests if now - t < 60]
    
    return {
        "client_ip": client_ip,
        "requests_per_minute": rate_limiter.requests_per_minute,
        "requests_per_hour": rate_limiter.requests_per_hour,
        "total_stored_minute": len(minute_requests),
        "recent_minute_count": len(recent_minute),
        "recent_minute_requests": sorted(recent_minute, reverse=True)[:10],  # Last 10
        "oldest_request_age": round(now - min(recent_minute), 2) if recent_minute else None,
        "newest_request_age": round(now - max(recent_minute), 2) if recent_minute else None,
    }

# Global exception handler for unhandled exceptions (but NOT HTTPException)
# FastAPI handles HTTPException automatically, so we don't need a custom handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch all unhandled exceptions and return proper JSON error response"""
    # Skip HTTPException - it's handled above
    if isinstance(exc, HTTPException):
        raise  # Re-raise to let the HTTPException handler deal with it
    
    logger = logging.getLogger(__name__)
    error_msg = str(exc)
    error_type = type(exc).__name__
    
    # Log the full traceback
    logger.error(f"Unhandled exception: {error_type}: {error_msg}", exc_info=True)
    
    # Return JSON error response
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": f"Internal server error: {error_type}: {error_msg}",
            "error_type": error_type
        }
    )