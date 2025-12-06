# api/middleware/request_logger.py
"""
Request Logging Middleware
Logs all requests with IP, method, path, auth status, response code, and response time
"""
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
import time
import logging

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware to log all HTTP requests"""
    
    async def dispatch(self, request: Request, call_next):
        """Process request and log details"""
        # Record start time
        start_time = time.time()
        
        # Get client IP (consider X-Forwarded-For if behind proxy)
        client_ip = request.client.host if request.client else "unknown"
        if "x-forwarded-for" in request.headers:
            # Get first IP from X-Forwarded-For header
            client_ip = request.headers["x-forwarded-for"].split(",")[0].strip()
        
        # Check authentication status
        api_key_header = request.headers.get("X-API-Key")
        auth_status = "authenticated" if api_key_header else "unauthenticated"
        
        # Process request
        response = await call_next(request)
        
        # Calculate response time
        process_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Get status code
        status_code = response.status_code
        
        # Log request details
        logger.info(
            f"[{client_ip}] [{request.method}] [{request.url.path}] "
            f"[{auth_status}] [{status_code}] [{process_time:.2f}ms]"
        )
        
        return response

