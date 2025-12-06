# api/middleware/rate_limit.py
"""
Simple In-Memory Rate Limiting Middleware
For portfolio use - consider Redis for production
"""
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from collections import defaultdict
from typing import Dict, List
import time
import logging
import os
from pathlib import Path

# Set up detailed logging to file (lazy initialization)
# Use a unique logger name to avoid conflicts
_logger_initialized = False
_file_handler = None

def _setup_logger():
    """Set up the rate limit logger (called lazily to avoid conflicts with basicConfig)"""
    global _logger_initialized, _file_handler
    
    if _logger_initialized:
        return
    
    logger = logging.getLogger("api.middleware.rate_limit")
    
    # Only set up handlers if they don't already exist
    if not logger.handlers or not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Create log filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"rate_limit_debug_{timestamp}.log"
        log_file_path = log_dir / log_filename
        
        # Test file write access first
        try:
            test_file = log_file_path.parent / "test_write.tmp"
            test_file.write_text("test")
            test_file.unlink()
        except Exception as e:
            print(f"ERROR: Cannot write to log directory: {e}")
            return
        
        # Create file handler for rate limit debugging
        try:
            _file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
            _file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            _file_handler.setFormatter(file_formatter)
            logger.addHandler(_file_handler)
        except Exception as e:
            print(f"ERROR: Cannot create file handler: {e}")
            return
        
        # Also log to console (but don't duplicate if root logger already has one)
        has_console = any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
        if not has_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter('%(levelname)s - %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        # Set logger level and prevent propagation to root logger
        logger.setLevel(logging.DEBUG)
        logger.propagate = False  # Prevent FastAPI from interfering
        
        # Force flush to ensure the log file is created
        _file_handler.flush()
        
        # Test logging with a direct write first
        try:
            test_msg = f"Rate limit logger initialized at {datetime.now()}, writing to: {log_file_path}\n"
            logger.info(f"Rate limit logger initialized, writing to: {log_file_path}")
            logger.debug("Debug logging enabled for rate limiter")
            logger.warning("TEST: This is a test warning message")
            logger.error("TEST: This is a test error message")
            _file_handler.flush()
            
            # Verify the file was written
            if log_file_path.exists() and log_file_path.stat().st_size > 0:
                print(f"SUCCESS: Logger initialized, log file created at {log_file_path}")
            else:
                print(f"WARNING: Logger initialized but log file is empty or doesn't exist")
        except Exception as e:
            print(f"ERROR: Failed to write to log file: {e}")
            import traceback
            traceback.print_exc()
        
        _logger_initialized = True

# Get the logger (will be set up on first use)
logger = logging.getLogger("api.middleware.rate_limit")


class RateLimiter:
    """
    Simple rate limiter using in-memory storage
    For production, use Redis or AWS API Gateway rate limiting
    """
    
    def __init__(self, requests_per_minute: int = 60, requests_per_hour: int = 1000):
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.minute_requests: Dict[str, List[float]] = defaultdict(list)
        self.hour_requests: Dict[str, List[float]] = defaultdict(list)
        self.last_cleanup = time.time()
    
    def _cleanup_old_requests(self, now: float):
        """Periodically clean up old request records"""
        # Only cleanup every 60 seconds to avoid performance issues
        # During rapid requests (within 60 seconds), this won't run
        # NOTE: We don't need to cleanup before counting because we filter by time anyway
        # But cleanup helps prevent memory growth over time
        time_since_cleanup = now - self.last_cleanup
        if time_since_cleanup < 60:
            logger.debug(f"[CLEANUP] Skipping cleanup (last cleanup {time_since_cleanup:.1f}s ago)")
            return
        
        logger.info(f"[CLEANUP] Running cleanup (last cleanup {time_since_cleanup:.1f}s ago)")
        
        # Clean minute-based requests (remove requests older than 60 seconds)
        for ip in list(self.minute_requests.keys()):
            before_count = len(self.minute_requests[ip])
            self.minute_requests[ip] = [
                req_time for req_time in self.minute_requests[ip]
                if now - req_time < 60
            ]
            after_count = len(self.minute_requests[ip])
            if after_count != before_count:
                logger.info(
                    f"[CLEANUP] IP={ip} | "
                    f"removed={before_count - after_count} | "
                    f"kept={after_count} | "
                    f"before={before_count} | "
                    f"after={after_count}"
                )
            # Don't delete the key even if list is empty - defaultdict will recreate it anyway
            # Keeping empty lists is fine, the count logic filters by time
        
        # Clean hour-based requests (remove requests older than 3600 seconds)
        for ip in list(self.hour_requests.keys()):
            before_count = len(self.hour_requests[ip])
            self.hour_requests[ip] = [
                req_time for req_time in self.hour_requests[ip]
                if now - req_time < 3600
            ]
            after_count = len(self.hour_requests[ip])
            if after_count != before_count:
                logger.debug(f"Cleaned up {before_count - after_count} old hour requests for IP {ip}")
            if not self.hour_requests[ip]:
                del self.hour_requests[ip]
        
        self.last_cleanup = now
    
    def check_rate_limit(self, client_ip: str) -> tuple[int, int]:
        """
        Check if client has exceeded rate limits
        
        Returns:
            (minute_remaining, hour_remaining) - remaining requests in each window
        """
        now = time.time()
        self._cleanup_old_requests(now)
        
        # Check minute limit
        # Filter to only requests in the last 60 seconds
        # IMPORTANT: Access the defaultdict directly - it will create an empty list if key doesn't exist
        # But we need to filter by time to get only recent requests
        # The cleanup may have removed old requests, but we filter by time anyway
        stored_requests = self.minute_requests[client_ip]  # defaultdict will create [] if missing
        recent_minute_requests = [
            req_time for req_time in stored_requests
            if now - req_time < 60
        ]
        minute_count = len(recent_minute_requests)
        
        # Detailed debug logging for every request
        oldest_recent = min(recent_minute_requests) if recent_minute_requests else None
        newest_recent = max(recent_minute_requests) if recent_minute_requests else None
        oldest_age = round(now - oldest_recent, 2) if oldest_recent else None
        newest_age = round(now - newest_recent, 2) if newest_recent else None
        
        logger.debug(
            f"[RATE_LIMIT] IP={client_ip} | "
            f"minute_count={minute_count} | "
            f"total_stored={len(stored_requests)} | "
            f"recent_in_60s={len(recent_minute_requests)} | "
            f"oldest_age={oldest_age}s | "
            f"newest_age={newest_age}s | "
            f"now={now:.3f}"
        )
        
        # Log when approaching limit or when count seems stuck
        if minute_count >= 25 or (minute_count > 0 and minute_count < 35):
            logger.info(
                f"[RATE_LIMIT] IP={client_ip} | "
                f"minute_count={minute_count} | "
                f"limit={self.requests_per_minute} | "
                f"total_stored={len(stored_requests)} | "
                f"recent_in_60s={len(recent_minute_requests)} | "
                f"oldest_age={oldest_age}s | "
                f"newest_age={newest_age}s"
            )
        
        # Check if limit exceeded (BEFORE recording this request)
        # If we have 60 requests already, the 61st should be blocked
        if minute_count >= self.requests_per_minute:
            logger.warning(
                f"[RATE_LIMIT] BLOCKED IP={client_ip} | "
                f"minute_count={minute_count} | "
                f"limit={self.requests_per_minute} | "
                f"total_stored={len(stored_requests)} | "
                f"recent_in_60s={len(recent_minute_requests)}"
            )
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Maximum {self.requests_per_minute} requests per minute."
            )
        
        # Check hour limit
        recent_hour_requests = [
            req_time for req_time in self.hour_requests[client_ip]
            if now - req_time < 3600
        ]
        hour_count = len(recent_hour_requests)
        
        if hour_count >= self.requests_per_hour:
            logger.warning(f"Rate limit exceeded (hour) for IP: {client_ip} (count: {hour_count}, limit: {self.requests_per_hour})")
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Maximum {self.requests_per_hour} requests per hour."
            )
        
        # Record this request (AFTER checking limits)
        # This ensures we check BEFORE recording, so if we have 60 requests,
        # the 61st will see count=60 and be blocked
        # defaultdict will automatically create an empty list if the key doesn't exist
        logger.debug(
            f"[RATE_LIMIT] Recording request | "
            f"IP={client_ip} | "
            f"before_count={minute_count} | "
            f"recording_time={now:.3f}"
        )
        
        self.minute_requests[client_ip].append(now)
        self.hour_requests[client_ip].append(now)
        
        # Verify the append worked
        stored_after = len(self.minute_requests[client_ip])
        logger.debug(
            f"[RATE_LIMIT] After recording | "
            f"IP={client_ip} | "
            f"stored_count={stored_after} | "
            f"expected={minute_count + 1}"
        )
        
        # Return remaining counts
        # minute_count is the count BEFORE we recorded this request
        # So after recording, we have (minute_count + 1) requests
        # Remaining = limit - (count + 1) = limit - count - 1
        minute_remaining = self.requests_per_minute - minute_count - 1
        hour_remaining = self.requests_per_hour - hour_count - 1
        
        # Ensure remaining is never negative (shouldn't happen, but safety check)
        minute_remaining = max(0, minute_remaining)
        hour_remaining = max(0, hour_remaining)
        
        logger.debug(
            f"[RATE_LIMIT] Returning | "
            f"IP={client_ip} | "
            f"minute_remaining={minute_remaining} | "
            f"hour_remaining={hour_remaining}"
        )
        
        return (minute_remaining, hour_remaining)


# Global rate limiter instance
rate_limiter = RateLimiter(
    requests_per_minute=60,  # 60 requests per minute
    requests_per_hour=1000   # 1000 requests per hour
)


async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware"""
    # Initialize logger on first use (after basicConfig has been called)
    _setup_logger()
    
    # Skip rate limiting for health checks and CORS preflight requests
    # Note: /test/rate-limit is NOT excluded - it's for testing rate limits
    if request.url.path in ["/", "/health", "/docs", "/openapi.json", "/redoc"]:
        return await call_next(request)
    
    # Log that middleware is being called (for debugging)
    try:
        logger.info(f"[MIDDLEWARE] Rate limit middleware called for path: {request.url.path}")
        if _file_handler:
            _file_handler.flush()
    except:
        pass  # Don't fail if logging fails
    
    # Skip rate limiting for OPTIONS requests (CORS preflight)
    if request.method == "OPTIONS":
        return await call_next(request)
    
    # Get client IP (consider X-Forwarded-For if behind proxy)
    client_ip = request.client.host if request.client else "unknown"
    if "x-forwarded-for" in request.headers:
        # Get first IP from X-Forwarded-For header
        client_ip = request.headers["x-forwarded-for"].split(",")[0].strip()
    
    logger.debug(
        f"[MIDDLEWARE] Request | "
        f"IP={client_ip} | "
        f"path={request.url.path} | "
        f"method={request.method}"
    )
    
    try:
        # Check rate limit (raises 429 if exceeded)
        # IMPORTANT: This checks BEFORE recording the current request
        # So if minute_count = 60, the 61st request will be blocked
        minute_remaining, hour_remaining = rate_limiter.check_rate_limit(client_ip)
    except HTTPException as e:
        # Rate limit exceeded - return JSONResponse directly (middleware can't raise HTTPException properly)
        logger.warning(f"[MIDDLEWARE] Rate limit exceeded for IP {client_ip}")
        # Calculate hour remaining for headers (even though minute limit was hit)
        now = time.time()
        hour_requests = rate_limiter.hour_requests.get(client_ip, [])
        recent_hour = [t for t in hour_requests if now - t < 3600]
        hour_remaining = max(0, rate_limiter.requests_per_hour - len(recent_hour))
        # Return 429 response with rate limit headers
        response = JSONResponse(
            status_code=429,
            content={"detail": e.detail}
        )
        response.headers["X-RateLimit-Limit-Minute"] = str(rate_limiter.requests_per_minute)
        response.headers["X-RateLimit-Remaining-Minute"] = "0"
        response.headers["X-RateLimit-Limit-Hour"] = str(rate_limiter.requests_per_hour)
        response.headers["X-RateLimit-Remaining-Hour"] = str(hour_remaining)
        logger.debug(
            f"[MIDDLEWARE] Response | "
            f"IP={client_ip} | "
            f"status=429 | "
            f"remaining=0"
        )
        return response
    except Exception as e:
        # Log unexpected errors but don't block the request
        logger.error(f"[MIDDLEWARE] Rate limiting error for IP {client_ip}: {e}", exc_info=True)
        # Continue without rate limiting if there's an error
        minute_remaining, hour_remaining = 60, 1000
    
    # Process request (this may return 401, 200, etc.)
    response = await call_next(request)
    
    # Add rate limit headers to ALL responses (even 401s, 404s, etc.)
    # This allows clients to see their rate limit status regardless of auth status
    response.headers["X-RateLimit-Limit-Minute"] = str(rate_limiter.requests_per_minute)
    response.headers["X-RateLimit-Remaining-Minute"] = str(max(0, minute_remaining))
    response.headers["X-RateLimit-Limit-Hour"] = str(rate_limiter.requests_per_hour)
    response.headers["X-RateLimit-Remaining-Hour"] = str(max(0, hour_remaining))
    
    logger.debug(
        f"[MIDDLEWARE] Response | "
        f"IP={client_ip} | "
        f"status={response.status_code} | "
        f"remaining={minute_remaining}"
    )
    
    return response

