"""
Rate limiting middleware
"""
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request, Response
from fastapi.responses import JSONResponse


def get_client_identifier(request: Request) -> str:
    """
    Get client identifier for rate limiting
    
    Uses X-Forwarded-For header if available, otherwise remote address
    """
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return get_remote_address(request)


# Create limiter instance
limiter = Limiter(
    key_func=get_client_identifier,
    default_limits=["1000/minute"],  # Default: 1000 requests per minute
    storage_uri="memory://",  # In-memory storage (use Redis for production)
)


async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> Response:
    """Handle rate limit exceeded errors"""
    request_id = getattr(request.state, 'request_id', 'unknown')
    
    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "code": 429,
                "message": "Rate limit exceeded. Please try again later.",
                "requestId": request_id,
                "retryAfter": exc.detail
            }
        },
        headers={
            "Retry-After": str(exc.detail)
        }
    )
