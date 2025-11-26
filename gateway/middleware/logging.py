"""
Logging middleware
"""
import time
import logging
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for logging HTTP requests and responses"""
    
    async def dispatch(self, request: Request, call_next):
        """Log request and response details"""
        
        # Get request ID
        request_id = getattr(request.state, 'request_id', 'unknown')
        
        # Start timer
        start_time = time.time()
        
        # Log request
        logger.info(
            f"Request started",
            extra={
                'request_id': request_id,
                'method': request.method,
                'path': request.url.path,
                'client_ip': request.client.host if request.client else None
            }
        )
        
        # Process request
        try:
            response = await call_next(request)
            
            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Log response
            logger.info(
                f"Request completed",
                extra={
                    'request_id': request_id,
                    'method': request.method,
                    'path': request.url.path,
                    'status_code': response.status_code,
                    'duration': duration_ms
                }
            )
            
            return response
            
        except Exception as e:
            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Log error
            logger.error(
                f"Request failed: {str(e)}",
                extra={
                    'request_id': request_id,
                    'method': request.method,
                    'path': request.url.path,
                    'duration': duration_ms,
                    'error': str(e)
                },
                exc_info=True
            )
            
            raise
