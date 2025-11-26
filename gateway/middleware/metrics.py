"""
Metrics middleware for tracking HTTP requests
"""
import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from services.metrics import request_count, request_duration, active_connections


class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for collecting HTTP request metrics"""
    
    async def dispatch(self, request: Request, call_next):
        """Track request metrics"""
        
        # Increment active connections
        active_connections.inc()
        
        # Start timer
        start_time = time.time()
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Record metrics
            request_count.labels(
                method=request.method,
                endpoint=request.url.path,
                status=response.status_code
            ).inc()
            
            request_duration.labels(
                method=request.method,
                endpoint=request.url.path
            ).observe(duration)
            
            return response
            
        finally:
            # Decrement active connections
            active_connections.dec()
