"""
Health Stack Kafka Gateway - Main Application
"""
from fastapi import FastAPI
from slowapi.errors import RateLimitExceeded
from config import settings
from middleware.request_id import RequestIDMiddleware
from middleware.logging import LoggingMiddleware
from middleware.metrics import MetricsMiddleware
from middleware.error_handler import setup_exception_handlers
from middleware.rate_limit import limiter, rate_limit_exceeded_handler
from middleware.cors import setup_cors
from routers import health_data, health, metrics
from utils.logger import setup_logging

# Setup structured logging
setup_logging()

# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    description="API Gateway for health-stack iOS app data processing",
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Setup rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# Setup exception handlers
setup_exception_handlers(app)

# Setup middleware (order matters - first added is outermost)
app.add_middleware(MetricsMiddleware)
app.add_middleware(LoggingMiddleware)
app.add_middleware(RequestIDMiddleware)
setup_cors(app)

# Include routers
app.include_router(health.router)
app.include_router(metrics.router)
app.include_router(health_data.router, prefix=settings.api_prefix)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": settings.app_name,
        "version": settings.app_version,
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    
    # SSL configuration
    ssl_config = {}
    if settings.ssl_enabled:
        ssl_config = {
            "ssl_certfile": settings.ssl_certfile,
            "ssl_keyfile": settings.ssl_keyfile,
        }
    
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
        log_level=settings.log_level.lower(),
        **ssl_config
    )
