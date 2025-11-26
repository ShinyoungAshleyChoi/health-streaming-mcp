"""
Health check router
"""
from fastapi import APIRouter
from services.health_check import health_check_service

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    """
    Health check endpoint
    
    Returns the health status of the gateway and its dependencies:
    - Kafka broker connectivity
    - Schema Registry connectivity
    """
    return await health_check_service.get_health_status()


@router.get("/ping")
async def ping():
    """Simple ping endpoint for basic availability check"""
    return {"status": "ok"}
