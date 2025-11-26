"""
Metrics router
"""
from fastapi import APIRouter, Response
from services.metrics import get_metrics, get_content_type

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def metrics():
    """
    Prometheus metrics endpoint
    
    Returns metrics in Prometheus text format for scraping.
    """
    return Response(
        content=get_metrics(),
        media_type=get_content_type()
    )
