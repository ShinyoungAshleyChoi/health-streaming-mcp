"""
Health check service
"""
import time
from typing import Dict, Any
from confluent_kafka import Producer
from config import settings


class HealthCheckService:
    """Service for checking system health"""
    
    def __init__(self):
        self.kafka_producer = None
    
    async def check_kafka(self) -> Dict[str, Any]:
        """Check Kafka broker connectivity"""
        try:
            start_time = time.time()
            
            # Create a temporary producer to test connection
            conf = {
                'bootstrap.servers': settings.kafka_brokers,
                'client.id': f'{settings.kafka_client_id}-health-check',
                'socket.timeout.ms': 5000,
            }
            
            producer = Producer(conf)
            
            # Get cluster metadata to verify connection
            metadata = producer.list_topics(timeout=5)
            
            latency_ms = int((time.time() - start_time) * 1000)
            
            producer.flush(timeout=1)
            
            return {
                "status": "up",
                "latency": f"{latency_ms}ms",
                "brokers": len(metadata.brokers),
                "topics": len(metadata.topics)
            }
            
        except Exception as e:
            return {
                "status": "down",
                "error": str(e)
            }
    
    async def check_schema_registry(self) -> Dict[str, Any]:
        """Check Schema Registry connectivity"""
        try:
            import httpx
            start_time = time.time()
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{settings.schema_registry_url}/subjects",
                    timeout=5.0
                )
                
                latency_ms = int((time.time() - start_time) * 1000)
                
                if response.status_code == 200:
                    subjects = response.json()
                    return {
                        "status": "up",
                        "latency": f"{latency_ms}ms",
                        "subjects": len(subjects)
                    }
                else:
                    return {
                        "status": "down",
                        "error": f"HTTP {response.status_code}"
                    }
                    
        except Exception as e:
            return {
                "status": "down",
                "error": str(e)
            }
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status"""
        kafka_status = await self.check_kafka()
        schema_registry_status = await self.check_schema_registry()
        
        # Determine overall status
        all_up = (
            kafka_status.get("status") == "up" and
            schema_registry_status.get("status") == "up"
        )
        
        return {
            "status": "healthy" if all_up else "degraded",
            "checks": {
                "kafka": kafka_status,
                "schemaRegistry": schema_registry_status
            }
        }


# Singleton instance
health_check_service = HealthCheckService()
