"""
Dead Letter Queue handler
"""
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from config import settings

logger = logging.getLogger(__name__)


class DLQHandler:
    """Handler for Dead Letter Queue operations"""
    
    def __init__(self, kafka_producer):
        """
        Initialize DLQ handler
        
        Args:
            kafka_producer: KafkaProducerService instance
        """
        self.kafka_producer = kafka_producer
        self.dlq_topic = settings.kafka_dlq_topic
    
    async def send_to_dlq(
        self,
        original_message: bytes,
        error_type: str,
        error_message: str,
        retry_count: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send failed message to Dead Letter Queue
        
        Args:
            original_message: Original message that failed
            error_type: Type of error
            error_message: Error message
            retry_count: Number of retries attempted
            metadata: Additional metadata
            
        Returns:
            True if successfully sent to DLQ, False otherwise
        """
        try:
            dlq_payload = {
                "originalMessage": original_message.hex(),  # Convert bytes to hex string
                "error": {
                    "type": error_type,
                    "message": error_message,
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                },
                "retryCount": retry_count,
                "metadata": metadata or {},
                "dlqTimestamp": datetime.utcnow().isoformat() + "Z"
            }
            
            # Convert to JSON bytes
            dlq_message = json.dumps(dlq_payload).encode('utf-8')
            
            # Send to DLQ topic
            await self.kafka_producer.send(
                topic=self.dlq_topic,
                value=dlq_message,
                headers={
                    "error-type": error_type,
                    "retry-count": str(retry_count)
                }
            )
            
            logger.info(
                f"Message sent to DLQ: error_type={error_type}, "
                f"retry_count={retry_count}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
            return False
