"""
Kafka Producer service
"""
import logging
from typing import Optional, Dict, Any
from confluent_kafka import Producer, KafkaError, KafkaException
from config import settings

logger = logging.getLogger(__name__)


class KafkaProducerError(Exception):
    """Kafka producer error"""
    pass


class KafkaProducerService:
    """Service for producing messages to Kafka"""
    
    def __init__(self):
        """Initialize Kafka producer"""
        self.producer: Optional[Producer] = None
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Kafka producer with configuration"""
        try:
            conf = {
                'bootstrap.servers': settings.kafka_brokers,
                'client.id': settings.kafka_client_id,
                
                # Idempotent producer for exactly-once semantics
                'enable.idempotence': True,
                
                # Acknowledgment settings
                'acks': 'all',  # Wait for all in-sync replicas
                
                # Retry settings
                'retries': 3,
                'retry.backoff.ms': 100,
                'max.in.flight.requests.per.connection': 5,
                
                # Compression
                'compression.type': 'snappy',
                
                # Batch settings for performance
                'batch.size': 16384,  # 16KB
                'linger.ms': 10,  # Wait up to 10ms to batch messages
                
                # Timeout settings
                'request.timeout.ms': 30000,
                'delivery.timeout.ms': 120000,
                
                # Buffer settings
                'queue.buffering.max.messages': 100000,
                'queue.buffering.max.kbytes': 1048576,  # 1GB
            }
            
            self.producer = Producer(conf)
            logger.info("Kafka producer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise KafkaProducerError(f"Producer initialization failed: {e}")
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(
                f"Message delivery failed: {err} "
                f"(topic={msg.topic()}, partition={msg.partition()})"
            )
        else:
            logger.debug(
                f"Message delivered: topic={msg.topic()}, "
                f"partition={msg.partition()}, offset={msg.offset()}"
            )
    
    async def send(
        self,
        topic: str,
        value: bytes,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        retry: bool = True
    ) -> Dict[str, Any]:
        """
        Send a message to Kafka with retry logic
        
        Args:
            topic: Kafka topic name
            value: Message value (bytes)
            key: Message key (optional)
            headers: Message headers (optional)
            retry: Enable retry logic (default: True)
            
        Returns:
            Dictionary with send result
            
        Raises:
            KafkaProducerError: If send fails after retries
        """
        if self.producer is None:
            raise KafkaProducerError("Producer not initialized")
        
        async def _send_message():
            try:
                # Convert headers to list of tuples if provided
                kafka_headers = None
                if headers:
                    kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
                
                # Produce message
                self.producer.produce(
                    topic=topic,
                    value=value,
                    key=key.encode('utf-8') if key else None,
                    headers=kafka_headers,
                    callback=self._delivery_callback
                )
                
                # Trigger delivery reports
                self.producer.poll(0)
                
                return {
                    "status": "sent",
                    "topic": topic,
                    "key": key
                }
                
            except BufferError as e:
                # Queue is full, need to flush
                logger.warning("Producer queue full, flushing...")
                self.producer.flush(timeout=10)
                raise KafkaProducerError(f"Producer queue full: {e}")
                
            except KafkaException as e:
                logger.error(f"Kafka error while sending message: {e}")
                raise KafkaProducerError(f"Failed to send message: {e}")
                
            except Exception as e:
                logger.error(f"Unexpected error while sending message: {e}")
                raise KafkaProducerError(f"Unexpected error: {e}")
        
        if retry:
            # Import here to avoid circular dependency
            from services.retry_handler import retry_handler
            return await retry_handler.execute_with_retry(_send_message)
        else:
            return await _send_message()
    
    def flush(self, timeout: float = 10.0) -> int:
        """
        Flush pending messages
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Number of messages still in queue
        """
        if self.producer is None:
            return 0
        
        try:
            remaining = self.producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning(f"{remaining} messages still in queue after flush")
            return remaining
        except Exception as e:
            logger.error(f"Error during flush: {e}")
            return -1
    
    def close(self):
        """Close the producer and flush remaining messages"""
        if self.producer is not None:
            logger.info("Closing Kafka producer...")
            self.flush(timeout=30)
            self.producer = None
    
    def __del__(self):
        """Cleanup on deletion"""
        self.close()


# Singleton instance
kafka_producer_service = KafkaProducerService()
