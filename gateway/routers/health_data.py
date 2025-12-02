"""
Health Data API Router
"""
import logging
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException
from gateway.middleware.rate_limit import limiter
from gateway.models.health_data import HealthDataPayload, HealthDataResponse
from gateway.validators.health_data_validator import HealthDataValidator, ValidationError
from gateway.converters.avro_converter import avro_converter, AvroConversionError
from gateway.services.schema_registry import schema_registry_client, SchemaRegistryError
from gateway.services.kafka_producer import kafka_producer_service, KafkaProducerError
from gateway.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health-data", tags=["health-data"])


@router.post("/", response_model=HealthDataResponse)
@limiter.limit("1000/minute")  # 1000 requests per minute per client
async def receive_health_data(request: Request, payload: HealthDataPayload):
    """
    Receive health data from iOS app
    
    Validates and processes health data samples from the health-stack iOS app.
    Converts to Avro format and sends to Kafka.
    """
    request_id = getattr(request.state, 'request_id', 'unknown')
    
    try:
        # 1. Sanitize input
        payload = HealthDataValidator.sanitize(payload)
        
        # 2. Validate payload
        # payload = HealthDataValidator.validate(payload)
        
        logger.info(
            f"Received health data: request_id={request_id}, "
            f"user_id={payload.userId}, samples={len(payload.samples)}"
        )
        
        # Track metrics
        from gateway.services.metrics import health_data_received, health_data_samples
        health_data_received.labels(user_id=payload.userId).inc()
        for sample in payload.samples:
            health_data_samples.labels(data_type=sample.type).inc()
        
        # 3. Convert to Avro
        try:
            avro_data = avro_converter.convert(payload)
            logger.debug(f"Converted to Avro: {len(avro_data)} bytes")
        except AvroConversionError as e:
            logger.error(f"Avro conversion failed: {e}")
            from gateway.services.metrics import avro_conversion_errors
            avro_conversion_errors.inc()
            raise HTTPException(
                status_code=500,
                detail={"message": "Failed to convert data to Avro format", "error": str(e)}
            )
        
        # 4. Get or register schema with Schema Registry
        subject = f"{settings.kafka_topic}-value"
        try:
            schema_info = await schema_registry_client.get_latest_schema(subject)
            schema_id = schema_info["id"]
            logger.debug(f"Using schema ID: {schema_id}")
        except SchemaRegistryError as e:
            # Try to register schema if not found
            logger.warning(f"Schema not found, attempting to register: {e}")
            try:
                schema = avro_converter.get_schema()
                schema_id = await schema_registry_client.register_schema(subject, schema)
                logger.info(f"Registered new schema with ID: {schema_id}")
            except SchemaRegistryError as reg_error:
                logger.error(f"Failed to register schema: {reg_error}")
                raise HTTPException(
                    status_code=500,
                    detail={"message": "Schema Registry error", "error": str(reg_error)}
                )
        
        # 5. Encode message with schema ID
        encoded_message = schema_registry_client.encode_message(schema_id, avro_data)
        
        # 6. Send to Kafka with retry
        try:
            from gateway.services.metrics import kafka_messages_sent
            await kafka_producer_service.send(
                topic=settings.kafka_topic,
                value=encoded_message,
                key=payload.userId,
                headers={
                    "request-id": request_id,
                    "device-id": payload.deviceId,
                    "app-version": payload.appVersion,
                    "sample-count": str(len(payload.samples))
                }
            )
            
            kafka_messages_sent.labels(topic=settings.kafka_topic, status="success").inc()
            
            logger.info(
                f"Successfully sent to Kafka: request_id={request_id}, "
                f"topic={settings.kafka_topic}"
            )
            
        except KafkaProducerError as e:
            logger.error(f"Failed to send to Kafka after retries: {e}")
            
            from gateway.services.metrics import kafka_messages_sent, kafka_producer_errors
            kafka_messages_sent.labels(topic=settings.kafka_topic, status="failed").inc()
            kafka_producer_errors.labels(error_type="KafkaProducerError").inc()
            
            raise HTTPException(
                status_code=500,
                detail={"message": "Failed to send message to Kafka", "error": str(e)}
            )
         
        
        # 7. Return success response
        return HealthDataResponse(
            status="success",
            requestId=request_id,
            timestamp=datetime.utcnow().isoformat() + "Z",
            samplesReceived=len(payload.samples)
        )
        
    except ValidationError as e:
        logger.warning(f"Validation error: request_id={request_id}, error={e.message}")
        raise HTTPException(
            status_code=422,
            detail={
                "message": e.message,
                "errors": e.errors
            }
        )
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error: request_id={request_id}, error={e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"message": "Internal server error", "error": str(e)}
        )
