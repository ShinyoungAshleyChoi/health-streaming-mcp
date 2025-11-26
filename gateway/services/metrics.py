"""
Prometheus metrics service
"""
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST


# Request metrics
request_count = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

request_duration = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

# Health data metrics
health_data_received = Counter(
    'health_data_received_total',
    'Total health data payloads received',
    ['user_id']
)

health_data_samples = Counter(
    'health_data_samples_total',
    'Total health data samples received',
    ['data_type']
)

# Kafka metrics
kafka_messages_sent = Counter(
    'kafka_messages_sent_total',
    'Total messages sent to Kafka',
    ['topic', 'status']
)

kafka_send_duration = Histogram(
    'kafka_send_duration_seconds',
    'Kafka message send duration in seconds',
    ['topic']
)

kafka_producer_errors = Counter(
    'kafka_producer_errors_total',
    'Total Kafka producer errors',
    ['error_type']
)

# DLQ metrics
dlq_messages = Counter(
    'dlq_messages_total',
    'Total messages sent to Dead Letter Queue',
    ['error_type']
)

# Validation metrics
validation_errors = Counter(
    'validation_errors_total',
    'Total validation errors',
    ['error_type']
)

# Avro conversion metrics
avro_conversion_errors = Counter(
    'avro_conversion_errors_total',
    'Total Avro conversion errors'
)

# Schema Registry metrics
schema_registry_requests = Counter(
    'schema_registry_requests_total',
    'Total Schema Registry requests',
    ['operation', 'status']
)

# Active connections
active_connections = Gauge(
    'active_connections',
    'Number of active connections'
)


def get_metrics() -> bytes:
    """Get Prometheus metrics in text format"""
    return generate_latest()


def get_content_type() -> str:
    """Get Prometheus metrics content type"""
    return CONTENT_TYPE_LATEST
