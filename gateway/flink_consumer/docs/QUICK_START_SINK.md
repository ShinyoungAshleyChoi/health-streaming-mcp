# Quick Start: Iceberg Sink

This guide shows you how to quickly get started with the Iceberg sink implementation.

## 1. Basic Setup

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.sink import IcebergSink

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Load settings from .env.local
settings = Settings()

# Create and initialize Iceberg sink
iceberg_sink = IcebergSink(settings, env)
success = iceberg_sink.initialize()

if not success:
    raise RuntimeError("Failed to initialize Iceberg sink")
```

## 2. Write DataStream to Iceberg

```python
# Assuming you have a DataStream of health data
from pyflink.common import Row

# Your data stream (from Kafka, transformation, etc.)
data_stream = env.from_collection([
    Row(
        device_id="device-001",
        user_id="user-001",
        sample_id="sample-001",
        data_type="heartRate",
        value=72.5,
        unit="count/min",
        # ... other fields
    )
])

# Write to Iceberg table
iceberg_sink.write_to_iceberg(
    data_stream=data_stream,
    table_name="health_data_raw",
    overwrite=False  # Append mode
)
```

## 3. Handle Errors with DLQ

```python
from flink_consumer.iceberg.sink import IcebergErrorSink

# Create error sink
error_sink = IcebergErrorSink(settings, iceberg_sink)

# Create error record
error_record = error_sink.create_error_record(
    error_type="validation_error",
    error_message="Missing required field: user_id",
    raw_payload='{"deviceId": "test-123"}',
    device_id="test-123",
    kafka_topic="health-data-raw",
    kafka_partition=0,
    kafka_offset=12345
)

# Write error stream to error table
# error_stream would come from side output
error_sink.write_errors_to_iceberg(error_stream)
```

## 4. Complete Pipeline Example

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.sink import IcebergSink, IcebergErrorSink
from flink_consumer.converters.health_data_transformer import HealthDataTransformer
from flink_consumer.validators.health_data_validator import HealthDataValidator

# Setup
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(6)
settings = Settings()

# Create Kafka source
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers(settings.kafka.brokers) \
    .set_topics(settings.kafka.topic) \
    .set_group_id(settings.kafka.group_id) \
    .build()

# Read from Kafka
raw_stream = env.from_source(kafka_source, "Kafka Source")

# Transform and validate
transformed_stream = raw_stream.flat_map(HealthDataTransformer())
valid_stream = transformed_stream.filter(HealthDataValidator())

# Create Iceberg sink
iceberg_sink = IcebergSink(settings, env)
iceberg_sink.initialize()

# Write to Iceberg
iceberg_sink.write_to_iceberg(
    data_stream=valid_stream,
    table_name=settings.iceberg.table_raw
)

# Execute pipeline
env.execute("Health Data to Iceberg Pipeline")
```

## 5. Configuration

Create `.env.local` file:

```bash
# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=health-data-raw
KAFKA_GROUP_ID=flink-iceberg-consumer

# Schema Registry
SCHEMA_REGISTRY_URL=http://localhost:8081

# Iceberg
ICEBERG_CATALOG_TYPE=rest
ICEBERG_CATALOG_NAME=health_catalog
ICEBERG_CATALOG_URI=http://localhost:8181
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse
ICEBERG_DATABASE=health_db
ICEBERG_TABLE_RAW=health_data_raw
ICEBERG_TABLE_ERRORS=health_data_errors

# S3/MinIO
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_PATH_STYLE_ACCESS=true
S3_BUCKET=data-lake

# Batch Writing
BATCH_SIZE=1000
BATCH_TIMEOUT_SECONDS=10
TARGET_FILE_SIZE_MB=256

# Flink
FLINK_PARALLELISM=6
FLINK_CHECKPOINT_INTERVAL_MS=60000
FLINK_CHECKPOINT_TIMEOUT_MS=600000
FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS=30000
FLINK_MAX_CONCURRENT_CHECKPOINTS=1
FLINK_CHECKPOINT_MODE=EXACTLY_ONCE
FLINK_STATE_BACKEND=rocksdb
FLINK_CHECKPOINT_STORAGE=s3a://flink-checkpoints/health-consumer
```

## 6. Run Tests

```bash
cd flink_consumer
python examples/test_iceberg_sink.py
```

Expected output:
```
================================================================================
ICEBERG SINK TEST SUITE
================================================================================

Test 1: Iceberg Sink Initialization
✓ Iceberg sink initialized successfully
✓ StreamTableEnvironment created

Test 2: Batch Sink Configuration
✓ Batch sink configuration created

Test 3: Error Sink (DLQ)
✓ Error record created
✓ Error metrics logged

Test 4: DataStream to Table Conversion
✓ Sample DataStream created
✓ DataStream converted to Table successfully

================================================================================
TEST SUMMARY
================================================================================
✓ PASSED: Sink Initialization
✓ PASSED: Batch Configuration
✓ PASSED: Error Sink (DLQ)
✓ PASSED: DataStream Conversion
================================================================================
Results: 4/4 tests passed
================================================================================
```

## 7. Monitoring

Check Iceberg tables:

```python
from flink_consumer.iceberg.catalog import IcebergCatalog
from flink_consumer.config.settings import Settings

settings = Settings()
catalog = IcebergCatalog(settings)
catalog.initialize()

# Check if tables exist
print(catalog.table_exists("health_db", "health_data_raw"))
print(catalog.table_exists("health_db", "health_data_errors"))
```

## 8. Troubleshooting

### Issue: Catalog connection failed

```python
# Test catalog connection
from flink_consumer.iceberg.catalog import IcebergCatalog

catalog = IcebergCatalog(settings)
if catalog.test_connection():
    print("✓ Catalog connection successful")
else:
    print("✗ Catalog connection failed")
```

### Issue: Tables not found

```python
# Initialize tables
from flink_consumer.iceberg.table_manager import IcebergTableManager

table_manager = IcebergTableManager(settings, catalog)
table_manager.initialize_all_tables()
```

## Next Steps

1. **Add Checkpointing**: Configure Flink checkpoints for exactly-once semantics
2. **Add Monitoring**: Set up Prometheus metrics and Grafana dashboards
3. **Optimize Performance**: Tune batch size and parallelism
4. **Deploy**: Package as Docker image and deploy to Kubernetes

## Resources

- [Full Documentation](./ICEBERG_SINK.md)
- [Task 5 Summary](./TASK_5_SUMMARY.md)
- [Iceberg Setup Guide](./ICEBERG_SETUP.md)
