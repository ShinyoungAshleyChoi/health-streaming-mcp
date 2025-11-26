# Iceberg Sink Implementation

This document describes the Iceberg sink implementation for writing PyFlink DataStreams to Apache Iceberg tables.

## Overview

The Iceberg sink provides a bridge between PyFlink's DataStream API and Apache Iceberg tables, enabling:

- **PyFlink Table API Integration**: Seamless conversion between DataStream and Table APIs
- **Batch Writing Strategy**: Optimized buffering and file size management
- **Error Handling**: Dead Letter Queue (DLQ) pattern for failed records
- **Exactly-Once Semantics**: Full end-to-end exactly-once guarantee through Flink checkpoints and Iceberg's 2-phase commit

## Exactly-Once Guarantee

The sink implements **exactly-once semantics** for the complete Kafka → Flink → Iceberg pipeline:

### How It Works

1. **Kafka Source**: Offsets are managed by Flink checkpoints (not auto-committed)
2. **Flink Processing**: State is checkpointed to RocksDB with exactly-once mode
3. **Iceberg Sink**: Data is buffered and only committed when checkpoint completes successfully
4. **2-Phase Commit**: Iceberg uses transaction protocol to ensure atomic commits

### Configuration

The following settings ensure exactly-once semantics:

```yaml
# Flink checkpoint configuration
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.interval: 60s

# Kafka source configuration
enable.auto.commit: false  # Flink manages offsets

# Iceberg sink configuration
write.upsert.enabled: false  # Append-only mode
write.distribution-mode: none  # No shuffle for consistency
```

### Failure Recovery

In case of failure:
- Flink restarts from last successful checkpoint
- Kafka offsets are restored to checkpoint position
- Iceberg uncommitted data is discarded
- Processing resumes without duplicates

**Result**: Each record is processed and written to Iceberg exactly once, even with failures.

## Architecture

```
┌─────────────────┐
│  DataStream     │
│  (Health Data)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  IcebergSink    │
│  - Register     │
│    Catalog      │
│  - Convert to   │
│    Table        │
│  - Batch Write  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Iceberg Table  │
│  (Parquet)      │
└─────────────────┘
```

## Components

### 1. IcebergSink

Main sink class for writing data to Iceberg tables.

**Key Features:**
- Catalog registration and management
- DataStream to Table conversion
- Batch writing with configurable buffering
- File size optimization (128-512MB)

**Usage:**

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.sink import IcebergSink

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Load settings
settings = Settings()

# Create and initialize sink
iceberg_sink = IcebergSink(settings, env)
iceberg_sink.initialize()

# Write DataStream to Iceberg
iceberg_sink.write_to_iceberg(
    data_stream=my_data_stream,
    table_name="health_data_raw",
    overwrite=False
)
```

### 2. IcebergErrorSink

Specialized sink for error records (Dead Letter Queue pattern).

**Key Features:**
- Error record creation with metadata
- Separate error table for debugging
- Error metrics logging

**Usage:**

```python
from flink_consumer.iceberg.sink import IcebergErrorSink

# Create error sink
error_sink = IcebergErrorSink(settings, iceberg_sink)

# Create error record
error_record = error_sink.create_error_record(
    error_type="validation_error",
    error_message="Missing required field: user_id",
    raw_payload=original_payload,
    kafka_topic="health-data-raw",
    kafka_partition=0,
    kafka_offset=12345
)

# Write errors to Iceberg
error_sink.write_errors_to_iceberg(error_stream)
```

## Configuration

### Batch Writing Settings

Configure buffering and file size optimization in `.env.local`:

```bash
# Batch writing configuration
BATCH_SIZE=1000                    # Buffer up to 1000 records
BATCH_TIMEOUT_SECONDS=10           # Or flush after 10 seconds
TARGET_FILE_SIZE_MB=256            # Target Parquet file size (128-512MB)
```

### Catalog Settings

```bash
# Iceberg catalog configuration
ICEBERG_CATALOG_TYPE=rest
ICEBERG_CATALOG_NAME=health_catalog
ICEBERG_CATALOG_URI=http://iceberg-rest:8181
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse
ICEBERG_DATABASE=health_db
ICEBERG_TABLE_RAW=health_data_raw
ICEBERG_TABLE_ERRORS=health_data_errors
```

### S3/MinIO Settings

```bash
# S3/MinIO configuration
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_PATH_STYLE_ACCESS=true
S3_BUCKET=data-lake
```

## Batch Writing Strategy

The sink implements an optimized batch writing strategy:

### Buffering

Records are buffered before writing to minimize small files:

- **Buffer Size**: 1000 records (configurable)
- **Buffer Timeout**: 10 seconds (configurable)
- **Trigger**: Whichever comes first

### File Size Optimization

Parquet files are written with optimal sizes for query performance:

- **Target Size**: 256MB (configurable: 128-512MB)
- **Page Size**: 1MB
- **Row Group Size**: 128MB
- **Compression**: Snappy

### Commit Strategy

- **Retry Count**: 3 attempts
- **Retry Delay**: 100ms minimum
- **Coordination**: Aligned with Flink checkpoints

## Error Handling

### Error Record Schema

Error records include comprehensive metadata for debugging:

```python
{
    "error_id": "uuid",
    "error_type": "validation_error | deserialization_error | ...",
    "error_message": "Detailed error description",
    "error_timestamp": "2025-11-16T10:00:00Z",
    "raw_payload": "Original payload string",
    "user_id": "user-123",
    "device_id": "device-456",
    "sample_id": "sample-789",
    "data_type": "heartRate",
    "kafka_topic": "health-data-raw",
    "kafka_partition": 0,
    "kafka_offset": 12345
}
```

### Error Types

Common error types:

- `validation_error`: Data validation failures
- `deserialization_error`: Avro deserialization failures
- `transformation_error`: Data transformation failures
- `schema_error`: Schema mismatch errors

## Integration with Flink Pipeline

### Complete Pipeline Example

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.sink import IcebergSink, IcebergErrorSink
from flink_consumer.services.kafka_source import create_kafka_source
from flink_consumer.converters.health_data_transformer import HealthDataTransformer
from flink_consumer.validators.health_data_validator import HealthDataValidator

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
settings = Settings()

# Create Kafka source
kafka_source = create_kafka_source(settings)
raw_stream = env.from_source(kafka_source, "Kafka Source")

# Transform and validate
transformed_stream = raw_stream.flat_map(HealthDataTransformer())
valid_stream = transformed_stream.filter(HealthDataValidator())

# Create Iceberg sink
iceberg_sink = IcebergSink(settings, env)
iceberg_sink.initialize()

# Write valid data to Iceberg
iceberg_sink.write_to_iceberg(
    data_stream=valid_stream,
    table_name=settings.iceberg.table_raw
)

# Handle errors
error_sink = IcebergErrorSink(settings, iceberg_sink)
# error_stream would come from side output
# error_sink.write_errors_to_iceberg(error_stream)

# Execute
env.execute("Health Data to Iceberg Pipeline")
```

## Performance Considerations

### Parallelism

Set appropriate parallelism for sink operations:

```python
# Configure parallelism
env.set_parallelism(6)  # Match Kafka partition count

# Or set per operator
valid_stream.set_parallelism(12)  # Higher for transformation
```

### Checkpoint Coordination

Iceberg commits are coordinated with Flink checkpoints for exactly-once guarantee:

```python
# Enable checkpointing with exactly-once mode
env.enable_checkpointing(60000)  # 60 seconds

# Configure checkpoint mode
checkpoint_config = env.get_checkpoint_config()
checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# Configure externalized checkpoints for recovery
checkpoint_config.enable_externalized_checkpoints(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
)
```

**Important**: Data is buffered in Flink state and only committed to Iceberg when the checkpoint completes successfully. This ensures exactly-once semantics even with failures.

### Memory Management

Monitor memory usage for buffering:

- **TaskManager Memory**: 4GB recommended
- **Managed Memory**: 1.5GB for state backend
- **Network Buffers**: 512MB

## Monitoring

### Metrics

Key metrics to monitor:

- `numRecordsOut`: Records written to Iceberg
- `checkpointDuration`: Time to complete checkpoint
- `lastCheckpointSize`: Checkpoint size in bytes
- `numRecordsFailed`: Failed writes

### Logging

Error sink logs include structured metadata:

```python
logger.warning(
    "Error recorded: validation_error",
    extra={
        "error_type": "validation_error",
        "count": 1,
        "table": "health_data_errors"
    }
)
```

## Testing

Run the test suite to verify sink functionality:

```bash
cd flink_consumer
python examples/test_iceberg_sink.py
```

Tests cover:
1. Sink initialization and catalog registration
2. Batch configuration
3. Error sink (DLQ pattern)
4. DataStream to Table conversion

## Troubleshooting

### Common Issues

**1. Catalog Connection Failed**

```
Error: Failed to register Iceberg catalog
```

**Solution**: Verify catalog URI and credentials:
- Check `ICEBERG_CATALOG_URI` is accessible
- Verify S3 credentials are correct
- Ensure network connectivity to catalog service

**2. Table Not Found**

```
Error: Table does not exist: health_db.health_data_raw
```

**Solution**: Initialize tables first:
```python
from flink_consumer.iceberg.table_manager import IcebergTableManager

table_manager = IcebergTableManager(settings, catalog)
table_manager.initialize_all_tables()
```

**3. Small File Problem**

```
Warning: Too many small files in Iceberg table
```

**Solution**: Adjust batch settings:
- Increase `BATCH_SIZE` (e.g., 5000)
- Increase `BATCH_TIMEOUT_SECONDS` (e.g., 30)
- Increase `TARGET_FILE_SIZE_MB` (e.g., 512)

**4. Memory Issues**

```
Error: OutOfMemoryError during checkpoint
```

**Solution**: Increase TaskManager memory:
- Increase heap size: 4GB → 8GB
- Increase managed memory: 1.5GB → 3GB
- Reduce parallelism if needed

## Best Practices

1. **Initialize Once**: Create sink instance once and reuse
2. **Buffer Appropriately**: Balance latency vs. file size
3. **Monitor Errors**: Set up alerts on error table growth
4. **Checkpoint Regularly**: 60-second intervals recommended
5. **Test Thoroughly**: Use test suite before production deployment

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [PyFlink Table API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table_api_tutorial/)
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/)
