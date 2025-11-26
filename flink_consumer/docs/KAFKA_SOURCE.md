# Kafka Source Connector Documentation

## Overview

The Kafka source connector provides integration between Apache Kafka and Apache Flink, enabling real-time consumption of health data messages with Avro deserialization and Schema Registry support.

## Components

### 1. Avro Deserializer (`converters/avro_deserializer.py`)

#### AvroDeserializationSchema

Custom PyFlink deserialization schema that integrates with Confluent Schema Registry.

**Features:**
- Automatic schema fetching from Schema Registry
- Avro message deserialization
- Error handling for corrupted messages
- Logging and monitoring support

**Usage:**
```python
from flink_consumer.converters import AvroDeserializationSchema

# Create deserializer
deserializer = AvroDeserializationSchema(
    schema_registry_url="http://schema-registry:8081",
    subject_name="health-data-raw-value"
)

# Use with Kafka source
kafka_source.set_value_only_deserializer(deserializer)
```

#### SchemaRegistryManager

Utility class for Schema Registry operations.

**Features:**
- Schema lookup by subject or ID
- Schema version management
- Compatibility checking

**Usage:**
```python
from flink_consumer.converters import SchemaRegistryManager

manager = SchemaRegistryManager("http://schema-registry:8081")
manager.connect()

# Get latest schema
schema_info = manager.get_latest_schema("health-data-raw-value")
print(f"Schema version: {schema_info['version']}")

# Check compatibility
is_compatible = manager.check_compatibility("health-data-raw-value", new_schema)
```

### 2. Kafka Source Builder (`services/kafka_source.py`)

#### KafkaSourceBuilder

Builder class for creating configured Kafka source connectors.

**Features:**
- Bootstrap server configuration
- Topic subscription
- Consumer group management
- Offset initialization strategies (earliest, latest, committed)
- Partition discovery
- Security configuration (SASL/SSL)
- Checkpoint-based offset management

**Usage:**
```python
from flink_consumer.services import KafkaSourceBuilder
from flink_consumer.config.settings import Settings

settings = Settings()

# Create builder
builder = KafkaSourceBuilder(
    kafka_settings=settings.kafka,
    schema_registry_settings=settings.schema_registry
)

# Build source
kafka_source = builder.build()
```

#### Convenience Functions

**create_kafka_source:**
```python
from flink_consumer.services import create_kafka_source

kafka_source = create_kafka_source(
    kafka_settings=settings.kafka,
    schema_registry_settings=settings.schema_registry
)
```

**add_kafka_source_to_env:**
```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.services import add_kafka_source_to_env

env = StreamExecutionEnvironment.get_execution_environment()

data_stream = add_kafka_source_to_env(
    env=env,
    kafka_settings=settings.kafka,
    schema_registry_settings=settings.schema_registry,
    source_name="Health Data Source"
)
```

### 3. Kafka Utilities (`utils/kafka_utils.py`)

#### KafkaConnectionTester

Utility class for testing Kafka connectivity and configuration.

**Features:**
- Broker connection testing
- Topic existence verification
- Partition information retrieval
- Consumer connection testing
- Consumer lag monitoring

**Usage:**
```python
from flink_consumer.utils import KafkaConnectionTester

tester = KafkaConnectionTester("kafka-broker:9092")

# Test broker connection
if tester.test_broker_connection():
    print("Connected to Kafka")

# Check topic exists
if tester.check_topic_exists("health-data-raw"):
    print("Topic exists")

# Get partition count
partition_count = tester.get_topic_partitions("health-data-raw")
print(f"Partitions: {partition_count}")

# Test consumer
if tester.test_consumer_connection("health-data-raw", "my-group"):
    print("Consumer can connect")

# Get consumer lag
lag_info = tester.get_consumer_lag("health-data-raw", "my-group")
print(f"Lag: {lag_info}")
```

#### validate_kafka_config

Convenience function to validate complete Kafka configuration.

**Usage:**
```python
from flink_consumer.utils import validate_kafka_config

is_valid = validate_kafka_config(
    bootstrap_servers="kafka-broker:9092",
    topic="health-data-raw",
    group_id="flink-consumer",
    auto_offset_reset="earliest"
)
```

## Configuration

### Environment Variables

Required Kafka configuration (`.env.local`):

```bash
# Kafka brokers
KAFKA_BROKERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092

# Topic to consume
KAFKA_TOPIC=health-data-raw

# Consumer group ID
KAFKA_GROUP_ID=flink-iceberg-consumer

# Offset reset strategy (earliest, latest, committed)
KAFKA_AUTO_OFFSET_RESET=earliest

# Partition discovery interval (milliseconds)
KAFKA_PARTITION_DISCOVERY_INTERVAL_MS=60000

# Schema Registry URL
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

Optional security configuration:

```bash
# Security protocol (PLAINTEXT, SASL_SSL, etc.)
KAFKA_SECURITY_PROTOCOL=SASL_SSL

# SASL mechanism (SCRAM-SHA-512, PLAIN, etc.)
KAFKA_SASL_MECHANISM=SCRAM-SHA-512

# SASL credentials
KAFKA_SASL_USERNAME=flink-consumer
KAFKA_SASL_PASSWORD=your-password-here
```

### Settings Classes

Configuration is managed through Pydantic settings classes:

```python
from flink_consumer.config.settings import Settings

settings = Settings()

# Access Kafka settings
print(settings.kafka.brokers)
print(settings.kafka.topic)
print(settings.kafka.group_id)

# Access Schema Registry settings
print(settings.schema_registry.url)
```

## Testing

### Running the Test Suite

A comprehensive test script is provided to validate the Kafka source configuration:

```bash
cd flink_consumer
python examples/test_kafka_source.py
```

The test suite validates:
1. Kafka broker connectivity
2. Topic existence and partition information
3. Consumer group configuration
4. Kafka source builder functionality
5. Schema Registry connectivity

### Manual Testing

Test individual components:

```python
# Test broker connection
from flink_consumer.utils import KafkaConnectionTester

tester = KafkaConnectionTester("kafka-broker:9092")
tester.test_broker_connection()

# Test Schema Registry
from flink_consumer.converters import SchemaRegistryManager

manager = SchemaRegistryManager("http://schema-registry:8081")
manager.connect()
schema = manager.get_latest_schema("health-data-raw-value")
```

## Integration with Flink Pipeline

### Basic Example

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.config.settings import Settings
from flink_consumer.services import add_kafka_source_to_env

# Initialize environment
env = StreamExecutionEnvironment.get_execution_environment()
settings = Settings()

# Add Kafka source
data_stream = add_kafka_source_to_env(
    env=env,
    kafka_settings=settings.kafka,
    schema_registry_settings=settings.schema_registry,
    source_name="Health Data Kafka Source"
)

# Process stream
data_stream.print()

# Execute
env.execute("Kafka Source Test")
```

### Advanced Example with Watermarks

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Duration
from flink_consumer.services import create_kafka_source

env = StreamExecutionEnvironment.get_execution_environment()
settings = Settings()

# Create Kafka source
kafka_source = create_kafka_source(
    kafka_settings=settings.kafka,
    schema_registry_settings=settings.schema_registry
)

# Define watermark strategy for event time processing
watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
    .with_timestamp_assigner(lambda event, timestamp: event['timestamp'])

# Add source with watermarks
data_stream = env.from_source(
    kafka_source,
    watermark_strategy=watermark_strategy,
    source_name="Health Data Source"
)

# Continue processing...
```

## Offset Management

### Checkpoint-Based Offsets

The Kafka source uses Flink's checkpoint mechanism for offset management:

- **Auto-commit disabled**: Offsets are not committed to Kafka automatically
- **Checkpoint-based commits**: Offsets are committed only when checkpoints complete
- **Exactly-once semantics**: Ensures no data loss or duplication on failure recovery

### Offset Reset Strategies

Configure via `KAFKA_AUTO_OFFSET_RESET`:

- **earliest**: Start from the beginning of the topic (default)
- **latest**: Start from the latest messages
- **committed**: Start from last committed offset, fallback to earliest

## Partition Discovery

The connector automatically discovers new partitions:

- **Discovery interval**: Configurable via `KAFKA_PARTITION_DISCOVERY_INTERVAL_MS`
- **Default**: 60 seconds
- **Behavior**: New partitions are automatically assigned to consumers

## Error Handling

### Deserialization Errors

- Corrupted Avro messages are logged and skipped
- Schema mismatches are logged with details
- Processing continues with valid messages

### Connection Errors

- Automatic reconnection to Kafka brokers
- Configurable retry strategies
- Failure triggers Flink restart strategy

### Schema Registry Errors

- Schema fetch failures are logged
- Application fails fast if Schema Registry is unavailable
- Ensures data consistency

## Monitoring

### Metrics

The Kafka source exposes standard Flink metrics:

- `numRecordsIn`: Number of records consumed
- `numRecordsInPerSecond`: Consumption rate
- `currentInputWatermark`: Current watermark value
- `kafkaCommittedOffsets`: Committed offset positions

### Logging

Structured logging is provided for:

- Connection events
- Configuration details
- Deserialization errors
- Schema Registry operations
- Partition assignments

## Troubleshooting

### Common Issues

**1. Cannot connect to Kafka brokers**
```
Solution: Verify KAFKA_BROKERS configuration and network connectivity
Test: python examples/test_kafka_source.py
```

**2. Topic not found**
```
Solution: Ensure topic exists and is spelled correctly
Test: Use KafkaConnectionTester.check_topic_exists()
```

**3. Schema Registry connection failed**
```
Solution: Verify SCHEMA_REGISTRY_URL and Schema Registry is running
Test: Use SchemaRegistryManager.connect()
```

**4. No messages being consumed**
```
Solution: Check offset reset strategy and topic has messages
Test: Use KafkaConnectionTester.get_consumer_lag()
```

**5. Deserialization errors**
```
Solution: Verify schema compatibility and message format
Check: Schema Registry for schema versions
```

## Performance Tuning

### Parallelism

Set parallelism to match Kafka partition count:

```python
env.set_parallelism(6)  # For 6 Kafka partitions
```

### Partition Assignment

- Optimal: 1-2 Flink tasks per Kafka partition
- Max parallelism: Number of Kafka partitions

### Batch Size

Configure consumer fetch settings:

```python
kafka_source_builder.set_property("fetch.min.bytes", "1024")
kafka_source_builder.set_property("fetch.max.wait.ms", "500")
```

## Security

### SASL/SSL Configuration

Enable security in `.env.local`:

```bash
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=flink-consumer
KAFKA_SASL_PASSWORD=secure-password
```

The connector automatically configures JAAS for SASL authentication.

### Best Practices

1. Store credentials in environment variables or secrets management
2. Use SSL/TLS for production deployments
3. Rotate credentials regularly
4. Use dedicated service accounts for Flink consumers

## Next Steps

After implementing the Kafka source connector:

1. Implement data transformation pipeline (Task 3)
2. Add data validation logic (Task 3.2)
3. Implement Iceberg sink (Task 5)
4. Configure checkpointing (Task 6)
5. Add monitoring and metrics (Task 7)
