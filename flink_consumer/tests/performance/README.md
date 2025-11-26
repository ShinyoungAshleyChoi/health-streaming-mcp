# Performance Tests

This directory contains performance and load tests for the Flink consumer application.

## Test Categories

### 1. Load Generator (`test_load_generator.py`)
Generates synthetic health data for performance testing.

**Features:**
- Configurable number of users and devices
- Multiple health data types (heart rate, steps, etc.)
- Realistic value ranges
- Batch and continuous stream generation

**Usage:**
```python
from flink_consumer.tests.performance.test_load_generator import HealthDataLoadGenerator

generator = HealthDataLoadGenerator(num_users=100, num_devices=50)

# Generate single payload
payload = generator.generate_payload(num_samples=5)

# Generate batch
batch = generator.generate_batch(batch_size=1000, samples_per_payload=5)

# Generate continuous stream
stream = generator.generate_continuous_stream(
    duration_seconds=60,
    rate_per_second=100,
    samples_per_payload=5
)
```

### 2. Throughput Tests (`test_throughput.py`)
Measures data processing throughput and latency.

**Test Cases:**
- Small batch throughput (1,000 payloads)
- Medium batch throughput (5,000 payloads)
- Transformation performance
- Validation performance
- Single payload latency
- Average latency
- Memory efficiency
- Parallelism scaling

**Running Tests:**
```bash
# Run all performance tests
pytest tests/performance/ -v

# Run specific test
pytest tests/performance/test_throughput.py::TestThroughputPerformance::test_small_batch_throughput -v

# Run with output
pytest tests/performance/ -v -s
```

## Performance Metrics

### Throughput
- **Target:** 5,000+ messages/second
- **Measurement:** Total samples processed / duration

### Latency
- **Target:** < 100ms per payload (p95)
- **Measurement:** End-to-end processing time

### Resource Usage
- **Memory:** Monitor heap usage with large batches
- **CPU:** Measure utilization at different parallelism levels

## Load Testing Scenarios

### Scenario 1: Steady State
- **Rate:** 5,000 msg/s
- **Duration:** 10 minutes
- **Purpose:** Baseline performance

### Scenario 2: Burst Load
- **Rate:** 20,000 msg/s
- **Duration:** 5 minutes
- **Purpose:** Test backpressure handling

### Scenario 3: Sustained Load
- **Rate:** 10,000 msg/s
- **Duration:** 1 hour
- **Purpose:** Test stability and memory leaks

## Generating Test Data

### Quick Start
```python
from flink_consumer.tests.performance.test_load_generator import generate_test_data

# Generate 10,000 payloads with 5 samples each
data = generate_test_data(
    num_payloads=10000,
    samples_per_payload=5,
    num_users=100
)
```

### Custom Generation
```python
generator = HealthDataLoadGenerator(num_users=1000, num_devices=500)

# Generate data for specific time period
from datetime import datetime, timezone
timestamp = datetime(2025, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
payload = generator.generate_payload(num_samples=10, timestamp=timestamp)
```

## Interpreting Results

### Good Performance Indicators
- Throughput > 5,000 samples/sec
- Latency < 100ms (p95)
- Linear scaling with parallelism (up to CPU cores)
- Stable memory usage over time

### Performance Issues
- Throughput < 1,000 samples/sec → Check transformation logic
- Latency > 500ms → Check validation rules
- Memory growth → Check for state leaks
- No scaling with parallelism → Check for bottlenecks

## Optimization Tips

1. **Increase Parallelism:** Match Kafka partition count
2. **Batch Processing:** Increase batch size for Iceberg writes
3. **State Management:** Use RocksDB for large state
4. **Checkpointing:** Tune checkpoint interval (60s recommended)
5. **Serialization:** Use Avro for efficient serialization

## Requirements

Performance tests require:
- PyFlink 1.18+
- Sufficient memory (4GB+ recommended)
- Multiple CPU cores for parallelism tests

## Notes

- Performance tests may take longer to run
- Results vary based on hardware
- Use `-s` flag to see detailed output
- Consider running on dedicated test environment
