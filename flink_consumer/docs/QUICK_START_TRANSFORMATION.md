# Quick Start: Data Transformation Pipeline

This guide helps you quickly get started with the data transformation pipeline.

## Installation

The transformation pipeline is part of the `flink_consumer` package. No additional dependencies are required beyond what's in `pyproject.toml`.

## Basic Usage

### 1. Transform Nested Payloads

```python
from flink_consumer.converters import HealthDataTransformer

# Create transformer
transformer = HealthDataTransformer()

# Apply to stream
transformed_stream = source_stream.flat_map(transformer)
```

### 2. Validate Data Quality

```python
from flink_consumer.validators import HealthDataValidator

# Create validator (non-strict mode)
validator = HealthDataValidator(strict_mode=False)

# Apply to stream
validated_stream = transformed_stream.filter(validator)
```

### 3. Handle Errors with DLQ

```python
from flink_consumer.services import (
    ValidationProcessFunction,
    get_error_output_tag
)

# Create validation process function
validation_process = ValidationProcessFunction()

# Apply to stream
processed_stream = validated_stream.process(validation_process)

# Get error stream
error_tag = get_error_output_tag()
error_stream = processed_stream.get_side_output(error_tag)
```

## Complete Example

```python
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.converters import HealthDataTransformer
from flink_consumer.validators import HealthDataValidator
from flink_consumer.services import (
    ValidationProcessFunction,
    ErrorEnricher,
    get_error_output_tag
)

# Setup
env = StreamExecutionEnvironment.get_execution_environment()
source_stream = env.from_source(kafka_source, ...)

# Pipeline
transformed = source_stream.flat_map(HealthDataTransformer())
validated = transformed.filter(HealthDataValidator())
processed = validated.process(ValidationProcessFunction())

# Error handling
error_stream = processed.get_side_output(get_error_output_tag())
enriched_errors = error_stream.process(ErrorEnricher())

# Sinks
processed.sink_to(main_sink)
enriched_errors.sink_to(error_sink)

# Execute
env.execute("Health Data Pipeline")
```

## Testing

Run the example:

```bash
cd flink_consumer
python examples/test_transformation_pipeline.py
```

## Configuration

### Validator Modes

**Non-strict mode (default):**
- Allows unknown data types
- Only validates known types

```python
validator = HealthDataValidator(strict_mode=False)
```

**Strict mode:**
- Rejects unknown data types
- Enforces all validation rules

```python
validator = HealthDataValidator(strict_mode=True)
```

### Error Handler Modes

**Non-strict validation:**
- More lenient validation
- Suitable for development

```python
process_fn = ValidationProcessFunction(strict_validation=False)
```

**Strict validation:**
- Stricter validation rules
- Suitable for production

```python
process_fn = ValidationProcessFunction(strict_validation=True)
```

## Common Patterns

### Pattern 1: Simple Pipeline (No Error Handling)

```python
stream = source_stream \
    .flat_map(HealthDataTransformer()) \
    .filter(HealthDataValidator()) \
    .sink_to(sink)
```

### Pattern 2: Pipeline with DLQ

```python
# Main pipeline
processed = source_stream \
    .flat_map(HealthDataTransformer()) \
    .filter(HealthDataValidator()) \
    .process(ValidationProcessFunction())

# Error pipeline
errors = processed.get_side_output(get_error_output_tag())
errors.sink_to(error_sink)

# Main sink
processed.sink_to(main_sink)
```

### Pattern 3: Pipeline with Enriched Errors

```python
# Main pipeline
processed = source_stream \
    .flat_map(HealthDataTransformer()) \
    .filter(HealthDataValidator()) \
    .process(ValidationProcessFunction())

# Error pipeline with enrichment
errors = processed.get_side_output(get_error_output_tag())
enriched = errors.process(ErrorEnricher())
enriched.sink_to(error_sink)

# Main sink
processed.sink_to(main_sink)
```

## Monitoring

### Enable Logging

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Check Validation Stats

The validator logs stats every 10,000 records:

```
Validation stats: 10000 records processed, 150 rejected (1.50%)
```

### Check Processing Stats

The error handler logs stats every 10,000 records:

```
Processing stats: 10000 records, 150 errors (1.50%)
```

## Troubleshooting

### No Output from Transformer

**Problem:** Transformer produces no records

**Solutions:**
1. Check if input payloads have `samples` array
2. Verify required fields are present
3. Check logs for transformation errors

### High Rejection Rate

**Problem:** Validator rejects too many records

**Solutions:**
1. Review validation logs for rejection reasons
2. Check if data types are in validation ranges
3. Consider using non-strict mode
4. Adjust validation ranges if needed

### Errors Not Routed to DLQ

**Problem:** Error stream is empty

**Solutions:**
1. Verify you're using `ValidationProcessFunction`
2. Check you're getting side output with correct tag
3. Ensure errors are actually occurring

## Next Steps

1. Integrate with Kafka source (Task 2)
2. Add Iceberg sink (Task 5)
3. Configure checkpointing (Task 6)
4. Add custom metrics (Task 7)

## API Reference

See [TRANSFORMATION_PIPELINE.md](./TRANSFORMATION_PIPELINE.md) for detailed API documentation.
