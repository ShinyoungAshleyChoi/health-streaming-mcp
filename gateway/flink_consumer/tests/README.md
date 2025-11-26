# Flink Consumer Tests

Comprehensive test suite for the Flink Iceberg consumer application.

## Test Structure

```
tests/
├── __init__.py
├── conftest.py                    # Shared fixtures
├── README.md                      # This file
│
├── test_health_data_transformer.py    # Unit tests for transformer
├── test_health_data_validator.py      # Unit tests for validator
├── test_timestamp_utils.py            # Unit tests for timestamp parsing
├── test_settings.py                   # Unit tests for configuration
│
├── integration/                   # Integration tests
│   ├── __init__.py
│   ├── test_transformation_pipeline.py
│   ├── test_error_handling.py
│   └── test_checkpoint_config.py
│
└── performance/                   # Performance tests
    ├── __init__.py
    ├── README.md
    ├── test_load_generator.py
    └── test_throughput.py
```

## Running Tests

### All Tests
```bash
pytest tests/ -v
```

### Unit Tests Only
```bash
pytest tests/test_*.py -v
```

### Integration Tests Only
```bash
pytest tests/integration/ -v
```

### Performance Tests Only
```bash
pytest tests/performance/ -v -s
```

### With Coverage
```bash
pytest tests/ --cov=flink_consumer --cov-report=html
```

## Test Categories

### 1. Unit Tests
Test individual components in isolation.

**Coverage:**
- `test_health_data_transformer.py` - Payload transformation logic
- `test_health_data_validator.py` - Data validation rules
- `test_timestamp_utils.py` - Timestamp parsing utilities
- `test_settings.py` - Configuration loading

**Requirements Tested:** 2.1, 2.2

### 2. Integration Tests
Test complete pipelines and component interactions.

**Coverage:**
- `test_transformation_pipeline.py` - End-to-end transformation
- `test_error_handling.py` - DLQ and error routing
- `test_checkpoint_config.py` - Checkpoint and recovery setup

**Requirements Tested:** 5.3, 5.4, 7.1

### 3. Performance Tests
Test throughput, latency, and resource usage.

**Coverage:**
- `test_load_generator.py` - Synthetic data generation
- `test_throughput.py` - Throughput and latency measurements

**Requirements Tested:** 9.1, 9.2, 9.5

## Test Fixtures

### Common Fixtures (conftest.py)
- `setup_logging` - Configure logging for tests
- `sample_health_data_payload` - Valid test payload
- `sample_transformed_row` - Transformed row example

### Test-Specific Fixtures
- `env` - Flink execution environment
- `transformer` - HealthDataTransformer instance
- `validator` - HealthDataValidator instance
- `load_generator` - Performance test data generator

## Writing New Tests

### Unit Test Template
```python
import pytest
from flink_consumer.module import Component

class TestComponent:
    @pytest.fixture
    def component(self):
        return Component()
    
    def test_basic_functionality(self, component):
        result = component.process(input_data)
        assert result == expected_output
```

### Integration Test Template
```python
import pytest
from pyflink.datastream import StreamExecutionEnvironment

class TestPipeline:
    @pytest.fixture
    def env(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        return env
    
    def test_end_to_end(self, env):
        source = env.from_collection(test_data)
        result = source.map(transform).filter(validate)
        results = list(result.execute_and_collect())
        assert len(results) > 0
```

## Test Data

### Valid Payload Example
```python
{
    'deviceId': 'device-123',
    'userId': 'user-456',
    'timestamp': '2025-11-15T10:00:00Z',
    'appVersion': '1.0.0',
    'samples': [
        {
            'id': 'sample-1',
            'type': 'heartRate',
            'value': 72.0,
            'unit': 'count/min',
            'startDate': '2025-11-15T10:00:00Z',
            'endDate': '2025-11-15T10:01:00Z',
            'metadata': {},
            'isSynced': True,
            'createdAt': '2025-11-15T10:01:00Z'
        }
    ]
}
```

### Transformed Row Example
```python
{
    'device_id': 'device-123',
    'user_id': 'user-456',
    'sample_id': 'sample-1',
    'data_type': 'heartRate',
    'value': 72.0,
    'unit': 'count/min',
    'start_date': 1700049600000,
    'end_date': 1700049660000,
    'source_bundle': None,
    'metadata': {},
    'is_synced': True,
    'created_at': 1700049660000,
    'payload_timestamp': 1700049660000,
    'app_version': '1.0.0',
    'processing_time': 1700049660000
}
```

## Continuous Integration

### GitHub Actions Example
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install uv
          uv sync
      - name: Run tests
        run: pytest tests/ -v --cov=flink_consumer
```

## Troubleshooting

### Common Issues

**Issue:** Tests fail with "command not found: python"
**Solution:** Use `python3` or activate virtual environment

**Issue:** PyFlink import errors
**Solution:** Ensure PyFlink 1.18+ is installed: `pip install apache-flink>=1.18.0`

**Issue:** Performance tests timeout
**Solution:** Increase timeout or reduce batch size

**Issue:** Integration tests fail with checkpoint errors
**Solution:** Ensure checkpoint storage path is writable

## Best Practices

1. **Isolation:** Each test should be independent
2. **Fixtures:** Use fixtures for common setup
3. **Assertions:** Use descriptive assertion messages
4. **Coverage:** Aim for >80% code coverage
5. **Performance:** Keep unit tests fast (<1s each)
6. **Documentation:** Add docstrings to test classes/methods

## Requirements Coverage

| Requirement | Test File | Status |
|-------------|-----------|--------|
| 2.1 | test_health_data_transformer.py | ✓ |
| 2.2 | test_health_data_validator.py | ✓ |
| 5.3 | test_checkpoint_config.py | ✓ |
| 5.4 | test_checkpoint_config.py | ✓ |
| 7.1 | test_transformation_pipeline.py | ✓ |
| 9.1 | test_throughput.py | ✓ |
| 9.2 | test_throughput.py | ✓ |
| 9.5 | test_throughput.py | ✓ |

## Contributing

When adding new tests:
1. Follow existing test structure
2. Add appropriate fixtures
3. Update this README
4. Ensure tests pass locally
5. Update requirements coverage table
