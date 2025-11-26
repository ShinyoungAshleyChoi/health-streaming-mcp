"""
Example demonstrating the complete data transformation pipeline.

This example shows how to use:
1. HealthDataTransformer - to flatten nested Kafka payloads
2. HealthDataValidator - to validate data quality
3. ValidationProcessFunction - to handle errors with DLQ pattern
"""

import logging
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment

from flink_consumer.converters import HealthDataTransformer
from flink_consumer.validators import HealthDataValidator
from flink_consumer.services import (
    ValidationProcessFunction,
    ErrorEnricher,
    get_error_output_tag
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_test_payload():
    """Create a test health data payload with multiple samples"""
    return {
        'deviceId': 'test-device-123',
        'userId': 'test-user-456',
        'timestamp': '2025-11-16T10:00:00Z',
        'appVersion': '1.0.0',
        'samples': [
            {
                'id': 'sample-1',
                'type': 'heartRate',
                'value': 72.0,
                'unit': 'count/min',
                'startDate': '2025-11-16T10:00:00Z',
                'endDate': '2025-11-16T10:01:00Z',
                'sourceBundle': 'com.apple.health',
                'metadata': {'device': 'Apple Watch'},
                'isSynced': True,
                'createdAt': '2025-11-16T10:01:00Z'
            },
            {
                'id': 'sample-2',
                'type': 'steps',
                'value': 1500.0,
                'unit': 'count',
                'startDate': '2025-11-16T10:00:00Z',
                'endDate': '2025-11-16T10:15:00Z',
                'sourceBundle': 'com.apple.health',
                'metadata': {},
                'isSynced': True,
                'createdAt': '2025-11-16T10:15:00Z'
            },
            {
                'id': 'sample-3-invalid',
                'type': 'heartRate',
                'value': -10.0,  # Invalid: negative value
                'unit': 'count/min',
                'startDate': '2025-11-16T10:15:00Z',
                'endDate': '2025-11-16T10:16:00Z',
                'sourceBundle': 'com.apple.health',
                'metadata': {},
                'isSynced': False,
                'createdAt': '2025-11-16T10:16:00Z'
            },
            {
                'id': 'sample-4',
                'type': 'activeEnergyBurned',
                'value': 250.5,
                'unit': 'kcal',
                'startDate': '2025-11-16T10:00:00Z',
                'endDate': '2025-11-16T10:30:00Z',
                'sourceBundle': 'com.apple.health',
                'metadata': {'workout': 'running'},
                'isSynced': True,
                'createdAt': '2025-11-16T10:30:00Z'
            }
        ]
    }


def create_invalid_payload():
    """Create an invalid payload for testing error handling"""
    return {
        'deviceId': 'test-device-999',
        'userId': '',  # Invalid: empty user ID
        'timestamp': '2025-11-16T11:00:00Z',
        'appVersion': '1.0.0',
        'samples': [
            {
                'id': 'sample-invalid-1',
                'type': 'heartRate',
                'value': 500.0,  # Invalid: out of range
                'unit': 'count/min',
                'startDate': '2025-11-16T11:00:00Z',
                'endDate': '2025-11-16T10:59:00Z',  # Invalid: end before start
                'sourceBundle': None,
                'metadata': {},
                'isSynced': False,
                'createdAt': '2025-11-16T11:00:00Z'
            }
        ]
    }


def run_transformation_pipeline():
    """
    Run the complete transformation pipeline with error handling.
    
    Pipeline flow:
    1. Source: Create test data
    2. Transform: Flatten nested payloads (HealthDataTransformer)
    3. Validate: Check data quality (HealthDataValidator)
    4. Error Handling: Route errors to DLQ (ValidationProcessFunction)
    5. Sink: Print valid and error records
    """
    logger.info("Starting transformation pipeline example...")
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Create test data
    test_data = [
        create_test_payload(),
        create_invalid_payload(),
        create_test_payload()  # Another valid payload
    ]
    
    logger.info(f"Created {len(test_data)} test payloads")
    
    # Step 1: Create source from test data
    source_stream = env.from_collection(test_data)
    logger.info("✓ Created source stream")
    
    # Step 2: Transform - Flatten nested payloads
    transformer = HealthDataTransformer()
    transformed_stream = source_stream.flat_map(transformer)
    logger.info("✓ Applied HealthDataTransformer")
    
    # Step 3: Validate - Filter invalid records
    validator = HealthDataValidator(strict_mode=False)
    validated_stream = transformed_stream.filter(validator)
    logger.info("✓ Applied HealthDataValidator")
    
    # Step 4: Error Handling - Use ProcessFunction with side output
    validation_process = ValidationProcessFunction(strict_validation=False)
    processed_stream = validated_stream.process(validation_process)
    logger.info("✓ Applied ValidationProcessFunction")
    
    # Get error stream from side output
    error_output_tag = get_error_output_tag()
    error_stream = processed_stream.get_side_output(error_output_tag)
    logger.info("✓ Extracted error stream")
    
    # Enrich error records
    error_enricher = ErrorEnricher()
    enriched_error_stream = error_stream.process(error_enricher)
    logger.info("✓ Applied ErrorEnricher")
    
    # Step 5: Sink - Print results
    # Print valid records
    processed_stream.map(
        lambda row: f"✓ VALID: user={row['user_id']}, type={row['data_type']}, "
                   f"value={row['value']}, sample_id={row['sample_id']}"
    ).print()
    
    # Print error records
    enriched_error_stream.map(
        lambda err: f"✗ ERROR: type={err['error_type']}, severity={err['severity']}, "
                   f"message={err['error_message']}, sample_id={err.get('sample_id', 'unknown')}"
    ).print()
    
    # Execute the pipeline
    logger.info("Executing pipeline...")
    env.execute("Health Data Transformation Pipeline Example")
    logger.info("Pipeline execution completed!")


def run_simple_transformation():
    """
    Run a simple transformation example without error handling.
    
    This demonstrates just the transformer and validator components.
    """
    logger.info("Starting simple transformation example...")
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Create test data
    test_payload = create_test_payload()
    
    # Create source
    source_stream = env.from_collection([test_payload])
    
    # Transform
    transformer = HealthDataTransformer()
    transformed_stream = source_stream.flat_map(transformer)
    
    # Validate
    validator = HealthDataValidator(strict_mode=False)
    validated_stream = transformed_stream.filter(validator)
    
    # Print results
    validated_stream.map(
        lambda row: f"Transformed: {row['user_id']} - {row['data_type']}: {row['value']} {row['unit']}"
    ).print()
    
    # Execute
    env.execute("Simple Transformation Example")
    logger.info("Simple transformation completed!")


def demonstrate_timestamp_parsing():
    """Demonstrate timestamp parsing functionality"""
    logger.info("\n=== Timestamp Parsing Examples ===")
    
    from flink_consumer.converters.health_data_transformer import HealthDataTransformer
    
    transformer = HealthDataTransformer()
    
    test_timestamps = [
        '2025-11-16T10:00:00Z',
        '2025-11-16T10:00:00.123Z',
        '2025-11-16T10:00:00+00:00',
        '2025-11-16T10:00:00.456+00:00',
    ]
    
    for ts_str in test_timestamps:
        ts_ms = transformer._parse_timestamp(ts_str)
        if ts_ms:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            logger.info(f"  {ts_str} → {ts_ms}ms → {dt.isoformat()}")
        else:
            logger.error(f"  {ts_str} → Failed to parse")


def demonstrate_validation_ranges():
    """Demonstrate validation range checking"""
    logger.info("\n=== Validation Range Examples ===")
    
    from flink_consumer.validators.health_data_validator import HealthDataValidator
    
    validator = HealthDataValidator(strict_mode=False)
    
    test_cases = [
        {'data_type': 'heartRate', 'value': 72, 'expected': 'VALID'},
        {'data_type': 'heartRate', 'value': 300, 'expected': 'INVALID (out of range)'},
        {'data_type': 'steps', 'value': 5000, 'expected': 'VALID'},
        {'data_type': 'steps', 'value': -100, 'expected': 'INVALID (negative)'},
        {'data_type': 'unknownType', 'value': 100, 'expected': 'VALID (unknown type allowed)'},
    ]
    
    for test in test_cases:
        logger.info(f"  {test['data_type']}={test['value']} → {test['expected']}")


if __name__ == '__main__':
    print("\n" + "="*70)
    print("Health Data Transformation Pipeline Examples")
    print("="*70 + "\n")
    
    # Demonstrate timestamp parsing
    demonstrate_timestamp_parsing()
    
    # Demonstrate validation ranges
    demonstrate_validation_ranges()
    
    print("\n" + "="*70)
    print("Running Pipeline Examples")
    print("="*70 + "\n")
    
    # Run examples
    try:
        # Run simple transformation
        logger.info("\n--- Example 1: Simple Transformation ---")
        run_simple_transformation()
        
        # Run full pipeline with error handling
        logger.info("\n--- Example 2: Full Pipeline with Error Handling ---")
        run_transformation_pipeline()
        
    except Exception as e:
        logger.error(f"Error running examples: {e}", exc_info=True)
