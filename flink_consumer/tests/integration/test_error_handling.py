"""Integration tests for error handling and DLQ"""

import pytest
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.converters.health_data_transformer import HealthDataTransformer
from flink_consumer.services.error_handler import (
    ValidationProcessFunction,
    ErrorEnricher,
    ErrorType,
    get_error_output_tag
)


class TestErrorHandling:
    """Integration tests for error handling and Dead Letter Queue"""

    @pytest.fixture
    def env(self):
        """Create Flink execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        return env

    @pytest.fixture
    def invalid_rows(self):
        """Create test data with various validation errors"""
        return [
            # Missing user_id
            {
                'device_id': 'device-1',
                'sample_id': 'sample-1',
                'data_type': 'heartRate',
                'value': 72.0,
                'start_date': 1700049600000,
                'end_date': 1700049660000
            },
            # Negative value
            {
                'device_id': 'device-2',
                'user_id': 'user-2',
                'sample_id': 'sample-2',
                'data_type': 'steps',
                'value': -100.0,
                'start_date': 1700049600000,
                'end_date': 1700049660000
            },
            # Invalid date range
            {
                'device_id': 'device-3',
                'user_id': 'user-3',
                'sample_id': 'sample-3',
                'data_type': 'heartRate',
                'value': 75.0,
                'start_date': 1700049660000,
                'end_date': 1700049600000  # End before start
            }
        ]

    def test_validation_process_function_error_routing(self, env, invalid_rows):
        """Test that invalid records are routed to error stream"""
        source = env.from_collection(invalid_rows)
        
        validation_process = ValidationProcessFunction(strict_validation=False)
        processed = source.process(validation_process)
        
        # Get error stream
        error_tag = get_error_output_tag()
        error_stream = processed.get_side_output(error_tag)
        
        # Collect error records
        error_results = list(error_stream.execute_and_collect())
        
        # All invalid rows should be in error stream
        assert len(error_results) >= 3
        
        # Verify error metadata
        for error in error_results:
            assert 'error_type' in error
            assert 'error_message' in error
            assert 'error_timestamp' in error
            assert 'original_data' in error

    def test_error_enricher(self, env):
        """Test error enrichment adds metadata"""
        # Create mock error record
        error_record = {
            'original_data': {
                'user_id': 'user-1',
                'sample_id': 'sample-1',
                'data_type': 'heartRate',
                'value': -10.0
            },
            'error_type': ErrorType.VALIDATION_ERROR,
            'error_message': 'Negative value',
            'error_timestamp': 1700049660000
        }
        
        source = env.from_collection([error_record])
        enricher = ErrorEnricher()
        enriched = source.process(enricher)
        
        results = list(enriched.execute_and_collect())
        
        assert len(results) == 1
        enriched_error = results[0]
        
        # Verify enrichment fields
        assert 'enriched_at' in enriched_error
        assert 'severity' in enriched_error
        assert 'retryable' in enriched_error
        assert enriched_error['severity'] == 'LOW'  # Validation errors are low severity

    def test_error_severity_classification(self, env):
        """Test that errors are classified by severity"""
        error_records = [
            {
                'error_type': ErrorType.VALIDATION_ERROR,
                'error_message': 'Validation failed',
                'original_data': {}
            },
            {
                'error_type': ErrorType.PROCESSING_ERROR,
                'error_message': 'Processing failed',
                'original_data': {}
            },
            {
                'error_type': ErrorType.DESERIALIZATION_ERROR,
                'error_message': 'Deserialization failed',
                'original_data': {}
            }
        ]
        
        source = env.from_collection(error_records)
        enricher = ErrorEnricher()
        enriched = source.process(enricher)
        
        results = list(enriched.execute_and_collect())
        
        assert len(results) == 3
        
        # Verify severity levels
        severities = {r['error_type']: r['severity'] for r in results}
        assert severities[ErrorType.VALIDATION_ERROR] == 'LOW'
        assert severities[ErrorType.PROCESSING_ERROR] == 'HIGH'
        assert severities[ErrorType.DESERIALIZATION_ERROR] == 'HIGH'

    def test_retryable_error_classification(self, env):
        """Test that errors are classified as retryable or not"""
        error_records = [
            {
                'error_type': ErrorType.VALIDATION_ERROR,
                'error_message': 'Validation failed',
                'original_data': {}
            },
            {
                'error_type': ErrorType.PROCESSING_ERROR,
                'error_message': 'Processing failed',
                'original_data': {}
            }
        ]
        
        source = env.from_collection(error_records)
        enricher = ErrorEnricher()
        enriched = source.process(enricher)
        
        results = list(enriched.execute_and_collect())
        
        # Validation errors are not retryable (data issue)
        validation_error = [r for r in results if r['error_type'] == ErrorType.VALIDATION_ERROR][0]
        assert validation_error['retryable'] is False
        
        # Processing errors are retryable (transient issue)
        processing_error = [r for r in results if r['error_type'] == ErrorType.PROCESSING_ERROR][0]
        assert processing_error['retryable'] is True

    def test_end_to_end_error_pipeline(self, env):
        """Test complete error handling pipeline"""
        # Create payload with mix of valid and invalid samples
        payload = {
            'deviceId': 'device-mixed',
            'userId': 'user-mixed',
            'timestamp': '2025-11-15T10:00:00Z',
            'appVersion': '1.0.0',
            'samples': [
                {
                    'id': 'valid-1',
                    'type': 'heartRate',
                    'value': 72.0,
                    'unit': 'count/min',
                    'startDate': '2025-11-15T10:00:00Z',
                    'endDate': '2025-11-15T10:01:00Z',
                    'metadata': {},
                    'isSynced': True,
                    'createdAt': '2025-11-15T10:01:00Z'
                },
                {
                    'id': 'invalid-1',
                    'type': 'heartRate',
                    'value': -50.0,  # Invalid
                    'unit': 'count/min',
                    'startDate': '2025-11-15T10:01:00Z',
                    'endDate': '2025-11-15T10:02:00Z',
                    'metadata': {},
                    'isSynced': False,
                    'createdAt': '2025-11-15T10:02:00Z'
                }
            ]
        }
        
        source = env.from_collection([payload])
        
        # Transform
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        
        # Validate with error handling
        validation_process = ValidationProcessFunction(strict_validation=False)
        processed = transformed.process(validation_process)
        
        # Get error stream and enrich
        error_tag = get_error_output_tag()
        error_stream = processed.get_side_output(error_tag)
        enricher = ErrorEnricher()
        enriched_errors = error_stream.process(enricher)
        
        # Collect results
        valid_results = list(processed.execute_and_collect())
        
        # Verify valid records passed through
        assert len(valid_results) >= 1
        assert all(r['value'] >= 0 for r in valid_results)

    def test_error_metadata_extraction(self, env):
        """Test that error records extract key fields for querying"""
        invalid_row = {
            'device_id': 'device-test',
            'user_id': 'user-test',
            'sample_id': 'sample-test',
            'data_type': 'heartRate',
            'value': -10.0,
            'start_date': 1700049600000,
            'end_date': 1700049660000,
            'processing_time': 1700049660000,
            'payload_timestamp': 1700049660000
        }
        
        source = env.from_collection([invalid_row])
        validation_process = ValidationProcessFunction(strict_validation=False)
        processed = source.process(validation_process)
        
        error_tag = get_error_output_tag()
        error_stream = processed.get_side_output(error_tag)
        
        error_results = list(error_stream.execute_and_collect())
        
        assert len(error_results) >= 1
        error = error_results[0]
        
        # Verify extracted fields
        assert error['user_id'] == 'user-test'
        assert error['sample_id'] == 'sample-test'
        assert error['data_type'] == 'heartRate'
        assert error['device_id'] == 'device-test'
        assert 'processing_time' in error
        assert 'payload_timestamp' in error
