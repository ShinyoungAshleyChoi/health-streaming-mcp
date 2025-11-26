"""Integration tests for the complete transformation pipeline"""

import pytest
from pyflink.datastream import StreamExecutionEnvironment
from flink_consumer.converters.health_data_transformer import HealthDataTransformer
from flink_consumer.validators.health_data_validator import HealthDataValidator
from flink_consumer.services.error_handler import (
    ValidationProcessFunction,
    get_error_output_tag
)


class TestTransformationPipeline:
    """Integration tests for Kafka → Flink → Iceberg pipeline"""

    @pytest.fixture
    def env(self):
        """Create Flink execution environment"""
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        return env

    @pytest.fixture
    def test_payloads(self):
        """Create test payloads with valid and invalid data"""
        return [
            # Valid payload
            {
                'deviceId': 'device-1',
                'userId': 'user-1',
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
                    },
                    {
                        'id': 'sample-2',
                        'type': 'steps',
                        'value': 1000.0,
                        'unit': 'count',
                        'startDate': '2025-11-15T10:00:00Z',
                        'endDate': '2025-11-15T10:05:00Z',
                        'metadata': {},
                        'isSynced': True,
                        'createdAt': '2025-11-15T10:05:00Z'
                    }
                ]
            },
            # Payload with invalid sample
            {
                'deviceId': 'device-2',
                'userId': 'user-2',
                'timestamp': '2025-11-15T11:00:00Z',
                'appVersion': '1.0.0',
                'samples': [
                    {
                        'id': 'sample-3',
                        'type': 'heartRate',
                        'value': -10.0,  # Invalid: negative
                        'unit': 'count/min',
                        'startDate': '2025-11-15T11:00:00Z',
                        'endDate': '2025-11-15T11:01:00Z',
                        'metadata': {},
                        'isSynced': False,
                        'createdAt': '2025-11-15T11:01:00Z'
                    }
                ]
            }
        ]

    def test_end_to_end_transformation(self, env, test_payloads):
        """Test complete transformation pipeline from payload to validated rows"""
        # Create source
        source = env.from_collection(test_payloads)
        
        # Apply transformer
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        
        # Apply validator
        validator = HealthDataValidator(strict_mode=False)
        validated = transformed.filter(validator)
        
        # Collect results
        results = list(validated.execute_and_collect())
        
        # Verify results
        assert len(results) == 2  # Only 2 valid samples
        assert all(r['value'] >= 0 for r in results)
        assert all(r['user_id'] for r in results)

    def test_transformation_with_error_handling(self, env, test_payloads):
        """Test pipeline with DLQ error handling"""
        # Create source
        source = env.from_collection(test_payloads)
        
        # Apply transformer
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        
        # Apply validation process function
        validation_process = ValidationProcessFunction(strict_validation=False)
        processed = transformed.process(validation_process)
        
        # Get error stream
        error_tag = get_error_output_tag()
        error_stream = processed.get_side_output(error_tag)
        
        # Collect valid and error records
        valid_results = list(processed.execute_and_collect())
        
        # Verify valid records
        assert len(valid_results) >= 2
        assert all(r['value'] >= 0 for r in valid_results)

    def test_multiple_data_types(self, env):
        """Test pipeline handles multiple health data types"""
        payload = {
            'deviceId': 'device-multi',
            'userId': 'user-multi',
            'timestamp': '2025-11-15T12:00:00Z',
            'appVersion': '1.0.0',
            'samples': [
                {
                    'id': 'hr-1',
                    'type': 'heartRate',
                    'value': 75.0,
                    'unit': 'count/min',
                    'startDate': '2025-11-15T12:00:00Z',
                    'endDate': '2025-11-15T12:01:00Z',
                    'metadata': {},
                    'isSynced': True,
                    'createdAt': '2025-11-15T12:01:00Z'
                },
                {
                    'id': 'steps-1',
                    'type': 'steps',
                    'value': 5000.0,
                    'unit': 'count',
                    'startDate': '2025-11-15T12:00:00Z',
                    'endDate': '2025-11-15T12:30:00Z',
                    'metadata': {},
                    'isSynced': True,
                    'createdAt': '2025-11-15T12:30:00Z'
                },
                {
                    'id': 'energy-1',
                    'type': 'activeEnergyBurned',
                    'value': 250.0,
                    'unit': 'kcal',
                    'startDate': '2025-11-15T12:00:00Z',
                    'endDate': '2025-11-15T12:30:00Z',
                    'metadata': {},
                    'isSynced': True,
                    'createdAt': '2025-11-15T12:30:00Z'
                }
            ]
        }
        
        source = env.from_collection([payload])
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        validator = HealthDataValidator(strict_mode=False)
        validated = transformed.filter(validator)
        
        results = list(validated.execute_and_collect())
        
        assert len(results) == 3
        data_types = {r['data_type'] for r in results}
        assert data_types == {'heartRate', 'steps', 'activeEnergyBurned'}

    def test_timestamp_conversion_in_pipeline(self, env):
        """Test that timestamps are correctly converted throughout pipeline"""
        payload = {
            'deviceId': 'device-ts',
            'userId': 'user-ts',
            'timestamp': '2025-11-15T10:00:00Z',
            'appVersion': '1.0.0',
            'samples': [{
                'id': 'sample-ts',
                'type': 'heartRate',
                'value': 72.0,
                'unit': 'count/min',
                'startDate': '2025-11-15T10:00:00.000Z',
                'endDate': '2025-11-15T10:01:00.000Z',
                'metadata': {},
                'isSynced': True,
                'createdAt': '2025-11-15T10:01:00.000Z'
            }]
        }
        
        source = env.from_collection([payload])
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        
        results = list(transformed.execute_and_collect())
        
        assert len(results) == 1
        row = results[0]
        
        # Verify timestamps are integers (milliseconds)
        assert isinstance(row['start_date'], int)
        assert isinstance(row['end_date'], int)
        assert isinstance(row['created_at'], int)
        assert isinstance(row['payload_timestamp'], int)
        assert isinstance(row['processing_time'], int)
        
        # Verify date ordering
        assert row['start_date'] <= row['end_date']

    def test_metadata_preservation(self, env):
        """Test that metadata is preserved through pipeline"""
        payload = {
            'deviceId': 'device-meta',
            'userId': 'user-meta',
            'timestamp': '2025-11-15T10:00:00Z',
            'appVersion': '2.0.0',
            'samples': [{
                'id': 'sample-meta',
                'type': 'heartRate',
                'value': 72.0,
                'unit': 'count/min',
                'startDate': '2025-11-15T10:00:00Z',
                'endDate': '2025-11-15T10:01:00Z',
                'sourceBundle': 'com.apple.health',
                'metadata': {'device': 'Apple Watch', 'version': '8.0'},
                'isSynced': True,
                'createdAt': '2025-11-15T10:01:00Z'
            }]
        }
        
        source = env.from_collection([payload])
        transformer = HealthDataTransformer()
        transformed = source.flat_map(transformer)
        
        results = list(transformed.execute_and_collect())
        
        assert len(results) == 1
        row = results[0]
        
        assert row['source_bundle'] == 'com.apple.health'
        assert row['metadata'] == {'device': 'Apple Watch', 'version': '8.0'}
        assert row['app_version'] == '2.0.0'
        assert row['is_synced'] is True
