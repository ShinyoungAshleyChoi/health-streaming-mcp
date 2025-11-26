"""Unit tests for HealthDataTransformer"""

import pytest
from datetime import datetime, timezone
from flink_consumer.converters.health_data_transformer import HealthDataTransformer


class TestHealthDataTransformer:
    """Test suite for HealthDataTransformer class"""

    @pytest.fixture
    def transformer(self):
        """Create transformer instance"""
        return HealthDataTransformer()

    @pytest.fixture
    def valid_payload(self):
        """Create valid test payload"""
        return {
            'deviceId': 'test-device-123',
            'userId': 'test-user-456',
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
                    'sourceBundle': 'com.apple.health',
                    'metadata': {'device': 'Apple Watch'},
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
                    'sourceBundle': None,
                    'metadata': {},
                    'isSynced': False,
                    'createdAt': '2025-11-15T10:05:00Z'
                }
            ]
        }

    def test_flat_map_valid_payload(self, transformer, valid_payload):
        """Test transformation of valid payload with multiple samples"""
        results = list(transformer.flat_map(valid_payload))
        
        assert len(results) == 2
        
        # Check first sample
        row1 = results[0]
        assert row1['device_id'] == 'test-device-123'
        assert row1['user_id'] == 'test-user-456'
        assert row1['sample_id'] == 'sample-1'
        assert row1['data_type'] == 'heartRate'
        assert row1['value'] == 72.0
        assert row1['unit'] == 'count/min'
        assert row1['source_bundle'] == 'com.apple.health'
        assert row1['is_synced'] is True
        assert row1['app_version'] == '1.0.0'
        assert 'processing_time' in row1
        
        # Check second sample
        row2 = results[1]
        assert row2['sample_id'] == 'sample-2'
        assert row2['data_type'] == 'steps'
        assert row2['value'] == 1000.0
        assert row2['is_synced'] is False

    def test_flat_map_empty_payload(self, transformer):
        """Test handling of empty payload"""
        results = list(transformer.flat_map(None))
        assert len(results) == 0
        
        results = list(transformer.flat_map({}))
        assert len(results) == 0

    def test_flat_map_missing_required_fields(self, transformer):
        """Test handling of payload with missing required fields"""
        # Missing userId
        payload = {
            'deviceId': 'test-device-123',
            'timestamp': '2025-11-15T10:00:00Z',
            'samples': []
        }
        results = list(transformer.flat_map(payload))
        assert len(results) == 0
        
        # Missing deviceId
        payload = {
            'userId': 'test-user-456',
            'timestamp': '2025-11-15T10:00:00Z',
            'samples': []
        }
        results = list(transformer.flat_map(payload))
        assert len(results) == 0

    def test_flat_map_no_samples(self, transformer):
        """Test handling of payload with no samples"""
        payload = {
            'deviceId': 'test-device-123',
            'userId': 'test-user-456',
            'timestamp': '2025-11-15T10:00:00Z',
            'samples': []
        }
        results = list(transformer.flat_map(payload))
        assert len(results) == 0

    def test_flat_map_invalid_sample(self, transformer, valid_payload):
        """Test handling of invalid sample in payload"""
        # Add invalid sample (missing required fields)
        valid_payload['samples'].append({
            'id': 'sample-3',
            # Missing 'type' and 'value'
            'unit': 'count',
            'startDate': '2025-11-15T10:00:00Z',
            'endDate': '2025-11-15T10:01:00Z',
        })
        
        results = list(transformer.flat_map(valid_payload))
        # Should only get 2 valid samples, invalid one is skipped
        assert len(results) == 2

    def test_parse_timestamp_valid_formats(self, transformer):
        """Test timestamp parsing with various valid formats"""
        # ISO 8601 with Z
        ts1 = transformer._parse_timestamp('2025-11-15T10:00:00Z')
        assert ts1 is not None
        assert isinstance(ts1, int)
        
        # ISO 8601 with timezone
        ts2 = transformer._parse_timestamp('2025-11-15T10:00:00+00:00')
        assert ts2 is not None
        
        # ISO 8601 with milliseconds
        ts3 = transformer._parse_timestamp('2025-11-15T10:00:00.123Z')
        assert ts3 is not None
        
        # Verify conversion is correct
        dt = datetime(2025, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
        expected_ms = int(dt.timestamp() * 1000)
        assert ts1 == expected_ms

    def test_parse_timestamp_invalid_formats(self, transformer):
        """Test timestamp parsing with invalid formats"""
        assert transformer._parse_timestamp(None) is None
        assert transformer._parse_timestamp('') is None
        assert transformer._parse_timestamp('invalid-date') is None
        assert transformer._parse_timestamp('2025-13-45') is None

    def test_transform_sample_with_metadata(self, transformer):
        """Test sample transformation preserves metadata"""
        payload = {
            'deviceId': 'test-device',
            'userId': 'test-user',
            'timestamp': '2025-11-15T10:00:00Z',
            'appVersion': '1.0.0',
            'samples': [{
                'id': 'sample-1',
                'type': 'heartRate',
                'value': 72.0,
                'unit': 'count/min',
                'startDate': '2025-11-15T10:00:00Z',
                'endDate': '2025-11-15T10:01:00Z',
                'metadata': {'source': 'watch', 'accuracy': 'high'},
                'isSynced': True,
                'createdAt': '2025-11-15T10:01:00Z'
            }]
        }
        
        results = list(transformer.flat_map(payload))
        assert len(results) == 1
        assert results[0]['metadata'] == {'source': 'watch', 'accuracy': 'high'}

    def test_transform_sample_invalid_timestamps(self, transformer):
        """Test handling of samples with invalid timestamps"""
        payload = {
            'deviceId': 'test-device',
            'userId': 'test-user',
            'timestamp': '2025-11-15T10:00:00Z',
            'samples': [{
                'id': 'sample-1',
                'type': 'heartRate',
                'value': 72.0,
                'unit': 'count/min',
                'startDate': 'invalid-date',
                'endDate': '2025-11-15T10:01:00Z',
                'isSynced': True,
                'createdAt': '2025-11-15T10:01:00Z'
            }]
        }
        
        results = list(transformer.flat_map(payload))
        # Invalid timestamp should cause sample to be skipped
        assert len(results) == 0

    def test_value_type_conversion(self, transformer):
        """Test that values are converted to float"""
        payload = {
            'deviceId': 'test-device',
            'userId': 'test-user',
            'timestamp': '2025-11-15T10:00:00Z',
            'samples': [{
                'id': 'sample-1',
                'type': 'steps',
                'value': 1000,  # Integer value
                'unit': 'count',
                'startDate': '2025-11-15T10:00:00Z',
                'endDate': '2025-11-15T10:01:00Z',
                'isSynced': True,
                'createdAt': '2025-11-15T10:01:00Z'
            }]
        }
        
        results = list(transformer.flat_map(payload))
        assert len(results) == 1
        assert isinstance(results[0]['value'], float)
        assert results[0]['value'] == 1000.0
