"""Pytest configuration and shared fixtures"""

import pytest
import logging


@pytest.fixture(autouse=True)
def setup_logging():
    """Configure logging for tests"""
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


@pytest.fixture
def sample_health_data_payload():
    """Fixture providing a complete sample health data payload"""
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
            }
        ]
    }


@pytest.fixture
def sample_transformed_row():
    """Fixture providing a sample transformed health data row"""
    return {
        'device_id': 'test-device-123',
        'user_id': 'test-user-456',
        'sample_id': 'sample-1',
        'data_type': 'heartRate',
        'value': 72.0,
        'unit': 'count/min',
        'start_date': 1700049600000,
        'end_date': 1700049660000,
        'source_bundle': 'com.apple.health',
        'metadata': {},
        'is_synced': True,
        'created_at': 1700049660000,
        'payload_timestamp': 1700049660000,
        'app_version': '1.0.0',
        'processing_time': 1700049660000
    }
