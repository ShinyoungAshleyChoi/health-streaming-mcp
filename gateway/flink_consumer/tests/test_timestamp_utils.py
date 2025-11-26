"""Unit tests for timestamp conversion utilities"""

import pytest
from datetime import datetime, timezone
from flink_consumer.converters.health_data_transformer import HealthDataTransformer


class TestTimestampUtils:
    """Test suite for timestamp parsing and conversion utilities"""

    def test_parse_timestamp_iso8601_with_z(self):
        """Test parsing ISO 8601 timestamp with Z suffix"""
        result = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00Z')
        
        # Verify it returns a timestamp in milliseconds
        assert result is not None
        assert isinstance(result, int)
        
        # Verify the conversion is correct
        expected_dt = datetime(2025, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
        expected_ms = int(expected_dt.timestamp() * 1000)
        assert result == expected_ms

    def test_parse_timestamp_iso8601_with_timezone(self):
        """Test parsing ISO 8601 timestamp with timezone offset"""
        result = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00+00:00')
        
        assert result is not None
        assert isinstance(result, int)
        
        # Should be same as Z format
        expected_dt = datetime(2025, 11, 15, 10, 0, 0, tzinfo=timezone.utc)
        expected_ms = int(expected_dt.timestamp() * 1000)
        assert result == expected_ms

    def test_parse_timestamp_with_milliseconds(self):
        """Test parsing ISO 8601 timestamp with milliseconds"""
        result = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00.123Z')
        
        assert result is not None
        assert isinstance(result, int)
        
        # Verify milliseconds are preserved
        expected_dt = datetime(2025, 11, 15, 10, 0, 0, 123000, tzinfo=timezone.utc)
        expected_ms = int(expected_dt.timestamp() * 1000)
        assert result == expected_ms

    def test_parse_timestamp_with_microseconds(self):
        """Test parsing ISO 8601 timestamp with microseconds"""
        result = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00.123456Z')
        
        assert result is not None
        assert isinstance(result, int)

    def test_parse_timestamp_different_timezones(self):
        """Test parsing timestamps with different timezone offsets"""
        # UTC
        ts_utc = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00+00:00')
        
        # UTC+5
        ts_plus5 = HealthDataTransformer._parse_timestamp('2025-11-15T15:00:00+05:00')
        
        # UTC-5
        ts_minus5 = HealthDataTransformer._parse_timestamp('2025-11-15T05:00:00-05:00')
        
        # All should represent the same moment in time
        assert ts_utc == ts_plus5
        assert ts_utc == ts_minus5

    def test_parse_timestamp_none_input(self):
        """Test parsing None returns None"""
        result = HealthDataTransformer._parse_timestamp(None)
        assert result is None

    def test_parse_timestamp_empty_string(self):
        """Test parsing empty string returns None"""
        result = HealthDataTransformer._parse_timestamp('')
        assert result is None

    def test_parse_timestamp_invalid_format(self):
        """Test parsing invalid timestamp format returns None"""
        invalid_formats = [
            'not-a-date',
            '2025-13-45',  # Invalid month/day
            '2025/11/15',  # Wrong separator
            '15-11-2025',  # Wrong order
            '2025-11-15',  # Missing time
            'November 15, 2025',  # Text format
        ]
        
        for invalid in invalid_formats:
            result = HealthDataTransformer._parse_timestamp(invalid)
            assert result is None, f"Expected None for invalid format: {invalid}"

    def test_parse_timestamp_edge_cases(self):
        """Test parsing edge case timestamps"""
        # Midnight
        result = HealthDataTransformer._parse_timestamp('2025-01-01T00:00:00Z')
        assert result is not None
        
        # End of day
        result = HealthDataTransformer._parse_timestamp('2025-12-31T23:59:59Z')
        assert result is not None
        
        # Leap year date
        result = HealthDataTransformer._parse_timestamp('2024-02-29T12:00:00Z')
        assert result is not None

    def test_parse_timestamp_consistency(self):
        """Test that parsing the same timestamp multiple times gives same result"""
        timestamp_str = '2025-11-15T10:00:00Z'
        
        result1 = HealthDataTransformer._parse_timestamp(timestamp_str)
        result2 = HealthDataTransformer._parse_timestamp(timestamp_str)
        result3 = HealthDataTransformer._parse_timestamp(timestamp_str)
        
        assert result1 == result2 == result3

    def test_parse_timestamp_millisecond_precision(self):
        """Test that millisecond precision is maintained"""
        # Test with specific milliseconds
        ts1 = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00.000Z')
        ts2 = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00.001Z')
        ts3 = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00.999Z')
        
        # Should differ by exactly 1ms and 999ms
        assert ts2 - ts1 == 1
        assert ts3 - ts1 == 999

    def test_parse_timestamp_year_range(self):
        """Test parsing timestamps across different years"""
        # Past
        result_past = HealthDataTransformer._parse_timestamp('2020-01-01T00:00:00Z')
        assert result_past is not None
        
        # Present
        result_present = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00Z')
        assert result_present is not None
        
        # Future
        result_future = HealthDataTransformer._parse_timestamp('2030-12-31T23:59:59Z')
        assert result_future is not None
        
        # Verify chronological order
        assert result_past < result_present < result_future

    def test_parse_timestamp_returns_integer(self):
        """Test that parsed timestamp is always an integer"""
        result = HealthDataTransformer._parse_timestamp('2025-11-15T10:00:00.123Z')
        
        assert isinstance(result, int)
        assert not isinstance(result, float)
        assert not isinstance(result, bool)  # bool is subclass of int in Python
