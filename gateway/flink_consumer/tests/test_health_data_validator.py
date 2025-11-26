"""Unit tests for HealthDataValidator"""

import pytest
from flink_consumer.validators.health_data_validator import HealthDataValidator


class TestHealthDataValidator:
    """Test suite for HealthDataValidator class"""

    @pytest.fixture
    def validator(self):
        """Create validator instance"""
        return HealthDataValidator(strict_mode=False)

    @pytest.fixture
    def strict_validator(self):
        """Create strict validator instance"""
        return HealthDataValidator(strict_mode=True)

    @pytest.fixture
    def valid_row(self):
        """Create valid test row"""
        return {
            'device_id': 'test-device-123',
            'user_id': 'test-user-456',
            'sample_id': 'sample-1',
            'data_type': 'heartRate',
            'value': 72.0,
            'unit': 'count/min',
            'start_date': 1700049600000,  # 2023-11-15 10:00:00 UTC
            'end_date': 1700049660000,    # 2023-11-15 10:01:00 UTC
            'source_bundle': 'com.apple.health',
            'metadata': {},
            'is_synced': True,
            'created_at': 1700049660000,
            'payload_timestamp': 1700049660000,
            'app_version': '1.0.0',
            'processing_time': 1700049660000
        }

    def test_filter_valid_row(self, validator, valid_row):
        """Test validation of valid row"""
        assert validator.filter(valid_row) is True

    def test_filter_missing_required_fields(self, validator, valid_row):
        """Test validation fails for missing required fields"""
        # Missing user_id
        row = valid_row.copy()
        del row['user_id']
        assert validator.filter(row) is False
        
        # Missing sample_id
        row = valid_row.copy()
        del row['sample_id']
        assert validator.filter(row) is False
        
        # Missing data_type
        row = valid_row.copy()
        del row['data_type']
        assert validator.filter(row) is False
        
        # Missing value
        row = valid_row.copy()
        del row['value']
        assert validator.filter(row) is False

    def test_filter_empty_required_fields(self, validator, valid_row):
        """Test validation fails for empty string fields"""
        row = valid_row.copy()
        row['user_id'] = ''
        assert validator.filter(row) is False
        
        row = valid_row.copy()
        row['user_id'] = '   '  # Whitespace only
        assert validator.filter(row) is False

    def test_filter_negative_value(self, validator, valid_row):
        """Test validation fails for negative values"""
        row = valid_row.copy()
        row['value'] = -10.0
        assert validator.filter(row) is False

    def test_filter_invalid_date_range(self, validator, valid_row):
        """Test validation fails when start_date > end_date"""
        row = valid_row.copy()
        row['start_date'] = 1700049660000
        row['end_date'] = 1700049600000  # Earlier than start
        assert validator.filter(row) is False

    def test_filter_heart_rate_range(self, validator, valid_row):
        """Test heart rate specific validation"""
        # Valid heart rate
        row = valid_row.copy()
        row['data_type'] = 'heartRate'
        row['value'] = 72.0
        assert validator.filter(row) is True
        
        # Too low
        row['value'] = 20.0
        assert validator.filter(row) is False
        
        # Too high
        row['value'] = 300.0
        assert validator.filter(row) is False
        
        # Edge cases
        row['value'] = 30.0  # Min
        assert validator.filter(row) is True
        
        row['value'] = 250.0  # Max
        assert validator.filter(row) is True

    def test_filter_steps_range(self, validator, valid_row):
        """Test steps specific validation"""
        row = valid_row.copy()
        row['data_type'] = 'steps'
        
        # Valid steps
        row['value'] = 5000.0
        assert validator.filter(row) is True
        
        # Negative steps
        row['value'] = -100.0
        assert validator.filter(row) is False
        
        # Too high
        row['value'] = 150000.0
        assert validator.filter(row) is False

    def test_filter_blood_pressure_range(self, validator, valid_row):
        """Test blood pressure specific validation"""
        # Systolic
        row = valid_row.copy()
        row['data_type'] = 'bloodPressureSystolic'
        row['value'] = 120.0
        assert validator.filter(row) is True
        
        row['value'] = 300.0  # Too high
        assert validator.filter(row) is False
        
        # Diastolic
        row['data_type'] = 'bloodPressureDiastolic'
        row['value'] = 80.0
        assert validator.filter(row) is True
        
        row['value'] = 200.0  # Too high
        assert validator.filter(row) is False

    def test_filter_unknown_data_type_non_strict(self, validator, valid_row):
        """Test unknown data type is allowed in non-strict mode"""
        row = valid_row.copy()
        row['data_type'] = 'unknownType'
        row['value'] = 100.0
        assert validator.filter(row) is True

    def test_filter_unknown_data_type_strict(self, strict_validator, valid_row):
        """Test unknown data type is rejected in strict mode"""
        row = valid_row.copy()
        row['data_type'] = 'unknownType'
        row['value'] = 100.0
        assert strict_validator.filter(row) is False

    def test_filter_invalid_numeric_value(self, validator, valid_row):
        """Test validation fails for non-numeric values"""
        row = valid_row.copy()
        row['value'] = 'not-a-number'
        assert validator.filter(row) is False

    def test_filter_oxygen_saturation_range(self, validator, valid_row):
        """Test oxygen saturation specific validation"""
        row = valid_row.copy()
        row['data_type'] = 'oxygenSaturation'
        
        # Valid percentage
        row['value'] = 98.0
        assert validator.filter(row) is True
        
        # Out of range
        row['value'] = 150.0
        assert validator.filter(row) is False
        
        row['value'] = -5.0
        assert validator.filter(row) is False

    def test_filter_body_temperature_range(self, validator, valid_row):
        """Test body temperature specific validation"""
        row = valid_row.copy()
        row['data_type'] = 'bodyTemperature'
        
        # Valid temperature
        row['value'] = 37.0
        assert validator.filter(row) is True
        
        # Too low
        row['value'] = 25.0
        assert validator.filter(row) is False
        
        # Too high
        row['value'] = 50.0
        assert validator.filter(row) is False

    def test_validation_count_tracking(self, validator, valid_row):
        """Test that validator tracks validation counts"""
        initial_count = validator.validation_count
        
        validator.filter(valid_row)
        assert validator.validation_count == initial_count + 1
        
        validator.filter(valid_row)
        assert validator.validation_count == initial_count + 2

    def test_rejection_count_tracking(self, validator, valid_row):
        """Test that validator tracks rejection counts"""
        initial_rejection = validator.rejection_count
        
        # Valid row - no rejection
        validator.filter(valid_row)
        assert validator.rejection_count == initial_rejection
        
        # Invalid row - rejection
        invalid_row = valid_row.copy()
        invalid_row['value'] = -10.0
        validator.filter(invalid_row)
        assert validator.rejection_count == initial_rejection + 1

    def test_filter_multiple_data_types(self, validator):
        """Test validation for multiple data types"""
        base_row = {
            'user_id': 'test-user',
            'sample_id': 'sample-1',
            'data_type': 'heartRate',
            'value': 72.0,
            'start_date': 1700049600000,
            'end_date': 1700049660000
        }
        
        # Test various data types with valid values
        test_cases = [
            ('heartRate', 72.0, True),
            ('steps', 5000.0, True),
            ('distance', 1000.0, True),
            ('activeEnergyBurned', 500.0, True),
            ('bloodGlucose', 100.0, True),
            ('bodyMass', 70.0, True),
            ('height', 175.0, True),
        ]
        
        for data_type, value, expected in test_cases:
            row = base_row.copy()
            row['data_type'] = data_type
            row['value'] = value
            assert validator.filter(row) == expected
