"""
Health data validator
"""
from typing import Dict, List, Any
from models.health_data import HealthDataPayload, HealthDataSample


class ValidationError(Exception):
    """Validation error exception"""
    def __init__(self, message: str, errors: List[Dict[str, Any]] = None):
        self.message = message
        self.errors = errors or []
        super().__init__(self.message)


class HealthDataValidator:
    """Validator for health data payloads"""
    
    # Value ranges for validation
    VALUE_RANGES = {
        'heartRate': (0, 250),
        'restingHeartRate': (0, 150),
        'bloodPressureSystolic': (0, 250),
        'bloodPressureDiastolic': (0, 150),
        'oxygenSaturation': (0, 100),
        'bodyTemperature': (0, 42),
        'bloodGlucose': (0, 600),
        'bodyMass': (0, 1000),
        'height': (0, 1000),
        'bodyMassIndex': (0, 60),
        'bodyFatPercentage': (0, 100),
    }
    
    @staticmethod
    def validate(payload: HealthDataPayload) -> HealthDataPayload:
        """
        Validate health data payload
        
        Args:
            payload: Health data payload to validate
            
        Returns:
            Validated payload
            
        Raises:
            ValidationError: If validation fails
        """
        errors = []
        
        # Validate each sample
        for idx, sample in enumerate(payload.samples):
            sample_errors = HealthDataValidator._validate_sample(sample, idx)
            errors.extend(sample_errors)
        
        if errors:
            raise ValidationError("Validation failed", errors)
        
        return payload
    
    @staticmethod
    def _validate_sample(sample: HealthDataSample, index: int) -> List[Dict[str, Any]]:
        """Validate individual sample"""
        errors = []
        
        # Validate value range if applicable
        if sample.type in HealthDataValidator.VALUE_RANGES:
            min_val, max_val = HealthDataValidator.VALUE_RANGES[sample.type]
            if not (min_val <= sample.value <= max_val):
                errors.append({
                    "sample_index": index,
                    "field": "value",
                    "type": sample.type,
                    "value": sample.value,
                    "message": f"Value {sample.value} out of range [{min_val}, {max_val}] for type {sample.type}"
                })
        
        # Validate value is positive for most types
        if sample.value < 0 and sample.type not in ['bodyTemperature']:
            errors.append({
                "sample_index": index,
                "field": "value",
                "type": sample.type,
                "value": sample.value,
                "message": f"Value must be positive for type {sample.type}"
            })
        
        return errors
    
    @staticmethod
    def sanitize(payload: HealthDataPayload) -> HealthDataPayload:
        """
        Sanitize health data payload
        
        Args:
            payload: Health data payload to sanitize
            
        Returns:
            Sanitized payload
        """
        # Trim whitespace from string fields
        payload.deviceId = payload.deviceId.strip()
        payload.userId = payload.userId.strip()
        payload.appVersion = payload.appVersion.strip()
        
        for sample in payload.samples:
            sample.id = sample.id.strip()
            sample.type = sample.type.strip()
            sample.unit = sample.unit.strip()
            
            # Sanitize metadata
            sample.metadata = {
                k.strip(): v.strip() 
                for k, v in sample.metadata.items()
            }
        
        return payload
