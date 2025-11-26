"""
Health data models
"""
from typing import Dict, List, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class HealthDataSample(BaseModel):
    """Individual health data sample"""
    id: str = Field(..., description="Sample unique identifier (UUID)")
    type: str = Field(..., description="Health data type")
    value: float = Field(..., description="Numeric value")
    unit: str = Field(..., description="Unit of measurement")
    startDate: str = Field(..., description="Start date/time (ISO 8601)")
    endDate: str = Field(..., description="End date/time (ISO 8601)")
    sourceBundle: Optional[str] = Field(None, description="Source bundle identifier")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Additional metadata")
    isSynced: bool = Field(..., description="Sync status")
    createdAt: str = Field(..., description="Creation timestamp (ISO 8601)")
    
    @field_validator('startDate', 'endDate', 'createdAt', mode='before')
    @classmethod
    def validate_iso8601(cls, v: Union[str, int, float]) -> str:
        """Validate and convert timestamp to ISO 8601 string format"""
        # If it's a number (Unix timestamp), convert to ISO 8601 string
        if isinstance(v, (int, float)):
            try:
                dt = datetime.fromtimestamp(v)
                return dt.isoformat() + 'Z'
            except (ValueError, OSError) as e:
                raise ValueError(f"Invalid Unix timestamp: {v}")
        
        # If it's already a string, validate ISO 8601 format
        if isinstance(v, str):
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
                return v
            except ValueError:
                raise ValueError(f"Invalid ISO 8601 timestamp: {v}")
        
        raise ValueError(f"Timestamp must be string or number, got {type(v)}")
    
    @field_validator('type')
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate health data type"""
        valid_types = {
            # Body Measurements
            'height', 'bodyMass', 'bodyMassIndex', 'bodyFatPercentage',
            'leanBodyMass', 'waistCircumference',
            # Activity
            'stepCount', 'distanceWalkingRunning', 'flightsClimbed',
            'activeEnergyBurned', 'basalEnergyBurned', 'exerciseTime', 'standHours',
            # Cardiovascular
            'heartRate', 'restingHeartRate', 'heartRateVariability',
            'bloodPressureSystolic', 'bloodPressureDiastolic', 'oxygenSaturation',
            # Sleep
            'sleepAnalysis', 'timeInBed',
            # Nutrition
            'dietaryEnergy', 'dietaryProtein', 'dietaryCarbohydrates',
            'dietaryFat', 'dietaryFiber', 'dietarySugar', 'dietaryWater',
            # Respiratory
            'respiratoryRate', 'vo2Max',
            # Other
            'bloodGlucose', 'bodyTemperature', 'mindfulMinutes'
        }
        
        if v not in valid_types:
            raise ValueError(f"Invalid health data type: {v}")
        
        return v


class HealthDataPayload(BaseModel):
    """Health data payload from iOS app"""
    deviceId: str = Field(..., description="Device unique identifier (UUID)")
    userId: str = Field(..., description="User identifier")
    samples: List[HealthDataSample] = Field(..., description="Array of health data samples")
    timestamp: str = Field(..., description="Payload timestamp (ISO 8601)")
    appVersion: str = Field(..., description="App version")
    
    @field_validator('timestamp')
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Validate ISO 8601 timestamp format"""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError(f"Invalid ISO 8601 timestamp: {v}")
    
    @field_validator('samples')
    @classmethod
    def validate_samples_not_empty(cls, v: List[HealthDataSample]) -> List[HealthDataSample]:
        """Ensure samples array is not empty"""
        if not v:
            raise ValueError("Samples array cannot be empty")
        return v


class HealthDataResponse(BaseModel):
    """Response for health data submission"""
    status: str = Field(..., description="Status of the request")
    requestId: str = Field(..., description="Request ID")
    timestamp: str = Field(..., description="Response timestamp (ISO 8601)")
    samplesReceived: int = Field(..., description="Number of samples received")
