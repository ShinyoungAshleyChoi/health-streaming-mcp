# Avro Schemas

This directory contains Avro schema definitions for health-stack iOS app data.

## Schema Files

- **health-data.avsc**: Health data payload schema matching health-stack iOS app format

## Schema Structure

### HealthDataPayload Record

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| deviceId | string | Yes | Device unique identifier (UUID) |
| userId | string | Yes | User identifier |
| samples | array[HealthDataSample] | Yes | Array of health data samples |
| timestamp | string | Yes | Payload timestamp (ISO 8601) |
| appVersion | string | Yes | App version |

### HealthDataSample Record

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| id | string | Yes | Sample unique identifier (UUID) |
| type | string | Yes | Health data type (see supported types below) |
| value | double | Yes | Numeric value |
| unit | string | Yes | Unit of measurement |
| startDate | string | Yes | Start date/time (ISO 8601) |
| endDate | string | Yes | End date/time (ISO 8601) |
| sourceBundle | string | No | Source bundle identifier |
| metadata | map<string> | Yes | Additional metadata |
| isSynced | boolean | Yes | Sync status |
| createdAt | string | Yes | Creation timestamp (ISO 8601) |

## Supported Data Types

### Body Measurements
- `height` - 키 (cm)
- `bodyMass` - 체중 (kg)
- `bodyMassIndex` - BMI (count)
- `bodyFatPercentage` - 체지방률 (%)
- `leanBodyMass` - 제지방량 (kg)
- `waistCircumference` - 허리둘레 (cm)

### Activity
- `stepCount` - 걸음 수 (count)
- `distanceWalkingRunning` - 걷기/달리기 거리 (m)
- `flightsClimbed` - 오른 층수 (count)
- `activeEnergyBurned` - 활동 칼로리 (kcal)
- `basalEnergyBurned` - 기초 칼로리 (kcal)
- `exerciseTime` - 운동 시간 (s)
- `standHours` - 서 있는 시간 (s)

### Cardiovascular
- `heartRate` - 심박수 (count/min)
- `restingHeartRate` - 안정 시 심박수 (count/min)
- `heartRateVariability` - 심박 변이도 (ms)
- `bloodPressureSystolic` - 수축기 혈압 (mmHg)
- `bloodPressureDiastolic` - 이완기 혈압 (mmHg)
- `oxygenSaturation` - 산소포화도 (%)

### Sleep
- `sleepAnalysis` - 수면 분석 (min)
- `timeInBed` - 침대에 있던 시간 (min)

### Nutrition
- `dietaryEnergy` - 섭취 칼로리 (kcal)
- `dietaryProtein` - 단백질 (g)
- `dietaryCarbohydrates` - 탄수화물 (g)
- `dietaryFat` - 지방 (g)
- `dietaryFiber` - 식이섬유 (g)
- `dietarySugar` - 당류 (g)
- `dietaryWater` - 수분 (mL)

### Respiratory
- `respiratoryRate` - 호흡수 (count/min)
- `vo2Max` - 최대 산소 섭취량 (mL/kg/min)

### Other
- `bloodGlucose` - 혈당 (mg/dL)
- `bodyTemperature` - 체온 (°C)
- `mindfulMinutes` - 마음챙김 시간 (min)

## Validation

Validate schemas:
```bash
cd schemas
uv run python validate_schema.py
```

## Example Data

### Single Sample (Heart Rate)
```json
{
  "deviceId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user123",
  "samples": [
    {
      "id": "sample-uuid-001",
      "type": "heartRate",
      "value": 72.0,
      "unit": "count/min",
      "startDate": "2025-11-12T10:30:00Z",
      "endDate": "2025-11-12T10:30:00Z",
      "sourceBundle": "com.apple.health",
      "metadata": {
        "device": "Apple Watch"
      },
      "isSynced": true,
      "createdAt": "2025-11-12T10:30:05Z"
    }
  ],
  "timestamp": "2025-11-12T10:30:10Z",
  "appVersion": "1.2.3"
}
```

### Multiple Samples (Activity Data)
```json
{
  "deviceId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user123",
  "samples": [
    {
      "id": "sample-uuid-002",
      "type": "stepCount",
      "value": 8543.0,
      "unit": "count",
      "startDate": "2025-11-12T00:00:00Z",
      "endDate": "2025-11-12T23:59:59Z",
      "sourceBundle": "com.apple.health",
      "metadata": {},
      "isSynced": true,
      "createdAt": "2025-11-12T23:59:59Z"
    },
    {
      "id": "sample-uuid-003",
      "type": "activeEnergyBurned",
      "value": 450.5,
      "unit": "kcal",
      "startDate": "2025-11-12T00:00:00Z",
      "endDate": "2025-11-12T23:59:59Z",
      "sourceBundle": "com.apple.health",
      "metadata": {},
      "isSynced": true,
      "createdAt": "2025-11-12T23:59:59Z"
    }
  ],
  "timestamp": "2025-11-12T23:59:59Z",
  "appVersion": "1.2.3"
}
```
