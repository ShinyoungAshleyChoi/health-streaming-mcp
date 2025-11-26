# Sample Requests and Responses

This document provides comprehensive examples of API requests and responses for the Health Stack Kafka Gateway.

## Table of Contents

- [Basic Examples](#basic-examples)
- [Data Type Examples](#data-type-examples)
- [Error Examples](#error-examples)
- [Advanced Examples](#advanced-examples)
- [Testing Scripts](#testing-scripts)

---

## Basic Examples

### Simple Heart Rate Submission

**Request:**
```bash
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2025-11-12T10:30:00Z",
    "dataType": "heart_rate",
    "value": 72,
    "unit": "bpm",
    "metadata": {
      "deviceId": "iPhone14-ABC123",
      "appVersion": "1.2.3",
      "platform": "iOS"
    }
  }'
```

**Response (200 OK):**
```json
{
  "status": "success",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### With Custom Request ID

**Request:**
```bash
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: 123e4567-e89b-12d3-a456-426614174000" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2025-11-12T10:30:00Z",
    "dataType": "heart_rate",
    "value": 72,
    "unit": "bpm",
    "metadata": {
      "deviceId": "iPhone14-ABC123",
      "appVersion": "1.2.3",
      "platform": "iOS"
    }
  }'
```

**Response (200 OK):**
```json
{
  "status": "success",
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

## Data Type Examples

### Heart Rate

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T10:30:00Z",
  "dataType": "heart_rate",
  "value": 72,
  "unit": "bpm",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 30-250 bpm

---

### Steps

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T23:59:59Z",
  "dataType": "steps",
  "value": 8543,
  "unit": "count",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 0-100000 steps

---

### Blood Pressure (Complex Value)

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T08:00:00Z",
  "dataType": "blood_pressure",
  "value": {
    "systolic": 120,
    "diastolic": 80
  },
  "unit": "mmHg",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 
- Systolic: 70-200 mmHg
- Diastolic: 40-130 mmHg

---

### Blood Glucose

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T07:30:00Z",
  "dataType": "blood_glucose",
  "value": 95,
  "unit": "mg/dL",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 20-600 mg/dL

---

### Body Temperature

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T06:00:00Z",
  "dataType": "body_temperature",
  "value": 36.8,
  "unit": "°C",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 35.0-42.0 °C

---

### Oxygen Saturation (SpO2)

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T22:00:00Z",
  "dataType": "oxygen_saturation",
  "value": 98,
  "unit": "%",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 70-100 %

---

### Sleep Data (Complex Value)

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T07:00:00Z",
  "dataType": "sleep",
  "value": {
    "duration_minutes": 480,
    "deep_sleep_minutes": 120,
    "light_sleep_minutes": 300,
    "rem_sleep_minutes": 60,
    "awake_minutes": 20,
    "quality_score": 85
  },
  "unit": "minutes",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

---

### Weight

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T06:30:00Z",
  "dataType": "weight",
  "value": 70.5,
  "unit": "kg",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 0.5-500 kg

---

### Respiratory Rate

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T22:30:00Z",
  "dataType": "respiratory_rate",
  "value": 16,
  "unit": "breaths/min",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Valid Range:** 8-60 breaths/min

---

## Error Examples

### 400 Bad Request - Invalid JSON

**Request:**
```bash
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d '{invalid json}'
```

**Response (400):**
```json
{
  "status": "error",
  "message": "Invalid JSON format",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### 422 Unprocessable Entity - Missing Required Field

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T10:30:00Z",
  "dataType": "heart_rate",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Response (422):**
```json
{
  "status": "error",
  "message": "Validation failed",
  "errors": [
    {
      "field": "value",
      "message": "Field required"
    }
  ],
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### 422 Unprocessable Entity - Invalid UUID

**Request:**
```json
{
  "userId": "invalid-uuid",
  "timestamp": "2025-11-12T10:30:00Z",
  "dataType": "heart_rate",
  "value": 72,
  "unit": "bpm",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Response (422):**
```json
{
  "status": "error",
  "message": "Validation failed",
  "errors": [
    {
      "field": "userId",
      "message": "Invalid UUID format"
    }
  ],
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### 422 Unprocessable Entity - Value Out of Range

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T10:30:00Z",
  "dataType": "heart_rate",
  "value": 300,
  "unit": "bpm",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Response (422):**
```json
{
  "status": "error",
  "message": "Validation failed",
  "errors": [
    {
      "field": "value",
      "message": "Heart rate must be between 30 and 250 bpm"
    }
  ],
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### 422 Unprocessable Entity - Invalid Timestamp

**Request:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-13-45T99:99:99Z",
  "dataType": "heart_rate",
  "value": 72,
  "unit": "bpm",
  "metadata": {
    "deviceId": "iPhone14-ABC123",
    "appVersion": "1.2.3",
    "platform": "iOS"
  }
}
```

**Response (422):**
```json
{
  "status": "error",
  "message": "Validation failed",
  "errors": [
    {
      "field": "timestamp",
      "message": "Invalid ISO 8601 timestamp format"
    }
  ],
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### 422 Unprocessable Entity - Multiple Errors

**Request:**
```json
{
  "userId": "invalid-uuid",
  "timestamp": "invalid-timestamp",
  "dataType": "heart_rate",
  "value": 300,
  "metadata": {
    "deviceId": "",
    "appVersion": "invalid",
    "platform": "iOS"
  }
}
```

**Response (422):**
```json
{
  "status": "error",
  "message": "Validation failed",
  "errors": [
    {
      "field": "userId",
      "message": "Invalid UUID format"
    },
    {
      "field": "timestamp",
      "message": "Invalid ISO 8601 timestamp format"
    },
    {
      "field": "value",
      "message": "Heart rate must be between 30 and 250 bpm"
    },
    {
      "field": "metadata.deviceId",
      "message": "Device ID cannot be empty"
    },
    {
      "field": "metadata.appVersion",
      "message": "Invalid semantic version format"
    }
  ],
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### 429 Too Many Requests

**Response (429):**
```json
{
  "status": "error",
  "message": "Rate limit exceeded. Please try again later.",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

**Headers:**
```
Retry-After: 60
```

---

### 500 Internal Server Error

**Response (500):**
```json
{
  "status": "error",
  "message": "Internal server error",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### 503 Service Unavailable - Kafka Down

**Response (503):**
```json
{
  "status": "error",
  "message": "Service temporarily unavailable. Please try again later.",
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

## Advanced Examples

### Batch Testing Script

```bash
#!/bin/bash

# Send multiple health data points
USER_ID="550e8400-e29b-41d4-a716-446655440000"
BASE_URL="http://localhost:3000/api/v1/health-data"

# Heart rate
curl -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d "{
    \"userId\": \"$USER_ID\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"dataType\": \"heart_rate\",
    \"value\": 72,
    \"unit\": \"bpm\",
    \"metadata\": {
      \"deviceId\": \"iPhone14-ABC123\",
      \"appVersion\": \"1.2.3\",
      \"platform\": \"iOS\"
    }
  }"

# Steps
curl -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d "{
    \"userId\": \"$USER_ID\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"dataType\": \"steps\",
    \"value\": 8543,
    \"unit\": \"count\",
    \"metadata\": {
      \"deviceId\": \"iPhone14-ABC123\",
      \"appVersion\": \"1.2.3\",
      \"platform\": \"iOS\"
    }
  }"

# Blood pressure
curl -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d "{
    \"userId\": \"$USER_ID\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"dataType\": \"blood_pressure\",
    \"value\": {\"systolic\": 120, \"diastolic\": 80},
    \"unit\": \"mmHg\",
    \"metadata\": {
      \"deviceId\": \"iPhone14-ABC123\",
      \"appVersion\": \"1.2.3\",
      \"platform\": \"iOS\"
    }
  }"
```

---

### Python Testing Script

```python
#!/usr/bin/env python3
import requests
import uuid
from datetime import datetime, timezone
import json

BASE_URL = "http://localhost:3000/api/v1/health-data"
USER_ID = "550e8400-e29b-41d4-a716-446655440000"

def send_health_data(data_type, value, unit=None):
    """Send health data to the gateway"""
    payload = {
        "userId": USER_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataType": data_type,
        "value": value,
        "unit": unit,
        "metadata": {
            "deviceId": "Python-Test-Client",
            "appVersion": "1.0.0",
            "platform": "iOS"
        }
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-Request-ID": str(uuid.uuid4())
    }
    
    try:
        response = requests.post(BASE_URL, json=payload, headers=headers)
        print(f"[{data_type}] Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}\n")
        return response
    except Exception as e:
        print(f"Error: {e}\n")
        return None

# Test various data types
print("=== Testing Health Data Submission ===\n")

send_health_data("heart_rate", 72, "bpm")
send_health_data("steps", 8543, "count")
send_health_data("blood_glucose", 95, "mg/dL")
send_health_data("body_temperature", 36.8, "°C")
send_health_data("oxygen_saturation", 98, "%")
send_health_data("blood_pressure", {"systolic": 120, "diastolic": 80}, "mmHg")

print("=== Testing Error Cases ===\n")

# Invalid heart rate
send_health_data("heart_rate", 300, "bpm")

# Missing unit
send_health_data("heart_rate", 72, None)
```

---

### Load Testing with Apache Bench

```bash
# Create a test payload file
cat > test_payload.json <<EOF
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-12T10:30:00Z",
  "dataType": "heart_rate",
  "value": 72,
  "unit": "bpm",
  "metadata": {
    "deviceId": "LoadTest-Client",
    "appVersion": "1.0.0",
    "platform": "iOS"
  }
}
EOF

# Run load test: 1000 requests, 10 concurrent
ab -n 1000 -c 10 -p test_payload.json \
  -T "application/json" \
  http://localhost:3000/api/v1/health-data
```

---

### Health Check Examples

**Request:**
```bash
curl http://localhost:3000/health
```

**Response (Healthy):**
```json
{
  "status": "healthy",
  "timestamp": "2025-11-12T10:30:00.123Z",
  "checks": {
    "kafka": {
      "status": "up",
      "latency_ms": 5
    },
    "schemaRegistry": {
      "status": "up",
      "latency_ms": 3
    }
  },
  "uptime_seconds": 3600
}
```

**Response (Degraded):**
```json
{
  "status": "degraded",
  "timestamp": "2025-11-12T10:30:00.123Z",
  "checks": {
    "kafka": {
      "status": "down",
      "error": "Connection timeout after 5000ms"
    },
    "schemaRegistry": {
      "status": "up",
      "latency_ms": 3
    }
  },
  "uptime_seconds": 3600
}
```

---

### Metrics Examples

**Request:**
```bash
curl http://localhost:3000/metrics
```

**Response (Prometheus Format):**
```
# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="POST",endpoint="/api/v1/health-data",status="200"} 1523
http_requests_total{method="POST",endpoint="/api/v1/health-data",status="422"} 45
http_requests_total{method="GET",endpoint="/health",status="200"} 234

# HELP http_request_duration_seconds HTTP request duration in seconds
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.005"} 500
http_request_duration_seconds_bucket{le="0.01"} 1000
http_request_duration_seconds_bucket{le="0.025"} 1400
http_request_duration_seconds_bucket{le="0.05"} 1480
http_request_duration_seconds_bucket{le="0.1"} 1500
http_request_duration_seconds_bucket{le="0.25"} 1515
http_request_duration_seconds_bucket{le="0.5"} 1520
http_request_duration_seconds_bucket{le="1.0"} 1522
http_request_duration_seconds_bucket{le="+Inf"} 1523
http_request_duration_seconds_sum 245.3
http_request_duration_seconds_count 1523

# HELP kafka_messages_sent_total Total Kafka messages sent
# TYPE kafka_messages_sent_total counter
kafka_messages_sent_total{topic="health-data-raw",status="success"} 1500
kafka_messages_sent_total{topic="health-data-dlq",status="success"} 23

# HELP kafka_send_errors_total Total Kafka send errors
# TYPE kafka_send_errors_total counter
kafka_send_errors_total{topic="health-data-raw",error_type="timeout"} 3
kafka_send_errors_total{topic="health-data-raw",error_type="broker_unavailable"} 2
```

---

## Testing Scripts

### Complete Test Suite

Save as `test_api.sh`:

```bash
#!/bin/bash

set -e

BASE_URL="http://localhost:3000"
USER_ID="550e8400-e29b-41d4-a716-446655440000"

echo "=== Health Stack Gateway API Tests ==="
echo ""

# Test 1: Health check
echo "Test 1: Health Check"
curl -s $BASE_URL/health | jq .
echo ""

# Test 2: Valid heart rate
echo "Test 2: Valid Heart Rate"
curl -s -X POST $BASE_URL/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d "{
    \"userId\": \"$USER_ID\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"dataType\": \"heart_rate\",
    \"value\": 72,
    \"unit\": \"bpm\",
    \"metadata\": {
      \"deviceId\": \"Test-Device\",
      \"appVersion\": \"1.0.0\",
      \"platform\": \"iOS\"
    }
  }" | jq .
echo ""

# Test 3: Invalid value (out of range)
echo "Test 3: Invalid Heart Rate (Out of Range)"
curl -s -X POST $BASE_URL/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d "{
    \"userId\": \"$USER_ID\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"dataType\": \"heart_rate\",
    \"value\": 300,
    \"unit\": \"bpm\",
    \"metadata\": {
      \"deviceId\": \"Test-Device\",
      \"appVersion\": \"1.0.0\",
      \"platform\": \"iOS\"
    }
  }" | jq .
echo ""

# Test 4: Missing required field
echo "Test 4: Missing Required Field"
curl -s -X POST $BASE_URL/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d "{
    \"userId\": \"$USER_ID\",
    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"dataType\": \"heart_rate\",
    \"metadata\": {
      \"deviceId\": \"Test-Device\",
      \"appVersion\": \"1.0.0\",
      \"platform\": \"iOS\"
    }
  }" | jq .
echo ""

# Test 5: Metrics
echo "Test 5: Metrics Endpoint"
curl -s $BASE_URL/metrics | head -20
echo ""

echo "=== All Tests Complete ==="
```

Make executable and run:
```bash
chmod +x test_api.sh
./test_api.sh
```

---

## Verifying Messages in Kafka

After sending messages, verify they reached Kafka:

```bash
# View messages in Kafka UI
open http://localhost:8080

# Or use CLI
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --from-beginning \
  --max-messages 10
```

---

## Additional Resources

- [API Documentation](./API.md)
- [Environment Variables](./ENVIRONMENT_VARIABLES.md)
- [Troubleshooting Guide](./TROUBLESHOOTING.md)
- [Main README](../README.md)
