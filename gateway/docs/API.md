# API Documentation

## Overview

The Health Stack Kafka Gateway provides a REST API for ingesting health data from iOS applications. All endpoints return JSON responses and follow standard HTTP status code conventions.

**Base URL:** `http://localhost:3000` (development)

**API Version:** v1

## Authentication

Currently, authentication is optional. When enabled, the API supports Bearer token authentication.

```http
Authorization: Bearer <your-token>
```

## Rate Limiting

- **Limit:** 1000 requests per minute per client
- **Response:** 429 Too Many Requests when exceeded
- **Headers:** `Retry-After` header indicates when to retry

## Common Headers

### Request Headers

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Yes | Must be `application/json` |
| `Authorization` | No | Bearer token for authentication |
| `X-Request-ID` | No | Client-provided request ID (UUID format) |

### Response Headers

| Header | Description |
|--------|-------------|
| `X-Request-ID` | Request identifier for tracing |
| `Content-Type` | Always `application/json` |
| `Retry-After` | Seconds to wait before retrying (on 429) |

## Endpoints

### POST /api/v1/health-data

Submit health data from iOS application.

#### Request

```http
POST /api/v1/health-data HTTP/1.1
Host: localhost:3000
Content-Type: application/json
X-Request-ID: 123e4567-e89b-12d3-a456-426614174000

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

#### Request Body Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `userId` | string (UUID) | Yes | Unique user identifier |
| `timestamp` | string (ISO8601) | Yes | Data collection timestamp |
| `dataType` | string (enum) | Yes | Type of health data (see Data Types) |
| `value` | number or object | Yes | Measurement value |
| `unit` | string | No | Unit of measurement |
| `metadata` | object | Yes | Device and app metadata |
| `metadata.deviceId` | string | Yes | Device identifier |
| `metadata.appVersion` | string | Yes | App version (semver) |
| `metadata.platform` | string | Yes | Platform (iOS, Android) |

#### Data Types

Supported `dataType` values:

- `heart_rate` - Heart rate in beats per minute
- `steps` - Step count
- `sleep` - Sleep duration or quality metrics
- `blood_pressure` - Blood pressure readings (systolic/diastolic)
- `blood_glucose` - Blood glucose level
- `body_temperature` - Body temperature
- `oxygen_saturation` - SpO2 percentage
- `respiratory_rate` - Breaths per minute
- `weight` - Body weight
- `height` - Body height

#### Success Response

**Status Code:** `200 OK`

```json
{
  "status": "success",
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

#### Error Responses

**400 Bad Request** - Invalid JSON format

```json
{
  "status": "error",
  "message": "Invalid JSON format",
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

**422 Unprocessable Entity** - Validation failed

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
      "field": "value",
      "message": "Heart rate must be between 30 and 250 bpm"
    }
  ],
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

**429 Too Many Requests** - Rate limit exceeded

```json
{
  "status": "error",
  "message": "Rate limit exceeded",
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

**500 Internal Server Error** - Server error

```json
{
  "status": "error",
  "message": "Internal server error",
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

**503 Service Unavailable** - Kafka unavailable

```json
{
  "status": "error",
  "message": "Service temporarily unavailable",
  "requestId": "123e4567-e89b-12d3-a456-426614174000",
  "timestamp": "2025-11-12T10:30:00.123Z"
}
```

---

### GET /health

Check service health and dependencies.

#### Request

```http
GET /health HTTP/1.1
Host: localhost:3000
```

#### Success Response

**Status Code:** `200 OK`

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

#### Degraded Response

**Status Code:** `200 OK` (still returns 200 but indicates issues)

```json
{
  "status": "degraded",
  "timestamp": "2025-11-12T10:30:00.123Z",
  "checks": {
    "kafka": {
      "status": "down",
      "error": "Connection timeout"
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

### GET /metrics

Prometheus-formatted metrics for monitoring.

#### Request

```http
GET /metrics HTTP/1.1
Host: localhost:3000
```

#### Response

**Status Code:** `200 OK`

**Content-Type:** `text/plain; version=0.0.4`

```
# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="POST",endpoint="/api/v1/health-data",status="200"} 1523

# HELP http_request_duration_seconds HTTP request duration in seconds
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.1"} 1200
http_request_duration_seconds_bucket{le="0.5"} 1500
http_request_duration_seconds_bucket{le="1.0"} 1520
http_request_duration_seconds_bucket{le="+Inf"} 1523
http_request_duration_seconds_sum 245.3
http_request_duration_seconds_count 1523

# HELP kafka_messages_sent_total Total Kafka messages sent
# TYPE kafka_messages_sent_total counter
kafka_messages_sent_total{topic="health-data-raw",status="success"} 1500
kafka_messages_sent_total{topic="health-data-dlq",status="success"} 23

# HELP kafka_send_errors_total Total Kafka send errors
# TYPE kafka_send_errors_total counter
kafka_send_errors_total{topic="health-data-raw"} 5
```

---

## Data Validation Rules

### User ID
- **Format:** UUID v4
- **Example:** `550e8400-e29b-41d4-a716-446655440000`

### Timestamp
- **Format:** ISO 8601 with timezone
- **Example:** `2025-11-12T10:30:00Z` or `2025-11-12T10:30:00+09:00`
- **Validation:** Must not be in the future

### Value Ranges

| Data Type | Valid Range | Unit |
|-----------|-------------|------|
| `heart_rate` | 30-250 | bpm |
| `steps` | 0-100000 | count |
| `blood_pressure` | systolic: 70-200, diastolic: 40-130 | mmHg |
| `blood_glucose` | 20-600 | mg/dL |
| `body_temperature` | 35.0-42.0 | °C |
| `oxygen_saturation` | 70-100 | % |
| `respiratory_rate` | 8-60 | breaths/min |
| `weight` | 0.5-500 | kg |

### Metadata Validation
- **deviceId:** Non-empty string, max 100 characters
- **appVersion:** Semantic versioning format (e.g., `1.2.3`)
- **platform:** Must be `iOS` or `Android`

---

## Example Requests

### Heart Rate Data

```bash
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: $(uuidgen)" \
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

### Blood Pressure Data

```bash
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2025-11-12T10:30:00Z",
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
  }'
```

### Steps Data

```bash
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2025-11-12T10:30:00Z",
    "dataType": "steps",
    "value": 8543,
    "unit": "count",
    "metadata": {
      "deviceId": "iPhone14-ABC123",
      "appVersion": "1.2.3",
      "platform": "iOS"
    }
  }'
```

---

## Error Handling

### Retry Strategy

For transient errors (500, 503), clients should implement exponential backoff:

```
Retry 1: Wait 1 second
Retry 2: Wait 2 seconds
Retry 3: Wait 4 seconds
Max retries: 3
```

### Non-Retryable Errors

Do not retry these errors:
- 400 Bad Request (fix the request format)
- 401 Unauthorized (check authentication)
- 422 Unprocessable Entity (fix validation errors)
- 429 Too Many Requests (wait for Retry-After period)

### Retryable Errors

Retry these errors with backoff:
- 500 Internal Server Error
- 503 Service Unavailable
- Network timeouts
- Connection errors

---

## Interactive API Documentation

For interactive API testing, visit the Swagger UI:

**URL:** http://localhost:3000/docs

The Swagger UI provides:
- Interactive request/response testing
- Schema validation
- Example payloads
- Authentication testing
- Response code documentation

---

## SDK Examples

### Python

```python
import requests
import uuid
from datetime import datetime, timezone

def send_health_data(user_id, data_type, value, unit=None):
    url = "http://localhost:3000/api/v1/health-data"
    
    payload = {
        "userId": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataType": data_type,
        "value": value,
        "unit": unit,
        "metadata": {
            "deviceId": "Python-Client",
            "appVersion": "1.0.0",
            "platform": "iOS"
        }
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-Request-ID": str(uuid.uuid4())
    }
    
    response = requests.post(url, json=payload, headers=headers)
    return response.json()

# Example usage
result = send_health_data(
    user_id="550e8400-e29b-41d4-a716-446655440000",
    data_type="heart_rate",
    value=72,
    unit="bpm"
)
print(result)
```

### Swift (iOS)

```swift
import Foundation

struct HealthData: Codable {
    let userId: String
    let timestamp: String
    let dataType: String
    let value: Double
    let unit: String?
    let metadata: Metadata
    
    struct Metadata: Codable {
        let deviceId: String
        let appVersion: String
        let platform: String
    }
}

func sendHealthData(userId: String, dataType: String, value: Double, unit: String?) async throws {
    let url = URL(string: "http://localhost:3000/api/v1/health-data")!
    
    let healthData = HealthData(
        userId: userId,
        timestamp: ISO8601DateFormatter().string(from: Date()),
        dataType: dataType,
        value: value,
        unit: unit,
        metadata: HealthData.Metadata(
            deviceId: UIDevice.current.identifierForVendor?.uuidString ?? "unknown",
            appVersion: Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "1.0.0",
            platform: "iOS"
        )
    )
    
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.setValue(UUID().uuidString, forHTTPHeaderField: "X-Request-ID")
    request.httpBody = try JSONEncoder().encode(healthData)
    
    let (data, response) = try await URLSession.shared.data(for: request)
    
    guard let httpResponse = response as? HTTPURLResponse,
          (200...299).contains(httpResponse.statusCode) else {
        throw URLError(.badServerResponse)
    }
}

// Example usage
Task {
    try await sendHealthData(
        userId: "550e8400-e29b-41d4-a716-446655440000",
        dataType: "heart_rate",
        value: 72,
        unit: "bpm"
    )
}
```

---

## Versioning

The API uses URL-based versioning. The current version is `v1`.

Future versions will be available at:
- `/api/v2/health-data`
- `/api/v3/health-data`

Version 1 will be maintained for backward compatibility.

---

## Support

For issues or questions:
- Check the troubleshooting guide in the main README
- Review logs: `docker-compose logs gateway`
- Open an issue on the project repository
