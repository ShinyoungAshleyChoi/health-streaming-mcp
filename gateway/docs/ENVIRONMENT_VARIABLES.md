# Environment Variables Documentation

This document describes all environment variables used by the Health Stack Kafka Gateway.

## Configuration Files

Environment variables can be set in multiple ways:

1. **`.env` file** - Main configuration file (gitignored)
2. **`.env.local`** - Local development template
3. **`.env.example`** - Example configuration with defaults
4. **Docker Compose** - Override via `docker-compose.yml` or `docker-compose.override.yml`
5. **System environment** - Direct environment variable export

## Quick Start

```bash
# Copy the local development template
cp gateway/.env.local gateway/.env

# Or copy the example file
cp gateway/.env.example gateway/.env

# Edit as needed
vim gateway/.env
```

## Core Configuration

### LOG_LEVEL

**Description:** Logging verbosity level

**Type:** String (enum)

**Valid Values:** `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Default:** `INFO`

**Development:** `DEBUG`

**Production:** `INFO` or `WARNING`

**Example:**
```bash
LOG_LEVEL=DEBUG
```

**Notes:**
- `DEBUG`: Verbose logging including request/response bodies
- `INFO`: Standard operational logging
- `WARNING`: Only warnings and errors
- `ERROR`: Only errors
- `CRITICAL`: Only critical failures

---

### PORT

**Description:** HTTP server port for the gateway API

**Type:** Integer

**Default:** `3000`

**Valid Range:** `1024-65535`

**Example:**
```bash
PORT=3000
```

**Notes:**
- Ports below 1024 require root privileges
- Ensure the port is not already in use
- Docker Compose maps this to host port 3000

---

### HOST

**Description:** Host address to bind the server

**Type:** String (IP address or hostname)

**Default:** `0.0.0.0` (all interfaces)

**Example:**
```bash
HOST=0.0.0.0
```

**Notes:**
- `0.0.0.0`: Listen on all network interfaces
- `127.0.0.1`: Listen only on localhost
- Use `0.0.0.0` in Docker containers

---

## Kafka Configuration

### KAFKA_BROKERS

**Description:** Comma-separated list of Kafka broker addresses

**Type:** String (comma-separated list)

**Default:** `localhost:19092,localhost:19093,localhost:19094`

**Format:** `host1:port1,host2:port2,host3:port3`

**Example:**
```bash
# Local development (Docker)
KAFKA_BROKERS=localhost:19092,localhost:19093,localhost:19094

# Production
KAFKA_BROKERS=kafka-1.example.com:9092,kafka-2.example.com:9092,kafka-3.example.com:9092

# Docker internal network
KAFKA_BROKERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
```

**Notes:**
- Use external ports (19092-19094) when gateway runs on host
- Use internal ports (9092) when gateway runs in Docker
- At least one broker must be reachable
- Multiple brokers provide failover capability

---

### KAFKA_TOPIC

**Description:** Main Kafka topic for health data messages

**Type:** String

**Default:** `health-data-raw`

**Example:**
```bash
KAFKA_TOPIC=health-data-raw
```

**Notes:**
- Topic must exist before sending messages
- Use `kafka-topics` command to create if needed
- Recommended: 6 partitions, replication factor 3

---

### KAFKA_DLQ_TOPIC

**Description:** Dead Letter Queue topic for failed messages

**Type:** String

**Default:** `health-data-dlq`

**Example:**
```bash
KAFKA_DLQ_TOPIC=health-data-dlq
```

**Notes:**
- Stores messages that failed after max retries
- Topic must exist before sending messages
- Recommended: 3 partitions, replication factor 3
- Monitor this topic for processing failures

---

### KAFKA_CLIENT_ID

**Description:** Unique identifier for this Kafka client

**Type:** String

**Default:** `health-gateway`

**Example:**
```bash
KAFKA_CLIENT_ID=health-gateway-prod-1
```

**Notes:**
- Used for logging and monitoring
- Should be unique per gateway instance
- Helps identify producers in Kafka metrics

---

### KAFKA_COMPRESSION_TYPE

**Description:** Compression algorithm for Kafka messages

**Type:** String (enum)

**Valid Values:** `none`, `gzip`, `snappy`, `lz4`, `zstd`

**Default:** `snappy`

**Example:**
```bash
KAFKA_COMPRESSION_TYPE=snappy
```

**Performance Comparison:**
- `none`: No compression, fastest, largest size
- `gzip`: High compression, slower, smallest size
- `snappy`: Balanced, good for most use cases
- `lz4`: Fast compression, good ratio
- `zstd`: Best compression, moderate speed

---

### KAFKA_ACKS

**Description:** Number of broker acknowledgments required

**Type:** String (enum)

**Valid Values:** `0`, `1`, `all`

**Default:** `all`

**Example:**
```bash
KAFKA_ACKS=all
```

**Options:**
- `0`: No acknowledgment (fire and forget, fastest, least reliable)
- `1`: Leader acknowledgment only (balanced)
- `all`: All in-sync replicas (slowest, most reliable)

**Recommendation:** Use `all` for production to prevent data loss

---

### KAFKA_MAX_RETRIES

**Description:** Maximum number of retry attempts for failed sends

**Type:** Integer

**Default:** `3`

**Valid Range:** `0-10`

**Example:**
```bash
KAFKA_MAX_RETRIES=3
```

**Notes:**
- After max retries, message is sent to DLQ
- Each retry uses exponential backoff
- Set to 0 to disable retries (not recommended)

---

### KAFKA_RETRY_BACKOFF_MS

**Description:** Initial backoff time between retries (milliseconds)

**Type:** Integer

**Default:** `100`

**Valid Range:** `0-10000`

**Example:**
```bash
KAFKA_RETRY_BACKOFF_MS=100
```

**Notes:**
- Uses exponential backoff: 100ms, 200ms, 400ms, etc.
- Maximum backoff is capped at 5000ms
- Lower values = faster retries but more load

---

### KAFKA_REQUEST_TIMEOUT_MS

**Description:** Timeout for Kafka requests (milliseconds)

**Type:** Integer

**Default:** `30000` (30 seconds)

**Valid Range:** `1000-120000`

**Example:**
```bash
KAFKA_REQUEST_TIMEOUT_MS=30000
```

**Notes:**
- Includes time for retries
- Should be higher than `KAFKA_RETRY_BACKOFF_MS * KAFKA_MAX_RETRIES`
- Increase for slow networks

---

### KAFKA_ENABLE_IDEMPOTENCE

**Description:** Enable idempotent producer (prevents duplicates)

**Type:** Boolean

**Valid Values:** `true`, `false`

**Default:** `true`

**Example:**
```bash
KAFKA_ENABLE_IDEMPOTENCE=true
```

**Notes:**
- Prevents duplicate messages on retry
- Recommended for production
- Requires `KAFKA_ACKS=all`

---

## Schema Registry Configuration

### SCHEMA_REGISTRY_URL

**Description:** URL of the Confluent Schema Registry

**Type:** String (URL)

**Default:** `http://localhost:8081`

**Example:**
```bash
# Local development
SCHEMA_REGISTRY_URL=http://localhost:8081

# Docker internal network
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Production
SCHEMA_REGISTRY_URL=https://schema-registry.example.com
```

**Notes:**
- Must be accessible from the gateway
- Used for schema registration and lookup
- Supports HTTP and HTTPS

---

### SCHEMA_REGISTRY_TIMEOUT_MS

**Description:** Timeout for Schema Registry requests (milliseconds)

**Type:** Integer

**Default:** `5000` (5 seconds)

**Valid Range:** `1000-30000`

**Example:**
```bash
SCHEMA_REGISTRY_TIMEOUT_MS=5000
```

---

### SCHEMA_CACHE_SIZE

**Description:** Maximum number of schemas to cache in memory

**Type:** Integer

**Default:** `100`

**Valid Range:** `10-1000`

**Example:**
```bash
SCHEMA_CACHE_SIZE=100
```

**Notes:**
- Reduces Schema Registry requests
- Increase for applications with many schemas
- Each schema is ~1-10KB in memory

---

## API Configuration

### RATE_LIMIT_REQUESTS

**Description:** Maximum requests per minute per client

**Type:** Integer

**Default:** `1000`

**Valid Range:** `1-100000`

**Example:**
```bash
RATE_LIMIT_REQUESTS=1000
```

**Notes:**
- Applied per client IP address
- Returns 429 when exceeded
- Set to 0 to disable rate limiting

---

### RATE_LIMIT_WINDOW_MS

**Description:** Time window for rate limiting (milliseconds)

**Type:** Integer

**Default:** `60000` (1 minute)

**Valid Range:** `1000-3600000`

**Example:**
```bash
RATE_LIMIT_WINDOW_MS=60000
```

---

### CORS_ORIGINS

**Description:** Allowed CORS origins (comma-separated)

**Type:** String (comma-separated list)

**Default:** `*` (all origins)

**Example:**
```bash
# Allow all (development only)
CORS_ORIGINS=*

# Specific origins (production)
CORS_ORIGINS=https://app.example.com,https://admin.example.com
```

**Notes:**
- Use `*` only in development
- Specify exact origins in production
- Supports multiple origins

---

### REQUEST_TIMEOUT_MS

**Description:** Maximum time to process a request (milliseconds)

**Type:** Integer

**Default:** `30000` (30 seconds)

**Valid Range:** `1000-120000`

**Example:**
```bash
REQUEST_TIMEOUT_MS=30000
```

---

### MAX_REQUEST_SIZE_MB

**Description:** Maximum request body size (megabytes)

**Type:** Integer

**Default:** `1`

**Valid Range:** `1-100`

**Example:**
```bash
MAX_REQUEST_SIZE_MB=1
```

**Notes:**
- Prevents large payload attacks
- Health data is typically < 10KB
- Increase if sending batch requests

---

## Security Configuration

### API_KEY_ENABLED

**Description:** Enable API key authentication

**Type:** Boolean

**Valid Values:** `true`, `false`

**Default:** `false`

**Example:**
```bash
API_KEY_ENABLED=true
```

---

### API_KEYS

**Description:** Comma-separated list of valid API keys

**Type:** String (comma-separated list)

**Default:** (empty)

**Example:**
```bash
API_KEYS=key1_abc123,key2_def456,key3_ghi789
```

**Notes:**
- Only used if `API_KEY_ENABLED=true`
- Store securely (use secrets management in production)
- Rotate keys regularly

---

## Monitoring Configuration

### METRICS_ENABLED

**Description:** Enable Prometheus metrics endpoint

**Type:** Boolean

**Valid Values:** `true`, `false`

**Default:** `true`

**Example:**
```bash
METRICS_ENABLED=true
```

---

### METRICS_PORT

**Description:** Port for metrics endpoint (if different from main port)

**Type:** Integer

**Default:** (same as PORT)

**Example:**
```bash
METRICS_PORT=9090
```

**Notes:**
- Leave empty to use main PORT
- Separate port useful for internal monitoring

---

## Development Configuration

### DEBUG

**Description:** Enable debug mode

**Type:** Boolean

**Valid Values:** `true`, `false`

**Default:** `false`

**Example:**
```bash
DEBUG=true
```

**Notes:**
- Enables detailed error messages
- Shows stack traces in responses
- **Never enable in production**

---

### RELOAD

**Description:** Enable auto-reload on code changes

**Type:** Boolean

**Valid Values:** `true`, `false`

**Default:** `false`

**Example:**
```bash
RELOAD=true
```

**Notes:**
- Only for development
- Automatically restarts on file changes
- Disable in production

---

## Example Configurations

### Local Development

```bash
# gateway/.env.local
LOG_LEVEL=DEBUG
PORT=3000
HOST=0.0.0.0

KAFKA_BROKERS=localhost:19092,localhost:19093,localhost:19094
KAFKA_TOPIC=health-data-raw
KAFKA_DLQ_TOPIC=health-data-dlq
KAFKA_CLIENT_ID=health-gateway-dev

SCHEMA_REGISTRY_URL=http://localhost:8081

RATE_LIMIT_REQUESTS=10000
CORS_ORIGINS=*

DEBUG=true
RELOAD=true
```

### Production

```bash
# Production environment
LOG_LEVEL=INFO
PORT=3000
HOST=0.0.0.0

KAFKA_BROKERS=kafka-1.prod:9092,kafka-2.prod:9092,kafka-3.prod:9092
KAFKA_TOPIC=health-data-raw
KAFKA_DLQ_TOPIC=health-data-dlq
KAFKA_CLIENT_ID=health-gateway-prod-1
KAFKA_COMPRESSION_TYPE=snappy
KAFKA_ACKS=all
KAFKA_ENABLE_IDEMPOTENCE=true

SCHEMA_REGISTRY_URL=https://schema-registry.prod:8081

RATE_LIMIT_REQUESTS=1000
CORS_ORIGINS=https://app.example.com

API_KEY_ENABLED=true
API_KEYS=${SECRET_API_KEYS}

METRICS_ENABLED=true

DEBUG=false
RELOAD=false
```

### Docker Compose

```yaml
# docker-compose.yml
services:
  gateway:
    environment:
      - LOG_LEVEL=INFO
      - KAFKA_BROKERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
      - SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

---

## Validation

To validate your configuration:

```bash
# Check if all required variables are set
cd gateway
uv run python -c "from config import settings; print('Config valid!')"

# View current configuration (sanitized)
docker-compose exec gateway env | grep -E "KAFKA|SCHEMA|LOG"
```

---

## Troubleshooting

### Configuration not loading

1. Check file location: `gateway/.env`
2. Verify file format (no spaces around `=`)
3. Restart the service: `docker-compose restart gateway`

### Environment variable not taking effect

1. Docker Compose overrides `.env` file
2. Check `docker-compose.yml` for hardcoded values
3. Use `docker-compose config` to see final configuration

### Sensitive data in logs

1. Never log API keys or tokens
2. Set `DEBUG=false` in production
3. Use secrets management (Vault, AWS Secrets Manager)

---

## Security Best Practices

1. **Never commit `.env` files** - Add to `.gitignore`
2. **Use secrets management** - Don't hardcode sensitive values
3. **Rotate credentials** - Change API keys regularly
4. **Limit access** - Use least privilege principle
5. **Audit logs** - Monitor who accesses configuration
6. **Encrypt at rest** - Use encrypted storage for secrets
7. **Use HTTPS** - Always use TLS in production

---

## References

- [Kafka Producer Configuration](https://kafka.apache.org/documentation/#producerconfigs)
- [Schema Registry Configuration](https://docs.confluent.io/platform/current/schema-registry/installation/config.html)
- [FastAPI Settings](https://fastapi.tiangolo.com/advanced/settings/)
