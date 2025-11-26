# Health Stack Kafka Gateway

API Gateway for health-stack iOS app data processing. Receives JSON health data, converts to Avro format, and sends to Kafka cluster.

## Project Structure

```
.
├── gateway/                    # FastAPI application
│   ├── main.py                # Application entry point
│   ├── config.py              # Configuration management
│   ├── converters/            # Avro conversion logic
│   ├── middleware/            # Request/response middleware
│   ├── models/                # Data models
│   ├── routers/               # API endpoints
│   ├── services/              # Business logic services
│   ├── utils/                 # Utility functions
│   ├── validators/            # Request validators
│   ├── .env.example           # Example environment variables
│   ├── .env.local             # Local development environment template
│   └── pyproject.toml         # Python dependencies (uv)
├── schemas/                   # Avro schema definitions
├── kafka/                     # Kafka configuration files
├── docker-compose.yml         # Production Docker Compose config
└── docker-compose.override.yml # Development overrides
```

## Prerequisites

- Python 3.11+
- uv (Python package manager)
- Docker & Docker Compose

## Installation

1. Install uv if not already installed:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Install dependencies:
```bash
cd gateway
uv sync
```

3. Set up environment variables:
```bash
cp gateway/.env.local gateway/.env
```

## Local Development Setup

### Option 1: Run Everything in Docker (Recommended)

This approach runs the entire stack (Kafka + Gateway) in Docker with hot reload enabled.

1. Start all services with development overrides:
```bash
docker-compose up -d
```

The `docker-compose.override.yml` file automatically:
- Enables hot reload for the gateway service
- Mounts your local code into the container
- Sets DEBUG log level
- Reduces resource usage for local development

2. View logs:
```bash
# All services
docker-compose logs -f

# Gateway only
docker-compose logs -f gateway

# Kafka brokers
docker-compose logs -f kafka-broker-1
```

3. Make code changes - the gateway will automatically reload!

### Option 2: Run Gateway Locally (Kafka in Docker)

This approach runs Kafka in Docker but the gateway on your host machine for faster iteration.

1. Start only Kafka infrastructure:
```bash
docker-compose up -d kafka-broker-1 kafka-broker-2 kafka-broker-3 schema-registry kafka-ui
```

2. Run the gateway locally with hot reload:
```bash
cd gateway
uv run uvicorn main:app --reload --host 0.0.0.0 --port 3000
```

3. Make code changes - uvicorn will automatically reload!

### Initialize Kafka Topics

After starting Kafka (either option), wait 30-60 seconds for brokers to be ready, then create topics:

```bash
# Create health-data-raw topic
docker-compose exec kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --partitions 6 \
  --replication-factor 3 \
  --if-not-exists

# Create dead letter queue topic
docker-compose exec kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-dlq \
  --partitions 3 \
  --replication-factor 3 \
  --if-not-exists
```

Verify topics were created:
```bash
docker-compose exec kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092
```

## Services

Once running, access:
- **API Gateway**: http://localhost:3000
- **API Docs (Swagger)**: http://localhost:3000/docs
- **Health Check**: http://localhost:3000/health
- **Metrics**: http://localhost:3000/metrics
- **Kafka UI**: http://localhost:8080
- **Schema Registry**: http://localhost:8081

## Testing the API

### Using curl

Send a test health data message:
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

Check health status:
```bash
curl http://localhost:3000/health
```

### Using Swagger UI

Navigate to http://localhost:3000/docs for interactive API documentation.

### Verify Messages in Kafka

1. Open Kafka UI at http://localhost:8080
2. Navigate to Topics → health-data-raw
3. View messages in the Messages tab

Or use the CLI:
```bash
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --from-beginning \
  --max-messages 10
```

## Development Workflow

### Making Code Changes

1. Edit files in the `gateway/` directory
2. Save your changes
3. The application automatically reloads (watch the logs)
4. Test your changes immediately

### Viewing Logs

```bash
# Gateway logs (structured JSON)
docker-compose logs -f gateway

# Kafka broker logs
docker-compose logs -f kafka-broker-1

# All services
docker-compose logs -f
```

### Debugging

Set `LOG_LEVEL=DEBUG` in `gateway/.env` for detailed logging:
```bash
LOG_LEVEL=DEBUG
```

Then restart the gateway:
```bash
docker-compose restart gateway
```

### Running Tests

#### Unit Tests

```bash
cd gateway
uv run pytest
```

#### Integration Tests

Integration tests require the full test environment (Kafka, Schema Registry, Gateway).

**Quick Start:**
```bash
# Run all integration tests with automated setup/teardown
./run_integration_tests.sh
```

**Manual Setup:**
```bash
# 1. Start test environment
docker-compose -f docker-compose.test.yml up -d

# 2. Wait for services to be ready (30-60 seconds)
sleep 60

# 3. Run tests
cd gateway
uv run pytest tests/ -v

# 4. Cleanup
docker-compose -f docker-compose.test.yml down -v
```

**Test Categories:**
- API endpoint integration tests
- Kafka message verification tests
- End-to-end pipeline tests
- Schema evolution tests
- Failover scenario tests

See `gateway/tests/README.md` for detailed testing documentation.

## Useful Commands

### Docker Compose

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# Rebuild gateway image
docker-compose build gateway

# Restart gateway only
docker-compose restart gateway

# View service status
docker-compose ps

# View resource usage
docker stats
```

### Kafka Management

```bash
# List topics
docker-compose exec kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092

# Describe topic
docker-compose exec kafka-broker-1 kafka-topics \
  --describe \
  --topic health-data-raw \
  --bootstrap-server kafka-broker-1:9092

# Delete topic (careful!)
docker-compose exec kafka-broker-1 kafka-topics \
  --delete \
  --topic health-data-raw \
  --bootstrap-server kafka-broker-1:9092

# Consume messages
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --from-beginning

# Check consumer groups
docker-compose exec kafka-broker-1 kafka-consumer-groups \
  --list \
  --bootstrap-server kafka-broker-1:9092
```

### Schema Registry

```bash
# List all schemas
curl http://localhost:8081/subjects

# Get schema for subject
curl http://localhost:8081/subjects/health-data-raw-value/versions/latest

# Check compatibility
curl -X POST http://localhost:8081/compatibility/subjects/health-data-raw-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @schemas/health-data.avsc
```

## Troubleshooting

### Gateway won't start

1. Check if port 3000 is already in use:
```bash
lsof -i :3000
```

2. Check gateway logs:
```bash
docker-compose logs gateway
```

3. Verify environment variables:
```bash
docker-compose exec gateway env | grep KAFKA
```

### Kafka connection issues

1. Verify Kafka brokers are running:
```bash
docker-compose ps
```

2. Check broker logs:
```bash
docker-compose logs kafka-broker-1
```

3. Test connectivity from gateway:
```bash
docker-compose exec gateway nc -zv kafka-broker-1 9092
```

### Schema Registry issues

1. Check Schema Registry is running:
```bash
curl http://localhost:8081/subjects
```

2. View Schema Registry logs:
```bash
docker-compose logs schema-registry
```

### Hot reload not working

1. Ensure you're using `docker-compose.override.yml` (it's automatic)
2. Check that volumes are mounted correctly:
```bash
docker-compose exec gateway ls -la /app
```

3. Restart the gateway:
```bash
docker-compose restart gateway
```

### Clean slate restart

If things get messy, start fresh:
```bash
# Stop everything and remove volumes
docker-compose down -v

# Remove any orphaned containers
docker-compose down --remove-orphans

# Start fresh
docker-compose up -d

# Recreate topics
# (run the topic creation commands from above)
```

## Environment Variables

Key environment variables for local development (in `gateway/.env`):

| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARN, ERROR) | `DEBUG` |
| `KAFKA_BROKERS` | Comma-separated Kafka broker addresses | `localhost:19092,localhost:19093,localhost:19094` |
| `KAFKA_TOPIC` | Main topic for health data | `health-data-raw` |
| `KAFKA_DLQ_TOPIC` | Dead letter queue topic | `health-data-dlq` |
| `SCHEMA_REGISTRY_URL` | Schema Registry endpoint | `http://localhost:8081` |
| `PORT` | Gateway server port | `3000` |

## Production Deployment

For production deployment, use the production Dockerfile target:

```bash
docker-compose -f docker-compose.yml up -d
```

This uses the optimized production build without development dependencies or hot reload.

## Documentation

### API Documentation
- **[API Reference](docs/API.md)** - Complete API documentation with examples
- **[OpenAPI Specification](docs/openapi.yaml)** - OpenAPI 3.0 spec for API integration
- **[Interactive Swagger UI](http://localhost:3000/docs)** - Test API endpoints interactively

### Configuration
- **[Environment Variables](docs/ENVIRONMENT_VARIABLES.md)** - Complete environment variable reference
- **[Configuration Examples](docs/ENVIRONMENT_VARIABLES.md#example-configurations)** - Sample configurations for different environments

### Examples
- **[Sample Requests & Responses](docs/EXAMPLES.md)** - Comprehensive API examples
- **[Testing Scripts](docs/EXAMPLES.md#testing-scripts)** - Ready-to-use test scripts
- **[SDK Examples](docs/API.md#sdk-examples)** - Python and Swift integration examples

### Troubleshooting
- **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** - Solutions to common issues
- **[Quick Diagnostics](docs/TROUBLESHOOTING.md#quick-diagnostics)** - Fast problem identification
- **[Emergency Procedures](docs/TROUBLESHOOTING.md#emergency-procedures)** - System recovery steps

### Architecture & Design
- **[Design Document](.kiro/specs/health-stack-kafka-gateway/design.md)** - System architecture and design decisions
- **[Requirements](.kiro/specs/health-stack-kafka-gateway/requirements.md)** - Detailed requirements specification
- **[Implementation Tasks](.kiro/specs/health-stack-kafka-gateway/tasks.md)** - Development task breakdown

### Additional Resources
- **[Avro Schema](gateway/schemas/health-data.avsc)** - Health data schema definition
- **[Schema Documentation](gateway/schemas/README.md)** - Schema usage and evolution guide
- **[Kafka Configuration](kafka/README.md)** - Kafka cluster setup and management
- **[Testing Guide](gateway/tests/README.md)** - Test suite documentation

## Next Steps

- Review the [API documentation](docs/API.md) for detailed endpoint information
- Explore the [Kafka UI](http://localhost:8080) to monitor message flow
- Check out the [examples](docs/EXAMPLES.md) for integration patterns
- Read the [troubleshooting guide](docs/TROUBLESHOOTING.md) for common issues
- Review the [design document](.kiro/specs/health-stack-kafka-gateway/design.md) for architecture details
