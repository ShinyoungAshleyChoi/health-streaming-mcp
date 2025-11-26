# Health Stack Kafka Gateway Documentation

Welcome to the Health Stack Kafka Gateway documentation. This directory contains comprehensive guides for using, configuring, and troubleshooting the gateway.

## Quick Links

### Getting Started
- **[Main README](../README.md)** - Installation and quick start guide
- **[API Reference](API.md)** - Complete API documentation
- **[Examples](EXAMPLES.md)** - Sample requests and integration examples

### Configuration
- **[Environment Variables](ENVIRONMENT_VARIABLES.md)** - Complete configuration reference
- **[OpenAPI Specification](openapi.yaml)** - Machine-readable API specification

### Operations
- **[Troubleshooting Guide](TROUBLESHOOTING.md)** - Solutions to common issues
- **[Kafka Management](../kafka/README.md)** - Kafka cluster operations
- **[Testing Guide](../gateway/tests/README.md)** - Running tests

### Architecture
- **[Design Document](../.kiro/specs/health-stack-kafka-gateway/design.md)** - System architecture
- **[Requirements](../.kiro/specs/health-stack-kafka-gateway/requirements.md)** - Detailed requirements
- **[Schema Documentation](../gateway/schemas/README.md)** - Avro schema guide

---

## Documentation Overview

### [API.md](API.md)
Complete API reference including:
- Endpoint descriptions
- Request/response formats
- Authentication details
- Rate limiting
- Error codes
- Data validation rules
- SDK examples (Python, Swift)

**Use this when:** You need to integrate with the API or understand endpoint behavior.

---

### [EXAMPLES.md](EXAMPLES.md)
Practical examples including:
- Sample requests for all data types
- Error response examples
- Testing scripts (bash, Python)
- Load testing examples
- Kafka message verification

**Use this when:** You want to test the API or see real-world usage examples.

---

### [ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md)
Complete configuration reference:
- All environment variables explained
- Default values and valid ranges
- Configuration examples (dev, prod)
- Security best practices
- Validation instructions

**Use this when:** You need to configure the gateway for different environments.

---

### [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
Problem-solving guide covering:
- Quick diagnostics
- Gateway issues
- Kafka issues
- Schema Registry issues
- Network problems
- Performance tuning
- Emergency procedures

**Use this when:** Something isn't working and you need to diagnose the issue.

---

### [openapi.yaml](openapi.yaml)
OpenAPI 3.0 specification:
- Machine-readable API definition
- Request/response schemas
- Validation rules
- Example payloads

**Use this when:** You need to generate client SDKs or integrate with API tools.

---

## Common Tasks

### Testing the API

1. **Quick test:**
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

2. **Interactive testing:**
   - Open http://localhost:3000/docs
   - Use Swagger UI to test endpoints

3. **Automated testing:**
   ```bash
   # See EXAMPLES.md for test scripts
   ./test_api.sh
   ```

---

### Configuring the Gateway

1. **Copy environment template:**
   ```bash
   cp gateway/.env.example gateway/.env
   ```

2. **Edit configuration:**
   ```bash
   vim gateway/.env
   ```

3. **See [ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md) for all options**

---

### Troubleshooting Issues

1. **Check service health:**
   ```bash
   curl http://localhost:3000/health
   docker-compose ps
   ```

2. **View logs:**
   ```bash
   docker-compose logs -f gateway
   ```

3. **See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for specific issues**

---

### Monitoring Messages

1. **Kafka UI (recommended):**
   - Open http://localhost:8080
   - Navigate to Topics → health-data-raw

2. **Command line:**
   ```bash
   docker-compose exec kafka-broker-1 kafka-console-consumer \
     --bootstrap-server kafka-broker-1:9092 \
     --topic health-data-raw \
     --from-beginning \
     --max-messages 10
   ```

---

## Architecture Overview

```
┌─────────────┐
│  iOS App    │
└──────┬──────┘
       │ JSON/HTTPS
       ▼
┌─────────────────────────────────────┐
│      API Gateway (FastAPI)          │
│  ┌──────────┐  ┌────────────────┐  │
│  │Validator │→ │Avro Converter  │  │
│  └──────────┘  └────────┬───────┘  │
└─────────────────────────┼───────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  Schema Registry      │
              └───────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   Kafka Cluster       │
              │  ┌─────────────────┐  │
              │  │ health-data-raw │  │
              │  └─────────────────┘  │
              │  ┌─────────────────┐  │
              │  │ health-data-dlq │  │
              │  └─────────────────┘  │
              └───────────────────────┘
```

**Key Components:**
- **API Gateway:** Receives JSON, validates, converts to Avro
- **Schema Registry:** Manages Avro schemas and versioning
- **Kafka Cluster:** Stores and distributes messages
- **Dead Letter Queue:** Captures failed messages

For detailed architecture, see [Design Document](../.kiro/specs/health-stack-kafka-gateway/design.md).

---

## Data Flow

1. **iOS app sends JSON** to POST /api/v1/health-data
2. **Gateway validates** request (schema, ranges, types)
3. **Gateway converts** JSON to Avro format
4. **Schema Registry** provides schema definition
5. **Gateway publishes** Avro message to Kafka
6. **Kafka stores** message across replicas
7. **Gateway returns** 200 OK to client

If any step fails:
- Retries with exponential backoff
- After max retries, sends to Dead Letter Queue
- Returns appropriate error to client

---

## Data Types Supported

| Type | Description | Example Value |
|------|-------------|---------------|
| `heart_rate` | Heart rate in BPM | 72 |
| `steps` | Step count | 8543 |
| `blood_pressure` | Systolic/diastolic | {systolic: 120, diastolic: 80} |
| `blood_glucose` | Blood glucose level | 95 |
| `body_temperature` | Body temperature | 36.8 |
| `oxygen_saturation` | SpO2 percentage | 98 |
| `respiratory_rate` | Breaths per minute | 16 |
| `weight` | Body weight | 70.5 |
| `sleep` | Sleep metrics | {duration: 480, ...} |

See [API.md](API.md#data-types) for complete list and validation rules.

---

## Monitoring & Metrics

### Health Check
```bash
curl http://localhost:3000/health
```

Returns status of gateway and dependencies (Kafka, Schema Registry).

### Prometheus Metrics
```bash
curl http://localhost:3000/metrics
```

Provides metrics for:
- Request rate and latency
- Error rates
- Kafka message counts
- Resource usage

### Kafka UI
Open http://localhost:8080 to:
- View topics and messages
- Monitor consumer lag
- Check broker health
- Inspect schemas

---

## Security Considerations

### API Security
- HTTPS/TLS for transport encryption
- Optional API key or JWT authentication
- Rate limiting (1000 req/min)
- Input validation and sanitization

### Kafka Security
- SASL/SCRAM authentication (optional)
- SSL/TLS encryption
- ACLs for topic access

### Data Privacy
- PII masking in logs
- Data encryption at rest
- GDPR compliance considerations

See [ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md#security-configuration) for security settings.

---

## Performance Tuning

### Gateway Optimization
```bash
# In gateway/.env
KAFKA_COMPRESSION_TYPE=snappy
KAFKA_BATCH_SIZE=16384
KAFKA_LINGER_MS=10
SCHEMA_CACHE_SIZE=1000
```

### Resource Limits
```yaml
# In docker-compose.yml
services:
  gateway:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
```

### Horizontal Scaling
```bash
# Run multiple gateway instances
docker-compose up -d --scale gateway=3
```

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md#performance-issues) for more tuning options.

---

## Development Workflow

### Local Development
1. Start services: `docker-compose up -d`
2. Make code changes in `gateway/`
3. Changes auto-reload (hot reload enabled)
4. Test changes immediately

### Running Tests
```bash
# Unit tests
cd gateway
uv run pytest

# Integration tests
./run_integration_tests.sh
```

### Debugging
```bash
# Enable debug logging
# In gateway/.env
LOG_LEVEL=DEBUG

# View logs
docker-compose logs -f gateway
```

---

## Production Deployment

### Pre-deployment Checklist
- [ ] Set `LOG_LEVEL=INFO`
- [ ] Enable authentication (`API_KEY_ENABLED=true`)
- [ ] Configure CORS origins (not `*`)
- [ ] Set resource limits
- [ ] Enable HTTPS/TLS
- [ ] Configure monitoring/alerting
- [ ] Set up backups
- [ ] Review security settings

### Deployment
```bash
# Use production compose file
docker-compose -f docker-compose.yml up -d

# Or deploy to Kubernetes
kubectl apply -f k8s/
```

See [Design Document](../.kiro/specs/health-stack-kafka-gateway/design.md#deployment-strategy) for deployment strategies.

---

## Getting Help

### Documentation
1. Check this documentation
2. Review [Troubleshooting Guide](TROUBLESHOOTING.md)
3. Read [Design Document](../.kiro/specs/health-stack-kafka-gateway/design.md)

### Diagnostics
```bash
# Collect diagnostic info
docker-compose logs > logs.txt
docker-compose config > config.yml
docker-compose ps > status.txt
```

### Support
- Open an issue on the project repository
- Include logs and error messages
- Describe steps to reproduce
- Specify environment details

---

## Additional Resources

### External Documentation
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
- [Apache Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

### Related Projects
- [health-stack iOS App](https://github.com/example/health-stack-ios)
- [Data Processing Pipeline](https://github.com/example/health-data-pipeline)

---

## Contributing

When updating documentation:
1. Keep examples up-to-date
2. Test all commands before documenting
3. Include error cases
4. Add troubleshooting tips
5. Update this index if adding new docs

---

## License

MIT License - See LICENSE file for details

---

**Last Updated:** 2025-11-12

**Documentation Version:** 1.0.0
