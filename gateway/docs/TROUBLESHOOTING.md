# Troubleshooting Guide

This guide helps you diagnose and resolve common issues with the Health Stack Kafka Gateway.

## Table of Contents

- [Quick Diagnostics](#quick-diagnostics)
- [Gateway Issues](#gateway-issues)
- [Kafka Issues](#kafka-issues)
- [Schema Registry Issues](#schema-registry-issues)
- [Network Issues](#network-issues)
- [Performance Issues](#performance-issues)
- [Data Issues](#data-issues)
- [Docker Issues](#docker-issues)
- [Development Issues](#development-issues)

---

## Quick Diagnostics

### Check All Services Status

```bash
# View running containers
docker-compose ps

# Check service health
curl http://localhost:3000/health

# View recent logs
docker-compose logs --tail=50
```

### Common Quick Fixes

```bash
# Restart all services
docker-compose restart

# Clean restart (removes volumes)
docker-compose down -v && docker-compose up -d

# Rebuild gateway image
docker-compose build gateway && docker-compose up -d gateway

# View gateway logs in real-time
docker-compose logs -f gateway
```

---

## Gateway Issues

### Gateway Won't Start

**Symptoms:**
- Container exits immediately
- "Connection refused" errors
- Port binding errors

**Diagnosis:**

```bash
# Check gateway logs
docker-compose logs gateway

# Check if port is in use
lsof -i :3000
netstat -an | grep 3000

# Verify environment variables
docker-compose exec gateway env | grep -E "KAFKA|SCHEMA|LOG"
```

**Solutions:**

1. **Port already in use:**
```bash
# Find process using port 3000
lsof -i :3000

# Kill the process
kill -9 <PID>

# Or change the port in docker-compose.yml
```

2. **Missing environment variables:**
```bash
# Copy environment template
cp gateway/.env.example gateway/.env

# Edit with correct values
vim gateway/.env

# Restart
docker-compose restart gateway
```

3. **Python dependency issues:**
```bash
# Rebuild with no cache
docker-compose build --no-cache gateway
docker-compose up -d gateway
```

---

### Gateway Returns 500 Errors

**Symptoms:**
- All requests return 500 Internal Server Error
- Gateway logs show exceptions

**Diagnosis:**

```bash
# Enable debug logging
# Edit gateway/.env
LOG_LEVEL=DEBUG

# Restart and watch logs
docker-compose restart gateway
docker-compose logs -f gateway
```

**Solutions:**

1. **Kafka connection issues:**
```bash
# Test Kafka connectivity from gateway
docker-compose exec gateway nc -zv kafka-broker-1 9092

# Check Kafka broker status
docker-compose ps kafka-broker-1 kafka-broker-2 kafka-broker-3

# Verify KAFKA_BROKERS setting
docker-compose exec gateway env | grep KAFKA_BROKERS
```

2. **Schema Registry issues:**
```bash
# Test Schema Registry connectivity
curl http://localhost:8081/subjects

# From gateway container
docker-compose exec gateway curl http://schema-registry:8081/subjects
```

3. **Application errors:**
```bash
# Check for Python exceptions in logs
docker-compose logs gateway | grep -i "error\|exception\|traceback"

# Restart with clean state
docker-compose restart gateway
```

---

### Gateway Responds Slowly

**Symptoms:**
- High response times (> 1 second)
- Timeouts
- 503 Service Unavailable errors

**Diagnosis:**

```bash
# Check resource usage
docker stats

# Check gateway metrics
curl http://localhost:3000/metrics | grep duration

# Monitor Kafka lag
docker-compose exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:9092 \
  --list
```

**Solutions:**

1. **Increase resources:**
```yaml
# In docker-compose.yml
services:
  gateway:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 1G
```

2. **Optimize Kafka producer settings:**
```bash
# In gateway/.env
KAFKA_COMPRESSION_TYPE=snappy
KAFKA_BATCH_SIZE=16384
KAFKA_LINGER_MS=10
```

3. **Enable connection pooling:**
```bash
# Check current connections
docker-compose exec gateway netstat -an | grep ESTABLISHED | wc -l
```

---

### Authentication Errors

**Symptoms:**
- 401 Unauthorized responses
- "Invalid API key" errors

**Diagnosis:**

```bash
# Check if authentication is enabled
docker-compose exec gateway env | grep API_KEY

# Test without authentication
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d '{"userId":"test",...}'

# Test with authentication
curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_KEY" \
  -d '{"userId":"test",...}'
```

**Solutions:**

1. **Disable authentication for testing:**
```bash
# In gateway/.env
API_KEY_ENABLED=false

# Restart
docker-compose restart gateway
```

2. **Verify API key:**
```bash
# Check configured keys
docker-compose exec gateway env | grep API_KEYS

# Ensure key matches
```

---

## Kafka Issues

### Kafka Brokers Won't Start

**Symptoms:**
- Kafka containers exit immediately
- "Broker not available" errors
- ZooKeeper connection errors

**Diagnosis:**

```bash
# Check broker logs
docker-compose logs kafka-broker-1
docker-compose logs kafka-broker-2
docker-compose logs kafka-broker-3

# Check ZooKeeper
docker-compose logs zookeeper

# Check disk space
df -h
```

**Solutions:**

1. **ZooKeeper not ready:**
```bash
# Wait for ZooKeeper to be ready (30-60 seconds)
sleep 60

# Check ZooKeeper status
docker-compose exec zookeeper zkServer.sh status

# Restart Kafka brokers
docker-compose restart kafka-broker-1 kafka-broker-2 kafka-broker-3
```

2. **Port conflicts:**
```bash
# Check if ports are in use
lsof -i :19092
lsof -i :19093
lsof -i :19094

# Stop conflicting services or change ports
```

3. **Disk space issues:**
```bash
# Check disk usage
df -h

# Clean up old Docker volumes
docker volume prune

# Remove old Kafka data
docker-compose down -v
docker-compose up -d
```

4. **Corrupted data:**
```bash
# Nuclear option: clean slate
docker-compose down -v
docker volume rm $(docker volume ls -q | grep kafka)
docker-compose up -d
```

---

### Topics Not Created

**Symptoms:**
- "Topic does not exist" errors
- Messages not being stored

**Diagnosis:**

```bash
# List existing topics
docker-compose exec kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092

# Check topic creation logs
docker-compose logs | grep "topic"
```

**Solutions:**

1. **Manually create topics:**
```bash
# Create health-data-raw topic
docker-compose exec kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --partitions 6 \
  --replication-factor 3 \
  --if-not-exists

# Create DLQ topic
docker-compose exec kafka-broker-1 kafka-topics \
  --create \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-dlq \
  --partitions 3 \
  --replication-factor 3 \
  --if-not-exists

# Verify
docker-compose exec kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092
```

2. **Enable auto-create (not recommended for production):**
```yaml
# In docker-compose.yml
kafka-broker-1:
  environment:
    - KAFKA_AUTO_CREATE_TOPICS_ENABLE=true
```

---

### Messages Not Appearing in Kafka

**Symptoms:**
- Gateway returns 200 OK
- No messages in Kafka topics
- Consumer sees no data

**Diagnosis:**

```bash
# Check if messages are being produced
docker-compose logs gateway | grep -i "kafka\|produce\|send"

# Check topic for messages
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --from-beginning \
  --max-messages 10

# Check producer metrics
curl http://localhost:3000/metrics | grep kafka_messages

# Check DLQ for failed messages
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-dlq \
  --from-beginning \
  --max-messages 10
```

**Solutions:**

1. **Check gateway Kafka connection:**
```bash
# Test connectivity
docker-compose exec gateway nc -zv kafka-broker-1 9092

# Verify broker addresses
docker-compose exec gateway env | grep KAFKA_BROKERS

# Should be: kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
```

2. **Check for serialization errors:**
```bash
# Look for Avro conversion errors
docker-compose logs gateway | grep -i "avro\|schema\|serializ"
```

3. **Verify topic exists:**
```bash
# List topics
docker-compose exec kafka-broker-1 kafka-topics \
  --list \
  --bootstrap-server kafka-broker-1:9092
```

---

### Kafka Broker Failures

**Symptoms:**
- One or more brokers down
- "Not enough replicas" errors
- Degraded cluster performance

**Diagnosis:**

```bash
# Check broker status
docker-compose ps

# Check cluster health
docker-compose exec kafka-broker-1 kafka-broker-api-versions \
  --bootstrap-server kafka-broker-1:9092

# Check under-replicated partitions
docker-compose exec kafka-broker-1 kafka-topics \
  --describe \
  --bootstrap-server kafka-broker-1:9092 \
  --under-replicated-partitions
```

**Solutions:**

1. **Restart failed broker:**
```bash
# Restart specific broker
docker-compose restart kafka-broker-2

# Wait for it to rejoin cluster (30-60 seconds)
sleep 60

# Verify it's back
docker-compose ps kafka-broker-2
```

2. **Check broker logs:**
```bash
# Look for errors
docker-compose logs kafka-broker-2 | grep -i "error\|exception"
```

3. **Increase broker resources:**
```yaml
# In docker-compose.yml
kafka-broker-1:
  deploy:
    resources:
      limits:
        memory: 2G
```

---

## Schema Registry Issues

### Schema Registry Won't Start

**Symptoms:**
- Schema Registry container exits
- Gateway can't register schemas
- 404 errors from Schema Registry

**Diagnosis:**

```bash
# Check Schema Registry logs
docker-compose logs schema-registry

# Test connectivity
curl http://localhost:8081/subjects

# Check Kafka connection
docker-compose logs schema-registry | grep -i "kafka\|connect"
```

**Solutions:**

1. **Wait for Kafka to be ready:**
```bash
# Schema Registry needs Kafka to be running
docker-compose ps kafka-broker-1

# Restart Schema Registry after Kafka is ready
docker-compose restart schema-registry
```

2. **Check configuration:**
```bash
# Verify Kafka bootstrap servers
docker-compose logs schema-registry | grep "bootstrap.servers"
```

3. **Clean restart:**
```bash
docker-compose stop schema-registry
docker-compose rm -f schema-registry
docker-compose up -d schema-registry
```

---

### Schema Compatibility Errors

**Symptoms:**
- "Schema incompatible" errors
- Gateway can't register new schema versions
- 409 Conflict from Schema Registry

**Diagnosis:**

```bash
# Check current schema
curl http://localhost:8081/subjects/health-data-raw-value/versions/latest

# Check compatibility mode
curl http://localhost:8081/config/health-data-raw-value

# Test schema compatibility
curl -X POST http://localhost:8081/compatibility/subjects/health-data-raw-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @schemas/health-data.avsc
```

**Solutions:**

1. **Change compatibility mode:**
```bash
# Set to BACKWARD (allows removing fields)
curl -X PUT http://localhost:8081/config/health-data-raw-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "BACKWARD"}'

# Or FORWARD (allows adding fields)
curl -X PUT http://localhost:8081/config/health-data-raw-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FORWARD"}'

# Or NONE (no compatibility checks - dangerous!)
curl -X PUT http://localhost:8081/config/health-data-raw-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "NONE"}'
```

2. **Delete and re-register schema (development only):**
```bash
# Delete schema (DANGEROUS - breaks consumers!)
curl -X DELETE http://localhost:8081/subjects/health-data-raw-value

# Restart gateway to re-register
docker-compose restart gateway
```

---

## Network Issues

### Cannot Connect to Services

**Symptoms:**
- "Connection refused" errors
- Timeouts
- DNS resolution failures

**Diagnosis:**

```bash
# Check Docker network
docker network ls
docker network inspect health-stack-kafka-gateway_default

# Test connectivity between containers
docker-compose exec gateway ping kafka-broker-1
docker-compose exec gateway nc -zv kafka-broker-1 9092
docker-compose exec gateway nc -zv schema-registry 8081

# Check from host
curl http://localhost:3000/health
curl http://localhost:8081/subjects
```

**Solutions:**

1. **Verify service names:**
```bash
# Services should use internal names in Docker network
# gateway/.env should have:
KAFKA_BROKERS=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

2. **Recreate network:**
```bash
docker-compose down
docker network prune
docker-compose up -d
```

3. **Check firewall:**
```bash
# Disable firewall temporarily (macOS)
sudo pfctl -d

# Re-enable
sudo pfctl -e
```

---

### Port Conflicts

**Symptoms:**
- "Address already in use" errors
- Services won't start

**Diagnosis:**

```bash
# Check what's using ports
lsof -i :3000   # Gateway
lsof -i :8081   # Schema Registry
lsof -i :19092  # Kafka broker 1
lsof -i :19093  # Kafka broker 2
lsof -i :19094  # Kafka broker 3
lsof -i :8080   # Kafka UI
```

**Solutions:**

1. **Kill conflicting process:**
```bash
# Find PID
lsof -i :3000

# Kill it
kill -9 <PID>
```

2. **Change ports in docker-compose.yml:**
```yaml
services:
  gateway:
    ports:
      - "3001:3000"  # Use 3001 instead of 3000
```

---

## Performance Issues

### High Latency

**Symptoms:**
- Slow API responses (> 500ms)
- Timeouts
- Poor throughput

**Diagnosis:**

```bash
# Check metrics
curl http://localhost:3000/metrics | grep duration

# Monitor resource usage
docker stats

# Check Kafka producer lag
docker-compose logs gateway | grep -i "lag\|latency"

# Test with single request
time curl -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

**Solutions:**

1. **Optimize Kafka producer:**
```bash
# In gateway/.env
KAFKA_COMPRESSION_TYPE=snappy
KAFKA_BATCH_SIZE=16384
KAFKA_LINGER_MS=10
KAFKA_ACKS=1  # Trade reliability for speed (not recommended for production)
```

2. **Increase resources:**
```yaml
# docker-compose.yml
services:
  gateway:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
```

3. **Enable caching:**
```bash
# In gateway/.env
SCHEMA_CACHE_SIZE=1000
```

4. **Check network latency:**
```bash
# Ping Kafka from gateway
docker-compose exec gateway ping -c 10 kafka-broker-1
```

---

### High Memory Usage

**Symptoms:**
- Gateway using > 1GB memory
- OOM (Out of Memory) errors
- Container restarts

**Diagnosis:**

```bash
# Check memory usage
docker stats gateway

# Check for memory leaks
docker-compose logs gateway | grep -i "memory\|oom"
```

**Solutions:**

1. **Set memory limits:**
```yaml
# docker-compose.yml
services:
  gateway:
    deploy:
      resources:
        limits:
          memory: 512M
        reservations:
          memory: 256M
```

2. **Reduce cache sizes:**
```bash
# In gateway/.env
SCHEMA_CACHE_SIZE=50
```

3. **Restart periodically:**
```bash
# Add to cron
0 3 * * * docker-compose restart gateway
```

---

### High CPU Usage

**Symptoms:**
- Gateway using > 80% CPU
- Slow responses
- High load average

**Diagnosis:**

```bash
# Check CPU usage
docker stats gateway

# Check for busy loops
docker-compose logs gateway | tail -100
```

**Solutions:**

1. **Limit CPU:**
```yaml
# docker-compose.yml
services:
  gateway:
    deploy:
      resources:
        limits:
          cpus: '1'
```

2. **Optimize compression:**
```bash
# Use faster compression
KAFKA_COMPRESSION_TYPE=lz4
```

3. **Scale horizontally:**
```bash
# Run multiple gateway instances
docker-compose up -d --scale gateway=3
```

---

## Data Issues

### Data Validation Failures

**Symptoms:**
- 422 Unprocessable Entity errors
- "Validation failed" messages

**Diagnosis:**

```bash
# Send test request and check response
curl -v -X POST http://localhost:3000/api/v1/health-data \
  -H "Content-Type: application/json" \
  -d @test_payload.json | jq .

# Check validation rules
cat gateway/validators/health_data_validator.py
```

**Solutions:**

1. **Fix request payload:**
```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",  // Must be valid UUID
  "timestamp": "2025-11-12T10:30:00Z",  // Must be ISO 8601
  "dataType": "heart_rate",  // Must be valid type
  "value": 72,  // Must be in valid range
  "unit": "bpm",
  "metadata": {
    "deviceId": "iPhone14-ABC123",  // Required
    "appVersion": "1.2.3",  // Required, semver format
    "platform": "iOS"  // Required
  }
}
```

2. **Check value ranges:**
- Heart rate: 30-250 bpm
- Steps: 0-100000
- Blood pressure: systolic 70-200, diastolic 40-130
- Temperature: 35.0-42.0 °C

---

### Messages in Dead Letter Queue

**Symptoms:**
- Messages appearing in DLQ topic
- Failed message processing

**Diagnosis:**

```bash
# Check DLQ messages
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-dlq \
  --from-beginning

# Check gateway logs for errors
docker-compose logs gateway | grep -i "dlq\|failed\|error"
```

**Solutions:**

1. **Analyze DLQ messages:**
```bash
# View DLQ messages with details
docker-compose exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-dlq \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property print.timestamp=true
```

2. **Fix root cause:**
- Schema incompatibility
- Network issues
- Kafka broker failures
- Invalid data format

3. **Replay DLQ messages (after fixing issue):**
```bash
# Implement DLQ replay logic or manually resubmit
```

---

## Docker Issues

### Docker Compose Fails

**Symptoms:**
- "docker-compose: command not found"
- Version compatibility errors
- YAML parsing errors

**Diagnosis:**

```bash
# Check Docker Compose version
docker-compose --version

# Validate docker-compose.yml
docker-compose config

# Check Docker daemon
docker ps
```

**Solutions:**

1. **Install/update Docker Compose:**
```bash
# macOS (Homebrew)
brew install docker-compose

# Or use Docker Desktop (includes Compose)
```

2. **Fix YAML syntax:**
```bash
# Validate YAML
docker-compose config

# Check indentation (use spaces, not tabs)
```

---

### Container Keeps Restarting

**Symptoms:**
- Container exits and restarts repeatedly
- "Restarting (1)" status

**Diagnosis:**

```bash
# Check container status
docker-compose ps

# View logs
docker-compose logs <service-name>

# Check exit code
docker inspect <container-id> | grep ExitCode
```

**Solutions:**

1. **Fix application errors:**
```bash
# Check logs for errors
docker-compose logs gateway | grep -i "error\|exception"
```

2. **Remove restart policy temporarily:**
```yaml
# docker-compose.yml
services:
  gateway:
    restart: "no"  # Temporarily disable restart
```

3. **Check dependencies:**
```yaml
# Ensure proper startup order
services:
  gateway:
    depends_on:
      - kafka-broker-1
      - schema-registry
```

---

## Development Issues

### Hot Reload Not Working

**Symptoms:**
- Code changes not reflected
- Need to manually restart

**Diagnosis:**

```bash
# Check if override file is being used
docker-compose config | grep -A 5 "gateway"

# Check volume mounts
docker inspect gateway | grep -A 10 "Mounts"
```

**Solutions:**

1. **Ensure docker-compose.override.yml exists:**
```yaml
# docker-compose.override.yml
services:
  gateway:
    command: uvicorn main:app --reload --host 0.0.0.0 --port 3000
    volumes:
      - ./gateway:/app
```

2. **Restart with override:**
```bash
docker-compose down
docker-compose up -d
```

3. **Check file permissions:**
```bash
# Ensure files are readable
ls -la gateway/
```

---

### Tests Failing

**Symptoms:**
- Integration tests fail
- Unit tests pass but integration tests fail

**Diagnosis:**

```bash
# Run tests with verbose output
cd gateway
uv run pytest -v

# Check test environment
docker-compose -f docker-compose.test.yml ps

# View test logs
docker-compose -f docker-compose.test.yml logs
```

**Solutions:**

1. **Ensure test environment is running:**
```bash
# Start test environment
docker-compose -f docker-compose.test.yml up -d

# Wait for services to be ready
sleep 60

# Run tests
cd gateway
uv run pytest tests/ -v
```

2. **Clean test environment:**
```bash
# Stop and remove test containers
docker-compose -f docker-compose.test.yml down -v

# Start fresh
docker-compose -f docker-compose.test.yml up -d
```

3. **Use test script:**
```bash
# Run automated test script
./run_integration_tests.sh
```

---

## Getting Help

If you're still experiencing issues:

1. **Collect diagnostic information:**
```bash
# Save logs
docker-compose logs > logs.txt

# Save configuration
docker-compose config > config.yml

# Save container status
docker-compose ps > status.txt
```

2. **Check documentation:**
- [API Documentation](./API.md)
- [Environment Variables](./ENVIRONMENT_VARIABLES.md)
- [Examples](./EXAMPLES.md)
- [Main README](../README.md)

3. **Search existing issues:**
- Check project issue tracker
- Search Stack Overflow
- Review Kafka documentation

4. **Create a detailed issue report:**
- Describe the problem
- Include error messages
- Attach logs
- List steps to reproduce
- Specify environment (OS, Docker version, etc.)

---

## Preventive Measures

### Regular Maintenance

```bash
# Weekly: Clean up Docker resources
docker system prune -a

# Monthly: Update dependencies
cd gateway
uv sync --upgrade

# Quarterly: Review and rotate API keys
```

### Monitoring

```bash
# Set up monitoring
# - Prometheus for metrics
# - Grafana for dashboards
# - Alertmanager for alerts

# Monitor key metrics:
# - Request rate
# - Error rate
# - Response time
# - Kafka lag
# - Resource usage
```

### Backup

```bash
# Backup Kafka data
docker-compose exec kafka-broker-1 kafka-dump-log \
  --files /var/lib/kafka/data/health-data-raw-0/00000000000000000000.log \
  --print-data-log > backup.log

# Backup Schema Registry
curl http://localhost:8081/subjects > schemas_backup.json
```

---

## Emergency Procedures

### Complete System Reset

```bash
#!/bin/bash
# WARNING: This will delete ALL data!

echo "Stopping all services..."
docker-compose down -v

echo "Removing all containers..."
docker-compose rm -f

echo "Pruning Docker resources..."
docker system prune -af
docker volume prune -f

echo "Starting fresh..."
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 60

echo "Creating topics..."
docker-compose exec kafka-broker-1 kafka-topics \
  --create --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw --partitions 6 --replication-factor 3 --if-not-exists

docker-compose exec kafka-broker-1 kafka-topics \
  --create --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-dlq --partitions 3 --replication-factor 3 --if-not-exists

echo "System reset complete!"
```

Save as `emergency_reset.sh` and run with:
```bash
chmod +x emergency_reset.sh
./emergency_reset.sh
```

---

## Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Docker Documentation](https://docs.docker.com/)
