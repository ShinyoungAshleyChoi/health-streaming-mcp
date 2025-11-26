# Deployment Guide

This guide covers deploying the Flink Iceberg Consumer using Docker Compose and Kubernetes.

## Docker Compose Deployment (Recommended for Development)

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- At least 8GB RAM available for Docker
- 20GB free disk space

### Quick Start

1. **Build the Flink consumer image:**

```bash
cd flink_consumer
docker build -t flink-iceberg-consumer:latest -f Dockerfile --target production .
```

2. **Start the entire stack:**

```bash
# From project root
docker-compose up -d
```

This will start:
- 3 Kafka brokers
- Schema Registry
- Kafka UI (http://localhost:8080)
- Gateway API (http://localhost:3000)
- MinIO (http://localhost:9001)
- Flink JobManager (http://localhost:8081)
- 3 Flink TaskManagers

3. **Verify services are running:**

```bash
# Check all containers
docker-compose ps

# Check Flink Web UI
open http://localhost:8081

# Check MinIO Console
open http://localhost:9001
# Login: minioadmin / minioadmin
```

4. **View logs:**

```bash
# Flink JobManager logs
docker-compose logs -f flink-jobmanager

# Flink TaskManager logs
docker-compose logs -f flink-taskmanager-1

# All Flink logs
docker-compose logs -f flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
```

### Configuration

Environment variables can be set in `docker-compose.yml` or via `.env` file:

```bash
# Create .env file in project root
cat > .env << EOF
KAFKA_BROKERS=kafka-broker-1:9092,kafka-broker-2:9094,kafka-broker-3:9096
SCHEMA_REGISTRY_URL=http://schema-registry:8081
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
FLINK_PARALLELISM=6
LOG_LEVEL=INFO
EOF
```

### Scaling TaskManagers

```bash
# Scale to 5 TaskManagers
docker-compose up -d --scale flink-taskmanager=5
```

### Stopping and Cleanup

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove only Flink services
docker-compose stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
docker-compose rm -f flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
```

## Development Workflow

### Building for Development

```bash
# Build development image with dev dependencies
docker build -t flink-iceberg-consumer:dev -f Dockerfile --target development .

# Run with mounted source code for live development
docker run -it --rm \
  -v $(pwd):/opt/flink-app \
  -e KAFKA_BROKERS=kafka-broker-1:9092 \
  --network health-stack_kafka-network \
  flink-iceberg-consumer:dev bash
```

### Running Tests in Container

```bash
docker run --rm \
  -v $(pwd):/opt/flink-app \
  flink-iceberg-consumer:dev \
  pytest tests/ -v
```

## Production Deployment

### Docker Compose (Production)

For production deployment with Docker Compose:

1. **Update secrets in docker-compose.yml:**
   - Change MinIO credentials
   - Add Kafka SASL credentials if needed
   - Configure proper S3 endpoint

2. **Enable resource limits:**

```yaml
# Add to docker-compose.yml services
flink-jobmanager:
  deploy:
    resources:
      limits:
        cpus: '2'
        memory: 2G
      reservations:
        cpus: '1'
        memory: 1G

flink-taskmanager-1:
  deploy:
    resources:
      limits:
        cpus: '4'
        memory: 4G
      reservations:
        cpus: '2'
        memory: 2G
```

3. **Enable monitoring:**

```bash
# Add Prometheus and Grafana to docker-compose.yml
# Metrics available at http://localhost:9249
```

### Kubernetes Deployment

See [k8s/README.md](../k8s/README.md) for Kubernetes deployment instructions.

## Monitoring

### Flink Web UI

Access at http://localhost:8081

- Job overview and status
- TaskManager metrics
- Checkpoint statistics
- Job graph visualization

### Prometheus Metrics

Metrics exposed at http://localhost:9249/metrics

Key metrics:
- `flink_jobmanager_job_numRecordsIn`
- `flink_jobmanager_job_numRecordsOut`
- `flink_jobmanager_job_lastCheckpointDuration`
- `flink_taskmanager_Status_JVM_Memory_Heap_Used`

### MinIO Console

Access at http://localhost:9001

- View data lake files
- Monitor checkpoint storage
- Check bucket policies

## Troubleshooting

### Flink Job Not Starting

```bash
# Check JobManager logs
docker-compose logs flink-jobmanager

# Common issues:
# 1. Kafka not ready - wait for kafka-init to complete
# 2. MinIO not ready - check minio-init logs
# 3. Python dependencies missing - rebuild image
```

### Checkpoint Failures

```bash
# Check S3 connectivity
docker-compose exec flink-jobmanager curl http://minio:9000/minio/health/live

# Verify bucket exists
docker-compose exec minio-init mc ls myminio/flink-checkpoints

# Check permissions
docker-compose logs minio
```

### High Memory Usage

```bash
# Check TaskManager memory
docker stats flink-taskmanager-1

# Reduce parallelism
# Edit docker-compose.yml: FLINK_PARALLELISM=3
docker-compose up -d flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3
```

### Kafka Connection Issues

```bash
# Test Kafka connectivity
docker-compose exec flink-jobmanager \
  kafka-console-consumer \
  --bootstrap-server kafka-broker-1:9092 \
  --topic health-data-raw \
  --from-beginning \
  --max-messages 1

# Check Schema Registry
docker-compose exec flink-jobmanager curl http://schema-registry:8081/subjects
```

## Backup and Recovery

### Create Savepoint

```bash
# Get Job ID from Flink Web UI or:
JOB_ID=$(docker-compose exec -T flink-jobmanager \
  flink list -r | grep "Flink Streaming Job" | awk '{print $4}')

# Trigger savepoint
docker-compose exec flink-jobmanager \
  flink savepoint $JOB_ID s3a://flink-checkpoints/health-consumer/savepoints
```

### Restore from Savepoint

```bash
# Stop current job
docker-compose stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 flink-taskmanager-3

# Start with savepoint
docker-compose run -e SAVEPOINT_PATH=s3a://flink-checkpoints/health-consumer/savepoints/savepoint-xxx \
  flink-jobmanager
```

## Performance Tuning

### Increase Throughput

1. **Scale TaskManagers:**
```bash
docker-compose up -d --scale flink-taskmanager=5
```

2. **Increase parallelism:**
```yaml
# docker-compose.yml
environment:
  - FLINK_PARALLELISM=12
```

3. **Tune batch writing:**
```yaml
environment:
  - BATCH_SIZE=2000
  - BATCH_TIMEOUT_SECONDS=5
```

### Reduce Latency

1. **Decrease checkpoint interval:**
```yaml
environment:
  - FLINK_CHECKPOINT_INTERVAL_MS=30000
```

2. **Reduce batch size:**
```yaml
environment:
  - BATCH_SIZE=500
  - BATCH_TIMEOUT_SECONDS=2
```

## Upgrading

### Rolling Update

```bash
# Build new image
docker build -t flink-iceberg-consumer:1.1.0 -f Dockerfile --target production .

# Create savepoint
JOB_ID=$(docker-compose exec -T flink-jobmanager flink list -r | grep "Flink Streaming Job" | awk '{print $4}')
docker-compose exec flink-jobmanager flink savepoint $JOB_ID

# Update docker-compose.yml with new image tag
# Restart services
docker-compose up -d
```

## Security Best Practices

1. **Change default credentials:**
   - MinIO: Update MINIO_ROOT_USER and MINIO_ROOT_PASSWORD
   - Kafka: Enable SASL authentication

2. **Use secrets management:**
   - Docker secrets for sensitive data
   - Kubernetes secrets for K8s deployment

3. **Network isolation:**
   - Use internal networks
   - Expose only necessary ports

4. **Enable TLS:**
   - Kafka TLS
   - S3 HTTPS
   - Flink internal TLS

## Additional Resources

- [Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.18/)
- [Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [PyFlink Examples](https://github.com/apache/flink/tree/master/flink-python/pyflink/examples)
