# Flink Iceberg Consumer

Apache Flink-based real-time stream processing application that consumes health data from Kafka and writes to Apache Iceberg data lake with time-based aggregations.

## Overview

This application provides a scalable, fault-tolerant data pipeline for processing health data streams. It consumes Avro-encoded messages from Kafka, performs real-time transformations and aggregations, and stores the results in Apache Iceberg tables for efficient querying and analysis.

### Key Features

- **Real-time Stream Processing**: Processes health data with sub-second latency using Apache Flink
- **Exactly-Once Semantics**: Guarantees no data loss or duplication through Flink checkpointing
- **Time-Based Aggregations**: Automatically generates daily, weekly, and monthly health statistics per user and data type
- **Schema Evolution**: Supports backward-compatible schema changes without pipeline downtime
- **Scalable Architecture**: Horizontally scalable with Kubernetes and Flink Operator
- **Data Quality**: Built-in validation and dead letter queue (DLQ) for error handling
- **Monitoring**: Comprehensive metrics exposed via Prometheus

### Architecture

```
┌─────────────────┐
│  Kafka Cluster  │
│ health-data-raw │
└────────┬────────┘
         │ Avro Messages
         ▼
┌─────────────────────────────────────────┐
│         Flink Application               │
│  ┌──────────────────────────────────┐  │
│  │  1. Kafka Source (Avro Deser)    │  │
│  └──────────┬───────────────────────┘  │
│             │                           │
│  ┌──────────▼───────────────────────┐  │
│  │  2. Transform & Validate         │  │
│  │     - Flatten nested data        │  │
│  │     - Convert timestamps         │  │
│  │     - Validate data quality      │  │
│  └──────────┬───────────────────────┘  │
│             │                           │
│       ┌─────┴─────┐                    │
│       │           │                     │
│  ┌────▼────┐ ┌───▼──────────────────┐ │
│  │ Raw     │ │ Aggregation Pipeline │ │
│  │ Data    │ │  - Daily Windows     │ │
│  │ Sink    │ │  - Weekly Windows    │ │
│  │         │ │  - Monthly Windows   │ │
│  └────┬────┘ └───┬──────────────────┘ │
└───────┼──────────┼────────────────────┘
        │          │
        ▼          ▼
┌────────────────────────────────────────┐
│      Apache Iceberg Data Lake          │
│  ┌──────────────────────────────────┐  │
│  │ health_data_raw                  │  │
│  │ health_data_daily_agg            │  │
│  │ health_data_weekly_agg           │  │
│  │ health_data_monthly_agg          │  │
│  │ health_data_errors (DLQ)         │  │
│  └──────────────────────────────────┘  │
│                                         │
│  Storage: MinIO/S3                     │
│  Catalog: Hadoop/Hive/REST             │
└─────────────────────────────────────────┘
```

## Project Structure

```
flink_consumer/
├── aggregations/        # Time-based aggregation pipeline
│   ├── __init__.py
│   ├── aggregator.py    # Aggregation functions (min, max, avg, stddev)
│   ├── metadata.py      # Window metadata enrichment
│   ├── metrics.py       # Aggregation metrics reporting
│   ├── pipeline.py      # Complete aggregation pipeline
│   ├── schemas.py       # Aggregate table schemas
│   ├── sink.py          # Iceberg sink with upsert support
│   ├── validator.py     # Aggregation data validation
│   ├── watermark.py     # Watermark strategy configuration
│   └── windows.py       # Window definitions (daily/weekly/monthly)
├── config/              # Configuration management
│   ├── __init__.py
│   ├── checkpoint.py    # Checkpoint configuration
│   ├── config.py        # Main configuration loader
│   ├── flink-conf.yaml  # Flink cluster configuration
│   ├── log4j2.properties # Logging configuration
│   ├── logging.py       # Structured logging setup
│   ├── prometheus.py    # Prometheus metrics configuration
│   ├── recovery.py      # Recovery strategy configuration
│   └── settings.py      # Pydantic settings models
├── converters/          # Data transformation logic
│   ├── __init__.py
│   ├── avro_deserializer.py      # Avro deserialization with Schema Registry
│   ├── health_data_transformer.py # Flatten nested health data
│   └── schema_evolution.py       # Schema evolution handling
├── iceberg/             # Iceberg catalog and table management
│   ├── __init__.py
│   ├── catalog.py       # Catalog connection (Hadoop/Hive/REST)
│   ├── schema_evolution.py # Iceberg schema evolution utilities
│   ├── schemas.py       # Table schema definitions
│   ├── sink.py          # Iceberg sink implementation
│   └── table_manager.py # Table creation and management
├── services/            # Service integrations
│   ├── __init__.py
│   ├── error_handler.py # Error handling and DLQ
│   ├── kafka_source.py  # Kafka source connector
│   └── metrics.py       # Custom metrics reporter
├── utils/               # Utility functions
│   ├── __init__.py
│   └── kafka_utils.py   # Kafka helper utilities
├── validators/          # Data validation logic
│   ├── __init__.py
│   └── health_data_validator.py # Health data validation rules
├── docs/                # Detailed documentation
│   ├── AGGREGATION_PIPELINE.md      # Aggregation pipeline guide
│   ├── AGGREGATION_TABLES.md        # Aggregate table schemas
│   ├── CHECKPOINT_RECOVERY.md       # Checkpoint and recovery guide
│   ├── DEPLOYMENT.md                # Deployment guide
│   ├── EXACTLY_ONCE.md              # Exactly-once semantics
│   ├── ICEBERG_SETUP.md             # Iceberg setup instructions
│   ├── ICEBERG_SINK.md              # Iceberg sink configuration
│   ├── KAFKA_SOURCE.md              # Kafka source configuration
│   ├── MONITORING_METRICS.md        # Monitoring and metrics
│   ├── SCHEMA_EVOLUTION.md          # Schema evolution guide
│   └── TRANSFORMATION_PIPELINE.md   # Transformation pipeline
├── examples/            # Example scripts and tests
│   ├── test_aggregation_pipeline.py
│   ├── test_checkpoint_recovery.py
│   ├── test_exactly_once.py
│   ├── test_iceberg_setup.py
│   ├── test_iceberg_sink.py
│   ├── test_kafka_source.py
│   ├── test_monitoring_metrics.py
│   ├── test_schema_evolution.py
│   └── test_transformation_pipeline.py
├── k8s/                 # Kubernetes deployment manifests
│   ├── README.md
│   ├── configmap.yaml
│   ├── flinkdeployment.yaml
│   ├── namespace.yaml
│   ├── secrets.yaml
│   └── serviceaccount.yaml
├── tests/               # Unit and integration tests
│   ├── integration/
│   ├── performance/
│   ├── conftest.py
│   └── test_*.py
├── .env.example         # Example environment variables
├── .env.local           # Local development environment variables
├── Dockerfile           # Docker image definition
├── main.py              # Application entry point
├── pyproject.toml       # Python dependencies and project metadata
└── README.md            # This file
```

## Prerequisites

- Python 3.11+
- Apache Flink 1.18+
- Apache Kafka (existing infrastructure)
- Schema Registry
- MinIO or S3-compatible storage
- uv (Python package manager)
- Docker and Docker Compose (for local development)
- Kubernetes cluster with Flink Operator (for production deployment)

## Quick Start

### Local Development Setup

1. **Clone the repository and navigate to the flink_consumer directory:**

```bash
cd flink_consumer
```

2. **Install dependencies using uv:**

```bash
uv sync
```

3. **Copy environment configuration:**

```bash
cp .env.example .env.local
```

4. **Update `.env.local` with your configuration:**

```bash
# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=health-data-raw
KAFKA_GROUP_ID=flink-iceberg-consumer

# Schema Registry
SCHEMA_REGISTRY_URL=http://localhost:8081

# Iceberg Configuration
ICEBERG_CATALOG_TYPE=hadoop
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse
ICEBERG_CATALOG_URI=http://localhost:8181

# S3/MinIO Configuration
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin

# Flink Configuration
FLINK_PARALLELISM=6
CHECKPOINT_INTERVAL=60000
CHECKPOINT_STORAGE=s3a://flink-checkpoints/health-consumer
```

5. **Start the infrastructure using Docker Compose:**

```bash
# From the project root directory
docker-compose up -d kafka schema-registry minio
```

6. **Run the Flink application locally:**

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the application
python main.py
```

### Running with Docker Compose (Full Stack)

To run the complete stack including Flink, Kafka, and storage:

```bash
# From the project root directory
docker-compose up -d

# View logs
docker-compose logs -f flink-jobmanager

# Stop the stack
docker-compose down
```

The Docker Compose setup includes:
- **Kafka Cluster**: 3 brokers for high availability
- **Schema Registry**: Avro schema management
- **MinIO**: S3-compatible object storage
- **Flink Cluster**: JobManager + 3 TaskManagers
- **Prometheus**: Metrics collection
- **Grafana**: Metrics visualization

Access the services:
- Flink Web UI: http://localhost:8081
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

## Configuration

All configuration is managed through environment variables. See `.env.example` for all available options.

### Key Configuration Sections

#### Kafka Configuration
```bash
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_TOPIC=health-data-raw
KAFKA_GROUP_ID=flink-iceberg-consumer
KAFKA_AUTO_OFFSET_RESET=earliest  # earliest or latest
KAFKA_PARTITION_DISCOVERY_INTERVAL=60000  # milliseconds
```

#### Schema Registry
```bash
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

#### Iceberg Configuration
```bash
ICEBERG_CATALOG_TYPE=hadoop  # hadoop, hive, or rest
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse
ICEBERG_CATALOG_URI=http://iceberg-rest:8181  # For REST catalog
ICEBERG_DATABASE=health_db
```

#### S3/MinIO Configuration
```bash
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_PATH_STYLE_ACCESS=true  # Required for MinIO
```

#### Flink Configuration
```bash
FLINK_PARALLELISM=6
CHECKPOINT_INTERVAL=60000  # milliseconds
CHECKPOINT_TIMEOUT=600000  # milliseconds
CHECKPOINT_STORAGE=s3a://flink-checkpoints/health-consumer
STATE_BACKEND=rocksdb  # rocksdb or hashmap
```

#### Monitoring Configuration
```bash
METRICS_ENABLED=true
PROMETHEUS_PORT=9249
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT=json  # json or text
```

For detailed configuration options, see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Aggregation Pipeline

The application automatically generates time-based aggregations for health data:

### Daily Aggregations
- **Window**: 24-hour tumbling windows aligned to midnight UTC
- **Allowed Lateness**: 1 hour
- **Parallelism**: 12 tasks
- **Output Table**: `health_data_daily_agg`

### Weekly Aggregations
- **Window**: 7-day tumbling windows aligned to Monday 00:00 UTC
- **Allowed Lateness**: 6 hours
- **Parallelism**: 6 tasks
- **Output Table**: `health_data_weekly_agg`

### Monthly Aggregations
- **Window**: 30-day tumbling windows aligned to 1st of month 00:00 UTC
- **Allowed Lateness**: 12 hours
- **Parallelism**: 3 tasks
- **Output Table**: `health_data_monthly_agg`

### Computed Statistics

For each user and data type, the following statistics are calculated:
- **min_value**: Minimum value in the window
- **max_value**: Maximum value in the window
- **avg_value**: Average value
- **sum_value**: Sum of all values
- **count**: Number of records
- **stddev_value**: Standard deviation
- **first_value**: First value in the window
- **last_value**: Last value in the window

### Late Data Handling

The aggregation pipeline supports late-arriving data through:
- **Watermarks**: 10-minute bounded out-of-orderness
- **Allowed Lateness**: Window-specific lateness tolerance
- **Upsert Mode**: Automatic updates to existing aggregations in Iceberg

For more details, see [docs/AGGREGATION_PIPELINE.md](docs/AGGREGATION_PIPELINE.md).

## Data Flow

### 1. Kafka Source
- Consumes Avro-encoded messages from `health-data-raw` topic
- Deserializes using Schema Registry
- Supports dynamic partition discovery
- Exactly-once offset management

### 2. Transformation
- Flattens nested health data payload
- Converts ISO 8601 timestamps to Unix timestamps
- Extracts individual samples from batch payloads
- Adds processing metadata

### 3. Validation
- Validates required fields (userId, value, timestamps)
- Checks value ranges (non-negative values, valid date ranges)
- Routes invalid records to error stream (DLQ)
- Logs validation failures with context

### 4. Aggregation (Parallel Branch)
- Assigns event-time watermarks
- Keys by (userId, dataType)
- Applies tumbling windows (daily/weekly/monthly)
- Calculates statistics using incremental aggregation
- Enriches with window metadata

### 5. Iceberg Sink
- Writes raw data to `health_data_raw` table
- Writes aggregations to respective aggregate tables
- Batches records for optimal file sizes (128-512MB)
- Commits on checkpoint completion
- Supports upsert for late data updates

### 6. Checkpointing
- Exactly-once semantics with RocksDB state backend
- 60-second checkpoint interval
- S3/MinIO checkpoint storage
- Automatic recovery on failure

## Development

### Running Locally

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the Flink application
python main.py
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=flink_consumer --cov-report=html

# Run specific test categories
uv run pytest tests/unit/
uv run pytest tests/integration/
uv run pytest tests/performance/
```

### Code Quality

```bash
# Format code
uv run black .

# Lint code
uv run ruff check .

# Type checking
uv run mypy flink_consumer
```

### Example Scripts

The `examples/` directory contains standalone scripts to test individual components:

```bash
# Test Iceberg setup and connectivity
python examples/test_iceberg_setup.py

# Test Kafka source connector
python examples/test_kafka_source.py

# Test transformation pipeline
python examples/test_transformation_pipeline.py

# Test aggregation pipeline
python examples/test_aggregation_pipeline.py

# Test checkpoint and recovery
python examples/test_checkpoint_recovery.py

# Test schema evolution
python examples/test_schema_evolution.py
```

## Deployment

The Flink Iceberg Consumer can be deployed in multiple ways depending on your infrastructure and requirements:

### 1. Local Development (Standalone)

Run directly on your local machine for development and testing:

```bash
# Activate virtual environment
source .venv/bin/activate

# Set environment variables
export KAFKA_BROKERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
# ... other environment variables

# Run the application
python main.py
```

**Pros:**
- Fast iteration during development
- Easy debugging with IDE
- No container overhead

**Cons:**
- Not suitable for production
- Manual dependency management
- Single point of failure

**Use Cases:** Local development, testing, debugging

---

### 2. Docker Standalone

Run as a single Docker container:

```bash
# Build the Docker image
docker build -t health-stack/flink-iceberg-consumer:1.0.0 .

# Run with environment file
docker run -d \
  --name flink-consumer \
  --env-file .env.local \
  -p 8081:8081 \
  -p 9249:9249 \
  health-stack/flink-iceberg-consumer:1.0.0
```

**Pros:**
- Consistent environment
- Easy to package and distribute
- Isolated dependencies

**Cons:**
- Single container (no fault tolerance)
- Limited scalability
- Manual restart on failure

**Use Cases:** Testing, small-scale deployments, CI/CD pipelines

---

### 3. Docker Compose (Multi-Container)

Run complete stack with Docker Compose (recommended for local/staging):

```bash
# From the project root directory
docker-compose up -d

# View logs
docker-compose logs -f flink-jobmanager flink-taskmanager

# Scale TaskManagers
docker-compose up -d --scale flink-taskmanager=5

# Stop the stack
docker-compose down
```

The Docker Compose setup includes:
- **Flink Cluster**: JobManager + multiple TaskManagers
- **Kafka Cluster**: 3 brokers for high availability
- **Schema Registry**: Avro schema management
- **MinIO**: S3-compatible object storage
- **Prometheus**: Metrics collection
- **Grafana**: Metrics visualization

**Pros:**
- Complete local environment
- Multi-container Flink cluster
- Easy to scale TaskManagers
- Includes monitoring stack

**Cons:**
- Resource intensive on local machine
- Not production-grade HA
- Manual orchestration

**Use Cases:** Local development, integration testing, staging environments

---

### 4. Kubernetes with Flink Operator (Recommended for Production)

Deploy to Kubernetes using Flink Kubernetes Operator:

#### Step 1: Install Flink Kubernetes Operator

```bash
kubectl create namespace flink-operator
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.6.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator
```

#### Step 2: Create namespace and secrets

```bash
kubectl create namespace data-platform
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/serviceaccount.yaml
```

#### Step 3: Deploy the Flink application

```bash
kubectl apply -f k8s/flinkdeployment.yaml
```

#### Step 4: Monitor the deployment

```bash
# Check FlinkDeployment status
kubectl get flinkdeployment -n data-platform

# View logs
kubectl logs -n data-platform -l app=health-data-consumer -f

# Access Flink Web UI (port-forward)
kubectl port-forward -n data-platform svc/health-data-consumer-rest 8081:8081
```

**Pros:**
- Production-grade orchestration
- Automatic failover and recovery
- Horizontal scaling
- Rolling updates with savepoints
- Resource management
- High availability

**Cons:**
- Requires Kubernetes cluster
- More complex setup
- Higher operational overhead

**Use Cases:** Production deployments, large-scale processing, mission-critical applications

---

### 5. Standalone Flink Cluster

Deploy to a standalone Flink cluster (without Kubernetes):

```bash
# Start Flink cluster
$FLINK_HOME/bin/start-cluster.sh

# Submit the job
$FLINK_HOME/bin/flink run \
  --python main.py \
  --pyFiles flink_consumer.zip \
  --parallelism 6

# Check job status
$FLINK_HOME/bin/flink list

# Stop the job
$FLINK_HOME/bin/flink cancel <job-id>
```

**Pros:**
- No container overhead
- Direct Flink cluster access
- Simpler than Kubernetes

**Cons:**
- Manual cluster management
- No automatic orchestration
- Requires Flink installation

**Use Cases:** Existing Flink infrastructure, bare-metal deployments

---

### 6. YARN Cluster

Deploy to Hadoop YARN cluster:

```bash
# Submit to YARN
$FLINK_HOME/bin/flink run \
  --target yarn-per-job \
  --python main.py \
  --pyFiles flink_consumer.zip \
  --parallelism 6 \
  --detached

# Check YARN application
yarn application -list
```

**Pros:**
- Integrates with Hadoop ecosystem
- Resource sharing with other YARN apps
- Existing YARN infrastructure

**Cons:**
- Requires YARN cluster
- More complex configuration
- YARN-specific limitations

**Use Cases:** Hadoop environments, data lake architectures with YARN

---

### Deployment Comparison

| Method | Complexity | Scalability | HA | Production Ready | Best For |
|--------|------------|-------------|----|--------------------|----------|
| Local Standalone | Low | None | No | No | Development |
| Docker Standalone | Low | Limited | No | No | Testing |
| Docker Compose | Medium | Medium | Limited | No | Staging |
| Kubernetes | High | High | Yes | Yes | Production |
| Standalone Flink | Medium | Medium | Limited | Yes | Existing Flink |
| YARN | High | High | Yes | Yes | Hadoop ecosystem |

### Recommended Deployment Strategy

1. **Development:** Local standalone or Docker Compose
2. **Testing/Staging:** Docker Compose with full stack
3. **Production:** Kubernetes with Flink Operator

For detailed deployment instructions, see [docs/OPERATIONS_GUIDE.md](docs/OPERATIONS_GUIDE.md).

## Monitoring

### Prometheus Metrics

The application exposes metrics on port 9249 in Prometheus format:

**Built-in Flink Metrics:**
- `flink_taskmanager_job_task_numRecordsIn`: Records consumed from Kafka
- `flink_taskmanager_job_task_numRecordsOut`: Records written to Iceberg
- `flink_jobmanager_job_lastCheckpointDuration`: Checkpoint duration
- `flink_jobmanager_job_lastCheckpointSize`: Checkpoint size in bytes
- `flink_taskmanager_Status_JVM_Memory_Heap_Used`: JVM heap memory usage

**Custom Application Metrics:**
- `valid_records`: Counter of valid records processed
- `invalid_records`: Counter of validation failures
- `processing_latency`: Distribution of processing latency (ms)
- `windows_processed`: Counter of aggregation windows completed
- `records_aggregated`: Counter of records included in aggregations
- `window_latency_ms`: Distribution of window processing latency

### Grafana Dashboards

Import the pre-built Grafana dashboards from `docs/grafana/`:
- **Flink Overview**: Job health, throughput, latency
- **Kafka Consumer**: Lag, partition distribution
- **Iceberg Writes**: Write throughput, file sizes, commit duration
- **Aggregations**: Window processing, late data, statistics

### Logging

Structured JSON logging with configurable levels:

```json
{
  "timestamp": "2025-11-17T10:30:45.123Z",
  "level": "INFO",
  "logger": "flink_consumer.services.kafka_source",
  "message": "Kafka source started",
  "context": {
    "topic": "health-data-raw",
    "group_id": "flink-iceberg-consumer",
    "partitions": 6
  }
}
```

### Alerting

Recommended Prometheus alerts:
- High checkpoint duration (> 5 minutes)
- High Kafka consumer lag (> 100k messages)
- High error rate (> 10 errors/minute)
- Low throughput (< 1000 records/minute)
- Job restarts (> 3 in 1 hour)

For detailed monitoring setup, see [docs/MONITORING_METRICS.md](docs/MONITORING_METRICS.md).

## Environment Variables Reference

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `KAFKA_BROKERS` | Comma-separated Kafka broker addresses | - | Yes |
| `KAFKA_TOPIC` | Kafka topic to consume | `health-data-raw` | Yes |
| `KAFKA_GROUP_ID` | Consumer group ID | `flink-iceberg-consumer` | Yes |
| `SCHEMA_REGISTRY_URL` | Schema Registry URL | - | Yes |
| `ICEBERG_CATALOG_TYPE` | Catalog type (hadoop/hive/rest) | `hadoop` | Yes |
| `ICEBERG_WAREHOUSE` | Iceberg warehouse location | - | Yes |
| `S3_ENDPOINT` | S3/MinIO endpoint URL | - | Yes |
| `S3_ACCESS_KEY` | S3 access key | - | Yes |
| `S3_SECRET_KEY` | S3 secret key | - | Yes |
| `FLINK_PARALLELISM` | Default parallelism | `6` | No |
| `CHECKPOINT_INTERVAL` | Checkpoint interval (ms) | `60000` | No |
| `STATE_BACKEND` | State backend (rocksdb/hashmap) | `rocksdb` | No |
| `LOG_LEVEL` | Logging level | `INFO` | No |
| `METRICS_ENABLED` | Enable Prometheus metrics | `true` | No |

For complete environment variable reference, see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Troubleshooting

### Common Issues

**Issue: Kafka connection timeout**
```
Solution: Check KAFKA_BROKERS configuration and network connectivity
```

**Issue: Schema Registry not found**
```
Solution: Verify SCHEMA_REGISTRY_URL and ensure Schema Registry is running
```

**Issue: Iceberg table not found**
```
Solution: Tables are auto-created on first run. Check catalog connectivity and permissions
```

**Issue: Checkpoint failures**
```
Solution: Verify S3/MinIO credentials and CHECKPOINT_STORAGE path
```

**Issue: High memory usage**
```
Solution: Tune RocksDB settings or increase TaskManager memory allocation
```

For detailed troubleshooting, see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Performance Tuning

### Throughput Optimization
- Increase `FLINK_PARALLELISM` to match Kafka partition count
- Tune batch sizes for Iceberg writes (target 128-512MB files)
- Adjust checkpoint interval based on throughput requirements

### Latency Optimization
- Reduce checkpoint interval for faster recovery
- Use HashMapStateBackend for small state
- Optimize watermark strategy for your data characteristics

### Resource Optimization
- Configure RocksDB block cache size
- Set appropriate TaskManager memory allocation
- Use incremental checkpoints for large state

For detailed performance tuning, see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Documentation

- [AGGREGATION_PIPELINE.md](docs/AGGREGATION_PIPELINE.md) - Aggregation pipeline details
- [AGGREGATION_TABLES.md](docs/AGGREGATION_TABLES.md) - Aggregate table schemas
- [CHECKPOINT_RECOVERY.md](docs/CHECKPOINT_RECOVERY.md) - Checkpoint and recovery
- [DEPLOYMENT.md](docs/DEPLOYMENT.md) - Deployment guide
- [EXACTLY_ONCE.md](docs/EXACTLY_ONCE.md) - Exactly-once semantics
- [ICEBERG_SETUP.md](docs/ICEBERG_SETUP.md) - Iceberg setup
- [ICEBERG_SINK.md](docs/ICEBERG_SINK.md) - Iceberg sink configuration
- [KAFKA_SOURCE.md](docs/KAFKA_SOURCE.md) - Kafka source configuration
- [MONITORING_METRICS.md](docs/MONITORING_METRICS.md) - Monitoring and metrics
- [SCHEMA_EVOLUTION.md](docs/SCHEMA_EVOLUTION.md) - Schema evolution
- [TRANSFORMATION_PIPELINE.md](docs/TRANSFORMATION_PIPELINE.md) - Transformation pipeline

## License

Proprietary - Health Stack Project
