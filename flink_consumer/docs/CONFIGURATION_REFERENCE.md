# Configuration Reference

Complete reference for all configuration options, environment variables, Flink settings, Iceberg table schemas, and monitoring metrics.

## Table of Contents

- [Environment Variables](#environment-variables)
- [Flink Configuration](#flink-configuration)
- [Iceberg Table Schemas](#iceberg-table-schemas)
- [Partition Strategies](#partition-strategies)
- [Metrics and Monitoring](#metrics-and-monitoring)
- [Alert Rules](#alert-rules)

## Environment Variables

### Kafka Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `KAFKA_BROKERS` | Comma-separated list of Kafka broker addresses | string | - | Yes |
| `KAFKA_TOPIC` | Kafka topic to consume health data from | string | `health-data-raw` | Yes |
| `KAFKA_GROUP_ID` | Consumer group ID for offset management | string | `flink-iceberg-consumer` | Yes |
| `KAFKA_AUTO_OFFSET_RESET` | Offset reset strategy when no initial offset | enum | `earliest` | No |
| `KAFKA_PARTITION_DISCOVERY_INTERVAL` | Interval for discovering new partitions (ms) | integer | `60000` | No |
| `KAFKA_ENABLE_AUTO_COMMIT` | Enable automatic offset commits | boolean | `false` | No |
| `KAFKA_SESSION_TIMEOUT` | Session timeout for consumer (ms) | integer | `30000` | No |
| `KAFKA_REQUEST_TIMEOUT` | Request timeout for Kafka operations (ms) | integer | `40000` | No |
| `KAFKA_FETCH_MIN_BYTES` | Minimum bytes to fetch in a request | integer | `1048576` | No |
| `KAFKA_FETCH_MAX_WAIT` | Maximum wait time for fetch request (ms) | integer | `500` | No |
| `KAFKA_MAX_PARTITION_FETCH_BYTES` | Maximum bytes per partition | integer | `10485760` | No |

**Example:**
```bash
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
KAFKA_TOPIC=health-data-raw
KAFKA_GROUP_ID=flink-iceberg-consumer
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_PARTITION_DISCOVERY_INTERVAL=60000
```

### Schema Registry Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `SCHEMA_REGISTRY_URL` | Schema Registry base URL | string | - | Yes |
| `SCHEMA_REGISTRY_BASIC_AUTH_USER` | Basic auth username | string | - | No |
| `SCHEMA_REGISTRY_BASIC_AUTH_PASSWORD` | Basic auth password | string | - | No |
| `SCHEMA_REGISTRY_SSL_ENABLED` | Enable SSL for Schema Registry | boolean | `false` | No |

**Example:**
```bash
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

### Iceberg Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `ICEBERG_CATALOG_TYPE` | Catalog type (hadoop/hive/rest) | enum | `hadoop` | Yes |
| `ICEBERG_CATALOG_NAME` | Catalog name | string | `health_catalog` | No |
| `ICEBERG_WAREHOUSE` | Warehouse location (S3/HDFS path) | string | - | Yes |
| `ICEBERG_CATALOG_URI` | Catalog URI (for Hive/REST) | string | - | Conditional |
| `ICEBERG_DATABASE` | Database name | string | `health_db` | No |
| `ICEBERG_TABLE_RAW` | Raw data table name | string | `health_data_raw` | No |
| `ICEBERG_TABLE_DAILY_AGG` | Daily aggregates table name | string | `health_data_daily_agg` | No |
| `ICEBERG_TABLE_WEEKLY_AGG` | Weekly aggregates table name | string | `health_data_weekly_agg` | No |
| `ICEBERG_TABLE_MONTHLY_AGG` | Monthly aggregates table name | string | `health_data_monthly_agg` | No |
| `ICEBERG_TABLE_ERRORS` | Error/DLQ table name | string | `health_data_errors` | No |

**Example:**
```bash
ICEBERG_CATALOG_TYPE=hadoop
ICEBERG_CATALOG_NAME=health_catalog
ICEBERG_WAREHOUSE=s3a://data-lake/warehouse
ICEBERG_DATABASE=health_db
```

### S3/MinIO Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `S3_ENDPOINT` | S3-compatible endpoint URL | string | - | Yes |
| `S3_ACCESS_KEY` | S3 access key ID | string | - | Yes |
| `S3_SECRET_KEY` | S3 secret access key | string | - | Yes |
| `S3_PATH_STYLE_ACCESS` | Use path-style access (required for MinIO) | boolean | `true` | No |
| `S3_REGION` | S3 region | string | `us-east-1` | No |
| `S3_SSL_ENABLED` | Enable SSL for S3 connections | boolean | `false` | No |

**Example:**
```bash
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_PATH_STYLE_ACCESS=true
S3_REGION=us-east-1
```

### Flink Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `FLINK_PARALLELISM` | Default parallelism for operators | integer | `6` | No |
| `FLINK_MAX_PARALLELISM` | Maximum parallelism (for rescaling) | integer | `128` | No |
| `CHECKPOINT_INTERVAL` | Checkpoint interval (ms) | integer | `60000` | No |
| `CHECKPOINT_TIMEOUT` | Checkpoint timeout (ms) | integer | `600000` | No |
| `CHECKPOINT_STORAGE` | Checkpoint storage location | string | - | Yes |
| `STATE_BACKEND` | State backend type (rocksdb/hashmap) | enum | `rocksdb` | No |
| `STATE_TTL_DAYS` | State TTL for windowed operations (days) | integer | `7` | No |
| `RESTART_ATTEMPTS` | Number of restart attempts on failure | integer | `3` | No |
| `RESTART_DELAY` | Delay between restart attempts (ms) | integer | `10000` | No |

**Example:**
```bash
FLINK_PARALLELISM=6
FLINK_MAX_PARALLELISM=128
CHECKPOINT_INTERVAL=60000
CHECKPOINT_TIMEOUT=600000
CHECKPOINT_STORAGE=s3a://flink-checkpoints/health-consumer
STATE_BACKEND=rocksdb
STATE_TTL_DAYS=7
RESTART_ATTEMPTS=3
RESTART_DELAY=10000
```

### Aggregation Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `WATERMARK_MAX_OUT_OF_ORDERNESS` | Max out-of-orderness for watermarks (minutes) | integer | `10` | No |
| `DAILY_WINDOW_ALLOWED_LATENESS` | Allowed lateness for daily windows (hours) | integer | `1` | No |
| `WEEKLY_WINDOW_ALLOWED_LATENESS` | Allowed lateness for weekly windows (hours) | integer | `6` | No |
| `MONTHLY_WINDOW_ALLOWED_LATENESS` | Allowed lateness for monthly windows (hours) | integer | `12` | No |
| `DAILY_AGG_PARALLELISM` | Parallelism for daily aggregations | integer | `12` | No |
| `WEEKLY_AGG_PARALLELISM` | Parallelism for weekly aggregations | integer | `6` | No |
| `MONTHLY_AGG_PARALLELISM` | Parallelism for monthly aggregations | integer | `3` | No |

**Example:**
```bash
WATERMARK_MAX_OUT_OF_ORDERNESS=10
DAILY_WINDOW_ALLOWED_LATENESS=1
WEEKLY_WINDOW_ALLOWED_LATENESS=6
MONTHLY_WINDOW_ALLOWED_LATENESS=12
DAILY_AGG_PARALLELISM=12
WEEKLY_AGG_PARALLELISM=6
MONTHLY_AGG_PARALLELISM=3
```

### Monitoring Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `METRICS_ENABLED` | Enable Prometheus metrics | boolean | `true` | No |
| `PROMETHEUS_PORT` | Prometheus metrics port | integer | `9249` | No |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR) | enum | `INFO` | No |
| `LOG_FORMAT` | Log format (json/text) | enum | `json` | No |
| `STRUCTURED_LOGGING` | Enable structured logging | boolean | `true` | No |

**Example:**
```bash
METRICS_ENABLED=true
PROMETHEUS_PORT=9249
LOG_LEVEL=INFO
LOG_FORMAT=json
STRUCTURED_LOGGING=true
```

### Validation Configuration

| Variable | Description | Type | Default | Required |
|----------|-------------|------|---------|----------|
| `VALIDATION_ENABLED` | Enable data validation | boolean | `true` | No |
| `VALIDATION_STRICT_MODE` | Fail on validation errors | boolean | `false` | No |
| `MIN_HEART_RATE` | Minimum valid heart rate (bpm) | integer | `30` | No |
| `MAX_HEART_RATE` | Maximum valid heart rate (bpm) | integer | `250` | No |
| `MAX_STEPS_PER_DAY` | Maximum valid steps per day | integer | `100000` | No |

**Example:**
```bash
VALIDATION_ENABLED=true
VALIDATION_STRICT_MODE=false
MIN_HEART_RATE=30
MAX_HEART_RATE=250
MAX_STEPS_PER_DAY=100000
```

## Flink Configuration

### flink-conf.yaml

Complete Flink configuration file for production deployment:

```yaml
# JobManager Configuration
jobmanager.rpc.address: health-data-consumer-rest
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 2048m
jobmanager.memory.flink.size: 1536m
jobmanager.memory.jvm-overhead.fraction: 0.1

# TaskManager Configuration
taskmanager.numberOfTaskSlots: 4
taskmanager.memory.process.size: 4096m
taskmanager.memory.flink.size: 3072m
taskmanager.memory.managed.fraction: 0.4
taskmanager.memory.network.fraction: 0.2
taskmanager.memory.jvm-overhead.fraction: 0.1

# Parallelism
parallelism.default: 6

# State Backend
state.backend: rocksdb
state.backend.incremental: true
state.backend.rocksdb.predefined-options: SPINNING_DISK_OPTIMIZED_HIGH_MEM
state.backend.rocksdb.block.cache-size: 512m
state.backend.rocksdb.writebuffer.size: 128m
state.backend.rocksdb.writebuffer.count: 4

# Checkpointing
state.checkpoints.dir: s3://flink-checkpoints/health-consumer
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 10min
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.min-pause: 30s
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# Savepoints
state.savepoints.dir: s3://flink-checkpoints/health-consumer/savepoints

# Restart Strategy
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10s

# Network
taskmanager.network.memory.fraction: 0.2
taskmanager.network.memory.min: 256m
taskmanager.network.memory.max: 1024m
execution.buffer-timeout: 100ms

# Metrics
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9249
metrics.reporter.prom.interval: 30s
metrics.scope.jm: flink.jobmanager
metrics.scope.tm: flink.taskmanager
metrics.scope.task: flink.taskmanager.task
metrics.scope.operator: flink.taskmanager.task.operator

# Web UI
web.submit.enable: false
web.cancel.enable: false
web.timeout: 600000

# High Availability (Optional)
# high-availability: zookeeper
# high-availability.storageDir: s3://flink-ha/
# high-availability.zookeeper.quorum: zk-1:2181,zk-2:2181,zk-3:2181
# high-availability.zookeeper.path.root: /flink
# high-availability.cluster-id: health-data-consumer

# Security (Optional)
# security.ssl.enabled: true
# security.ssl.keystore: /path/to/keystore.jks
# security.ssl.keystore-password: password
# security.ssl.truststore: /path/to/truststore.jks
# security.ssl.truststore-password: password

# Kubernetes
kubernetes.namespace: data-platform
kubernetes.cluster-id: health-data-consumer
kubernetes.container.image: health-stack/flink-iceberg-consumer:1.0.0
kubernetes.jobmanager.service-account: flink-service-account
kubernetes.taskmanager.service-account: flink-service-account
```

### RocksDB Configuration

Advanced RocksDB tuning options:

```yaml
# Compression per level
state.backend.rocksdb.compression.per.level: >
  NO_COMPRESSION:NO_COMPRESSION:LZ4_COMPRESSION:LZ4_COMPRESSION:LZ4_COMPRESSION:ZSTD_COMPRESSION:ZSTD_COMPRESSION

# Block size
state.backend.rocksdb.block.blocksize: 16kb

# Bloom filter
state.backend.rocksdb.use-bloom-filter: true

# Thread configuration
state.backend.rocksdb.thread.num: 4

# Memory budget
state.backend.rocksdb.memory.managed: true
state.backend.rocksdb.memory.write-buffer-ratio: 0.5
state.backend.rocksdb.memory.high-prio-pool-ratio: 0.1

# Compaction
state.backend.rocksdb.compaction.level.max-size-level-base: 256mb
state.backend.rocksdb.compaction.level.target-file-size-base: 64mb
```

## Iceberg Table Schemas

### Raw Data Table: health_data_raw

Complete schema for the raw health data table:

```sql
CREATE TABLE health_catalog.health_db.health_data_raw (
    -- Device and User Information
    device_id STRING COMMENT 'Unique device identifier',
    user_id STRING COMMENT 'Unique user identifier',
    
    -- Sample Information
    sample_id STRING COMMENT 'Unique sample identifier',
    data_type STRING COMMENT 'Type of health data (heartRate, steps, etc.)',
    
    -- Measurement Data
    value DOUBLE COMMENT 'Measured value',
    unit STRING COMMENT 'Unit of measurement',
    
    -- Timestamps
    start_date TIMESTAMP COMMENT 'Sample start timestamp (event time)',
    end_date TIMESTAMP COMMENT 'Sample end timestamp',
    created_at TIMESTAMP COMMENT 'Sample creation timestamp',
    payload_timestamp TIMESTAMP COMMENT 'Payload received timestamp',
    processing_time TIMESTAMP COMMENT 'Flink processing timestamp',
    
    -- Metadata
    source_bundle STRING COMMENT 'Source application bundle ID',
    metadata MAP<STRING, STRING> COMMENT 'Additional metadata key-value pairs',
    is_synced BOOLEAN COMMENT 'Sync status flag',
    app_version STRING COMMENT 'Application version',
    
    -- Partitioning Column
    ingestion_date DATE COMMENT 'Date partition (derived from start_date)'
)
USING iceberg
PARTITIONED BY (days(start_date), data_type)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.parquet.page-size-bytes' = '1048576',
    'write.parquet.row-group-size-bytes' = '134217728',
    'write.metadata.compression-codec' = 'gzip',
    'write.target-file-size-bytes' = '134217728',
    'commit.retry.num-retries' = '3',
    'commit.retry.min-wait-ms' = '100',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'format-version' = '2'
);
```

**Field Descriptions:**

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| device_id | STRING | No | Unique identifier for the device |
| user_id | STRING | No | Unique identifier for the user |
| sample_id | STRING | No | Unique identifier for the sample |
| data_type | STRING | No | Type of health data (e.g., heartRate, steps, bloodPressure) |
| value | DOUBLE | No | Numeric measurement value |
| unit | STRING | No | Unit of measurement (e.g., count/min, count, mmHg) |
| start_date | TIMESTAMP | No | Start time of the measurement (used for event time) |
| end_date | TIMESTAMP | No | End time of the measurement |
| created_at | TIMESTAMP | No | Timestamp when sample was created on device |
| payload_timestamp | TIMESTAMP | No | Timestamp when payload was received by gateway |
| processing_time | TIMESTAMP | No | Timestamp when Flink processed the record |
| source_bundle | STRING | Yes | Bundle ID of the source application |
| metadata | MAP<STRING, STRING> | Yes | Additional metadata as key-value pairs |
| is_synced | BOOLEAN | No | Whether the sample has been synced |
| app_version | STRING | No | Version of the application that generated the data |
| ingestion_date | DATE | No | Date derived from start_date for partitioning |

### Daily Aggregates Table: health_data_daily_agg

```sql
CREATE TABLE health_catalog.health_db.health_data_daily_agg (
    -- Grouping Keys
    user_id STRING COMMENT 'User identifier',
    data_type STRING COMMENT 'Type of health data',
    
    -- Time Dimensions
    aggregation_date DATE COMMENT 'Date of aggregation',
    window_start TIMESTAMP COMMENT 'Window start timestamp',
    window_end TIMESTAMP COMMENT 'Window end timestamp',
    
    -- Statistical Aggregates
    min_value DOUBLE COMMENT 'Minimum value in the window',
    max_value DOUBLE COMMENT 'Maximum value in the window',
    avg_value DOUBLE COMMENT 'Average value',
    sum_value DOUBLE COMMENT 'Sum of all values',
    count BIGINT COMMENT 'Number of measurements',
    stddev_value DOUBLE COMMENT 'Standard deviation',
    
    -- Additional Statistics
    first_value DOUBLE COMMENT 'First value in the window',
    last_value DOUBLE COMMENT 'Last value in the window',
    record_count BIGINT COMMENT 'Total number of records processed',
    
    -- Metadata
    updated_at TIMESTAMP COMMENT 'Last update timestamp',
    
    PRIMARY KEY (user_id, data_type, aggregation_date) NOT ENFORCED
)
USING iceberg
PARTITIONED BY (aggregation_date, data_type)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.upsert.enabled' = 'true',
    'write.merge.mode' = 'merge-on-read',
    'format-version' = '2'
);
```

### Weekly Aggregates Table: health_data_weekly_agg

```sql
CREATE TABLE health_catalog.health_db.health_data_weekly_agg (
    -- Grouping Keys
    user_id STRING COMMENT 'User identifier',
    data_type STRING COMMENT 'Type of health data',
    
    -- Time Dimensions
    year INT COMMENT 'Year',
    week_of_year INT COMMENT 'ISO week number',
    week_start_date DATE COMMENT 'Monday of the week',
    week_end_date DATE COMMENT 'Sunday of the week',
    window_start TIMESTAMP COMMENT 'Window start timestamp',
    window_end TIMESTAMP COMMENT 'Window end timestamp',
    
    -- Statistical Aggregates
    min_value DOUBLE COMMENT 'Minimum value in the week',
    max_value DOUBLE COMMENT 'Maximum value in the week',
    avg_value DOUBLE COMMENT 'Average value',
    sum_value DOUBLE COMMENT 'Sum of all values',
    count BIGINT COMMENT 'Number of measurements',
    stddev_value DOUBLE COMMENT 'Standard deviation',
    daily_avg_of_avg DOUBLE COMMENT 'Average of daily averages',
    
    -- Metadata
    record_count BIGINT COMMENT 'Total number of records processed',
    updated_at TIMESTAMP COMMENT 'Last update timestamp',
    
    PRIMARY KEY (user_id, data_type, year, week_of_year) NOT ENFORCED
)
USING iceberg
PARTITIONED BY (year, week_of_year, data_type)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.upsert.enabled' = 'true',
    'write.merge.mode' = 'merge-on-read',
    'format-version' = '2'
);
```

### Monthly Aggregates Table: health_data_monthly_agg

```sql
CREATE TABLE health_catalog.health_db.health_data_monthly_agg (
    -- Grouping Keys
    user_id STRING COMMENT 'User identifier',
    data_type STRING COMMENT 'Type of health data',
    
    -- Time Dimensions
    year INT COMMENT 'Year',
    month INT COMMENT 'Month (1-12)',
    month_start_date DATE COMMENT 'First day of the month',
    month_end_date DATE COMMENT 'Last day of the month',
    window_start TIMESTAMP COMMENT 'Window start timestamp',
    window_end TIMESTAMP COMMENT 'Window end timestamp',
    
    -- Statistical Aggregates
    min_value DOUBLE COMMENT 'Minimum value in the month',
    max_value DOUBLE COMMENT 'Maximum value in the month',
    avg_value DOUBLE COMMENT 'Average value',
    sum_value DOUBLE COMMENT 'Sum of all values',
    count BIGINT COMMENT 'Number of measurements',
    stddev_value DOUBLE COMMENT 'Standard deviation',
    daily_avg_of_avg DOUBLE COMMENT 'Average of daily averages',
    
    -- Metadata
    record_count BIGINT COMMENT 'Total number of records processed',
    updated_at TIMESTAMP COMMENT 'Last update timestamp',
    
    PRIMARY KEY (user_id, data_type, year, month) NOT ENFORCED
)
USING iceberg
PARTITIONED BY (year, month, data_type)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.upsert.enabled' = 'true',
    'write.merge.mode' = 'merge-on-read',
    'format-version' = '2'
);
```

### Error/DLQ Table: health_data_errors

```sql
CREATE TABLE health_catalog.health_db.health_data_errors (
    -- Original Record
    raw_payload STRING COMMENT 'Original raw payload',
    
    -- Error Information
    error_type STRING COMMENT 'Type of error (validation, deserialization, etc.)',
    error_message STRING COMMENT 'Error message',
    error_timestamp TIMESTAMP COMMENT 'When the error occurred',
    
    -- Context
    user_id STRING COMMENT 'User ID if available',
    data_type STRING COMMENT 'Data type if available',
    sample_id STRING COMMENT 'Sample ID if available',
    
    -- Metadata
    processing_time TIMESTAMP COMMENT 'Flink processing timestamp',
    kafka_offset BIGINT COMMENT 'Kafka offset of the failed message',
    kafka_partition INT COMMENT 'Kafka partition',
    kafka_topic STRING COMMENT 'Kafka topic',
    
    -- Partitioning
    error_date DATE COMMENT 'Date partition'
)
USING iceberg
PARTITIONED BY (days(error_timestamp), error_type)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'format-version' = '2'
);
```

## Partition Strategies

### Raw Data Table Partitioning

**Strategy:** Two-level partitioning by date and data type

```
Partition Spec:
  - days(start_date) → Daily partitions
  - data_type → Sub-partitions by health data type
```

**Benefits:**
- Efficient time-range queries
- Data type-specific queries avoid full table scans
- Balanced partition sizes

**Example Partition Layout:**
```
s3://data-lake/warehouse/health_db/health_data_raw/
├── start_date_day=2025-11-01/
│   ├── data_type=heartRate/
│   │   ├── 00000-0-data.parquet
│   │   └── 00001-0-data.parquet
│   ├── data_type=steps/
│   │   └── 00000-0-data.parquet
│   └── data_type=bloodPressure/
│       └── 00000-0-data.parquet
├── start_date_day=2025-11-02/
│   └── ...
```

**Query Optimization:**
```sql
-- Efficient: Uses partition pruning
SELECT * FROM health_data_raw
WHERE start_date >= '2025-11-01'
  AND start_date < '2025-11-02'
  AND data_type = 'heartRate';

-- Less efficient: Full table scan
SELECT * FROM health_data_raw
WHERE user_id = 'user-123';
```

### Daily Aggregates Partitioning

**Strategy:** Two-level partitioning by aggregation date and data type

```
Partition Spec:
  - aggregation_date → Daily partitions
  - data_type → Sub-partitions by health data type
```

**Benefits:**
- Fast lookups for specific dates
- Efficient upsert operations for late data
- Compact partition sizes

### Weekly Aggregates Partitioning

**Strategy:** Three-level partitioning by year, week, and data type

```
Partition Spec:
  - year → Year partitions
  - week_of_year → Week partitions within year
  - data_type → Sub-partitions by health data type
```

**Benefits:**
- Natural alignment with ISO week calendar
- Efficient year-over-year comparisons
- Manageable partition count

### Monthly Aggregates Partitioning

**Strategy:** Three-level partitioning by year, month, and data type

```
Partition Spec:
  - year → Year partitions
  - month → Month partitions within year
  - data_type → Sub-partitions by health data type
```

**Benefits:**
- Natural alignment with calendar months
- Efficient monthly reporting queries
- Long-term trend analysis

### Partition Evolution

Iceberg supports partition evolution without rewriting data:

```sql
-- Add new partition field
ALTER TABLE health_data_raw
ADD PARTITION FIELD bucket(16, user_id);

-- Drop partition field
ALTER TABLE health_data_raw
DROP PARTITION FIELD data_type;

-- Replace partition spec
ALTER TABLE health_data_raw
REPLACE PARTITION FIELD days(start_date) WITH hours(start_date);
```

## Metrics and Monitoring

### Built-in Flink Metrics

#### Job Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `flink_jobmanager_job_uptime` | Gauge | Job uptime in milliseconds |
| `flink_jobmanager_job_numRestarts` | Counter | Number of job restarts |
| `flink_jobmanager_job_lastCheckpointDuration` | Gauge | Duration of last checkpoint (ms) |
| `flink_jobmanager_job_lastCheckpointSize` | Gauge | Size of last checkpoint (bytes) |
| `flink_jobmanager_job_numberOfCompletedCheckpoints` | Counter | Number of completed checkpoints |
| `flink_jobmanager_job_numberOfFailedCheckpoints` | Counter | Number of failed checkpoints |
| `flink_jobmanager_job_totalNumberOfCheckpoints` | Counter | Total number of checkpoints |

#### Task Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `flink_taskmanager_job_task_numRecordsIn` | Counter | Records received by task |
| `flink_taskmanager_job_task_numRecordsOut` | Counter | Records emitted by task |
| `flink_taskmanager_job_task_numRecordsInPerSecond` | Meter | Input rate (records/second) |
| `flink_taskmanager_job_task_numRecordsOutPerSecond` | Meter | Output rate (records/second) |
| `flink_taskmanager_job_task_numBytesIn` | Counter | Bytes received by task |
| `flink_taskmanager_job_task_numBytesOut` | Counter | Bytes emitted by task |
| `flink_taskmanager_job_task_currentInputWatermark` | Gauge | Current input watermark |
| `flink_taskmanager_job_task_currentOutputWatermark` | Gauge | Current output watermark |

#### Resource Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `flink_taskmanager_Status_JVM_Memory_Heap_Used` | Gauge | JVM heap memory used (bytes) |
| `flink_taskmanager_Status_JVM_Memory_Heap_Max` | Gauge | JVM heap memory max (bytes) |
| `flink_taskmanager_Status_JVM_Memory_NonHeap_Used` | Gauge | JVM non-heap memory used (bytes) |
| `flink_taskmanager_Status_JVM_CPU_Load` | Gauge | JVM CPU load (0.0-1.0) |
| `flink_taskmanager_Status_JVM_Threads_Count` | Gauge | Number of JVM threads |
| `flink_taskmanager_Status_Network_TotalMemorySegments` | Gauge | Total network memory segments |
| `flink_taskmanager_Status_Network_AvailableMemorySegments` | Gauge | Available network memory segments |

### Custom Application Metrics

#### Data Quality Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `valid_records` | Counter | Number of valid records processed | `data_type` |
| `invalid_records` | Counter | Number of validation failures | `data_type`, `error_type` |
| `validation_errors_total` | Counter | Total validation errors | `error_type` |
| `deserialization_errors` | Counter | Avro deserialization errors | - |
| `schema_evolution_events` | Counter | Schema evolution events detected | `schema_version` |

#### Processing Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `processing_latency` | Histogram | End-to-end processing latency (ms) | `data_type` |
| `transformation_duration` | Histogram | Transformation duration (ms) | `operation` |
| `kafka_to_flink_latency` | Histogram | Kafka to Flink ingestion latency (ms) | - |
| `flink_to_iceberg_latency` | Histogram | Flink to Iceberg write latency (ms) | - |

#### Aggregation Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `windows_processed` | Counter | Number of windows processed | `window_type` |
| `records_aggregated` | Counter | Number of records aggregated | `window_type`, `data_type` |
| `window_latency_ms` | Histogram | Window processing latency (ms) | `window_type` |
| `late_records` | Counter | Number of late records | `window_type` |
| `aggregation_errors` | Counter | Aggregation calculation errors | `window_type`, `error_type` |

#### Iceberg Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `iceberg_writes_total` | Counter | Total Iceberg write operations | `table_name` |
| `iceberg_write_duration` | Histogram | Iceberg write duration (ms) | `table_name` |
| `iceberg_commit_duration` | Histogram | Iceberg commit duration (ms) | `table_name` |
| `iceberg_files_written` | Counter | Number of Parquet files written | `table_name` |
| `iceberg_bytes_written` | Counter | Bytes written to Iceberg | `table_name` |
| `iceberg_commit_failures` | Counter | Iceberg commit failures | `table_name`, `error_type` |

#### Kafka Consumer Metrics

| Metric Name | Type | Description | Labels |
|-------------|------|-------------|--------|
| `kafka_consumer_lag` | Gauge | Consumer lag per partition | `partition` |
| `kafka_consumer_offset` | Gauge | Current consumer offset | `partition` |
| `kafka_consumer_committed_offset` | Gauge | Last committed offset | `partition` |
| `kafka_fetch_latency` | Histogram | Kafka fetch latency (ms) | - |
| `kafka_connection_errors` | Counter | Kafka connection errors | `error_type` |

### Metric Collection Configuration

#### Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'flink-jobmanager'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - data-platform
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_component]
        action: keep
        regex: jobmanager
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
    metrics_path: /metrics
    scrape_interval: 30s
    scrape_timeout: 10s

  - job_name: 'flink-taskmanager'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - data-platform
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_component]
        action: keep
        regex: taskmanager
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
    metrics_path: /metrics
    scrape_interval: 30s
    scrape_timeout: 10s
```

### Metric Queries

#### PromQL Examples

**Throughput:**
```promql
# Records per second (input)
rate(flink_taskmanager_job_task_numRecordsIn[5m])

# Records per second (output)
rate(flink_taskmanager_job_task_numRecordsOut[5m])

# Total throughput across all tasks
sum(rate(flink_taskmanager_job_task_numRecordsIn[5m]))
```

**Latency:**
```promql
# Processing latency p50
histogram_quantile(0.5, rate(processing_latency_bucket[5m]))

# Processing latency p95
histogram_quantile(0.95, rate(processing_latency_bucket[5m]))

# Processing latency p99
histogram_quantile(0.99, rate(processing_latency_bucket[5m]))
```

**Error Rate:**
```promql
# Validation error rate
rate(invalid_records[5m])

# Error percentage
rate(invalid_records[5m]) / rate(valid_records[5m]) * 100

# Errors by type
sum by (error_type) (rate(validation_errors_total[5m]))
```

**Kafka Lag:**
```promql
# Total consumer lag
sum(kafka_consumer_lag)

# Lag per partition
kafka_consumer_lag

# Lag growth rate
rate(kafka_consumer_lag[5m])
```

**Checkpoint Health:**
```promql
# Checkpoint duration
flink_jobmanager_job_lastCheckpointDuration

# Checkpoint failure rate
rate(flink_jobmanager_job_numberOfFailedCheckpoints[10m])

# Checkpoint size growth
rate(flink_jobmanager_job_lastCheckpointSize[1h])
```

**Resource Usage:**
```promql
# JVM heap usage percentage
flink_taskmanager_Status_JVM_Memory_Heap_Used / flink_taskmanager_Status_JVM_Memory_Heap_Max * 100

# CPU usage
flink_taskmanager_Status_JVM_CPU_Load * 100

# Network buffer usage
(flink_taskmanager_Status_Network_TotalMemorySegments - flink_taskmanager_Status_Network_AvailableMemorySegments) / flink_taskmanager_Status_Network_TotalMemorySegments * 100
```

**Aggregation Metrics:**
```promql
# Windows processed per minute
rate(windows_processed[1m]) * 60

# Records per window
rate(records_aggregated[5m]) / rate(windows_processed[5m])

# Late data percentage
rate(late_records[5m]) / rate(records_aggregated[5m]) * 100
```

## Alert Rules

### Critical Alerts

#### Job Down
```yaml
- alert: FlinkJobDown
  expr: flink_jobmanager_job_uptime == 0
  for: 5m
  labels:
    severity: critical
    component: flink
  annotations:
    summary: "Flink job is down"
    description: "Flink job {{ $labels.job_name }} has been down for more than 5 minutes"
    runbook_url: "https://runbook.health-stack.io/flink-job-down"
```

#### Checkpoint Failures
```yaml
- alert: CheckpointFailures
  expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[10m]) > 3
  for: 5m
  labels:
    severity: critical
    component: flink
  annotations:
    summary: "Multiple checkpoint failures"
    description: "{{ $value }} checkpoint failures in the last 10 minutes"
    runbook_url: "https://runbook.health-stack.io/checkpoint-failures"
```

#### High Memory Usage
```yaml
- alert: HighMemoryUsage
  expr: flink_taskmanager_Status_JVM_Memory_Heap_Used / flink_taskmanager_Status_JVM_Memory_Heap_Max > 0.9
  for: 5m
  labels:
    severity: critical
    component: flink
  annotations:
    summary: "High JVM heap memory usage"
    description: "JVM heap usage is {{ $value | humanizePercentage }} on {{ $labels.pod }}"
    runbook_url: "https://runbook.health-stack.io/high-memory-usage"
```

### Warning Alerts

#### High Checkpoint Duration
```yaml
- alert: HighCheckpointDuration
  expr: flink_jobmanager_job_lastCheckpointDuration > 300000
  for: 5m
  labels:
    severity: warning
    component: flink
  annotations:
    summary: "Checkpoint taking too long"
    description: "Checkpoint duration is {{ $value }}ms (> 5 minutes)"
    runbook_url: "https://runbook.health-stack.io/high-checkpoint-duration"
```

#### High Kafka Lag
```yaml
- alert: HighKafkaLag
  expr: sum(kafka_consumer_lag) > 100000
  for: 10m
  labels:
    severity: warning
    component: kafka
  annotations:
    summary: "High Kafka consumer lag"
    description: "Consumer lag is {{ $value }} messages (> 100k)"
    runbook_url: "https://runbook.health-stack.io/high-kafka-lag"
```

#### High Error Rate
```yaml
- alert: HighErrorRate
  expr: rate(invalid_records[5m]) > 10
  for: 5m
  labels:
    severity: warning
    component: validation
  annotations:
    summary: "High validation error rate"
    description: "Error rate is {{ $value }} errors/second (> 10)"
    runbook_url: "https://runbook.health-stack.io/high-error-rate"
```

#### Low Throughput
```yaml
- alert: LowThroughput
  expr: rate(flink_taskmanager_job_task_numRecordsIn[5m]) < 1000
  for: 10m
  labels:
    severity: warning
    component: flink
  annotations:
    summary: "Low throughput detected"
    description: "Throughput is {{ $value }} records/second (< 1000)"
    runbook_url: "https://runbook.health-stack.io/low-throughput"
```

#### Backpressure Detected
```yaml
- alert: BackpressureDetected
  expr: flink_taskmanager_job_task_backPressuredTimeMsPerSecond > 100
  for: 5m
  labels:
    severity: warning
    component: flink
  annotations:
    summary: "Backpressure detected"
    description: "Task {{ $labels.task_name }} is experiencing backpressure"
    runbook_url: "https://runbook.health-stack.io/backpressure"
```

### Info Alerts

#### Job Restart
```yaml
- alert: JobRestart
  expr: increase(flink_jobmanager_job_numRestarts[1h]) > 0
  labels:
    severity: info
    component: flink
  annotations:
    summary: "Job restarted"
    description: "Job has restarted {{ $value }} times in the last hour"
```

#### Schema Evolution
```yaml
- alert: SchemaEvolution
  expr: increase(schema_evolution_events[1h]) > 0
  labels:
    severity: info
    component: schema
  annotations:
    summary: "Schema evolution detected"
    description: "Schema version changed {{ $value }} times in the last hour"
```

### Alert Routing

```yaml
route:
  group_by: ['alertname', 'cluster', 'service']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty-critical'
      continue: true
    - match:
        severity: warning
      receiver: 'slack-warnings'
    - match:
        severity: info
      receiver: 'slack-info'

receivers:
  - name: 'default'
    slack_configs:
      - api_url: '<SLACK_WEBHOOK_URL>'
        channel: '#data-platform-alerts'
  
  - name: 'pagerduty-critical'
    pagerduty_configs:
      - service_key: '<PAGERDUTY_SERVICE_KEY>'
  
  - name: 'slack-warnings'
    slack_configs:
      - api_url: '<SLACK_WEBHOOK_URL>'
        channel: '#data-platform-warnings'
  
  - name: 'slack-info'
    slack_configs:
      - api_url: '<SLACK_WEBHOOK_URL>'
        channel: '#data-platform-info'
```

## Configuration Best Practices

### Production Recommendations

1. **Checkpointing:**
   - Use RocksDB state backend for large state
   - Enable incremental checkpoints
   - Set checkpoint interval to 60-120 seconds
   - Configure externalized checkpoint retention

2. **Resource Allocation:**
   - JobManager: 2-4 GB memory, 1-2 CPU cores
   - TaskManager: 4-8 GB memory, 2-4 CPU cores
   - Allocate 40% of memory to managed memory (RocksDB)

3. **Parallelism:**
   - Match source parallelism to Kafka partition count
   - Set transform parallelism to 2× source parallelism
   - Use lower parallelism for aggregations

4. **Monitoring:**
   - Enable Prometheus metrics
   - Set up comprehensive alerting
   - Monitor checkpoint duration and size
   - Track Kafka consumer lag

5. **High Availability:**
   - Deploy multiple TaskManagers
   - Use ZooKeeper for HA (optional)
   - Configure restart strategy
   - Enable externalized checkpoints

6. **Security:**
   - Use Kubernetes secrets for credentials
   - Enable SSL for Kafka connections
   - Implement RBAC for Kubernetes
   - Encrypt checkpoint storage

## Appendix

### Configuration Templates

Complete configuration templates are available in the repository:

- `k8s/flinkdeployment.yaml` - Kubernetes FlinkDeployment
- `k8s/configmap.yaml` - Flink configuration
- `k8s/secrets.yaml` - Credentials and secrets
- `.env.example` - Environment variables template
- `config/flink-conf.yaml` - Flink cluster configuration

### Related Documentation

- [OPERATIONS_GUIDE.md](OPERATIONS_GUIDE.md) - Operations and maintenance
- [DEPLOYMENT.md](DEPLOYMENT.md) - Deployment procedures
- [MONITORING_METRICS.md](MONITORING_METRICS.md) - Detailed monitoring guide
- [AGGREGATION_PIPELINE.md](AGGREGATION_PIPELINE.md) - Aggregation pipeline details
- [ICEBERG_SETUP.md](ICEBERG_SETUP.md) - Iceberg setup instructions
