# Monitoring and Metrics Guide

This guide covers the monitoring and metrics implementation for the Flink Health Data Consumer, including custom metrics, Prometheus integration, and structured logging.

## Table of Contents

1. [Overview](#overview)
2. [Custom Metrics](#custom-metrics)
3. [Prometheus Integration](#prometheus-integration)
4. [Structured Logging](#structured-logging)
5. [Alerting Rules](#alerting-rules)
6. [Grafana Dashboards](#grafana-dashboards)
7. [Troubleshooting](#troubleshooting)

## Overview

The monitoring system provides comprehensive observability into the health data processing pipeline:

- **Custom Metrics**: Application-specific metrics for data quality and processing
- **Prometheus Integration**: Industry-standard metrics collection and storage
- **Structured Logging**: JSON-formatted logs with rich context
- **Alerting**: Pre-configured alerts for common issues
- **Dashboards**: Grafana dashboards for visualization

## Custom Metrics

### MetricsReporter

The `MetricsReporter` class tracks general processing metrics:

```python
from flink_consumer.services import MetricsReporter

# Create metrics reporter
metrics_reporter = MetricsReporter(metric_prefix="health_data")

# Apply to data stream
stream_with_metrics = data_stream.map(metrics_reporter)
```

**Tracked Metrics:**
- `health_data.valid_records`: Counter for valid records
- `health_data.invalid_records`: Counter for invalid records
- `health_data.processing_latency_ms`: Distribution of processing latency
- `health_data.throughput`: Overall throughput counter
- `health_data.records_by_type.{data_type}`: Per-data-type counters

### ValidationMetricsReporter

Tracks validation-specific metrics:

```python
from flink_consumer.services import ValidationMetricsReporter

validation_metrics = ValidationMetricsReporter()
validated_stream = validated_stream.map(validation_metrics)
```

**Tracked Metrics:**
- `validation.success`: Successful validations
- `validation.failures.{reason}.{data_type}`: Failures by reason and type

### IcebergSinkMetrics

Monitors Iceberg write operations:

```python
from flink_consumer.services import IcebergSinkMetrics

iceberg_metrics = IcebergSinkMetrics()
stream_to_sink = stream.map(iceberg_metrics)
```

**Tracked Metrics:**
- `iceberg.records_written`: Records written to Iceberg
- `iceberg.write_failures`: Write operation failures
- `iceberg.write_latency_ms`: Write latency distribution
- `iceberg.batch_size`: Batch size distribution

### KafkaSourceMetrics

Monitors Kafka consumption:

```python
from flink_consumer.services import KafkaSourceMetrics

kafka_metrics = KafkaSourceMetrics()
consumed_stream = kafka_stream.map(kafka_metrics)
```

**Tracked Metrics:**
- `kafka.records_consumed`: Records consumed from Kafka
- `kafka.deserialization_errors`: Deserialization failures
- `kafka.consumption_latency_ms`: Consumption latency

## Prometheus Integration

### Configuration

Prometheus metrics are configured in `flink-conf.yaml`:

```yaml
# Metrics reporters
metrics.reporters: prom

# Prometheus reporter configuration
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9249
metrics.reporter.prom.host: 0.0.0.0
metrics.reporter.prom.interval: 10s
```

### Accessing Metrics

Metrics are exposed at: `http://<flink-host>:9249/metrics`

### Prometheus Scrape Configuration

Generate scrape configuration:

```python
from flink_consumer.config import generate_prometheus_scrape_config

scrape_config = generate_prometheus_scrape_config(
    job_name="flink-health-consumer",
    targets=["localhost:9249"],
    scrape_interval="15s"
)

print(scrape_config)
```

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'flink-health-consumer'
    scrape_interval: 15s
    metrics_path: /metrics
    static_configs:
      - targets:
          - 'localhost:9249'
        labels:
          application: 'flink-health-consumer'
          environment: 'production'
```

### Key Metrics

**Throughput Metrics:**
- `flink_taskmanager_job_task_numRecordsIn`: Records received
- `flink_taskmanager_job_task_numRecordsOut`: Records emitted
- `health_data_valid_records_total`: Valid records processed
- `health_data_invalid_records_total`: Invalid records

**Latency Metrics:**
- `processing_latency_ms`: End-to-end processing latency
- `kafka_consumption_latency_ms`: Kafka to Flink latency
- `iceberg_write_latency_ms`: Iceberg write latency

**Checkpoint Metrics:**
- `flink_jobmanager_job_lastCheckpointDuration`: Last checkpoint duration
- `flink_jobmanager_job_lastCheckpointSize`: Last checkpoint size
- `flink_jobmanager_job_numberOfFailedCheckpoints`: Failed checkpoints

**Resource Metrics:**
- `flink_taskmanager_Status_JVM_Memory_Heap_Used`: Heap memory used
- `flink_taskmanager_Status_JVM_Memory_Heap_Max`: Max heap memory
- `flink_taskmanager_Status_JVM_CPU_Load`: CPU load

## Structured Logging

### Setup

Configure logging at application startup:

```python
from flink_consumer.config import setup_logging

# Setup with JSON formatting
logger = setup_logging(
    log_level="INFO",
    json_format=True,
    log_file="/var/log/flink-consumer/app.log"
)
```

### Environment Variables

Control logging via environment variables:

```bash
# Set log level
export LOG_LEVEL=DEBUG

# Disable JSON formatting
export JSON_FORMAT=false

# Set log file
export LOG_FILE=/var/log/flink-consumer/app.log
```

### Context Logger

Use context logger for enriched logging:

```python
from flink_consumer.config import get_logger

logger = get_logger(__name__, context={
    "component": "kafka_source",
    "job_id": "health-consumer-001"
})

logger.info("Processing started")
# Output: {"timestamp": "2025-11-16T10:00:00Z", "level": "INFO", 
#          "message": "Processing started", "context": {"component": "kafka_source", ...}}
```

### Specialized Logging Functions

**Validation Errors:**
```python
from flink_consumer.config import log_validation_error

log_validation_error(logger, "Missing user_id", row)
```

**Kafka Errors:**
```python
from flink_consumer.config import log_kafka_error

log_kafka_error(
    logger,
    "Failed to consume message",
    topic="health-data-raw",
    partition=0,
    offset=12345,
    error=exception
)
```

**Iceberg Errors:**
```python
from flink_consumer.config import log_iceberg_error

log_iceberg_error(
    logger,
    "Failed to write to table",
    table_name="health_data_raw",
    database="health_db",
    error=exception
)
```

**Checkpoint Events:**
```python
from flink_consumer.config import log_checkpoint_event

log_checkpoint_event(
    logger,
    event_type="completed",
    checkpoint_id=42,
    duration_ms=5000,
    size_bytes=1024000
)
```

### Log Format

JSON log format includes:

```json
{
  "timestamp": "2025-11-16T10:00:00.123Z",
  "level": "INFO",
  "logger": "flink_consumer.services.kafka_source",
  "message": "Consumed message from Kafka",
  "thread": {
    "id": 12345,
    "name": "main"
  },
  "source": {
    "file": "/app/flink_consumer/services/kafka_source.py",
    "line": 42,
    "function": "consume_message"
  },
  "process": {
    "id": 1234,
    "name": "MainProcess"
  },
  "context": {
    "kafka_topic": "health-data-raw",
    "kafka_partition": 0,
    "kafka_offset": 12345,
    "user_id": "user-123",
    "data_type": "heartRate"
  }
}
```

## Alerting Rules

### Generate Alert Rules

```python
from flink_consumer.config import generate_alerting_rules

alert_rules = generate_alerting_rules()
print(alert_rules)
```

### Pre-configured Alerts

**High Checkpoint Duration:**
- Triggers when checkpoint takes > 5 minutes
- Severity: Warning
- Action: Investigate state size and I/O performance

**High Kafka Lag:**
- Triggers when consumer lag > 100,000 messages
- Severity: Warning
- Action: Scale up TaskManagers or increase parallelism

**High Error Rate:**
- Triggers when error rate > 10 errors/second
- Severity: Critical
- Action: Check logs for error patterns

**Checkpoint Failures:**
- Triggers when > 3 checkpoints fail in 10 minutes
- Severity: Critical
- Action: Check state backend and storage

**Low Throughput:**
- Triggers when throughput < 100 records/second
- Severity: Warning
- Action: Check upstream data source

**High Validation Failure Rate:**
- Triggers when > 10% of records fail validation
- Severity: Warning
- Action: Investigate data quality issues

**Iceberg Write Failures:**
- Triggers when > 5 write failures in 5 minutes
- Severity: Critical
- Action: Check Iceberg catalog and storage

**High Processing Latency:**
- Triggers when P95 latency > 10 seconds
- Severity: Warning
- Action: Optimize processing logic or scale up

**TaskManager Down:**
- Triggers when TaskManager is unreachable
- Severity: Critical
- Action: Restart TaskManager or check infrastructure

**High Memory Usage:**
- Triggers when heap usage > 90%
- Severity: Warning
- Action: Increase TaskManager memory or optimize state

### Alert Configuration

Add to `prometheus/alerts.yml`:

```yaml
groups:
  - name: flink_health_alerts
    interval: 30s
    rules:
      - alert: HighCheckpointDuration
        expr: flink_jobmanager_job_lastCheckpointDuration > 300000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Checkpoint taking too long"
          description: "Checkpoint duration is {{ $value }}ms"
```

## Grafana Dashboards

### Generate Dashboard

```python
from flink_consumer.config import get_grafana_dashboard_json
import json

dashboard = get_grafana_dashboard_json()
print(json.dumps(dashboard, indent=2))
```

### Dashboard Panels

**Throughput Panel:**
- Valid records/second
- Invalid records/second
- Total throughput

**Latency Panel:**
- P50, P95, P99 processing latency
- Kafka consumption latency
- Iceberg write latency

**Kafka Consumer Lag:**
- Current lag by partition
- Lag trend over time

**Checkpoint Health:**
- Checkpoint duration
- Checkpoint size
- Checkpoint failures

**Resource Usage:**
- CPU usage by TaskManager
- Memory usage by TaskManager
- Network I/O

**Data Quality:**
- Validation success rate
- Validation failures by reason
- Records by data type

### Import Dashboard

1. Open Grafana UI
2. Navigate to Dashboards → Import
3. Paste the generated JSON
4. Select Prometheus data source
5. Click Import

## Troubleshooting

### Metrics Not Appearing

**Check Prometheus Reporter:**
```bash
# Verify metrics endpoint is accessible
curl http://localhost:9249/metrics
```

**Check Flink Configuration:**
```bash
# Verify flink-conf.yaml has Prometheus reporter configured
grep "metrics.reporter.prom" flink-conf.yaml
```

**Check Flink Logs:**
```bash
# Look for metrics reporter initialization
grep "PrometheusReporter" /tmp/flink-logs/*.log
```

### High Latency

**Check Processing Latency:**
```promql
histogram_quantile(0.95, rate(processing_latency_ms_bucket[5m]))
```

**Check Kafka Lag:**
```promql
flink_consumer_lag
```

**Check Checkpoint Duration:**
```promql
flink_jobmanager_job_lastCheckpointDuration
```

### Logs Not Structured

**Verify JSON Formatter:**
```python
import logging
logger = logging.getLogger()
print(logger.handlers[0].formatter.__class__.__name__)
# Should output: JSONFormatter
```

**Check Environment Variable:**
```bash
echo $JSON_FORMAT
# Should be: true or unset
```

### Missing Context in Logs

**Use Context Logger:**
```python
from flink_consumer.config import get_logger

# Instead of:
logger = logging.getLogger(__name__)

# Use:
logger = get_logger(__name__, context={"component": "my_component"})
```

**Add Context to Log Calls:**
```python
logger.info("Message", extra={"context": {"key": "value"}})
```

## Best Practices

1. **Use Appropriate Log Levels:**
   - DEBUG: Detailed diagnostic information
   - INFO: General informational messages
   - WARNING: Warning messages for recoverable issues
   - ERROR: Error messages for failures
   - CRITICAL: Critical issues requiring immediate attention

2. **Add Context to Logs:**
   - Always include relevant context (user_id, data_type, etc.)
   - Use structured logging for better searchability

3. **Monitor Key Metrics:**
   - Throughput (records/second)
   - Latency (P95, P99)
   - Error rate
   - Checkpoint health
   - Resource usage

4. **Set Up Alerts:**
   - Configure alerts for critical issues
   - Use appropriate thresholds
   - Test alerts regularly

5. **Regular Review:**
   - Review metrics dashboards daily
   - Investigate anomalies promptly
   - Adjust thresholds as needed

## References

- [Apache Flink Metrics](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Structured Logging Best Practices](https://www.structlog.org/)
