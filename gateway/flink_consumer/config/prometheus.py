"""
Prometheus metrics configuration and helpers.

This module provides utilities for configuring and managing Prometheus metrics
in the Flink application.
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PrometheusConfig:
    """
    Configuration for Prometheus metrics reporter.
    
    Attributes:
        enabled: Whether Prometheus reporter is enabled
        port: Port to expose metrics endpoint
        host: Host to bind metrics endpoint
        interval_seconds: Metric reporting interval in seconds
    """
    enabled: bool = True
    port: int = 9249
    host: str = "0.0.0.0"
    interval_seconds: int = 10
    
    def to_flink_config(self) -> Dict[str, str]:
        """
        Convert to Flink configuration properties.
        
        Returns:
            Dictionary of Flink configuration properties
        """
        if not self.enabled:
            return {}
        
        return {
            "metrics.reporters": "prom",
            "metrics.reporter.prom.class": "org.apache.flink.metrics.prometheus.PrometheusReporter",
            "metrics.reporter.prom.port": str(self.port),
            "metrics.reporter.prom.host": self.host,
            "metrics.reporter.prom.interval": f"{self.interval_seconds}s",
        }


class MetricLabels:
    """
    Standard metric labels for health data processing.
    
    Provides consistent labeling across all metrics.
    """
    
    # Job labels
    JOB_NAME = "job_name"
    JOB_ID = "job_id"
    
    # Operator labels
    OPERATOR_NAME = "operator_name"
    SUBTASK_INDEX = "subtask_index"
    
    # Data labels
    DATA_TYPE = "data_type"
    USER_ID = "user_id"
    
    # Source labels
    KAFKA_TOPIC = "kafka_topic"
    KAFKA_PARTITION = "kafka_partition"
    
    # Sink labels
    ICEBERG_TABLE = "iceberg_table"
    ICEBERG_DATABASE = "iceberg_database"
    
    # Error labels
    ERROR_TYPE = "error_type"
    ERROR_REASON = "error_reason"
    
    @classmethod
    def get_standard_labels(cls) -> List[str]:
        """
        Get list of standard label names.
        
        Returns:
            List of standard label names
        """
        return [
            cls.JOB_NAME,
            cls.OPERATOR_NAME,
            cls.DATA_TYPE,
            cls.KAFKA_TOPIC,
            cls.ICEBERG_TABLE,
        ]


class MetricNames:
    """
    Standard metric names for health data processing.
    
    Provides consistent naming across all metrics.
    """
    
    # Throughput metrics
    RECORDS_IN = "records_in_total"
    RECORDS_OUT = "records_out_total"
    RECORDS_VALID = "records_valid_total"
    RECORDS_INVALID = "records_invalid_total"
    
    # Latency metrics
    PROCESSING_LATENCY = "processing_latency_ms"
    KAFKA_CONSUMPTION_LATENCY = "kafka_consumption_latency_ms"
    ICEBERG_WRITE_LATENCY = "iceberg_write_latency_ms"
    
    # Kafka metrics
    KAFKA_RECORDS_CONSUMED = "kafka_records_consumed_total"
    KAFKA_DESERIALIZATION_ERRORS = "kafka_deserialization_errors_total"
    KAFKA_LAG = "kafka_consumer_lag"
    
    # Iceberg metrics
    ICEBERG_RECORDS_WRITTEN = "iceberg_records_written_total"
    ICEBERG_WRITE_FAILURES = "iceberg_write_failures_total"
    ICEBERG_BATCH_SIZE = "iceberg_batch_size"
    
    # Validation metrics
    VALIDATION_SUCCESS = "validation_success_total"
    VALIDATION_FAILURES = "validation_failures_total"
    
    # Checkpoint metrics
    CHECKPOINT_DURATION = "checkpoint_duration_ms"
    CHECKPOINT_SIZE = "checkpoint_size_bytes"
    CHECKPOINT_FAILURES = "checkpoint_failures_total"
    
    # System metrics
    CPU_USAGE = "cpu_usage_percent"
    MEMORY_USAGE = "memory_usage_bytes"
    NETWORK_IO = "network_io_bytes"
    
    @classmethod
    def get_all_metrics(cls) -> List[str]:
        """
        Get list of all metric names.
        
        Returns:
            List of all metric names
        """
        return [
            cls.RECORDS_IN,
            cls.RECORDS_OUT,
            cls.RECORDS_VALID,
            cls.RECORDS_INVALID,
            cls.PROCESSING_LATENCY,
            cls.KAFKA_CONSUMPTION_LATENCY,
            cls.ICEBERG_WRITE_LATENCY,
            cls.KAFKA_RECORDS_CONSUMED,
            cls.KAFKA_DESERIALIZATION_ERRORS,
            cls.ICEBERG_RECORDS_WRITTEN,
            cls.ICEBERG_WRITE_FAILURES,
            cls.VALIDATION_SUCCESS,
            cls.VALIDATION_FAILURES,
        ]


def generate_prometheus_scrape_config(
    job_name: str = "flink-health-consumer",
    targets: Optional[List[str]] = None,
    scrape_interval: str = "15s",
) -> str:
    """
    Generate Prometheus scrape configuration for Flink metrics.
    
    Args:
        job_name: Prometheus job name
        targets: List of target endpoints (default: ["localhost:9249"])
        scrape_interval: Scrape interval
        
    Returns:
        YAML configuration string
    """
    if targets is None:
        targets = ["localhost:9249"]
    
    targets_str = "\n".join([f"      - '{target}'" for target in targets])
    
    config = f"""
# Prometheus scrape configuration for Flink Health Data Consumer
scrape_configs:
  - job_name: '{job_name}'
    scrape_interval: {scrape_interval}
    metrics_path: /metrics
    static_configs:
      - targets:
{targets_str}
        labels:
          application: 'flink-health-consumer'
          environment: 'production'
"""
    
    return config


def generate_alerting_rules() -> str:
    """
    Generate Prometheus alerting rules for health data processing.
    
    Returns:
        YAML alerting rules configuration
    """
    rules = """
# Prometheus alerting rules for Flink Health Data Consumer
groups:
  - name: flink_health_alerts
    interval: 30s
    rules:
      # High checkpoint duration
      - alert: HighCheckpointDuration
        expr: flink_jobmanager_job_lastCheckpointDuration > 300000
        for: 5m
        labels:
          severity: warning
          component: checkpoint
        annotations:
          summary: "Checkpoint taking too long"
          description: "Checkpoint duration is {{ $value }}ms, exceeding 5 minutes"
      
      # High Kafka consumer lag
      - alert: HighKafkaLag
        expr: flink_consumer_lag > 100000
        for: 10m
        labels:
          severity: warning
          component: kafka
        annotations:
          summary: "High Kafka consumer lag"
          description: "Consumer lag is {{ $value }} messages"
      
      # High error rate
      - alert: HighErrorRate
        expr: rate(flink_taskmanager_job_task_numRecordsFailed[5m]) > 10
        for: 5m
        labels:
          severity: critical
          component: processing
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} errors/second"
      
      # Checkpoint failures
      - alert: CheckpointFailures
        expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[10m]) > 3
        for: 5m
        labels:
          severity: critical
          component: checkpoint
        annotations:
          summary: "Multiple checkpoint failures"
          description: "{{ $value }} checkpoints failed in the last 10 minutes"
      
      # Low throughput
      - alert: LowThroughput
        expr: rate(health_data_valid_records[5m]) < 100
        for: 10m
        labels:
          severity: warning
          component: processing
        annotations:
          summary: "Low processing throughput"
          description: "Processing only {{ $value }} records/second"
      
      # High validation failure rate
      - alert: HighValidationFailureRate
        expr: rate(validation_failures_total[5m]) / rate(health_data_throughput[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
          component: validation
        annotations:
          summary: "High validation failure rate"
          description: "{{ $value | humanizePercentage }} of records failing validation"
      
      # Iceberg write failures
      - alert: IcebergWriteFailures
        expr: increase(iceberg_write_failures_total[5m]) > 5
        for: 5m
        labels:
          severity: critical
          component: iceberg
        annotations:
          summary: "Iceberg write failures detected"
          description: "{{ $value }} write failures in the last 5 minutes"
      
      # High processing latency
      - alert: HighProcessingLatency
        expr: histogram_quantile(0.95, rate(processing_latency_ms_bucket[5m])) > 10000
        for: 10m
        labels:
          severity: warning
          component: processing
        annotations:
          summary: "High processing latency"
          description: "P95 latency is {{ $value }}ms"
      
      # TaskManager down
      - alert: TaskManagerDown
        expr: up{job="flink-health-consumer"} == 0
        for: 2m
        labels:
          severity: critical
          component: infrastructure
        annotations:
          summary: "TaskManager is down"
          description: "TaskManager {{ $labels.instance }} is not responding"
      
      # High memory usage
      - alert: HighMemoryUsage
        expr: flink_taskmanager_Status_JVM_Memory_Heap_Used / flink_taskmanager_Status_JVM_Memory_Heap_Max > 0.9
        for: 5m
        labels:
          severity: warning
          component: resources
        annotations:
          summary: "High memory usage"
          description: "Memory usage is {{ $value | humanizePercentage }} on {{ $labels.tm_id }}"
"""
    
    return rules


def get_grafana_dashboard_json() -> Dict:
    """
    Generate Grafana dashboard JSON for health data metrics.
    
    Returns:
        Dictionary representing Grafana dashboard JSON
    """
    dashboard = {
        "dashboard": {
            "title": "Flink Health Data Consumer",
            "tags": ["flink", "health-data", "streaming"],
            "timezone": "browser",
            "panels": [
                {
                    "title": "Throughput",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "rate(health_data_valid_records[1m])",
                            "legendFormat": "Valid Records/s"
                        },
                        {
                            "expr": "rate(health_data_invalid_records[1m])",
                            "legendFormat": "Invalid Records/s"
                        }
                    ]
                },
                {
                    "title": "Processing Latency",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "histogram_quantile(0.50, rate(processing_latency_ms_bucket[5m]))",
                            "legendFormat": "P50"
                        },
                        {
                            "expr": "histogram_quantile(0.95, rate(processing_latency_ms_bucket[5m]))",
                            "legendFormat": "P95"
                        },
                        {
                            "expr": "histogram_quantile(0.99, rate(processing_latency_ms_bucket[5m]))",
                            "legendFormat": "P99"
                        }
                    ]
                },
                {
                    "title": "Kafka Consumer Lag",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "flink_consumer_lag",
                            "legendFormat": "Lag (messages)"
                        }
                    ]
                },
                {
                    "title": "Checkpoint Duration",
                    "type": "graph",
                    "targets": [
                        {
                            "expr": "flink_jobmanager_job_lastCheckpointDuration",
                            "legendFormat": "Duration (ms)"
                        }
                    ]
                }
            ]
        }
    }
    
    return dashboard


if __name__ == "__main__":
    # Example usage
    config = PrometheusConfig(port=9249)
    print("Flink Configuration:")
    print(config.to_flink_config())
    
    print("\nPrometheus Scrape Config:")
    print(generate_prometheus_scrape_config())
    
    print("\nAlerting Rules:")
    print(generate_alerting_rules())
