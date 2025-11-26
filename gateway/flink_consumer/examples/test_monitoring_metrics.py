"""
Example script demonstrating monitoring and metrics usage.

This script shows how to:
1. Configure structured logging
2. Use custom metrics reporters
3. Generate Prometheus configuration
4. Use context-aware logging
"""

import time
from typing import Dict, Any

# Configure logging before other imports
import os
os.environ["LOG_LEVEL"] = "INFO"
os.environ["JSON_FORMAT"] = "true"

from flink_consumer.config import (
    setup_logging,
    get_logger,
    log_validation_error,
    log_kafka_error,
    log_iceberg_error,
    log_checkpoint_event,
    PrometheusConfig,
    generate_prometheus_scrape_config,
    generate_alerting_rules,
)


def example_structured_logging():
    """Demonstrate structured logging with context."""
    print("\n=== Structured Logging Example ===\n")
    
    # Setup logging
    setup_logging(log_level="INFO", json_format=True)
    
    # Get context logger
    logger = get_logger(__name__, context={
        "component": "example",
        "job_id": "test-job-001"
    })
    
    # Basic logging
    logger.info("Application started")
    logger.debug("Debug information", extra={"context": {"detail": "value"}})
    logger.warning("Warning message")
    
    # Log with additional context
    logger.info(
        "Processing record",
        extra={
            "context": {
                "user_id": "user-123",
                "data_type": "heartRate",
                "value": 72.0
            }
        }
    )
    
    print("\n✓ Structured logging examples completed\n")


def example_validation_logging():
    """Demonstrate validation error logging."""
    print("\n=== Validation Error Logging Example ===\n")
    
    logger = get_logger(__name__)
    
    # Example health data row
    invalid_row = {
        "sample_id": "sample-123",
        "user_id": "user-456",
        "data_type": "heartRate",
        "value": -10.0,  # Invalid negative value
    }
    
    # Log validation error
    log_validation_error(logger.logger, "Negative value", invalid_row)
    
    print("\n✓ Validation error logging completed\n")


def example_kafka_logging():
    """Demonstrate Kafka error logging."""
    print("\n=== Kafka Error Logging Example ===\n")
    
    logger = get_logger(__name__)
    
    # Simulate Kafka error
    try:
        raise ConnectionError("Failed to connect to Kafka broker")
    except ConnectionError as e:
        log_kafka_error(
            logger.logger,
            "Kafka connection failed",
            topic="health-data-raw",
            partition=0,
            offset=12345,
            error=e
        )
    
    print("\n✓ Kafka error logging completed\n")


def example_iceberg_logging():
    """Demonstrate Iceberg error logging."""
    print("\n=== Iceberg Error Logging Example ===\n")
    
    logger = get_logger(__name__)
    
    # Simulate Iceberg error
    try:
        raise IOError("Failed to write to Iceberg table")
    except IOError as e:
        log_iceberg_error(
            logger.logger,
            "Iceberg write failed",
            table_name="health_data_raw",
            database="health_db",
            error=e
        )
    
    print("\n✓ Iceberg error logging completed\n")


def example_checkpoint_logging():
    """Demonstrate checkpoint event logging."""
    print("\n=== Checkpoint Event Logging Example ===\n")
    
    logger = get_logger(__name__)
    
    # Log checkpoint events
    log_checkpoint_event(
        logger.logger,
        event_type="started",
        checkpoint_id=42
    )
    
    time.sleep(0.1)  # Simulate checkpoint duration
    
    log_checkpoint_event(
        logger.logger,
        event_type="completed",
        checkpoint_id=42,
        duration_ms=5000,
        size_bytes=1024000
    )
    
    print("\n✓ Checkpoint event logging completed\n")


def example_prometheus_config():
    """Demonstrate Prometheus configuration generation."""
    print("\n=== Prometheus Configuration Example ===\n")
    
    # Create Prometheus config
    config = PrometheusConfig(
        enabled=True,
        port=9249,
        host="0.0.0.0",
        interval_seconds=10
    )
    
    print("Flink Configuration Properties:")
    for key, value in config.to_flink_config().items():
        print(f"  {key}: {value}")
    
    print("\n✓ Prometheus configuration completed\n")


def example_scrape_config():
    """Demonstrate Prometheus scrape configuration generation."""
    print("\n=== Prometheus Scrape Configuration Example ===\n")
    
    scrape_config = generate_prometheus_scrape_config(
        job_name="flink-health-consumer",
        targets=["localhost:9249", "taskmanager-1:9249", "taskmanager-2:9249"],
        scrape_interval="15s"
    )
    
    print("Prometheus Scrape Configuration:")
    print(scrape_config)
    
    print("\n✓ Scrape configuration generated\n")


def example_alerting_rules():
    """Demonstrate alerting rules generation."""
    print("\n=== Alerting Rules Example ===\n")
    
    alert_rules = generate_alerting_rules()
    
    print("Prometheus Alerting Rules (first 500 chars):")
    print(alert_rules[:500] + "...")
    
    print("\n✓ Alerting rules generated\n")


def example_metrics_usage():
    """Demonstrate custom metrics usage (conceptual)."""
    print("\n=== Custom Metrics Usage Example ===\n")
    
    print("Custom metrics can be used in Flink pipelines:")
    print("""
    from flink_consumer.services import MetricsReporter
    
    # Create metrics reporter
    metrics_reporter = MetricsReporter(metric_prefix="health_data")
    
    # Apply to data stream
    stream_with_metrics = data_stream.map(metrics_reporter)
    
    # Metrics will be automatically reported to Prometheus
    """)
    
    print("\n✓ Metrics usage example completed\n")


def simulate_processing_with_metrics():
    """Simulate data processing with metrics and logging."""
    print("\n=== Simulated Processing with Metrics ===\n")
    
    logger = get_logger(__name__, context={"component": "processor"})
    
    # Simulate processing records
    records = [
        {"user_id": "user-1", "data_type": "heartRate", "value": 72.0},
        {"user_id": "user-2", "data_type": "steps", "value": 5000.0},
        {"user_id": "user-3", "data_type": "heartRate", "value": -10.0},  # Invalid
    ]
    
    valid_count = 0
    invalid_count = 0
    
    for i, record in enumerate(records):
        logger.info(
            f"Processing record {i+1}/{len(records)}",
            extra={"context": record}
        )
        
        # Validate
        if record["value"] < 0:
            log_validation_error(logger.logger, "Negative value", record)
            invalid_count += 1
        else:
            valid_count += 1
            logger.debug(
                "Record validated successfully",
                extra={"context": {"record_id": i+1}}
            )
    
    # Summary
    logger.info(
        "Processing completed",
        extra={
            "context": {
                "total_records": len(records),
                "valid_records": valid_count,
                "invalid_records": invalid_count
            }
        }
    )
    
    print(f"\nProcessed {len(records)} records:")
    print(f"  Valid: {valid_count}")
    print(f"  Invalid: {invalid_count}")
    
    print("\n✓ Simulated processing completed\n")


def main():
    """Run all examples."""
    print("=" * 70)
    print("Monitoring and Metrics Examples")
    print("=" * 70)
    
    # Run examples
    example_structured_logging()
    example_validation_logging()
    example_kafka_logging()
    example_iceberg_logging()
    example_checkpoint_logging()
    example_prometheus_config()
    example_scrape_config()
    example_alerting_rules()
    example_metrics_usage()
    simulate_processing_with_metrics()
    
    print("=" * 70)
    print("All examples completed successfully!")
    print("=" * 70)
    
    print("\nNext Steps:")
    print("1. Review the generated logs in JSON format")
    print("2. Configure Prometheus to scrape metrics from port 9249")
    print("3. Import Grafana dashboard for visualization")
    print("4. Set up alerting rules in Prometheus")
    print("5. Integrate metrics reporters into your Flink pipeline")


if __name__ == "__main__":
    main()
