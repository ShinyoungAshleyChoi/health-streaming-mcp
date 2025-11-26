"""
Custom metrics implementation for Flink health data processing.

This module provides custom metrics reporters for monitoring data quality,
processing latency, and pipeline health.
"""

import time
import logging
from typing import Dict, Any, Optional
from pyflink.datastream import MapFunction, RuntimeContext

logger = logging.getLogger(__name__)


class MetricsReporter(MapFunction):
    """
    Custom metrics reporter for health data processing.
    
    Tracks:
    - Valid/invalid record counts
    - Processing latency distribution
    - Data type specific metrics
    - Throughput metrics
    """
    
    def __init__(self, metric_prefix: str = "health_data"):
        """
        Initialize metrics reporter.
        
        Args:
            metric_prefix: Prefix for all metric names
        """
        self.metric_prefix = metric_prefix
        self.valid_records = None
        self.invalid_records = None
        self.processing_latency = None
        self.records_by_type = {}
        self.throughput_counter = None
        
    def open(self, runtime_context: RuntimeContext):
        """
        Initialize metrics when operator starts.
        
        Args:
            runtime_context: Flink runtime context for accessing metrics
        """
        metric_group = runtime_context.get_metric_group()
        
        # Counter metrics for valid/invalid records
        self.valid_records = metric_group.counter(
            f"{self.metric_prefix}.valid_records"
        )
        self.invalid_records = metric_group.counter(
            f"{self.metric_prefix}.invalid_records"
        )
        
        # Throughput counter
        self.throughput_counter = metric_group.counter(
            f"{self.metric_prefix}.throughput"
        )
        
        # Distribution metric for processing latency
        self.processing_latency = metric_group.distribution(
            f"{self.metric_prefix}.processing_latency_ms"
        )
        
        logger.info(f"Metrics reporter initialized with prefix: {self.metric_prefix}")
    
    def map(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process row and update metrics.
        
        Args:
            row: Health data row dictionary
            
        Returns:
            Unchanged row (pass-through)
        """
        try:
            # Increment valid records counter
            if self.valid_records:
                self.valid_records.inc()
            
            # Increment throughput counter
            if self.throughput_counter:
                self.throughput_counter.inc()
            
            # Calculate and record processing latency
            if self.processing_latency and 'payload_timestamp' in row:
                current_time = int(time.time() * 1000)
                payload_time = row.get('payload_timestamp', current_time)
                latency = current_time - payload_time
                
                # Only record positive latencies (avoid clock skew issues)
                if latency >= 0:
                    self.processing_latency.update(latency)
            
            # Track metrics by data type
            data_type = row.get('data_type', 'unknown')
            if data_type not in self.records_by_type:
                # Lazily create counter for this data type
                metric_group = self.get_runtime_context().get_metric_group()
                self.records_by_type[data_type] = metric_group.counter(
                    f"{self.metric_prefix}.records_by_type.{data_type}"
                )
            
            if data_type in self.records_by_type:
                self.records_by_type[data_type].inc()
            
        except Exception as e:
            logger.error(f"Error updating metrics: {e}", exc_info=True)
        
        return row


class ValidationMetricsReporter(MapFunction):
    """
    Metrics reporter specifically for data validation.
    
    Tracks validation failures by reason and data type.
    """
    
    def __init__(self):
        """Initialize validation metrics reporter."""
        self.validation_failures = {}
        self.validation_success = None
        
    def open(self, runtime_context: RuntimeContext):
        """
        Initialize validation metrics.
        
        Args:
            runtime_context: Flink runtime context
        """
        metric_group = runtime_context.get_metric_group()
        
        # Success counter
        self.validation_success = metric_group.counter(
            "validation.success"
        )
        
        logger.info("Validation metrics reporter initialized")
    
    def map(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process validated row and update metrics.
        
        Args:
            row: Validated health data row
            
        Returns:
            Unchanged row
        """
        try:
            # Increment success counter
            if self.validation_success:
                self.validation_success.inc()
                
        except Exception as e:
            logger.error(f"Error updating validation metrics: {e}", exc_info=True)
        
        return row
    
    def report_failure(self, reason: str, data_type: str):
        """
        Report a validation failure.
        
        Args:
            reason: Reason for validation failure
            data_type: Type of health data that failed validation
        """
        try:
            metric_key = f"{reason}.{data_type}"
            
            if metric_key not in self.validation_failures:
                metric_group = self.get_runtime_context().get_metric_group()
                self.validation_failures[metric_key] = metric_group.counter(
                    f"validation.failures.{metric_key}"
                )
            
            if metric_key in self.validation_failures:
                self.validation_failures[metric_key].inc()
                
        except Exception as e:
            logger.error(f"Error reporting validation failure: {e}", exc_info=True)


class IcebergSinkMetrics(MapFunction):
    """
    Metrics reporter for Iceberg sink operations.
    
    Tracks write operations, batch sizes, and write latencies.
    """
    
    def __init__(self):
        """Initialize Iceberg sink metrics reporter."""
        self.records_written = None
        self.write_latency = None
        self.batch_size = None
        self.write_failures = None
        
    def open(self, runtime_context: RuntimeContext):
        """
        Initialize Iceberg sink metrics.
        
        Args:
            runtime_context: Flink runtime context
        """
        metric_group = runtime_context.get_metric_group()
        
        # Counter for records written
        self.records_written = metric_group.counter(
            "iceberg.records_written"
        )
        
        # Counter for write failures
        self.write_failures = metric_group.counter(
            "iceberg.write_failures"
        )
        
        # Distribution for write latency
        self.write_latency = metric_group.distribution(
            "iceberg.write_latency_ms"
        )
        
        # Distribution for batch size
        self.batch_size = metric_group.distribution(
            "iceberg.batch_size"
        )
        
        logger.info("Iceberg sink metrics reporter initialized")
    
    def map(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process row before writing to Iceberg.
        
        Args:
            row: Health data row to be written
            
        Returns:
            Unchanged row
        """
        try:
            if self.records_written:
                self.records_written.inc()
                
        except Exception as e:
            logger.error(f"Error updating Iceberg metrics: {e}", exc_info=True)
        
        return row
    
    def report_write_success(self, batch_size: int, latency_ms: int):
        """
        Report successful write operation.
        
        Args:
            batch_size: Number of records in the batch
            latency_ms: Write latency in milliseconds
        """
        try:
            if self.batch_size:
                self.batch_size.update(batch_size)
            
            if self.write_latency:
                self.write_latency.update(latency_ms)
                
        except Exception as e:
            logger.error(f"Error reporting write success: {e}", exc_info=True)
    
    def report_write_failure(self):
        """Report a write failure."""
        try:
            if self.write_failures:
                self.write_failures.inc()
                
        except Exception as e:
            logger.error(f"Error reporting write failure: {e}", exc_info=True)


class KafkaSourceMetrics(MapFunction):
    """
    Metrics reporter for Kafka source operations.
    
    Tracks consumption rate, lag, and deserialization metrics.
    """
    
    def __init__(self):
        """Initialize Kafka source metrics reporter."""
        self.records_consumed = None
        self.deserialization_errors = None
        self.consumption_latency = None
        
    def open(self, runtime_context: RuntimeContext):
        """
        Initialize Kafka source metrics.
        
        Args:
            runtime_context: Flink runtime context
        """
        metric_group = runtime_context.get_metric_group()
        
        # Counter for records consumed
        self.records_consumed = metric_group.counter(
            "kafka.records_consumed"
        )
        
        # Counter for deserialization errors
        self.deserialization_errors = metric_group.counter(
            "kafka.deserialization_errors"
        )
        
        # Distribution for consumption latency
        self.consumption_latency = metric_group.distribution(
            "kafka.consumption_latency_ms"
        )
        
        logger.info("Kafka source metrics reporter initialized")
    
    def map(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process consumed record and update metrics.
        
        Args:
            row: Consumed health data row
            
        Returns:
            Unchanged row
        """
        try:
            if self.records_consumed:
                self.records_consumed.inc()
            
            # Calculate consumption latency (time from Kafka timestamp to now)
            if self.consumption_latency and 'payload_timestamp' in row:
                current_time = int(time.time() * 1000)
                kafka_time = row.get('payload_timestamp', current_time)
                latency = current_time - kafka_time
                
                if latency >= 0:
                    self.consumption_latency.update(latency)
                    
        except Exception as e:
            logger.error(f"Error updating Kafka metrics: {e}", exc_info=True)
        
        return row
    
    def report_deserialization_error(self):
        """Report a deserialization error."""
        try:
            if self.deserialization_errors:
                self.deserialization_errors.inc()
                
        except Exception as e:
            logger.error(f"Error reporting deserialization error: {e}", exc_info=True)
