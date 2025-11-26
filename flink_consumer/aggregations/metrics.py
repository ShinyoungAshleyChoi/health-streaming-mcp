"""
Monitoring metrics for aggregation pipeline.

This module provides custom metrics for tracking aggregation pipeline
performance and data quality.
"""

from typing import Dict, Any, Iterable, Tuple, Optional
import time
import logging

from flink_consumer.aggregations.windows import WindowType

logger = logging.getLogger(__name__)


class AggregationMetrics:
    """
    Custom metrics reporter for aggregation windows.
    
    This class implements PyFlink's ProcessWindowFunction interface to
    report metrics about window processing.
    """
    
    def __init__(self, window_type: WindowType):
        """
        Initialize metrics reporter.
        
        Args:
            window_type: Type of window (DAILY, WEEKLY, or MONTHLY)
        """
        self.window_type = window_type
        self.runtime_context = None
        
        # Metric references (initialized in open())
        self.windows_processed_counter = None
        self.records_aggregated_counter = None
        self.window_latency_distribution = None
        self.window_size_distribution = None
    
    def open(self, runtime_context):
        """
        Initialize metrics when operator starts.
        
        This method is called by Flink when the operator is initialized.
        
        Args:
            runtime_context: Flink runtime context
        """
        self.runtime_context = runtime_context
        
        # Get metric group
        metric_group = runtime_context.get_metric_group()
        
        # Add window type label
        metric_group = metric_group.add_group("window_type", self.window_type.value)
        
        # Create counter metrics
        self.windows_processed_counter = metric_group.counter("windows_processed")
        self.records_aggregated_counter = metric_group.counter("records_aggregated")
        
        # Create distribution metrics
        self.window_latency_distribution = metric_group.distribution("window_latency_ms")
        self.window_size_distribution = metric_group.distribution("window_size_records")
        
        logger.info(f"Initialized metrics for {self.window_type.value} aggregations")
    
    def process(self, key: Tuple[str, str], context, elements: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """
        Process window elements and report metrics.
        
        Args:
            key: Tuple of (user_id, data_type)
            context: Window context
            elements: Aggregated statistics from the window
            
        Yields:
            Elements with metrics recorded
        """
        user_id, data_type = key
        window = context.window()
        
        for element in elements:
            if element is None:
                continue
            
            # Increment windows processed counter
            if self.windows_processed_counter:
                self.windows_processed_counter.inc()
            
            # Increment records aggregated counter
            record_count = element.get('record_count', 0)
            if self.records_aggregated_counter and record_count > 0:
                self.records_aggregated_counter.inc(record_count)
            
            # Calculate and record window latency
            window_end = window.end
            current_time = int(time.time() * 1000)
            latency_ms = current_time - window_end
            
            if self.window_latency_distribution:
                self.window_latency_distribution.update(latency_ms)
            
            # Record window size
            if self.window_size_distribution:
                self.window_size_distribution.update(record_count)
            
            # Add metrics to element for logging
            element['_metrics'] = {
                'window_latency_ms': latency_ms,
                'window_size': record_count,
                'window_type': self.window_type.value,
            }
            
            logger.debug(f"Window metrics: type={self.window_type.value}, "
                        f"user={user_id}, data_type={data_type}, "
                        f"records={record_count}, latency={latency_ms}ms")
            
            yield element


class MetricsCollector:
    """
    Collect and aggregate metrics from multiple sources.
    """
    
    def __init__(self):
        self.metrics = {
            'daily': WindowMetrics(WindowType.DAILY),
            'weekly': WindowMetrics(WindowType.WEEKLY),
            'monthly': WindowMetrics(WindowType.MONTHLY),
        }
    
    def record_window(self, window_type: WindowType, record_count: int, latency_ms: int):
        """
        Record window processing metrics.
        
        Args:
            window_type: Type of window
            record_count: Number of records in window
            latency_ms: Processing latency in milliseconds
        """
        key = window_type.value
        if key in self.metrics:
            self.metrics[key].record_window(record_count, latency_ms)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of all metrics.
        
        Returns:
            Dictionary with metrics summary
        """
        summary = {}
        for window_type, metrics in self.metrics.items():
            summary[window_type] = metrics.get_summary()
        
        # Calculate totals
        summary['total'] = {
            'windows_processed': sum(m.windows_processed for m in self.metrics.values()),
            'records_aggregated': sum(m.total_records for m in self.metrics.values()),
        }
        
        return summary


class WindowMetrics:
    """
    Track metrics for a specific window type.
    """
    
    def __init__(self, window_type: WindowType):
        self.window_type = window_type
        self.windows_processed = 0
        self.total_records = 0
        self.total_latency_ms = 0
        self.min_latency_ms = float('inf')
        self.max_latency_ms = 0
        self.latencies = []
    
    def record_window(self, record_count: int, latency_ms: int):
        """
        Record window processing.
        
        Args:
            record_count: Number of records in window
            latency_ms: Processing latency in milliseconds
        """
        self.windows_processed += 1
        self.total_records += record_count
        self.total_latency_ms += latency_ms
        self.min_latency_ms = min(self.min_latency_ms, latency_ms)
        self.max_latency_ms = max(self.max_latency_ms, latency_ms)
        self.latencies.append(latency_ms)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get metrics summary.
        
        Returns:
            Dictionary with metric values
        """
        avg_latency = (self.total_latency_ms / self.windows_processed 
                      if self.windows_processed > 0 else 0)
        
        avg_records = (self.total_records / self.windows_processed
                      if self.windows_processed > 0 else 0)
        
        # Calculate percentiles
        p50, p95, p99 = self._calculate_percentiles()
        
        return {
            'window_type': self.window_type.value,
            'windows_processed': self.windows_processed,
            'total_records': self.total_records,
            'avg_records_per_window': avg_records,
            'latency_ms': {
                'min': self.min_latency_ms if self.min_latency_ms != float('inf') else 0,
                'max': self.max_latency_ms,
                'avg': avg_latency,
                'p50': p50,
                'p95': p95,
                'p99': p99,
            }
        }
    
    def _calculate_percentiles(self) -> Tuple[float, float, float]:
        """
        Calculate latency percentiles.
        
        Returns:
            Tuple of (p50, p95, p99)
        """
        if not self.latencies:
            return 0.0, 0.0, 0.0
        
        sorted_latencies = sorted(self.latencies)
        n = len(sorted_latencies)
        
        p50_idx = int(n * 0.50)
        p95_idx = int(n * 0.95)
        p99_idx = int(n * 0.99)
        
        p50 = sorted_latencies[min(p50_idx, n - 1)]
        p95 = sorted_latencies[min(p95_idx, n - 1)]
        p99 = sorted_latencies[min(p99_idx, n - 1)]
        
        return p50, p95, p99


class PrometheusMetricsExporter:
    """
    Export aggregation metrics in Prometheus format.
    """
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
    
    def export_metrics(self) -> str:
        """
        Export metrics in Prometheus text format.
        
        Returns:
            Prometheus-formatted metrics string
        """
        summary = self.metrics_collector.get_summary()
        lines = []
        
        # Windows processed
        lines.append("# HELP flink_aggregation_windows_processed_total Total number of windows processed")
        lines.append("# TYPE flink_aggregation_windows_processed_total counter")
        for window_type, metrics in summary.items():
            if window_type == 'total':
                continue
            count = metrics['windows_processed']
            lines.append(f'flink_aggregation_windows_processed_total{{window_type="{window_type}"}} {count}')
        
        # Records aggregated
        lines.append("# HELP flink_aggregation_records_total Total number of records aggregated")
        lines.append("# TYPE flink_aggregation_records_total counter")
        for window_type, metrics in summary.items():
            if window_type == 'total':
                continue
            count = metrics['total_records']
            lines.append(f'flink_aggregation_records_total{{window_type="{window_type}"}} {count}')
        
        # Window latency
        lines.append("# HELP flink_aggregation_window_latency_ms Window processing latency in milliseconds")
        lines.append("# TYPE flink_aggregation_window_latency_ms summary")
        for window_type, metrics in summary.items():
            if window_type == 'total':
                continue
            latency = metrics['latency_ms']
            lines.append(f'flink_aggregation_window_latency_ms{{window_type="{window_type}",quantile="0.5"}} {latency["p50"]}')
            lines.append(f'flink_aggregation_window_latency_ms{{window_type="{window_type}",quantile="0.95"}} {latency["p95"]}')
            lines.append(f'flink_aggregation_window_latency_ms{{window_type="{window_type}",quantile="0.99"}} {latency["p99"]}')
        
        return '\n'.join(lines)


class MetricsLogger:
    """
    Log metrics periodically for monitoring.
    """
    
    def __init__(self, metrics_collector: MetricsCollector, log_interval_seconds: int = 60):
        self.metrics_collector = metrics_collector
        self.log_interval_seconds = log_interval_seconds
        self.last_log_time = time.time()
    
    def maybe_log_metrics(self):
        """
        Log metrics if interval has elapsed.
        """
        current_time = time.time()
        if current_time - self.last_log_time >= self.log_interval_seconds:
            self._log_metrics()
            self.last_log_time = current_time
    
    def _log_metrics(self):
        """
        Log current metrics.
        """
        summary = self.metrics_collector.get_summary()
        
        logger.info("=== Aggregation Pipeline Metrics ===")
        
        for window_type, metrics in summary.items():
            if window_type == 'total':
                continue
            
            logger.info(f"{window_type.upper()} Aggregations:")
            logger.info(f"  Windows processed: {metrics['windows_processed']}")
            logger.info(f"  Records aggregated: {metrics['total_records']}")
            logger.info(f"  Avg records/window: {metrics['avg_records_per_window']:.1f}")
            
            latency = metrics['latency_ms']
            logger.info(f"  Latency (ms): min={latency['min']:.1f}, "
                       f"avg={latency['avg']:.1f}, max={latency['max']:.1f}, "
                       f"p95={latency['p95']:.1f}, p99={latency['p99']:.1f}")
        
        total = summary.get('total', {})
        logger.info(f"TOTAL: {total.get('windows_processed', 0)} windows, "
                   f"{total.get('records_aggregated', 0)} records")
        logger.info("=" * 40)


def create_metrics_reporter(window_type: WindowType):
    """
    Create a metrics reporter for a specific window type.
    
    Args:
        window_type: Type of window
        
    Returns:
        AggregationMetrics instance
        
    Example:
        >>> reporter = create_metrics_reporter(WindowType.DAILY)
        >>> # Use with PyFlink window aggregation
        >>> windowed_stream.aggregate(aggregator, reporter)
    """
    return AggregationMetrics(window_type)


def setup_metrics_collection(enable_prometheus: bool = True,
                            enable_logging: bool = True,
                            log_interval_seconds: int = 60) -> MetricsCollector:
    """
    Setup metrics collection infrastructure.
    
    Args:
        enable_prometheus: Enable Prometheus metrics export
        enable_logging: Enable periodic metrics logging
        log_interval_seconds: Interval for logging metrics
        
    Returns:
        MetricsCollector instance
        
    Example:
        >>> collector = setup_metrics_collection()
        >>> # Record metrics
        >>> collector.record_window(WindowType.DAILY, 1000, 500)
        >>> # Get summary
        >>> summary = collector.get_summary()
    """
    collector = MetricsCollector()
    
    if enable_prometheus:
        exporter = PrometheusMetricsExporter(collector)
        logger.info("Prometheus metrics exporter enabled")
    
    if enable_logging:
        metrics_logger = MetricsLogger(collector, log_interval_seconds)
        logger.info(f"Metrics logging enabled (interval: {log_interval_seconds}s)")
    
    return collector
