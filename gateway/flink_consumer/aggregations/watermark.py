"""
Watermark strategies for event-time processing in aggregation pipeline.

This module provides watermark configuration to handle out-of-order events
and enable time-based windowing operations.
"""

from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class TimestampExtractor:
    """
    Extract timestamp from health data records for event-time processing.
    
    Uses the start_date field as the event timestamp.
    """
    
    @staticmethod
    def extract_timestamp(event: Dict[str, Any]) -> int:
        """
        Extract event timestamp from health data record.
        
        Args:
            event: Health data record dictionary
            
        Returns:
            Unix timestamp in milliseconds
            
        Raises:
            ValueError: If start_date field is missing or invalid
        """
        if 'start_date' not in event:
            logger.error(f"Missing start_date field in event: {event.get('sample_id', 'unknown')}")
            raise ValueError("start_date field is required for timestamp extraction")
        
        timestamp = event['start_date']
        
        if not isinstance(timestamp, int) or timestamp <= 0:
            logger.error(f"Invalid timestamp value: {timestamp}")
            raise ValueError(f"Invalid timestamp: {timestamp}")
        
        return timestamp


def create_watermark_strategy(
    max_out_of_orderness_minutes: int = 10,
    timestamp_field: str = 'start_date',
    idle_timeout_minutes: int = 5
):
    """
    Create watermark strategy for handling out-of-order events.
    
    This function creates a PyFlink WatermarkStrategy that can be directly
    applied to a DataStream.
    
    Args:
        max_out_of_orderness_minutes: Maximum allowed lateness in minutes (default: 10)
        timestamp_field: Field name to extract timestamp from (default: 'start_date')
        idle_timeout_minutes: Mark source as idle after this many minutes (default: 5)
        
    Returns:
        PyFlink WatermarkStrategy instance
        
    Example:
        >>> watermark_strategy = create_watermark_strategy(10)
        >>> stream_with_watermarks = data_stream.assign_timestamps_and_watermarks(watermark_strategy)
    """
    try:
        from pyflink.common import WatermarkStrategy, Duration
    except ImportError:
        logger.error("PyFlink is not available. Cannot create watermark strategy.")
        raise ImportError("PyFlink is required for watermark strategy")
    
    logger.info(
        f"Creating watermark strategy: {max_out_of_orderness_minutes}min out-of-orderness, "
        f"{idle_timeout_minutes}min idle timeout, timestamp field: {timestamp_field}"
    )
    
    # Create watermark strategy
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_minutes(max_out_of_orderness_minutes)) \
        .with_timestamp_assigner(lambda event, timestamp: event.get(timestamp_field, 0)) \
        .with_idleness(Duration.of_minutes(idle_timeout_minutes))
    
    return watermark_strategy


def assign_watermarks_to_stream(data_stream, watermark_config: Dict[str, Any]):
    """
    Assign watermarks to a data stream for event-time processing.
    
    This is a helper function that applies watermark strategy to a PyFlink DataStream.
    Note: This function requires PyFlink to be available at runtime.
    
    Args:
        data_stream: PyFlink DataStream
        watermark_config: Watermark configuration from create_watermark_strategy()
        
    Returns:
        DataStream with watermarks assigned
        
    Example:
        >>> from pyflink.datastream import StreamExecutionEnvironment
        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> stream = env.from_collection([...])
        >>> config = create_watermark_strategy(10)
        >>> stream_with_watermarks = assign_watermarks_to_stream(stream, config)
    """
    try:
        from pyflink.common import WatermarkStrategy, Duration
    except ImportError:
        logger.error("PyFlink is not available. Cannot assign watermarks.")
        raise ImportError("PyFlink is required for watermark assignment")
    
    out_of_orderness = watermark_config['out_of_orderness_minutes']
    timestamp_assigner = watermark_config['timestamp_assigner']
    idle_timeout = watermark_config.get('idle_timeout_minutes', 5)
    
    logger.info(f"Assigning watermarks with {out_of_orderness}min out-of-orderness, "
                f"{idle_timeout}min idle timeout")
    
    # Create watermark strategy
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_minutes(out_of_orderness)) \
        .with_timestamp_assigner(lambda event, timestamp: timestamp_assigner(event)) \
        .with_idleness(Duration.of_minutes(idle_timeout))
    
    # Apply watermark strategy to stream
    return data_stream.assign_timestamps_and_watermarks(watermark_strategy)


class WatermarkMetrics:
    """
    Track watermark-related metrics for monitoring.
    """
    
    def __init__(self):
        self.late_events_count = 0
        self.on_time_events_count = 0
        self.max_lateness_ms = 0
    
    def record_event(self, event_time: int, watermark: int):
        """
        Record an event and update metrics.
        
        Args:
            event_time: Event timestamp in milliseconds
            watermark: Current watermark in milliseconds
        """
        if event_time < watermark:
            self.late_events_count += 1
            lateness = watermark - event_time
            self.max_lateness_ms = max(self.max_lateness_ms, lateness)
            logger.warning(f"Late event detected: {lateness}ms behind watermark")
        else:
            self.on_time_events_count += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current watermark metrics.
        
        Returns:
            Dictionary with metric values
        """
        total_events = self.late_events_count + self.on_time_events_count
        late_event_rate = (self.late_events_count / total_events * 100) if total_events > 0 else 0
        
        return {
            'late_events_count': self.late_events_count,
            'on_time_events_count': self.on_time_events_count,
            'total_events': total_events,
            'late_event_rate_percent': late_event_rate,
            'max_lateness_ms': self.max_lateness_ms,
        }
