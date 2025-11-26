"""
Window configurations for time-based aggregations.

This module provides window configurations for daily, weekly, and monthly
aggregations with support for late-arriving data.
"""

from enum import Enum
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class WindowType(Enum):
    """Enumeration of supported window types."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class WindowConfig:
    """
    Configuration for time-based windows.
    
    Defines window size and allowed lateness for different aggregation periods.
    """
    
    # Window configurations
    DAILY = {
        'type': WindowType.DAILY,
        'size_days': 1,
        'allowed_lateness_hours': 1,
        'description': '24-hour tumbling window aligned to midnight UTC',
    }
    
    WEEKLY = {
        'type': WindowType.WEEKLY,
        'size_days': 7,
        'allowed_lateness_hours': 6,
        'description': '7-day tumbling window aligned to Monday 00:00 UTC',
    }
    
    MONTHLY = {
        'type': WindowType.MONTHLY,
        'size_days': 30,
        'allowed_lateness_hours': 12,
        'description': '30-day tumbling window aligned to 1st of month 00:00 UTC',
    }
    
    @classmethod
    def get_config(cls, window_type: WindowType) -> Dict[str, Any]:
        """
        Get window configuration for specified type.
        
        Args:
            window_type: Type of window (DAILY, WEEKLY, or MONTHLY)
            
        Returns:
            Dictionary with window configuration
            
        Raises:
            ValueError: If window_type is not supported
        """
        if window_type == WindowType.DAILY:
            return cls.DAILY
        elif window_type == WindowType.WEEKLY:
            return cls.WEEKLY
        elif window_type == WindowType.MONTHLY:
            return cls.MONTHLY
        else:
            raise ValueError(f"Unsupported window type: {window_type}")


def create_tumbling_window(window_type: WindowType):
    """
    Create a tumbling event-time window configuration.
    
    Tumbling windows are fixed-size, non-overlapping windows that partition
    the data stream into distinct time intervals.
    
    Args:
        window_type: Type of window (DAILY, WEEKLY, or MONTHLY)
        
    Returns:
        Dictionary with window parameters for PyFlink
        
    Example:
        >>> config = create_tumbling_window(WindowType.DAILY)
        >>> # Use with PyFlink:
        >>> # from pyflink.datastream.window import TumblingEventTimeWindows
        >>> # from pyflink.common import Time
        >>> # window = TumblingEventTimeWindows.of(Time.days(config['size_days']))
    """
    config = WindowConfig.get_config(window_type)
    
    logger.info(f"Creating {window_type.value} tumbling window: {config['description']}")
    
    return {
        'window_type': window_type,
        'size_days': config['size_days'],
        'allowed_lateness_hours': config['allowed_lateness_hours'],
        'description': config['description'],
    }


def apply_tumbling_window(keyed_stream, window_type: WindowType):
    """
    Apply tumbling event-time window to a keyed stream.
    
    This function configures and applies a tumbling window with allowed lateness
    to handle late-arriving events.
    
    Args:
        keyed_stream: PyFlink KeyedStream
        window_type: Type of window (DAILY, WEEKLY, or MONTHLY)
        
    Returns:
        WindowedStream ready for aggregation
        
    Example:
        >>> from pyflink.datastream import StreamExecutionEnvironment
        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> stream = env.from_collection([...])
        >>> keyed = stream.key_by(lambda x: (x['user_id'], x['data_type']))
        >>> windowed = apply_tumbling_window(keyed, WindowType.DAILY)
    """
    try:
        from pyflink.datastream.window import TumblingEventTimeWindows
        from pyflink.common import Time
    except ImportError:
        logger.error("PyFlink is not available. Cannot apply window.")
        raise ImportError("PyFlink is required for window operations")
    
    window_config = create_tumbling_window(window_type)
    size_days = window_config['size_days']
    lateness_hours = window_config['allowed_lateness_hours']
    
    logger.info(f"Applying {window_type.value} window: {size_days} days, "
                f"allowed lateness: {lateness_hours} hours")
    
    # Create tumbling window
    windowed_stream = keyed_stream.window(
        TumblingEventTimeWindows.of(Time.days(size_days))
    )
    
    # Set allowed lateness
    windowed_stream = windowed_stream.allowed_lateness(Time.hours(lateness_hours))
    
    return windowed_stream


class WindowAlignmentHelper:
    """
    Helper class for window alignment calculations.
    
    Provides utilities to align windows to specific time boundaries
    (midnight UTC, Monday, first of month).
    """
    
    @staticmethod
    def align_to_midnight_utc(timestamp_ms: int) -> int:
        """
        Align timestamp to midnight UTC.
        
        Args:
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            Timestamp aligned to midnight UTC
        """
        from datetime import datetime, timezone
        
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        aligned = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(aligned.timestamp() * 1000)
    
    @staticmethod
    def align_to_monday(timestamp_ms: int) -> int:
        """
        Align timestamp to Monday 00:00 UTC.
        
        Args:
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            Timestamp aligned to Monday 00:00 UTC
        """
        from datetime import datetime, timezone, timedelta
        
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        # Get to midnight
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        # Go back to Monday (weekday 0)
        days_since_monday = dt.weekday()
        monday = dt - timedelta(days=days_since_monday)
        return int(monday.timestamp() * 1000)
    
    @staticmethod
    def align_to_first_of_month(timestamp_ms: int) -> int:
        """
        Align timestamp to first day of month 00:00 UTC.
        
        Args:
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            Timestamp aligned to first of month 00:00 UTC
        """
        from datetime import datetime, timezone
        
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        first_of_month = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return int(first_of_month.timestamp() * 1000)
    
    @staticmethod
    def get_window_bounds(window_type: WindowType, timestamp_ms: int) -> tuple[int, int]:
        """
        Calculate window start and end bounds for a given timestamp.
        
        Args:
            window_type: Type of window
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            Tuple of (window_start_ms, window_end_ms)
        """
        from datetime import datetime, timezone, timedelta
        
        if window_type == WindowType.DAILY:
            start = WindowAlignmentHelper.align_to_midnight_utc(timestamp_ms)
            dt = datetime.fromtimestamp(start / 1000, tz=timezone.utc)
            end_dt = dt + timedelta(days=1)
            end = int(end_dt.timestamp() * 1000)
            
        elif window_type == WindowType.WEEKLY:
            start = WindowAlignmentHelper.align_to_monday(timestamp_ms)
            dt = datetime.fromtimestamp(start / 1000, tz=timezone.utc)
            end_dt = dt + timedelta(days=7)
            end = int(end_dt.timestamp() * 1000)
            
        elif window_type == WindowType.MONTHLY:
            start = WindowAlignmentHelper.align_to_first_of_month(timestamp_ms)
            dt = datetime.fromtimestamp(start / 1000, tz=timezone.utc)
            # Add 30 days for monthly window
            end_dt = dt + timedelta(days=30)
            end = int(end_dt.timestamp() * 1000)
            
        else:
            raise ValueError(f"Unsupported window type: {window_type}")
        
        return start, end


class WindowMetrics:
    """
    Track window-related metrics for monitoring.
    """
    
    def __init__(self):
        self.windows_created = 0
        self.windows_triggered = 0
        self.late_data_updates = 0
        self.dropped_late_events = 0
    
    def record_window_created(self):
        """Record that a new window was created."""
        self.windows_created += 1
    
    def record_window_triggered(self):
        """Record that a window was triggered for computation."""
        self.windows_triggered += 1
    
    def record_late_data_update(self):
        """Record that a window was updated with late data."""
        self.late_data_updates += 1
    
    def record_dropped_late_event(self):
        """Record that an event was dropped for being too late."""
        self.dropped_late_events += 1
    
    def get_metrics(self) -> Dict[str, int]:
        """
        Get current window metrics.
        
        Returns:
            Dictionary with metric values
        """
        return {
            'windows_created': self.windows_created,
            'windows_triggered': self.windows_triggered,
            'late_data_updates': self.late_data_updates,
            'dropped_late_events': self.dropped_late_events,
        }
