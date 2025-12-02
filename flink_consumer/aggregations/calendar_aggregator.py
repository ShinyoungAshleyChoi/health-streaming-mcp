"""
Flink ProcessFunction for calendar-based aggregations.

This module provides a Flink ProcessFunction that performs calendar-aware
aggregations using Flink's state management.
"""

import logging
from typing import Dict, Any, Iterator
from datetime import datetime
from zoneinfo import ZoneInfo
import calendar

from pyflink.datastream import ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ValueState

logger = logging.getLogger(__name__)


class CalendarAggregateState:
    """State for calendar-based aggregation."""
    
    def __init__(self):
        self.count = 0
        self.sum_value = 0.0
        self.min_value = float('inf')
        self.max_value = float('-inf')
        self.sum_of_squares = 0.0
        self.first_value = None
        self.last_value = None
        self.window_start = None
        self.window_end = None
        self.timezone = 'UTC'  # Timezone used for this aggregation
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary."""
        return {
            'count': self.count,
            'sum_value': self.sum_value,
            'min_value': self.min_value if self.min_value != float('inf') else None,
            'max_value': self.max_value if self.max_value != float('-inf') else None,
            'sum_of_squares': self.sum_of_squares,
            'first_value': self.first_value,
            'last_value': self.last_value,
            'window_start': self.window_start,
            'window_end': self.window_end,
            'timezone': self.timezone,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CalendarAggregateState':
        """Create state from dictionary."""
        state = cls()
        state.count = data.get('count', 0)
        state.sum_value = data.get('sum_value', 0.0)
        state.min_value = data.get('min_value', float('inf'))
        state.max_value = data.get('max_value', float('-inf'))
        state.sum_of_squares = data.get('sum_of_squares', 0.0)
        state.first_value = data.get('first_value')
        state.last_value = data.get('last_value')
        state.window_start = data.get('window_start')
        state.window_end = data.get('window_end')
        state.timezone = data.get('timezone', 'UTC')
        return state


class CalendarDailyAggregator(ProcessFunction):
    """
    Process function for daily aggregations based on start_date.
    
    This uses Flink's keyed state to maintain aggregations per (user_id, data_type, date).
    Records are grouped by the calendar date of their start_date field.
    Each record's timezone field is used for calendar calculations.
    """
    
    def __init__(self, default_timezone: str = "UTC", emit_on_watermark: bool = True):
        """
        Initialize daily aggregator.
        
        Args:
            default_timezone: Default timezone if record doesn't have timezone field
            emit_on_watermark: If True, emit results when watermark passes day boundary
        """
        self.default_timezone = default_timezone
        self.emit_on_watermark = emit_on_watermark
        self.state = None
        
        # Import timezone utilities
        from flink_consumer.utils.timezone_utils import get_timezone_cache
        self.timezone_cache = get_timezone_cache()
    
    def open(self, runtime_context):
        """Initialize state."""
        state_descriptor = ValueStateDescriptor(
            "daily_aggregate_state",
            type_info=None  # Will be inferred
        )
        self.state = runtime_context.get_state(state_descriptor)
    
    def process_element(
        self,
        record: Dict[str, Any],
        ctx: ProcessFunction.Context
    ) -> Iterator[Dict[str, Any]]:
        """
        Process a single record and update daily aggregation.
        
        Args:
            record: Health data record with start_date, user_id, data_type, value, timezone
            ctx: Process function context
            
        Yields:
            Aggregated results when day is complete
        """
        try:
            # Extract fields
            user_id = record['user_id']
            data_type = record['data_type']
            value = float(record['value'])
            start_date_ms = record['start_date']
            
            # Resolve timezone from record (uses iOS TimeZone.current.identifier)
            from flink_consumer.utils.timezone_utils import resolve_timezone
            user_timezone = resolve_timezone(
                timezone=record.get('timezone'),
                timezone_offset=record.get('timezone_offset'),
                default_timezone=self.default_timezone
            )
            
            # Get calendar date in user's timezone
            tz = self.timezone_cache.get_timezone(user_timezone)
            dt = datetime.fromtimestamp(start_date_ms / 1000, tz=tz)
            aggregation_date = dt.date()
            
            # Get day boundaries
            day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start.replace(hour=23, minute=59, second=59, microsecond=999999)
            window_start = int(day_start.timestamp() * 1000)
            window_end = int(day_end.timestamp() * 1000)
            
            # Get or create state
            current_state = self.state.value()
            if current_state is None:
                agg_state = CalendarAggregateState()
                agg_state.window_start = window_start
                agg_state.window_end = window_end
                agg_state.timezone = user_timezone  # Store user's timezone
            else:
                agg_state = CalendarAggregateState.from_dict(current_state)
            
            # Update aggregation
            agg_state.count += 1
            agg_state.sum_value += value
            agg_state.min_value = min(agg_state.min_value, value)
            agg_state.max_value = max(agg_state.max_value, value)
            agg_state.sum_of_squares += value * value
            
            if agg_state.first_value is None:
                agg_state.first_value = value
            agg_state.last_value = value
            
            # Save state
            self.state.update(agg_state.to_dict())
            
            # Register timer for end of day (optional)
            if self.emit_on_watermark:
                ctx.timer_service().register_event_time_timer(window_end)
            
        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
    
    def on_timer(
        self,
        timestamp: int,
        ctx: ProcessFunction.OnTimerContext
    ) -> Iterator[Dict[str, Any]]:
        """
        Called when timer fires (end of day).
        
        Args:
            timestamp: Timer timestamp
            ctx: Timer context
            
        Yields:
            Final aggregation result for the day
        """
        current_state = self.state.value()
        if current_state is None:
            return
        
        agg_state = CalendarAggregateState.from_dict(current_state)
        
        # Calculate final statistics
        count = agg_state.count
        if count == 0:
            return
        
        avg_value = agg_state.sum_value / count
        
        # Calculate standard deviation
        variance = (agg_state.sum_of_squares / count) - (avg_value * avg_value)
        stddev_value = variance ** 0.5 if variance > 0 else 0.0
        
        # Get date from window_start using stored timezone
        user_timezone = agg_state.timezone or self.default_timezone
        tz = self.timezone_cache.get_timezone(user_timezone)
        dt = datetime.fromtimestamp(agg_state.window_start / 1000, tz=tz)
        aggregation_date = dt.date()
        
        # Create result
        result = {
            'user_id': ctx.get_current_key()[0],  # Assuming key is (user_id, data_type)
            'data_type': ctx.get_current_key()[1],
            'aggregation_date': aggregation_date,
            'window_start': agg_state.window_start,
            'window_end': agg_state.window_end,
            'min_value': agg_state.min_value,
            'max_value': agg_state.max_value,
            'avg_value': avg_value,
            'sum_value': agg_state.sum_value,
            'count': count,
            'stddev_value': stddev_value,
            'first_value': agg_state.first_value,
            'last_value': agg_state.last_value,
            'record_count': count,
            'updated_at': int(datetime.now(tz=ZoneInfo('UTC')).timestamp() * 1000),
            'timezone': agg_state.timezone,  # Include timezone in result
        }
        
        yield result
        
        # Clear state after emitting
        self.state.clear()


class CalendarMonthlyAggregator(ProcessFunction):
    """
    Process function for monthly aggregations based on actual calendar months.
    
    This respects actual month lengths (28-31 days) and user timezones.
    Each record's timezone field is used for calendar calculations.
    """
    
    def __init__(self, default_timezone: str = "UTC"):
        """
        Initialize monthly aggregator.
        
        Args:
            default_timezone: Default timezone if record doesn't have timezone field
        """
        self.default_timezone = default_timezone
        self.state = None
        
        # Import timezone utilities
        from flink_consumer.utils.timezone_utils import get_timezone_cache
        self.timezone_cache = get_timezone_cache()
    
    def open(self, runtime_context):
        """Initialize state."""
        state_descriptor = ValueStateDescriptor(
            "monthly_aggregate_state",
            type_info=None
        )
        self.state = runtime_context.get_state(state_descriptor)
    
    def process_element(
        self,
        record: Dict[str, Any],
        ctx: ProcessFunction.Context
    ) -> Iterator[Dict[str, Any]]:
        """
        Process a single record and update monthly aggregation.
        
        Args:
            record: Health data record with timezone field
            ctx: Process function context
            
        Yields:
            Aggregated results when month is complete
        """
        try:
            value = float(record['value'])
            start_date_ms = record['start_date']
            
            # Resolve timezone from record (uses iOS TimeZone.current.identifier)
            from flink_consumer.utils.timezone_utils import resolve_timezone
            user_timezone = resolve_timezone(
                timezone=record.get('timezone'),
                timezone_offset=record.get('timezone_offset'),
                default_timezone=self.default_timezone
            )
            
            # Get calendar month in user's timezone
            tz = self.timezone_cache.get_timezone(user_timezone)
            dt = datetime.fromtimestamp(start_date_ms / 1000, tz=tz)
            year = dt.year
            month = dt.month
            
            # Get month boundaries
            month_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            days_in_month = calendar.monthrange(year, month)[1]
            month_end = month_start.replace(day=days_in_month, hour=23, minute=59, second=59, microsecond=999999)
            
            window_start = int(month_start.timestamp() * 1000)
            window_end = int(month_end.timestamp() * 1000)
            
            # Get or create state
            current_state = self.state.value()
            if current_state is None:
                agg_state = CalendarAggregateState()
                agg_state.window_start = window_start
                agg_state.window_end = window_end
                agg_state.timezone = user_timezone  # Store timezone in state
            else:
                agg_state = CalendarAggregateState.from_dict(current_state)
            
            # Update aggregation
            agg_state.count += 1
            agg_state.sum_value += value
            agg_state.min_value = min(agg_state.min_value, value)
            agg_state.max_value = max(agg_state.max_value, value)
            agg_state.sum_of_squares += value * value
            
            if agg_state.first_value is None:
                agg_state.first_value = value
            agg_state.last_value = value
            
            # Save state
            self.state.update(agg_state.to_dict())
            
            # Register timer for end of month
            ctx.timer_service().register_event_time_timer(window_end)
            
        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
    
    def on_timer(
        self,
        timestamp: int,
        ctx: ProcessFunction.OnTimerContext
    ) -> Iterator[Dict[str, Any]]:
        """
        Called when timer fires (end of month).
        
        Args:
            timestamp: Timer timestamp
            ctx: Timer context
            
        Yields:
            Final aggregation result for the month
        """
        current_state = self.state.value()
        if current_state is None:
            return
        
        agg_state = CalendarAggregateState.from_dict(current_state)
        
        count = agg_state.count
        if count == 0:
            return
        
        avg_value = agg_state.sum_value / count
        variance = (agg_state.sum_of_squares / count) - (avg_value * avg_value)
        stddev_value = variance ** 0.5 if variance > 0 else 0.0
        
        # Get month info from window_start using stored timezone
        user_timezone = agg_state.timezone or self.default_timezone
        tz = self.timezone_cache.get_timezone(user_timezone)
        dt = datetime.fromtimestamp(agg_state.window_start / 1000, tz=tz)
        year = dt.year
        month = dt.month
        days_in_month = calendar.monthrange(year, month)[1]
        
        month_start_date = dt.date()
        month_end_date = dt.replace(day=days_in_month).date()
        
        result = {
            'user_id': ctx.get_current_key()[0],
            'data_type': ctx.get_current_key()[1],
            'year': year,
            'month': month,
            'month_start_date': month_start_date,
            'month_end_date': month_end_date,
            'window_start': agg_state.window_start,
            'window_end': agg_state.window_end,
            'min_value': agg_state.min_value,
            'max_value': agg_state.max_value,
            'avg_value': avg_value,
            'sum_value': agg_state.sum_value,
            'count': count,
            'stddev_value': stddev_value,
            'daily_avg_of_avg': avg_value,  # Can be calculated from daily aggregates
            'record_count': count,
            'days_in_month': days_in_month,
            'updated_at': int(datetime.now(tz=ZoneInfo('UTC')).timestamp() * 1000),
            'timezone': agg_state.timezone,  # Include timezone in result
        }
        
        yield result
        
        # Clear state
        self.state.clear()
