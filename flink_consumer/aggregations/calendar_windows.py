"""
Calendar-based window aggregations with timezone support.

This module provides calendar-aware window aggregations that respect:
- Actual calendar days (not fixed 24-hour periods)
- Actual calendar months (28-31 days)
- User timezones (not just UTC)
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class CalendarWindowType(Enum):
    """Calendar-based window types."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class CalendarWindowAssigner:
    """
    Assigns records to calendar-based windows using start_date.
    
    Instead of using Flink's built-in tumbling windows (which are fixed-duration),
    this uses custom logic to assign records to calendar periods based on the
    start_date field in the data.
    
    Benefits:
    - Respects actual calendar boundaries (months with 28-31 days)
    - Can support user timezones
    - More intuitive for end users
    """
    
    def __init__(self, user_timezone: str = "UTC"):
        """
        Initialize calendar window assigner.
        
        Args:
            user_timezone: Timezone for calendar calculations (e.g., "Asia/Seoul", "America/New_York")
        """
        self.user_timezone = user_timezone
        logger.info(f"Initialized CalendarWindowAssigner with timezone: {user_timezone}")
    
    def assign_daily_window(self, start_date_ms: int) -> Dict[str, Any]:
        """
        Assign record to daily window based on start_date.
        
        Args:
            start_date_ms: start_date timestamp in milliseconds
            
        Returns:
            Dictionary with window metadata:
            - window_date: Date (YYYY-MM-DD)
            - window_start: Start of day timestamp (ms)
            - window_end: End of day timestamp (ms)
            - year, month, day
        """
        from zoneinfo import ZoneInfo
        
        # Convert to user's timezone
        dt = datetime.fromtimestamp(start_date_ms / 1000, tz=ZoneInfo(self.user_timezone))
        
        # Get start of day in user's timezone
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        
        return {
            'window_date': day_start.date(),
            'window_start': int(day_start.timestamp() * 1000),
            'window_end': int(day_end.timestamp() * 1000),
            'year': day_start.year,
            'month': day_start.month,
            'day': day_start.day,
            'timezone': self.user_timezone,
        }
    
    def assign_weekly_window(self, start_date_ms: int) -> Dict[str, Any]:
        """
        Assign record to weekly window (Monday-Sunday) based on start_date.
        
        Args:
            start_date_ms: start_date timestamp in milliseconds
            
        Returns:
            Dictionary with window metadata:
            - week_start_date: Monday of the week
            - week_end_date: Sunday of the week
            - window_start: Start of week timestamp (ms)
            - window_end: End of week timestamp (ms)
            - year, week_of_year
        """
        from zoneinfo import ZoneInfo
        
        # Convert to user's timezone
        dt = datetime.fromtimestamp(start_date_ms / 1000, tz=ZoneInfo(self.user_timezone))
        
        # Get start of day
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Go back to Monday (weekday 0)
        days_since_monday = day_start.weekday()
        week_start = day_start - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=7)
        
        # ISO week number
        iso_year, iso_week, _ = week_start.isocalendar()
        
        return {
            'week_start_date': week_start.date(),
            'week_end_date': (week_end - timedelta(microseconds=1)).date(),
            'window_start': int(week_start.timestamp() * 1000),
            'window_end': int(week_end.timestamp() * 1000),
            'year': iso_year,
            'week_of_year': iso_week,
            'timezone': self.user_timezone,
        }
    
    def assign_monthly_window(self, start_date_ms: int) -> Dict[str, Any]:
        """
        Assign record to monthly window based on actual calendar month.
        
        This respects actual month lengths (28-31 days), not fixed 30-day periods.
        
        Args:
            start_date_ms: start_date timestamp in milliseconds
            
        Returns:
            Dictionary with window metadata:
            - month_start_date: First day of month
            - month_end_date: Last day of month
            - window_start: Start of month timestamp (ms)
            - window_end: End of month timestamp (ms)
            - year, month
            - days_in_month: Actual number of days
        """
        from zoneinfo import ZoneInfo
        import calendar
        
        # Convert to user's timezone
        dt = datetime.fromtimestamp(start_date_ms / 1000, tz=ZoneInfo(self.user_timezone))
        
        # Get first day of month
        month_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Get last day of month (actual calendar month)
        days_in_month = calendar.monthrange(month_start.year, month_start.month)[1]
        month_end = month_start.replace(day=days_in_month) + timedelta(days=1)
        
        return {
            'month_start_date': month_start.date(),
            'month_end_date': (month_end - timedelta(microseconds=1)).date(),
            'window_start': int(month_start.timestamp() * 1000),
            'window_end': int(month_end.timestamp() * 1000),
            'year': month_start.year,
            'month': month_start.month,
            'days_in_month': days_in_month,
            'timezone': self.user_timezone,
        }
    
    def get_window_key(self, window_type: CalendarWindowType, start_date_ms: int) -> str:
        """
        Generate a unique key for the window.
        
        This key is used for grouping records into the same window.
        
        Args:
            window_type: Type of window
            start_date_ms: start_date timestamp in milliseconds
            
        Returns:
            Window key string (e.g., "2025-11-17", "2025-W46", "2025-11")
        """
        if window_type == CalendarWindowType.DAILY:
            window = self.assign_daily_window(start_date_ms)
            return str(window['window_date'])
        
        elif window_type == CalendarWindowType.WEEKLY:
            window = self.assign_weekly_window(start_date_ms)
            return f"{window['year']}-W{window['week_of_year']:02d}"
        
        elif window_type == CalendarWindowType.MONTHLY:
            window = self.assign_monthly_window(start_date_ms)
            return f"{window['year']}-{window['month']:02d}"
        
        else:
            raise ValueError(f"Unsupported window type: {window_type}")


class CalendarAggregationFunction:
    """
    Custom aggregation function for calendar-based windows.
    
    Instead of using Flink's WindowFunction, this uses ProcessFunction
    with state to manually manage calendar-based aggregations.
    """
    
    def __init__(self, window_type: CalendarWindowType, user_timezone: str = "UTC"):
        """
        Initialize calendar aggregation function.
        
        Args:
            window_type: Type of calendar window
            user_timezone: Timezone for calendar calculations
        """
        self.window_type = window_type
        self.assigner = CalendarWindowAssigner(user_timezone)
        self.aggregates = {}  # In-memory state (should use Flink state in production)
    
    def process_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single record and update aggregations.
        
        Args:
            record: Health data record with start_date, user_id, data_type, value
            
        Returns:
            Aggregated result if window is complete, None otherwise
        """
        user_id = record['user_id']
        data_type = record['data_type']
        value = record['value']
        start_date_ms = record['start_date']
        
        # Assign to window
        window_key = self.assigner.get_window_key(self.window_type, start_date_ms)
        
        # Create aggregate key
        agg_key = (user_id, data_type, window_key)
        
        # Initialize or update aggregate
        if agg_key not in self.aggregates:
            self.aggregates[agg_key] = {
                'user_id': user_id,
                'data_type': data_type,
                'window_key': window_key,
                'count': 0,
                'sum': 0.0,
                'min': float('inf'),
                'max': float('-inf'),
                'values': [],
            }
        
        agg = self.aggregates[agg_key]
        agg['count'] += 1
        agg['sum'] += value
        agg['min'] = min(agg['min'], value)
        agg['max'] = max(agg['max'], value)
        agg['values'].append(value)
        
        return None  # Return when window is triggered
    
    def get_window_result(self, user_id: str, data_type: str, window_key: str) -> Dict[str, Any]:
        """
        Get aggregation result for a completed window.
        
        Args:
            user_id: User identifier
            data_type: Data type
            window_key: Window key
            
        Returns:
            Aggregated statistics
        """
        agg_key = (user_id, data_type, window_key)
        agg = self.aggregates.get(agg_key)
        
        if not agg:
            return None
        
        # Calculate statistics
        count = agg['count']
        avg = agg['sum'] / count if count > 0 else 0.0
        
        # Calculate standard deviation
        if count > 1:
            variance = sum((v - avg) ** 2 for v in agg['values']) / count
            stddev = variance ** 0.5
        else:
            stddev = 0.0
        
        # Get window metadata
        start_date_ms = agg['values'][0] if agg['values'] else 0
        window_metadata = self._get_window_metadata(window_key, start_date_ms)
        
        result = {
            'user_id': user_id,
            'data_type': data_type,
            'min_value': agg['min'],
            'max_value': agg['max'],
            'avg_value': avg,
            'sum_value': agg['sum'],
            'count': count,
            'stddev_value': stddev,
            'first_value': agg['values'][0] if agg['values'] else None,
            'last_value': agg['values'][-1] if agg['values'] else None,
            'record_count': len(agg['values']),
            **window_metadata,
        }
        
        return result
    
    def _get_window_metadata(self, window_key: str, start_date_ms: int) -> Dict[str, Any]:
        """Get window metadata based on window type."""
        if self.window_type == CalendarWindowType.DAILY:
            return self.assigner.assign_daily_window(start_date_ms)
        elif self.window_type == CalendarWindowType.WEEKLY:
            return self.assigner.assign_weekly_window(start_date_ms)
        elif self.window_type == CalendarWindowType.MONTHLY:
            return self.assigner.assign_monthly_window(start_date_ms)
        else:
            return {}


# Example usage
def example_calendar_aggregation():
    """
    Example of how to use calendar-based aggregations.
    """
    # Initialize assigner for Korean timezone
    assigner = CalendarWindowAssigner(user_timezone="Asia/Seoul")
    
    # Sample record
    record = {
        'user_id': 'user-123',
        'data_type': 'heartRate',
        'value': 75.0,
        'start_date': 1700179200000,  # 2023-11-17 00:00:00 UTC
    }
    
    # Assign to daily window
    daily_window = assigner.assign_daily_window(record['start_date'])
    print(f"Daily window: {daily_window}")
    # Output: Daily window in Korean timezone (UTC+9)
    
    # Assign to monthly window
    monthly_window = assigner.assign_monthly_window(record['start_date'])
    print(f"Monthly window: {monthly_window}")
    # Output: Actual calendar month (28-31 days)
    
    # Get window key for grouping
    window_key = assigner.get_window_key(CalendarWindowType.MONTHLY, record['start_date'])
    print(f"Window key: {window_key}")
    # Output: "2023-11"


if __name__ == "__main__":
    example_calendar_aggregation()
