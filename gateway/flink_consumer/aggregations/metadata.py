"""
Window metadata enrichment for aggregation results.

This module adds window-specific metadata (timestamps, date fields) to
aggregation results for proper partitioning and querying.
"""

from typing import Dict, Any, Iterable, Tuple
from datetime import datetime, timezone, timedelta
import time
import logging

from flink_consumer.aggregations.windows import WindowType

logger = logging.getLogger(__name__)


class WindowMetadataEnricher:
    """
    Enrich aggregation results with window metadata.
    
    This class implements PyFlink's ProcessWindowFunction interface to add
    window-specific information to aggregation results.
    """
    
    def __init__(self, window_type: WindowType):
        """
        Initialize metadata enricher.
        
        Args:
            window_type: Type of window (DAILY, WEEKLY, or MONTHLY)
        """
        self.window_type = window_type
    
    def process(self, key: Tuple[str, str], context, elements: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """
        Process window elements and add metadata.
        
        This method is called by PyFlink when a window is triggered.
        
        Args:
            key: Tuple of (user_id, data_type)
            context: Window context with window information
            elements: Aggregated statistics from the window
            
        Yields:
            Enriched aggregation results with metadata
        """
        user_id, data_type = key
        
        # Get window bounds from context
        window = context.window()
        window_start = window.start
        window_end = window.end
        
        logger.debug(f"Enriching {self.window_type.value} window for user={user_id}, "
                    f"type={data_type}, window=[{window_start}, {window_end})")
        
        for element in elements:
            if element is None:
                logger.warning(f"Skipping None element in window for {user_id}/{data_type}")
                continue
            
            # Add key fields
            element['user_id'] = user_id
            element['data_type'] = data_type
            
            # Add window timestamps
            element['window_start'] = window_start
            element['window_end'] = window_end
            
            # Add update timestamp
            element['updated_at'] = int(time.time() * 1000)
            
            # Add window-type specific metadata
            if self.window_type == WindowType.DAILY:
                self._add_daily_metadata(element, window_start)
            elif self.window_type == WindowType.WEEKLY:
                self._add_weekly_metadata(element, window_start, window_end)
            elif self.window_type == WindowType.MONTHLY:
                self._add_monthly_metadata(element, window_start, window_end)
            
            yield element
    
    def _add_daily_metadata(self, element: Dict[str, Any], window_start: int):
        """
        Add daily aggregation specific metadata.
        
        Args:
            element: Aggregation result dictionary
            window_start: Window start timestamp in milliseconds
        """
        window_start_dt = datetime.fromtimestamp(window_start / 1000, tz=timezone.utc)
        
        # Add aggregation date
        element['aggregation_date'] = window_start_dt.date().isoformat()
        
        logger.debug(f"Daily metadata: date={element['aggregation_date']}")
    
    def _add_weekly_metadata(self, element: Dict[str, Any], window_start: int, window_end: int):
        """
        Add weekly aggregation specific metadata.
        
        Args:
            element: Aggregation result dictionary
            window_start: Window start timestamp in milliseconds
            window_end: Window end timestamp in milliseconds
        """
        window_start_dt = datetime.fromtimestamp(window_start / 1000, tz=timezone.utc)
        window_end_dt = datetime.fromtimestamp(window_end / 1000, tz=timezone.utc)
        
        # Add week start and end dates
        element['week_start_date'] = window_start_dt.date().isoformat()
        element['week_end_date'] = window_end_dt.date().isoformat()
        
        # Add year and week of year
        iso_calendar = window_start_dt.isocalendar()
        element['year'] = iso_calendar[0]
        element['week_of_year'] = iso_calendar[1]
        
        logger.debug(f"Weekly metadata: year={element['year']}, "
                    f"week={element['week_of_year']}, "
                    f"dates=[{element['week_start_date']}, {element['week_end_date']}]")
    
    def _add_monthly_metadata(self, element: Dict[str, Any], window_start: int, window_end: int):
        """
        Add monthly aggregation specific metadata.
        
        Args:
            element: Aggregation result dictionary
            window_start: Window start timestamp in milliseconds
            window_end: Window end timestamp in milliseconds
        """
        window_start_dt = datetime.fromtimestamp(window_start / 1000, tz=timezone.utc)
        window_end_dt = datetime.fromtimestamp(window_end / 1000, tz=timezone.utc)
        
        # Add year and month
        element['year'] = window_start_dt.year
        element['month'] = window_start_dt.month
        
        # Add month start and end dates
        month_start = window_start_dt.replace(day=1)
        element['month_start_date'] = month_start.date().isoformat()
        element['month_end_date'] = window_end_dt.date().isoformat()
        
        logger.debug(f"Monthly metadata: year={element['year']}, "
                    f"month={element['month']}, "
                    f"dates=[{element['month_start_date']}, {element['month_end_date']}]")


class MetadataCalculator:
    """
    Utility class for calculating date-related metadata.
    """
    
    @staticmethod
    def calculate_aggregation_date(timestamp_ms: int) -> str:
        """
        Calculate aggregation date from timestamp.
        
        Args:
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            Date string in ISO format (YYYY-MM-DD)
        """
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return dt.date().isoformat()
    
    @staticmethod
    def calculate_week_info(timestamp_ms: int) -> Dict[str, Any]:
        """
        Calculate week-related information from timestamp.
        
        Args:
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            Dictionary with year, week_of_year, week_start_date, week_end_date
        """
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        iso_calendar = dt.isocalendar()
        
        # Calculate week start (Monday)
        days_since_monday = dt.weekday()
        week_start = dt - timedelta(days=days_since_monday)
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate week end (Sunday)
        week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        
        return {
            'year': iso_calendar[0],
            'week_of_year': iso_calendar[1],
            'week_start_date': week_start.date().isoformat(),
            'week_end_date': week_end.date().isoformat(),
        }
    
    @staticmethod
    def calculate_month_info(timestamp_ms: int) -> Dict[str, Any]:
        """
        Calculate month-related information from timestamp.
        
        Args:
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            Dictionary with year, month, month_start_date, month_end_date
        """
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        
        # Calculate month start
        month_start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate month end (last day of month)
        if dt.month == 12:
            next_month = month_start.replace(year=dt.year + 1, month=1)
        else:
            next_month = month_start.replace(month=dt.month + 1)
        
        month_end = next_month - timedelta(seconds=1)
        
        return {
            'year': dt.year,
            'month': dt.month,
            'month_start_date': month_start.date().isoformat(),
            'month_end_date': month_end.date().isoformat(),
        }
    
    @staticmethod
    def format_timestamp(timestamp_ms: int) -> str:
        """
        Format timestamp as ISO 8601 string.
        
        Args:
            timestamp_ms: Unix timestamp in milliseconds
            
        Returns:
            ISO 8601 formatted timestamp string
        """
        dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return dt.isoformat()


class AggregationMetadata:
    """
    Container for aggregation metadata.
    
    Provides a structured way to store and access metadata fields.
    """
    
    def __init__(self, user_id: str, data_type: str, window_type: WindowType,
                 window_start: int, window_end: int):
        """
        Initialize aggregation metadata.
        
        Args:
            user_id: User identifier
            data_type: Health data type
            window_type: Type of window
            window_start: Window start timestamp (ms)
            window_end: Window end timestamp (ms)
        """
        self.user_id = user_id
        self.data_type = data_type
        self.window_type = window_type
        self.window_start = window_start
        self.window_end = window_end
        self.updated_at = int(time.time() * 1000)
        
        # Calculate date fields based on window type
        if window_type == WindowType.DAILY:
            self.aggregation_date = MetadataCalculator.calculate_aggregation_date(window_start)
        elif window_type == WindowType.WEEKLY:
            week_info = MetadataCalculator.calculate_week_info(window_start)
            self.year = week_info['year']
            self.week_of_year = week_info['week_of_year']
            self.week_start_date = week_info['week_start_date']
            self.week_end_date = week_info['week_end_date']
        elif window_type == WindowType.MONTHLY:
            month_info = MetadataCalculator.calculate_month_info(window_start)
            self.year = month_info['year']
            self.month = month_info['month']
            self.month_start_date = month_info['month_start_date']
            self.month_end_date = month_info['month_end_date']
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert metadata to dictionary.
        
        Returns:
            Dictionary with all metadata fields
        """
        base_dict = {
            'user_id': self.user_id,
            'data_type': self.data_type,
            'window_start': self.window_start,
            'window_end': self.window_end,
            'updated_at': self.updated_at,
        }
        
        # Add window-type specific fields
        if self.window_type == WindowType.DAILY:
            base_dict['aggregation_date'] = self.aggregation_date
        elif self.window_type == WindowType.WEEKLY:
            base_dict.update({
                'year': self.year,
                'week_of_year': self.week_of_year,
                'week_start_date': self.week_start_date,
                'week_end_date': self.week_end_date,
            })
        elif self.window_type == WindowType.MONTHLY:
            base_dict.update({
                'year': self.year,
                'month': self.month,
                'month_start_date': self.month_start_date,
                'month_end_date': self.month_end_date,
            })
        
        return base_dict
