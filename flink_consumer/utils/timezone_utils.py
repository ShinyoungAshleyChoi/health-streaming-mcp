"""
Timezone utilities for health data processing.

This module provides utilities for resolving and validating timezones
from health data records.
"""

import logging
from typing import Optional
from zoneinfo import ZoneInfo, available_timezones

logger = logging.getLogger(__name__)


class TimezoneResolver:
    """
    Resolves timezone information from health data records.
    
    Uses the timezone field from iOS app (TimeZone.current.identifier)
    which provides IANA timezone identifiers like 'Asia/Seoul', 'America/New_York'.
    """
    
    # Cache of valid timezones for performance
    _valid_timezones = None
    
    @classmethod
    def _get_valid_timezones(cls) -> set:
        """Get set of valid IANA timezone identifiers."""
        if cls._valid_timezones is None:
            cls._valid_timezones = available_timezones()
        return cls._valid_timezones
    
    @classmethod
    def resolve_timezone(
        cls,
        timezone: Optional[str],
        timezone_offset: Optional[int] = None,
        default_timezone: str = "UTC"
    ) -> str:
        """
        Resolve timezone from record fields.
        
        Priority:
        1. Use timezone field if valid IANA identifier
        2. Fall back to default_timezone
        
        Args:
            timezone: IANA timezone identifier from iOS (e.g., "Asia/Seoul")
            timezone_offset: Timezone offset in minutes (for validation/logging)
            default_timezone: Default timezone if resolution fails
            
        Returns:
            Valid IANA timezone identifier
            
        Example:
            >>> resolver = TimezoneResolver()
            >>> tz = resolver.resolve_timezone("Asia/Seoul", 540)
            >>> print(tz)  # "Asia/Seoul"
        """
        # Try to use provided timezone
        if timezone:
            if cls.is_valid_timezone(timezone):
                logger.debug(f"Using timezone from record: {timezone}")
                return timezone
            else:
                logger.warning(
                    f"Invalid timezone identifier: {timezone}, "
                    f"falling back to {default_timezone}"
                )
        
        # Log if we have offset but no valid timezone
        if timezone_offset is not None and not timezone:
            logger.debug(
                f"No timezone provided, only offset: {timezone_offset} minutes. "
                f"Using default: {default_timezone}"
            )
        
        return default_timezone
    
    @classmethod
    def is_valid_timezone(cls, timezone: str) -> bool:
        """
        Check if timezone is a valid IANA identifier.
        
        Args:
            timezone: Timezone identifier to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not timezone:
            return False
        
        # Quick check against available timezones
        valid_timezones = cls._get_valid_timezones()
        if timezone in valid_timezones:
            return True
        
        # Try to create ZoneInfo to validate
        try:
            ZoneInfo(timezone)
            return True
        except Exception as e:
            logger.debug(f"Invalid timezone {timezone}: {e}")
            return False
    
    @classmethod
    def get_timezone_info(cls, timezone: str) -> dict:
        """
        Get information about a timezone.
        
        Args:
            timezone: IANA timezone identifier
            
        Returns:
            Dictionary with timezone information
        """
        try:
            from datetime import datetime
            
            tz = ZoneInfo(timezone)
            now = datetime.now(tz)
            
            return {
                'timezone': timezone,
                'is_valid': True,
                'current_offset_seconds': now.utcoffset().total_seconds(),
                'current_offset_minutes': now.utcoffset().total_seconds() / 60,
                'tzname': now.tzname(),
            }
        except Exception as e:
            logger.error(f"Error getting timezone info for {timezone}: {e}")
            return {
                'timezone': timezone,
                'is_valid': False,
                'error': str(e),
            }
    
    @classmethod
    def validate_timezone_consistency(
        cls,
        timezone: str,
        timezone_offset: Optional[int]
    ) -> bool:
        """
        Validate that timezone and offset are consistent.
        
        This is useful for detecting data quality issues where the
        timezone identifier doesn't match the offset.
        
        Args:
            timezone: IANA timezone identifier
            timezone_offset: Offset in minutes from GMT
            
        Returns:
            True if consistent or offset not provided, False if mismatch
        """
        if not timezone_offset:
            return True  # Can't validate without offset
        
        try:
            from datetime import datetime
            
            tz = ZoneInfo(timezone)
            now = datetime.now(tz)
            actual_offset_minutes = now.utcoffset().total_seconds() / 60
            
            # Allow small tolerance for DST transitions
            tolerance_minutes = 60
            diff = abs(actual_offset_minutes - timezone_offset)
            
            if diff > tolerance_minutes:
                logger.warning(
                    f"Timezone offset mismatch: {timezone} has offset "
                    f"{actual_offset_minutes} min but record has {timezone_offset} min"
                )
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating timezone consistency: {e}")
            return False


class TimezoneCache:
    """
    Cache for timezone objects to avoid repeated ZoneInfo creation.
    
    This improves performance when processing many records with the same timezone.
    """
    
    def __init__(self, max_size: int = 100):
        """
        Initialize timezone cache.
        
        Args:
            max_size: Maximum number of timezones to cache
        """
        self._cache = {}
        self._max_size = max_size
        self._access_count = {}
    
    def get_timezone(self, timezone_str: str) -> ZoneInfo:
        """
        Get ZoneInfo object from cache or create new one.
        
        Args:
            timezone_str: IANA timezone identifier
            
        Returns:
            ZoneInfo object
        """
        if timezone_str in self._cache:
            self._access_count[timezone_str] += 1
            return self._cache[timezone_str]
        
        # Create new ZoneInfo
        tz = ZoneInfo(timezone_str)
        
        # Add to cache if not full
        if len(self._cache) < self._max_size:
            self._cache[timezone_str] = tz
            self._access_count[timezone_str] = 1
        else:
            # Evict least accessed timezone
            least_accessed = min(self._access_count, key=self._access_count.get)
            del self._cache[least_accessed]
            del self._access_count[least_accessed]
            
            self._cache[timezone_str] = tz
            self._access_count[timezone_str] = 1
        
        return tz
    
    def clear(self):
        """Clear the cache."""
        self._cache.clear()
        self._access_count.clear()
    
    def get_stats(self) -> dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        return {
            'size': len(self._cache),
            'max_size': self._max_size,
            'total_accesses': sum(self._access_count.values()),
            'cached_timezones': list(self._cache.keys()),
        }


# Global timezone cache instance
_timezone_cache = TimezoneCache()


def get_timezone_cache() -> TimezoneCache:
    """
    Get global timezone cache instance.
    
    Returns:
        TimezoneCache instance
    """
    return _timezone_cache


# Convenience functions
def resolve_timezone(
    timezone: Optional[str],
    timezone_offset: Optional[int] = None,
    default_timezone: str = "UTC"
) -> str:
    """
    Convenience function to resolve timezone.
    
    Args:
        timezone: IANA timezone identifier
        timezone_offset: Timezone offset in minutes
        default_timezone: Default timezone
        
    Returns:
        Valid IANA timezone identifier
    """
    return TimezoneResolver.resolve_timezone(
        timezone, timezone_offset, default_timezone
    )


def is_valid_timezone(timezone: str) -> bool:
    """
    Convenience function to check if timezone is valid.
    
    Args:
        timezone: Timezone identifier
        
    Returns:
        True if valid, False otherwise
    """
    return TimezoneResolver.is_valid_timezone(timezone)
