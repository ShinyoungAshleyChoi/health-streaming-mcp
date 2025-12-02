"""
Tests for timezone utilities.
"""

import pytest
from flink_consumer.utils.timezone_utils import (
    TimezoneResolver,
    TimezoneCache,
    resolve_timezone,
    is_valid_timezone,
)


class TestTimezoneResolver:
    """Tests for TimezoneResolver class."""
    
    def test_resolve_valid_timezone(self):
        """Test resolving a valid timezone."""
        result = TimezoneResolver.resolve_timezone("Asia/Seoul", 540)
        assert result == "Asia/Seoul"
    
    def test_resolve_utc(self):
        """Test resolving UTC timezone."""
        result = TimezoneResolver.resolve_timezone("UTC", 0)
        assert result == "UTC"
    
    def test_resolve_invalid_timezone_fallback(self):
        """Test that invalid timezone falls back to default."""
        result = TimezoneResolver.resolve_timezone("Invalid/Timezone", None, "UTC")
        assert result == "UTC"
    
    def test_resolve_none_timezone(self):
        """Test resolving when timezone is None."""
        result = TimezoneResolver.resolve_timezone(None, 540, "UTC")
        assert result == "UTC"
    
    def test_resolve_with_offset_only(self):
        """Test resolving with only offset provided."""
        result = TimezoneResolver.resolve_timezone(None, 540, "Asia/Seoul")
        assert result == "Asia/Seoul"  # Falls back to default
    
    def test_is_valid_timezone_valid(self):
        """Test validation of valid timezones."""
        assert TimezoneResolver.is_valid_timezone("Asia/Seoul") is True
        assert TimezoneResolver.is_valid_timezone("America/New_York") is True
        assert TimezoneResolver.is_valid_timezone("UTC") is True
        assert TimezoneResolver.is_valid_timezone("Europe/London") is True
    
    def test_is_valid_timezone_invalid(self):
        """Test validation of invalid timezones."""
        assert TimezoneResolver.is_valid_timezone("Invalid/Timezone") is False
        assert TimezoneResolver.is_valid_timezone("") is False
        assert TimezoneResolver.is_valid_timezone(None) is False
    
    def test_get_timezone_info(self):
        """Test getting timezone information."""
        info = TimezoneResolver.get_timezone_info("Asia/Seoul")
        assert info['timezone'] == "Asia/Seoul"
        assert info['is_valid'] is True
        assert 'current_offset_seconds' in info
        assert 'current_offset_minutes' in info
    
    def test_validate_timezone_consistency(self):
        """Test timezone and offset consistency validation."""
        # Asia/Seoul is UTC+9 (540 minutes)
        assert TimezoneResolver.validate_timezone_consistency("Asia/Seoul", 540) is True
        
        # Inconsistent offset should fail
        assert TimezoneResolver.validate_timezone_consistency("Asia/Seoul", 0) is False
        
        # No offset provided should pass
        assert TimezoneResolver.validate_timezone_consistency("Asia/Seoul", None) is True


class TestTimezoneCache:
    """Tests for TimezoneCache class."""
    
    def test_cache_basic(self):
        """Test basic cache functionality."""
        cache = TimezoneCache(max_size=10)
        
        tz1 = cache.get_timezone("Asia/Seoul")
        tz2 = cache.get_timezone("Asia/Seoul")
        
        # Should return same object from cache
        assert tz1 is tz2
    
    def test_cache_multiple_timezones(self):
        """Test caching multiple timezones."""
        cache = TimezoneCache(max_size=10)
        
        tz_seoul = cache.get_timezone("Asia/Seoul")
        tz_ny = cache.get_timezone("America/New_York")
        tz_london = cache.get_timezone("Europe/London")
        
        assert tz_seoul is not tz_ny
        assert tz_ny is not tz_london
        
        stats = cache.get_stats()
        assert stats['size'] == 3
    
    def test_cache_eviction(self):
        """Test cache eviction when max size reached."""
        cache = TimezoneCache(max_size=2)
        
        cache.get_timezone("Asia/Seoul")
        cache.get_timezone("America/New_York")
        
        stats = cache.get_stats()
        assert stats['size'] == 2
        
        # Adding third timezone should evict least accessed
        cache.get_timezone("Europe/London")
        
        stats = cache.get_stats()
        assert stats['size'] == 2
    
    def test_cache_clear(self):
        """Test clearing cache."""
        cache = TimezoneCache(max_size=10)
        
        cache.get_timezone("Asia/Seoul")
        cache.get_timezone("America/New_York")
        
        cache.clear()
        
        stats = cache.get_stats()
        assert stats['size'] == 0
        assert stats['total_accesses'] == 0


class TestConvenienceFunctions:
    """Tests for convenience functions."""
    
    def test_resolve_timezone_function(self):
        """Test resolve_timezone convenience function."""
        result = resolve_timezone("Asia/Seoul", 540)
        assert result == "Asia/Seoul"
        
        result = resolve_timezone(None, None, "UTC")
        assert result == "UTC"
    
    def test_is_valid_timezone_function(self):
        """Test is_valid_timezone convenience function."""
        assert is_valid_timezone("Asia/Seoul") is True
        assert is_valid_timezone("Invalid/Timezone") is False


class TestRealWorldScenarios:
    """Tests for real-world timezone scenarios."""
    
    def test_korean_user_timezone(self):
        """Test Korean user timezone (Asia/Seoul)."""
        # iOS sends TimeZone.current.identifier = "Asia/Seoul"
        # and timezoneOffset = 540 (9 hours * 60 minutes)
        result = resolve_timezone("Asia/Seoul", 540)
        assert result == "Asia/Seoul"
    
    def test_us_east_coast_timezone(self):
        """Test US East Coast timezone."""
        # America/New_York is UTC-5 (EST) or UTC-4 (EDT)
        result = resolve_timezone("America/New_York", -300)
        assert result == "America/New_York"
    
    def test_utc_timezone(self):
        """Test UTC timezone."""
        result = resolve_timezone("UTC", 0)
        assert result == "UTC"
    
    def test_missing_timezone_with_offset(self):
        """Test when timezone is missing but offset is provided."""
        # Should fall back to default
        result = resolve_timezone(None, 540, "UTC")
        assert result == "UTC"
    
    def test_ios_timezone_formats(self):
        """Test various iOS timezone identifier formats."""
        # iOS uses IANA timezone identifiers
        ios_timezones = [
            "Asia/Seoul",
            "America/New_York",
            "Europe/London",
            "Australia/Sydney",
            "Pacific/Auckland",
            "Asia/Tokyo",
        ]
        
        for tz in ios_timezones:
            result = resolve_timezone(tz, None)
            assert result == tz
            assert is_valid_timezone(tz) is True
