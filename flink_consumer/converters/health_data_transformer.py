"""Health data transformation module for flattening nested Kafka payloads"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Iterator, Optional

from pyflink.datastream import FlatMapFunction

logger = logging.getLogger(__name__)


class HealthDataTransformer(FlatMapFunction):
    """
    Flatten nested health data payload into individual rows.
    
    Transforms a single Kafka message containing multiple health data samples
    into individual rows, one per sample. Also converts ISO 8601 timestamps
    to Unix timestamps (milliseconds).
    
    Requirements: 2.1, 2.3, 2.4
    """

    def flat_map(self, payload: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Transform payload with nested samples into flat rows.
        
        Args:
            payload: Dictionary with deviceId, userId, samples array, etc.
            
        Yields:
            Individual health data rows (one per sample)
        """
        if not payload:
            logger.warning("Received empty payload, skipping transformation")
            return
        
        try:
            # Extract top-level fields
            device_id = payload.get('deviceId')
            user_id = payload.get('userId')
            payload_timestamp_str = payload.get('timestamp')
            app_version = payload.get('appVersion')
            
            # Validate required top-level fields
            if not device_id or not user_id:
                logger.warning(
                    f"Missing required fields in payload: "
                    f"deviceId={device_id}, userId={user_id}"
                )
                return
            
            # Parse payload timestamp
            payload_timestamp = self._parse_timestamp(payload_timestamp_str)
            if payload_timestamp is None:
                logger.warning(f"Invalid payload timestamp: {payload_timestamp_str}")
                return
            
            # Extract samples array
            samples = payload.get('samples', [])
            if not samples:
                logger.debug(f"No samples in payload for user {user_id}")
                return
            
            # Process each sample
            for sample in samples:
                try:
                    row = self._transform_sample(
                        sample=sample,
                        device_id=device_id,
                        user_id=user_id,
                        payload_timestamp=payload_timestamp,
                        app_version=app_version
                    )
                    
                    if row:
                        yield row
                        
                except Exception as e:
                    logger.error(
                        f"Error transforming sample {sample.get('id', 'unknown')}: {e}",
                        exc_info=True
                    )
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing payload: {e}", exc_info=True)

    def _transform_sample(
        self,
        sample: Dict[str, Any],
        device_id: str,
        user_id: str,
        payload_timestamp: int,
        app_version: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Transform a single sample into a flat row.
        
        Args:
            sample: Sample dictionary from the payload
            device_id: Device ID from payload
            user_id: User ID from payload
            payload_timestamp: Parsed payload timestamp (Unix ms)
            app_version: App version from payload
            
        Returns:
            Transformed row dictionary or None if transformation fails
        """
        # Extract sample fields
        sample_id = sample.get('id')
        data_type = sample.get('type')
        value = sample.get('value')
        unit = sample.get('unit')
        start_date_str = sample.get('startDate')
        end_date_str = sample.get('endDate')
        source_bundle = sample.get('sourceBundle')
        metadata = sample.get('metadata', {})
        is_synced = sample.get('isSynced', False)
        created_at_str = sample.get('createdAt')
        
        # Extract timezone information
        timezone = sample.get('timezone')  # IANA timezone (e.g., "Asia/Seoul")
        timezone_offset = sample.get('timezoneOffset')  # Offset in minutes from GMT
        
        # Validate required sample fields
        if not sample_id or not data_type or value is None:
            logger.warning(
                f"Missing required sample fields: "
                f"id={sample_id}, type={data_type}, value={value}"
            )
            return None
        
        # Parse timestamps
        start_date = self._parse_timestamp(start_date_str)
        end_date = self._parse_timestamp(end_date_str)
        created_at = self._parse_timestamp(created_at_str)
        
        if start_date is None or end_date is None:
            logger.warning(
                f"Invalid timestamps in sample {sample_id}: "
                f"startDate={start_date_str}, endDate={end_date_str}"
            )
            return None
        
        # Resolve timezone (use sample timezone or default to UTC)
        resolved_timezone = self._resolve_timezone(timezone, timezone_offset)
        
        # Get current processing time
        processing_time = int(time.time() * 1000)
        
        # Build flat row
        row = {
            'device_id': device_id,
            'user_id': user_id,
            'sample_id': sample_id,
            'data_type': data_type,
            'value': float(value),
            'unit': unit,
            'start_date': start_date,
            'end_date': end_date,
            'source_bundle': source_bundle,
            'metadata': metadata if isinstance(metadata, dict) else {},
            'is_synced': bool(is_synced),
            'created_at': created_at if created_at else processing_time,
            'payload_timestamp': payload_timestamp,
            'app_version': app_version,
            'processing_time': processing_time,
            # Timezone information
            'timezone': resolved_timezone,
            'timezone_offset': timezone_offset,
        }
        
        logger.debug(
            f"Transformed sample: user={user_id}, type={data_type}, "
            f"sample_id={sample_id}, timezone={resolved_timezone}"
        )
        
        return row
    
    @staticmethod
    def _resolve_timezone(timezone: Optional[str], timezone_offset: Optional[int]) -> str:
        """
        Resolve timezone from available information.
        
        Uses timezone utility to validate and resolve IANA timezone identifiers.
        
        Args:
            timezone: IANA timezone identifier from iOS (e.g., "Asia/Seoul")
            timezone_offset: Timezone offset in minutes from GMT
            
        Returns:
            Resolved IANA timezone identifier
        """
        from flink_consumer.utils.timezone_utils import resolve_timezone
        
        return resolve_timezone(
            timezone=timezone,
            timezone_offset=timezone_offset,
            default_timezone='UTC'
        )

    @staticmethod
    def _parse_timestamp(iso_string: Optional[str]) -> Optional[int]:
        """
        Convert ISO 8601 string to Unix timestamp (milliseconds).
        
        Supports formats:
        - 2025-11-15T10:00:00Z
        - 2025-11-15T10:00:00.123Z
        - 2025-11-15T10:00:00+00:00
        - 2025-11-15T10:00:00.123+00:00
        
        Args:
            iso_string: ISO 8601 formatted timestamp string
            
        Returns:
            Unix timestamp in milliseconds, or None if parsing fails
        """
        if not iso_string:
            return None
        
        try:
            # Replace 'Z' with '+00:00' for proper timezone handling
            normalized = iso_string.replace('Z', '+00:00')
            
            # Parse ISO 8601 string
            dt = datetime.fromisoformat(normalized)
            
            # Convert to Unix timestamp (milliseconds)
            timestamp_ms = int(dt.timestamp() * 1000)
            
            return timestamp_ms
            
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse timestamp '{iso_string}': {e}")
            return None


def create_transformer() -> HealthDataTransformer:
    """
    Factory function to create a HealthDataTransformer instance.
    
    Returns:
        HealthDataTransformer instance
    """
    return HealthDataTransformer()
