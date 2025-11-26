"""
Validation logic for aggregated health data.

This module provides validation functions to ensure data quality and
consistency of aggregation results before writing to Iceberg tables.
"""

from typing import Dict, Any, Optional
import logging
import math

logger = logging.getLogger(__name__)


class AggregationValidator:
    """
    Validator for aggregation results.
    
    Implements PyFlink's FilterFunction interface to validate aggregation
    results before writing to Iceberg.
    """
    
    def __init__(self, enable_anomaly_detection: bool = True):
        """
        Initialize validator.
        
        Args:
            enable_anomaly_detection: Whether to flag anomalies
        """
        self.enable_anomaly_detection = enable_anomaly_detection
        self.validation_errors = 0
        self.validation_warnings = 0
    
    def filter(self, agg: Dict[str, Any]) -> bool:
        """
        Validate aggregation result.
        
        This method is called by PyFlink's filter() operation.
        
        Args:
            agg: Aggregation result dictionary
            
        Returns:
            True if valid and should be kept, False if invalid and should be filtered out
        """
        if agg is None:
            logger.error("Received None aggregation result")
            self.validation_errors += 1
            return False
        
        # Validate record count
        if not self._validate_record_count(agg):
            self.validation_errors += 1
            return False
        
        # Validate value ranges
        if not self._validate_value_ranges(agg):
            self.validation_errors += 1
            return False
        
        # Validate statistical consistency
        if not self._validate_statistical_consistency(agg):
            self.validation_errors += 1
            return False
        
        # Check for anomalies (warning only, doesn't filter out)
        if self.enable_anomaly_detection:
            if self._detect_anomalies(agg):
                agg['has_anomaly'] = True
                self.validation_warnings += 1
            else:
                agg['has_anomaly'] = False
        
        return True
    
    def _validate_record_count(self, agg: Dict[str, Any]) -> bool:
        """
        Validate that record count is positive.
        
        Args:
            agg: Aggregation result
            
        Returns:
            True if valid, False otherwise
        """
        record_count = agg.get('record_count', 0)
        count = agg.get('count', 0)
        
        if record_count <= 0:
            logger.error(f"Invalid record_count: {record_count} for "
                        f"user={agg.get('user_id')}, type={agg.get('data_type')}")
            return False
        
        if count <= 0:
            logger.error(f"Invalid count: {count} for "
                        f"user={agg.get('user_id')}, type={agg.get('data_type')}")
            return False
        
        return True
    
    def _validate_value_ranges(self, agg: Dict[str, Any]) -> bool:
        """
        Validate value ranges based on data type.
        
        Args:
            agg: Aggregation result
            
        Returns:
            True if valid, False otherwise
        """
        data_type = agg.get('data_type')
        min_value = agg.get('min_value')
        max_value = agg.get('max_value')
        
        if min_value is None or max_value is None:
            logger.error(f"Missing min/max values for {data_type}")
            return False
        
        # Check for NaN or Inf
        if math.isnan(min_value) or math.isinf(min_value):
            logger.error(f"Invalid min_value: {min_value} for {data_type}")
            return False
        
        if math.isnan(max_value) or math.isinf(max_value):
            logger.error(f"Invalid max_value: {max_value} for {data_type}")
            return False
        
        # Data type specific validation
        if data_type == 'heartRate':
            if not (30 <= min_value <= 250 and 30 <= max_value <= 250):
                logger.error(f"Heart rate out of valid range: [{min_value}, {max_value}]")
                return False
        
        elif data_type == 'steps':
            if min_value < 0 or max_value > 100000:
                logger.error(f"Steps out of valid range: [{min_value}, {max_value}]")
                return False
        
        elif data_type == 'bloodPressureSystolic':
            if not (70 <= min_value <= 200 and 70 <= max_value <= 200):
                logger.error(f"Blood pressure out of valid range: [{min_value}, {max_value}]")
                return False
        
        elif data_type == 'bloodPressureDiastolic':
            if not (40 <= min_value <= 130 and 40 <= max_value <= 130):
                logger.error(f"Blood pressure diastolic out of valid range: [{min_value}, {max_value}]")
                return False
        
        elif data_type == 'bloodGlucose':
            if not (50 <= min_value <= 400 and 50 <= max_value <= 400):
                logger.error(f"Blood glucose out of valid range: [{min_value}, {max_value}]")
                return False
        
        elif data_type == 'bodyTemperature':
            if not (35.0 <= min_value <= 42.0 and 35.0 <= max_value <= 42.0):
                logger.error(f"Body temperature out of valid range: [{min_value}, {max_value}]")
                return False
        
        elif data_type == 'oxygenSaturation':
            if not (70 <= min_value <= 100 and 70 <= max_value <= 100):
                logger.error(f"Oxygen saturation out of valid range: [{min_value}, {max_value}]")
                return False
        
        return True
    
    def _validate_statistical_consistency(self, agg: Dict[str, Any]) -> bool:
        """
        Validate statistical consistency (min <= avg <= max).
        
        Args:
            agg: Aggregation result
            
        Returns:
            True if valid, False otherwise
        """
        min_value = agg.get('min_value')
        max_value = agg.get('max_value')
        avg_value = agg.get('avg_value')
        
        if min_value is None or max_value is None or avg_value is None:
            logger.error("Missing statistical values")
            return False
        
        # Check min <= max
        if min_value > max_value:
            logger.error(f"Min > Max: {min_value} > {max_value} for "
                        f"user={agg.get('user_id')}, type={agg.get('data_type')}")
            return False
        
        # Check min <= avg <= max
        if not (min_value <= avg_value <= max_value):
            logger.error(f"Avg outside range: {avg_value} not in [{min_value}, {max_value}] for "
                        f"user={agg.get('user_id')}, type={agg.get('data_type')}")
            return False
        
        # Check stddev is non-negative
        stddev = agg.get('stddev_value')
        if stddev is not None and stddev < 0:
            logger.error(f"Negative stddev: {stddev}")
            return False
        
        return True
    
    def _detect_anomalies(self, agg: Dict[str, Any]) -> bool:
        """
        Detect anomalies in aggregation results.
        
        This method flags potential anomalies but doesn't filter them out.
        
        Args:
            agg: Aggregation result
            
        Returns:
            True if anomaly detected, False otherwise
        """
        data_type = agg.get('data_type')
        min_value = agg.get('min_value')
        max_value = agg.get('max_value')
        avg_value = agg.get('avg_value')
        stddev = agg.get('stddev_value', 0)
        
        # Check for extreme values
        if data_type == 'heartRate':
            # Flag if heart rate is consistently very high or very low
            if avg_value > 180 or avg_value < 40:
                logger.warning(f"Anomaly: Unusual average heart rate: {avg_value}")
                return True
            
            # Flag if range is very large (might indicate measurement errors)
            if (max_value - min_value) > 100:
                logger.warning(f"Anomaly: Large heart rate range: {max_value - min_value}")
                return True
        
        elif data_type == 'steps':
            # Flag if steps are unusually high
            if avg_value > 50000:
                logger.warning(f"Anomaly: Unusually high step count: {avg_value}")
                return True
        
        elif data_type == 'bloodPressureSystolic':
            # Flag if blood pressure is consistently high or low
            if avg_value > 160 or avg_value < 90:
                logger.warning(f"Anomaly: Unusual blood pressure: {avg_value}")
                return True
        
        # Check for zero variance (all values identical)
        if stddev == 0 and agg.get('count', 0) > 1:
            logger.warning(f"Anomaly: Zero variance for {data_type} with {agg.get('count')} records")
            return True
        
        # Check for very high variance
        if stddev > 0 and avg_value > 0:
            coefficient_of_variation = stddev / avg_value
            if coefficient_of_variation > 1.0:  # Stddev > mean
                logger.warning(f"Anomaly: High coefficient of variation: {coefficient_of_variation}")
                return True
        
        return False
    
    def get_validation_stats(self) -> Dict[str, int]:
        """
        Get validation statistics.
        
        Returns:
            Dictionary with error and warning counts
        """
        return {
            'validation_errors': self.validation_errors,
            'validation_warnings': self.validation_warnings,
        }


class DataQualityChecker:
    """
    Additional data quality checks for aggregations.
    """
    
    @staticmethod
    def check_completeness(agg: Dict[str, Any]) -> Dict[str, bool]:
        """
        Check completeness of aggregation fields.
        
        Args:
            agg: Aggregation result
            
        Returns:
            Dictionary with completeness flags
        """
        required_fields = [
            'user_id', 'data_type', 'count', 'min_value', 'max_value',
            'avg_value', 'sum_value', 'stddev_value', 'record_count',
            'window_start', 'window_end', 'updated_at'
        ]
        
        completeness = {}
        for field in required_fields:
            completeness[field] = field in agg and agg[field] is not None
        
        return completeness
    
    @staticmethod
    def check_timeliness(agg: Dict[str, Any], max_delay_hours: int = 24) -> bool:
        """
        Check if aggregation is timely (not too delayed).
        
        Args:
            agg: Aggregation result
            max_delay_hours: Maximum allowed delay in hours
            
        Returns:
            True if timely, False if delayed
        """
        import time
        
        window_end = agg.get('window_end')
        if window_end is None:
            return False
        
        current_time = int(time.time() * 1000)
        delay_ms = current_time - window_end
        delay_hours = delay_ms / (1000 * 60 * 60)
        
        if delay_hours > max_delay_hours:
            logger.warning(f"Aggregation delayed by {delay_hours:.1f} hours")
            return False
        
        return True
    
    @staticmethod
    def calculate_quality_score(agg: Dict[str, Any]) -> float:
        """
        Calculate overall quality score for aggregation.
        
        Args:
            agg: Aggregation result
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        score = 1.0
        
        # Check completeness
        completeness = DataQualityChecker.check_completeness(agg)
        completeness_ratio = sum(completeness.values()) / len(completeness)
        score *= completeness_ratio
        
        # Check timeliness
        if not DataQualityChecker.check_timeliness(agg):
            score *= 0.9  # 10% penalty for delayed data
        
        # Check for anomalies
        if agg.get('has_anomaly', False):
            score *= 0.95  # 5% penalty for anomalies
        
        # Check record count (prefer more data)
        record_count = agg.get('record_count', 0)
        if record_count < 10:
            score *= 0.9  # 10% penalty for low sample size
        
        return max(0.0, min(1.0, score))
