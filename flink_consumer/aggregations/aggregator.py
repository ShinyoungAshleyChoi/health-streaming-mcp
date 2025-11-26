"""
Aggregation functions for health data statistics.

This module implements the core aggregation logic for computing statistics
(min, max, avg, sum, count, stddev) over windowed health data.
"""

from typing import Tuple, List, Optional, Dict, Any
import logging
import math

logger = logging.getLogger(__name__)


class HealthDataAccumulator:
    """
    Accumulator for health data aggregation.
    
    Maintains running statistics for efficient computation of min, max, avg,
    sum, count, and standard deviation.
    
    Attributes:
        count: Number of values processed
        sum_value: Sum of all values
        min_value: Minimum value seen
        max_value: Maximum value seen
        sum_of_squares: Sum of squared values (for stddev calculation)
        values: List of all values (for first/last tracking)
    """
    
    def __init__(self):
        self.count: int = 0
        self.sum_value: float = 0.0
        self.min_value: float = float('inf')
        self.max_value: float = float('-inf')
        self.sum_of_squares: float = 0.0
        self.values: List[float] = []
    
    def add(self, value: float):
        """
        Add a value to the accumulator.
        
        Args:
            value: Numeric value to add
        """
        self.count += 1
        self.sum_value += value
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)
        self.sum_of_squares += value * value
        self.values.append(value)
    
    def merge(self, other: 'HealthDataAccumulator'):
        """
        Merge another accumulator into this one.
        
        Used for parallel processing where multiple accumulators need to be combined.
        
        Args:
            other: Another HealthDataAccumulator to merge
        """
        self.count += other.count
        self.sum_value += other.sum_value
        self.min_value = min(self.min_value, other.min_value)
        self.max_value = max(self.max_value, other.max_value)
        self.sum_of_squares += other.sum_of_squares
        self.values.extend(other.values)
    
    def get_result(self) -> Optional[Dict[str, Any]]:
        """
        Calculate final statistics from accumulator.
        
        Returns:
            Dictionary with computed statistics, or None if no data
        """
        if self.count == 0:
            logger.warning("Attempting to get result from empty accumulator")
            return None
        
        avg_value = self.sum_value / self.count
        
        # Calculate standard deviation
        # stddev = sqrt(E[X^2] - (E[X])^2)
        variance = (self.sum_of_squares / self.count) - (avg_value * avg_value)
        stddev_value = math.sqrt(variance) if variance > 0 else 0.0
        
        return {
            'count': self.count,
            'sum_value': self.sum_value,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'avg_value': avg_value,
            'stddev_value': stddev_value,
            'first_value': self.values[0] if self.values else None,
            'last_value': self.values[-1] if self.values else None,
            'record_count': len(self.values),
        }
    
    def to_tuple(self) -> Tuple:
        """
        Convert accumulator to tuple for serialization.
        
        Returns:
            Tuple representation of accumulator state
        """
        return (
            self.count,
            self.sum_value,
            self.min_value,
            self.max_value,
            self.sum_of_squares,
            self.values
        )
    
    @classmethod
    def from_tuple(cls, data: Tuple) -> 'HealthDataAccumulator':
        """
        Create accumulator from tuple.
        
        Args:
            data: Tuple with accumulator state
            
        Returns:
            HealthDataAccumulator instance
        """
        acc = cls()
        acc.count = data[0]
        acc.sum_value = data[1]
        acc.min_value = data[2]
        acc.max_value = data[3]
        acc.sum_of_squares = data[4]
        acc.values = data[5]
        return acc


class HealthDataAggregator:
    """
    PyFlink AggregateFunction for health data statistics.
    
    This class implements the AggregateFunction interface for use with
    PyFlink's windowed aggregations.
    
    Note: This is a wrapper that can be used with PyFlink's aggregate() method.
    The actual implementation uses HealthDataAccumulator for state management.
    """
    
    def create_accumulator(self) -> Tuple:
        """
        Create a new accumulator.
        
        Returns:
            Empty accumulator as tuple
        """
        acc = HealthDataAccumulator()
        return acc.to_tuple()
    
    def add(self, value: Dict[str, Any], accumulator: Tuple) -> Tuple:
        """
        Add a value to the accumulator.
        
        Args:
            value: Health data record dictionary
            accumulator: Current accumulator state as tuple
            
        Returns:
            Updated accumulator as tuple
        """
        acc = HealthDataAccumulator.from_tuple(accumulator)
        
        # Extract numeric value from record
        numeric_value = value.get('value')
        if numeric_value is None:
            logger.warning(f"Missing 'value' field in record: {value.get('sample_id', 'unknown')}")
            return accumulator
        
        try:
            numeric_value = float(numeric_value)
            acc.add(numeric_value)
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid numeric value: {numeric_value}, error: {e}")
            return accumulator
        
        return acc.to_tuple()
    
    def get_result(self, accumulator: Tuple) -> Optional[Dict[str, Any]]:
        """
        Get final aggregation result.
        
        Args:
            accumulator: Accumulator state as tuple
            
        Returns:
            Dictionary with computed statistics, or None if no data
        """
        acc = HealthDataAccumulator.from_tuple(accumulator)
        return acc.get_result()
    
    def merge(self, acc1: Tuple, acc2: Tuple) -> Tuple:
        """
        Merge two accumulators.
        
        Used for parallel processing where multiple task instances need to
        combine their partial results.
        
        Args:
            acc1: First accumulator as tuple
            acc2: Second accumulator as tuple
            
        Returns:
            Merged accumulator as tuple
        """
        accumulator1 = HealthDataAccumulator.from_tuple(acc1)
        accumulator2 = HealthDataAccumulator.from_tuple(acc2)
        
        accumulator1.merge(accumulator2)
        
        return accumulator1.to_tuple()


class IncrementalAggregator:
    """
    Incremental aggregator for efficient updates.
    
    Supports adding and removing values for sliding window scenarios
    or late data updates.
    """
    
    def __init__(self):
        self.accumulator = HealthDataAccumulator()
    
    def add_value(self, value: float):
        """
        Add a value to the aggregation.
        
        Args:
            value: Numeric value to add
        """
        self.accumulator.add(value)
    
    def get_statistics(self) -> Optional[Dict[str, Any]]:
        """
        Get current statistics.
        
        Returns:
            Dictionary with computed statistics
        """
        return self.accumulator.get_result()
    
    def reset(self):
        """Reset the aggregator to initial state."""
        self.accumulator = HealthDataAccumulator()


class AggregationValidator:
    """
    Validator for aggregation results.
    
    Ensures that computed statistics are valid and consistent.
    """
    
    @staticmethod
    def validate(agg_result: Dict[str, Any]) -> bool:
        """
        Validate aggregation result.
        
        Args:
            agg_result: Aggregation result dictionary
            
        Returns:
            True if valid, False otherwise
        """
        if agg_result is None:
            logger.error("Aggregation result is None")
            return False
        
        # Check required fields
        required_fields = ['count', 'min_value', 'max_value', 'avg_value']
        for field in required_fields:
            if field not in agg_result:
                logger.error(f"Missing required field: {field}")
                return False
        
        # Check count is positive
        if agg_result['count'] <= 0:
            logger.error(f"Invalid count: {agg_result['count']}")
            return False
        
        # Check min <= avg <= max
        min_val = agg_result['min_value']
        max_val = agg_result['max_value']
        avg_val = agg_result['avg_value']
        
        if min_val > max_val:
            logger.error(f"Min > Max: {min_val} > {max_val}")
            return False
        
        if not (min_val <= avg_val <= max_val):
            logger.error(f"Avg outside range: {avg_val} not in [{min_val}, {max_val}]")
            return False
        
        # Check for NaN or Inf
        for field in ['min_value', 'max_value', 'avg_value', 'sum_value', 'stddev_value']:
            value = agg_result.get(field)
            if value is not None and (math.isnan(value) or math.isinf(value)):
                logger.error(f"Invalid value for {field}: {value}")
                return False
        
        return True
    
    @staticmethod
    def validate_with_data_type(agg_result: Dict[str, Any], data_type: str) -> bool:
        """
        Validate aggregation result with data type specific rules.
        
        Args:
            agg_result: Aggregation result dictionary
            data_type: Health data type (e.g., 'heartRate', 'steps')
            
        Returns:
            True if valid, False otherwise
        """
        if not AggregationValidator.validate(agg_result):
            return False
        
        min_val = agg_result['min_value']
        max_val = agg_result['max_value']
        
        # Data type specific validation
        if data_type == 'heartRate':
            if not (30 <= min_val <= 250 and 30 <= max_val <= 250):
                logger.warning(f"Heart rate out of expected range: [{min_val}, {max_val}]")
                return False
        
        elif data_type == 'steps':
            if min_val < 0 or max_val > 100000:
                logger.warning(f"Steps out of expected range: [{min_val}, {max_val}]")
                return False
        
        elif data_type == 'bloodPressureSystolic':
            if not (70 <= min_val <= 200 and 70 <= max_val <= 200):
                logger.warning(f"Blood pressure out of expected range: [{min_val}, {max_val}]")
                return False
        
        elif data_type == 'bloodGlucose':
            if not (50 <= min_val <= 400 and 50 <= max_val <= 400):
                logger.warning(f"Blood glucose out of expected range: [{min_val}, {max_val}]")
                return False
        
        return True
