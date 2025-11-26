"""Health data validation module for data quality checks"""

import logging
from typing import Any, Dict, Optional

from pyflink.datastream import FilterFunction

logger = logging.getLogger(__name__)


class HealthDataValidator(FilterFunction):
    """
    Validate health data rows for data quality and integrity.
    
    Performs validation checks on transformed health data rows including:
    - Required field presence
    - Value range validation
    - Date range validation
    - Data type specific validation
    
    Requirements: 2.1, 2.2
    """

    # Data type specific validation ranges
    VALIDATION_RANGES = {
        'heartRate': {'min': 30, 'max': 250, 'unit': 'count/min'},
        'steps': {'min': 0, 'max': 100000, 'unit': 'count'},
        'distance': {'min': 0, 'max': 100000, 'unit': 'm'},
        'activeEnergyBurned': {'min': 0, 'max': 10000, 'unit': 'kcal'},
        'basalEnergyBurned': {'min': 0, 'max': 5000, 'unit': 'kcal'},
        'bloodPressureSystolic': {'min': 50, 'max': 250, 'unit': 'mmHg'},
        'bloodPressureDiastolic': {'min': 30, 'max': 150, 'unit': 'mmHg'},
        'bloodGlucose': {'min': 20, 'max': 600, 'unit': 'mg/dL'},
        'bodyTemperature': {'min': 30, 'max': 45, 'unit': 'degC'},
        'oxygenSaturation': {'min': 0, 'max': 100, 'unit': '%'},
        'respiratoryRate': {'min': 5, 'max': 60, 'unit': 'count/min'},
        'vo2Max': {'min': 10, 'max': 100, 'unit': 'mL/(kg·min)'},
        'bodyMass': {'min': 20, 'max': 500, 'unit': 'kg'},
        'height': {'min': 50, 'max': 300, 'unit': 'cm'},
        'bodyMassIndex': {'min': 10, 'max': 100, 'unit': 'count'},
        'bodyFatPercentage': {'min': 0, 'max': 100, 'unit': '%'},
        'leanBodyMass': {'min': 10, 'max': 300, 'unit': 'kg'},
        'waistCircumference': {'min': 30, 'max': 300, 'unit': 'cm'},
        'sleepAnalysis': {'min': 0, 'max': 24, 'unit': 'hr'},
        'mindfulSession': {'min': 0, 'max': 24, 'unit': 'hr'},
    }

    def __init__(self, strict_mode: bool = False):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, reject records with unknown data types.
                        If False, allow unknown data types with basic validation.
        """
        self.strict_mode = strict_mode
        self.validation_count = 0
        self.rejection_count = 0

    def filter(self, row: Dict[str, Any]) -> bool:
        """
        Validate row data.
        
        Args:
            row: Health data row dictionary
            
        Returns:
            True if valid, False otherwise
        """
        self.validation_count += 1
        
        # Check required fields
        if not self._validate_required_fields(row):
            self.rejection_count += 1
            return False
        
        # Check value is non-negative (basic check)
        if not self._validate_value_non_negative(row):
            self.rejection_count += 1
            return False
        
        # Check date range
        if not self._validate_date_range(row):
            self.rejection_count += 1
            return False
        
        # Check data type specific validation
        if not self._validate_data_type_specific(row):
            self.rejection_count += 1
            return False
        
        # Log validation stats periodically
        if self.validation_count % 10000 == 0:
            rejection_rate = (self.rejection_count / self.validation_count) * 100
            logger.info(
                f"Validation stats: {self.validation_count} records processed, "
                f"{self.rejection_count} rejected ({rejection_rate:.2f}%)"
            )
        
        return True

    def _validate_required_fields(self, row: Dict[str, Any]) -> bool:
        """
        Validate that all required fields are present and non-empty.
        
        Args:
            row: Health data row
            
        Returns:
            True if all required fields are present, False otherwise
        """
        required_fields = [
            'user_id',
            'sample_id',
            'data_type',
            'value',
            'start_date',
            'end_date'
        ]
        
        for field in required_fields:
            if field not in row or row[field] is None:
                self._log_validation_error(
                    f"Missing required field: {field}",
                    row
                )
                return False
            
            # Check for empty strings
            if isinstance(row[field], str) and not row[field].strip():
                self._log_validation_error(
                    f"Empty required field: {field}",
                    row
                )
                return False
        
        return True

    def _validate_value_non_negative(self, row: Dict[str, Any]) -> bool:
        """
        Validate that the value is non-negative.
        
        Most health metrics should be non-negative values.
        
        Args:
            row: Health data row
            
        Returns:
            True if value is non-negative, False otherwise
        """
        value = row.get('value')
        
        if value is None:
            return False
        
        try:
            numeric_value = float(value)
            if numeric_value < 0:
                self._log_validation_error(
                    f"Negative value: {numeric_value}",
                    row
                )
                return False
        except (ValueError, TypeError):
            self._log_validation_error(
                f"Invalid numeric value: {value}",
                row
            )
            return False
        
        return True

    def _validate_date_range(self, row: Dict[str, Any]) -> bool:
        """
        Validate that date range is logical (start_date <= end_date).
        
        Args:
            row: Health data row
            
        Returns:
            True if date range is valid, False otherwise
        """
        start_date = row.get('start_date', 0)
        end_date = row.get('end_date', 0)
        
        if start_date > end_date:
            self._log_validation_error(
                f"Invalid date range: start_date ({start_date}) > end_date ({end_date})",
                row
            )
            return False
        
        # Check for unreasonably long duration (> 7 days in milliseconds)
        max_duration_ms = 7 * 24 * 60 * 60 * 1000
        duration = end_date - start_date
        
        if duration > max_duration_ms:
            self._log_validation_error(
                f"Unreasonably long duration: {duration}ms (> 7 days)",
                row,
                level='warning'
            )
            # Don't reject, just warn
        
        return True

    def _validate_data_type_specific(self, row: Dict[str, Any]) -> bool:
        """
        Validate value based on data type specific ranges.
        
        Args:
            row: Health data row
            
        Returns:
            True if value is within expected range, False otherwise
        """
        data_type = row.get('data_type')
        value = row.get('value')
        
        # If data type not in validation ranges
        if data_type not in self.VALIDATION_RANGES:
            if self.strict_mode:
                self._log_validation_error(
                    f"Unknown data type: {data_type}",
                    row
                )
                return False
            else:
                # In non-strict mode, allow unknown types with basic validation
                logger.debug(f"Unknown data type '{data_type}', skipping range validation")
                return True
        
        # Get validation range for this data type
        validation_range = self.VALIDATION_RANGES[data_type]
        min_value = validation_range['min']
        max_value = validation_range['max']
        
        try:
            numeric_value = float(value)
            
            if numeric_value < min_value or numeric_value > max_value:
                self._log_validation_error(
                    f"Value out of range for {data_type}: {numeric_value} "
                    f"(expected {min_value}-{max_value})",
                    row
                )
                return False
                
        except (ValueError, TypeError):
            self._log_validation_error(
                f"Invalid numeric value for {data_type}: {value}",
                row
            )
            return False
        
        return True

    def _log_validation_error(
        self,
        reason: str,
        row: Dict[str, Any],
        level: str = 'warning'
    ) -> None:
        """
        Log validation error with context.
        
        Args:
            reason: Reason for validation failure
            row: Health data row that failed validation
            level: Log level ('warning', 'error', 'info')
        """
        log_data = {
            'sample_id': row.get('sample_id', 'unknown'),
            'user_id': row.get('user_id', 'unknown'),
            'data_type': row.get('data_type', 'unknown'),
            'value': row.get('value'),
            'reason': reason
        }
        
        log_message = f"Validation failed: {reason}"
        
        if level == 'error':
            logger.error(log_message, extra=log_data)
        elif level == 'info':
            logger.info(log_message, extra=log_data)
        else:  # warning
            logger.warning(log_message, extra=log_data)


def create_validator(strict_mode: bool = False) -> HealthDataValidator:
    """
    Factory function to create a HealthDataValidator instance.
    
    Args:
        strict_mode: If True, reject records with unknown data types
        
    Returns:
        HealthDataValidator instance
    """
    return HealthDataValidator(strict_mode=strict_mode)
