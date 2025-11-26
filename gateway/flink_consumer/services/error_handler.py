"""Error handling and Dead Letter Queue (DLQ) implementation"""

import logging
import time
from typing import Any, Dict, Iterator, Optional, Union

from pyflink.datastream import ProcessFunction, OutputTag

logger = logging.getLogger(__name__)


# Define side output tag for error stream
ERROR_OUTPUT_TAG = OutputTag("error-records", type_info=None)


class ErrorType:
    """Error type constants for categorizing failures"""
    
    VALIDATION_ERROR = "VALIDATION_ERROR"
    TRANSFORMATION_ERROR = "TRANSFORMATION_ERROR"
    DESERIALIZATION_ERROR = "DESERIALIZATION_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


class ValidationProcessFunction(ProcessFunction):
    """
    Process function with error handling via side output pattern.
    
    This function validates health data records and routes invalid records
    to a side output (error stream) while allowing valid records to pass
    through to the main output.
    
    Requirements: 2.3
    """

    def __init__(self, strict_validation: bool = False):
        """
        Initialize validation process function.
        
        Args:
            strict_validation: If True, apply stricter validation rules
        """
        self.strict_validation = strict_validation
        self.processed_count = 0
        self.error_count = 0

    def process_element(
        self,
        row: Dict[str, Any],
        ctx: ProcessFunction.Context
    ) -> Iterator[Union[Dict[str, Any], tuple]]:
        """
        Process and validate a single record.
        
        Args:
            row: Health data row to validate
            ctx: Process function context
            
        Yields:
            Valid records to main output, invalid records to side output
        """
        self.processed_count += 1
        
        try:
            # Validate the record
            if self._validate(row):
                # Valid record - emit to main output
                yield row
            else:
                # Invalid record - emit to side output with error metadata
                error_record = self._create_error_record(
                    row,
                    error_type=ErrorType.VALIDATION_ERROR,
                    error_message="Record failed validation checks"
                )
                yield ERROR_OUTPUT_TAG, error_record
                self.error_count += 1
                
        except Exception as e:
            # Exception during processing - emit to side output
            logger.error(
                f"Exception processing record: {e}",
                exc_info=True,
                extra={'sample_id': row.get('sample_id', 'unknown')}
            )
            
            error_record = self._create_error_record(
                row,
                error_type=ErrorType.PROCESSING_ERROR,
                error_message=f"Processing exception: {str(e)}"
            )
            yield ERROR_OUTPUT_TAG, error_record
            self.error_count += 1
        
        # Log stats periodically
        if self.processed_count % 10000 == 0:
            error_rate = (self.error_count / self.processed_count) * 100
            logger.info(
                f"Processing stats: {self.processed_count} records, "
                f"{self.error_count} errors ({error_rate:.2f}%)"
            )

    def _validate(self, row: Dict[str, Any]) -> bool:
        """
        Validate a health data record.
        
        Args:
            row: Health data row
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        required_fields = ['user_id', 'sample_id', 'data_type', 'value']
        for field in required_fields:
            if field not in row or row[field] is None:
                logger.debug(f"Missing required field: {field}")
                return False
        
        # Check value is non-negative
        try:
            value = float(row.get('value', -1))
            if value < 0:
                logger.debug(f"Negative value: {value}")
                return False
        except (ValueError, TypeError):
            logger.debug(f"Invalid value type: {row.get('value')}")
            return False
        
        # Check date range
        start_date = row.get('start_date', 0)
        end_date = row.get('end_date', 0)
        if start_date > end_date:
            logger.debug(f"Invalid date range: {start_date} > {end_date}")
            return False
        
        return True

    def _create_error_record(
        self,
        original_row: Dict[str, Any],
        error_type: str,
        error_message: str
    ) -> Dict[str, Any]:
        """
        Create an error record with metadata.
        
        Args:
            original_row: Original health data row that failed
            error_type: Type of error (from ErrorType constants)
            error_message: Descriptive error message
            
        Returns:
            Error record with original data and error metadata
        """
        error_timestamp = int(time.time() * 1000)
        
        error_record = {
            # Original data
            'original_data': original_row,
            
            # Error metadata
            'error_type': error_type,
            'error_message': error_message,
            'error_timestamp': error_timestamp,
            
            # Extracted fields for easier querying
            'user_id': original_row.get('user_id'),
            'sample_id': original_row.get('sample_id'),
            'data_type': original_row.get('data_type'),
            'device_id': original_row.get('device_id'),
            
            # Processing context
            'processing_time': original_row.get('processing_time'),
            'payload_timestamp': original_row.get('payload_timestamp'),
        }
        
        return error_record


class ErrorEnricher(ProcessFunction):
    """
    Enrich error records with additional metadata and context.
    
    This function can be used to add more detailed error information,
    categorization, or routing information before writing to error storage.
    """

    def process_element(
        self,
        error_record: Dict[str, Any],
        ctx: ProcessFunction.Context
    ) -> Iterator[Dict[str, Any]]:
        """
        Enrich error record with additional metadata.
        
        Args:
            error_record: Error record to enrich
            ctx: Process function context
            
        Yields:
            Enriched error record
        """
        try:
            # Add enrichment metadata
            error_record['enriched_at'] = int(time.time() * 1000)
            
            # Categorize error severity
            error_type = error_record.get('error_type', ErrorType.UNKNOWN_ERROR)
            error_record['severity'] = self._determine_severity(error_type)
            
            # Add retry eligibility flag
            error_record['retryable'] = self._is_retryable(error_type)
            
            # Extract data type for partitioning
            original_data = error_record.get('original_data', {})
            error_record['data_type_partition'] = original_data.get('data_type', 'unknown')
            
            yield error_record
            
        except Exception as e:
            logger.error(f"Failed to enrich error record: {e}", exc_info=True)
            # Yield original record even if enrichment fails
            yield error_record

    @staticmethod
    def _determine_severity(error_type: str) -> str:
        """
        Determine error severity based on error type.
        
        Args:
            error_type: Error type constant
            
        Returns:
            Severity level: 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
        """
        severity_map = {
            ErrorType.VALIDATION_ERROR: 'LOW',
            ErrorType.TRANSFORMATION_ERROR: 'MEDIUM',
            ErrorType.DESERIALIZATION_ERROR: 'HIGH',
            ErrorType.PROCESSING_ERROR: 'HIGH',
            ErrorType.UNKNOWN_ERROR: 'CRITICAL'
        }
        return severity_map.get(error_type, 'MEDIUM')

    @staticmethod
    def _is_retryable(error_type: str) -> bool:
        """
        Determine if error is retryable.
        
        Args:
            error_type: Error type constant
            
        Returns:
            True if error is retryable, False otherwise
        """
        # Validation errors are typically not retryable (data issue)
        # Processing errors might be retryable (transient issue)
        retryable_types = {
            ErrorType.PROCESSING_ERROR,
            ErrorType.DESERIALIZATION_ERROR
        }
        return error_type in retryable_types


def create_validation_process_function(
    strict_validation: bool = False
) -> ValidationProcessFunction:
    """
    Factory function to create a ValidationProcessFunction.
    
    Args:
        strict_validation: If True, apply stricter validation rules
        
    Returns:
        ValidationProcessFunction instance
    """
    return ValidationProcessFunction(strict_validation=strict_validation)


def create_error_enricher() -> ErrorEnricher:
    """
    Factory function to create an ErrorEnricher.
    
    Returns:
        ErrorEnricher instance
    """
    return ErrorEnricher()


def get_error_output_tag() -> OutputTag:
    """
    Get the error output tag for side output pattern.
    
    Returns:
        OutputTag for error records
    """
    return ERROR_OUTPUT_TAG
