"""Tests for parameter validation."""

import pytest
from health_data_mcp.exceptions import ValidationError


class TestValidationError:
    """Tests for ValidationError exception class."""
    
    def test_validation_error_with_message_only(self):
        """Test ValidationError with message only."""
        error = ValidationError("Test error message")
        
        assert error.message == "Test error message"
        assert error.details == {}
        assert str(error) == "Test error message"
    
    def test_validation_error_with_details(self):
        """Test ValidationError with message and details."""
        details = {
            "field": "user_id",
            "provided": "",
            "expected": "non-empty string"
        }
        error = ValidationError("Required parameter missing", details=details)
        
        assert error.message == "Required parameter missing"
        assert error.details == details
        assert error.details["field"] == "user_id"
    
    def test_validation_error_to_dict(self):
        """Test ValidationError to_dict method."""
        details = {
            "field": "start_date",
            "provided": "2025/11/17",
            "expected": "YYYY-MM-DD"
        }
        error = ValidationError("Invalid date format", details=details)
        
        error_dict = error.to_dict()
        
        assert "error" in error_dict
        assert error_dict["error"]["type"] == "ValidationError"
        assert error_dict["error"]["message"] == "Invalid date format"
        assert error_dict["error"]["details"] == details
    
    def test_validation_error_to_dict_without_details(self):
        """Test ValidationError to_dict method without details."""
        error = ValidationError("Simple error")
        
        error_dict = error.to_dict()
        
        assert "error" in error_dict
        assert error_dict["error"]["type"] == "ValidationError"
        assert error_dict["error"]["message"] == "Simple error"
        assert "details" not in error_dict["error"]
    
    def test_validation_error_can_be_raised(self):
        """Test ValidationError can be raised and caught."""
        with pytest.raises(ValidationError) as exc_info:
            raise ValidationError("Test error")
        
        assert "Test error" in str(exc_info.value)
    
    def test_validation_error_inheritance(self):
        """Test ValidationError inherits from Exception."""
        error = ValidationError("Test")
        assert isinstance(error, Exception)


class TestParameterValidationScenarios:
    """Tests for common parameter validation scenarios."""
    
    def test_missing_required_parameter(self):
        """Test validation error for missing required parameter."""
        error = ValidationError(
            "Required parameter 'user_id' is missing or empty",
            details={
                "field": "user_id",
                "provided": "null",
                "expected": "non-empty string"
            }
        )
        
        error_dict = error.to_dict()
        assert error_dict["error"]["details"]["field"] == "user_id"
    
    def test_invalid_date_format(self):
        """Test validation error for invalid date format."""
        error = ValidationError(
            "Invalid date format for start_date",
            details={
                "field": "start_date",
                "provided": "2025/11/17",
                "expected": "YYYY-MM-DD (e.g., 2025-11-17)"
            }
        )
        
        error_dict = error.to_dict()
        assert "YYYY-MM-DD" in error_dict["error"]["details"]["expected"]
    
    def test_invalid_date_range(self):
        """Test validation error for invalid date range."""
        error = ValidationError(
            "Invalid date range: start_date must be before or equal to end_date",
            details={
                "field": "date_range",
                "provided": "start=2025-11-30, end=2025-11-01",
                "expected": "start_date <= end_date"
            }
        )
        
        error_dict = error.to_dict()
        assert "start_date <= end_date" in error_dict["error"]["details"]["expected"]
    
    def test_invalid_week_format(self):
        """Test validation error for invalid week format."""
        error = ValidationError(
            "Invalid week format for start_week",
            details={
                "field": "start_week",
                "provided": "2025-40",
                "expected": "YYYY-Www (e.g., 2025-W46, week 1-53)"
            }
        )
        
        error_dict = error.to_dict()
        assert "YYYY-Www" in error_dict["error"]["details"]["expected"]
    
    def test_invalid_month_format(self):
        """Test validation error for invalid month format."""
        error = ValidationError(
            "Invalid month format for start_month",
            details={
                "field": "start_month",
                "provided": "2025/11",
                "expected": "YYYY-MM (e.g., 2025-11)"
            }
        )
        
        error_dict = error.to_dict()
        assert "YYYY-MM" in error_dict["error"]["details"]["expected"]
    
    def test_invalid_sort_by(self):
        """Test validation error for invalid sort_by value."""
        error = ValidationError(
            "Invalid sort_by value",
            details={
                "field": "sort_by",
                "provided": "invalid_field",
                "expected": "one of: max_value, avg_value, sum_value, count"
            }
        )
        
        error_dict = error.to_dict()
        assert "max_value" in error_dict["error"]["details"]["expected"]
    
    def test_invalid_order(self):
        """Test validation error for invalid order value."""
        error = ValidationError(
            "Invalid order value",
            details={
                "field": "order",
                "provided": "invalid",
                "expected": "one of: asc, desc"
            }
        )
        
        error_dict = error.to_dict()
        assert "asc, desc" in error_dict["error"]["details"]["expected"]
    
    def test_invalid_limit(self):
        """Test validation error for invalid limit value."""
        error = ValidationError(
            "Invalid limit value",
            details={
                "field": "limit",
                "provided": "20000",
                "expected": "integer between 1 and 10000"
            }
        )
        
        error_dict = error.to_dict()
        assert "1 and 10000" in error_dict["error"]["details"]["expected"]
    
    def test_multiple_validation_errors_can_be_created(self):
        """Test multiple validation errors can be created independently."""
        error1 = ValidationError("Error 1", details={"field": "field1"})
        error2 = ValidationError("Error 2", details={"field": "field2"})
        
        assert error1.message != error2.message
        assert error1.details["field"] != error2.details["field"]
