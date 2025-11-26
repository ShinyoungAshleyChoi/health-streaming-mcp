"""Simple validation test for logging configuration."""

import os
import sys
import tempfile
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from health_data_mcp.logging_config import (
    setup_logging,
    get_logger,
    log_tool_call,
    log_tool_result,
    log_error,
    SensitiveDataFilter,
)


def test_logging_setup():
    """Test basic logging setup"""
    print("Testing logging setup...")
    
    # Test human format
    setup_logging(log_level="INFO", log_format="human")
    logger = get_logger("test")
    logger.info("Test message - human format")
    print("✓ Human format logging works")
    
    # Test JSON format
    setup_logging(log_level="INFO", log_format="json")
    logger = get_logger("test")
    logger.info("Test message - JSON format")
    print("✓ JSON format logging works")
    
    # Test with log file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.log') as f:
        log_file = f.name
    
    try:
        setup_logging(log_level="DEBUG", log_format="human", log_file=log_file)
        logger = get_logger("test")
        logger.debug("Test debug message")
        logger.info("Test info message")
        
        # Check file was created and has content
        assert Path(log_file).exists(), "Log file was not created"
        with open(log_file, 'r') as f:
            content = f.read()
            assert len(content) > 0, "Log file is empty"
            assert "Test info message" in content, "Log message not in file"
        
        print("✓ File logging works")
    finally:
        # Cleanup
        if Path(log_file).exists():
            Path(log_file).unlink()


def test_sensitive_data_masking():
    """Test sensitive data masking"""
    print("\nTesting sensitive data masking...")
    
    filter = SensitiveDataFilter()
    
    # Test masking in text
    text = "password=secret123 and api_key=abc123"
    masked = filter._mask_sensitive_data(text)
    assert "secret123" not in masked, "Password not masked"
    assert "abc123" not in masked, "API key not masked"
    assert "***MASKED***" in masked, "Mask placeholder not found"
    print("✓ Text masking works")
    
    # Test masking in dict
    data = {
        "user_id": "user-123",
        "password": "secret123",
        "s3_secret_key": "aws-secret",
        "data_type": "heartRate"
    }
    masked_dict = filter._mask_dict(data)
    assert masked_dict["user_id"] == "user-123", "Non-sensitive data was masked"
    assert masked_dict["password"] == "***MASKED***", "Password not masked in dict"
    assert masked_dict["s3_secret_key"] == "***MASKED***", "S3 secret not masked in dict"
    assert masked_dict["data_type"] == "heartRate", "Non-sensitive data was masked"
    print("✓ Dictionary masking works")


def test_logging_helpers():
    """Test logging helper functions"""
    print("\nTesting logging helpers...")
    
    setup_logging(log_level="INFO", log_format="human")
    logger = get_logger("test")
    
    # Test tool call logging
    log_tool_call(logger, "test_tool", {
        "user_id": "user-123",
        "password": "secret",  # Should be masked
        "data_type": "heartRate"
    })
    print("✓ Tool call logging works")
    
    # Test tool result logging
    log_tool_result(logger, "test_tool", 10, 123.45)
    print("✓ Tool result logging works")
    
    # Test error logging
    try:
        raise ValueError("Test error")
    except Exception as e:
        log_error(logger, e, {"user_id": "user-123", "api_key": "secret"})
    print("✓ Error logging works")


def test_environment_based_config():
    """Test environment-based configuration"""
    print("\nTesting environment-based configuration...")
    
    # Test development environment
    os.environ["ENVIRONMENT"] = "development"
    os.environ["LOG_LEVEL"] = "INFO"
    setup_logging()
    logger = get_logger("test")
    logger.debug("This should appear in development mode")
    print("✓ Development environment config works")
    
    # Test production environment
    os.environ["ENVIRONMENT"] = "production"
    os.environ["LOG_LEVEL"] = "INFO"
    setup_logging()
    logger = get_logger("test")
    logger.info("This should appear in production mode")
    print("✓ Production environment config works")
    
    # Cleanup
    os.environ.pop("ENVIRONMENT", None)
    os.environ.pop("LOG_LEVEL", None)


def main():
    """Run all validation tests"""
    print("=" * 60)
    print("Health Data MCP Server - Logging Configuration Validation")
    print("=" * 60)
    
    try:
        test_logging_setup()
        test_sensitive_data_masking()
        test_logging_helpers()
        test_environment_based_config()
        
        print("\n" + "=" * 60)
        print("✓ All logging configuration tests passed!")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
