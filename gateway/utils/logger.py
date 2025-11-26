"""
Structured logging configuration
"""
import logging
import sys
from pythonjsonlogger import jsonlogger
from config import settings


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Custom JSON formatter with additional fields"""
    
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        
        # Add standard fields
        log_record['timestamp'] = self.formatTime(record, self.datefmt)
        log_record['level'] = record.levelname
        log_record['service'] = settings.app_name
        log_record['logger'] = record.name
        
        # Add context fields if available
        if hasattr(record, 'request_id'):
            log_record['requestId'] = record.request_id
        if hasattr(record, 'user_id'):
            log_record['userId'] = record.user_id
        if hasattr(record, 'device_id'):
            log_record['deviceId'] = record.device_id
        if hasattr(record, 'duration'):
            log_record['duration'] = record.duration


def setup_logging():
    """Setup structured logging for the application"""
    
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.log_level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, settings.log_level.upper()))
    
    # Create JSON formatter
    formatter = CustomJsonFormatter(
        '%(timestamp)s %(level)s %(service)s %(logger)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S.%fZ'
    )
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Set log levels for third-party libraries
    logging.getLogger('uvicorn').setLevel(logging.INFO)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    logging.getLogger('confluent_kafka').setLevel(logging.WARNING)
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter for adding context to log messages"""
    
    def process(self, msg, kwargs):
        """Add extra context to log records"""
        # Merge extra fields
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].update(self.extra)
        return msg, kwargs


def get_logger_with_context(name: str, **context) -> LoggerAdapter:
    """
    Get a logger with context
    
    Args:
        name: Logger name
        **context: Context fields to add to all log messages
        
    Returns:
        LoggerAdapter instance
    """
    logger = logging.getLogger(name)
    return LoggerAdapter(logger, context)
