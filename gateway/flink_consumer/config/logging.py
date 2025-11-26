"""
Structured logging configuration for Flink health data consumer.

This module provides JSON-formatted logging with context enrichment
and environment-based log level configuration.
"""

import logging
import json
import sys
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from logging.handlers import RotatingFileHandler


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    
    Formats log records as JSON with standard fields and custom context.
    """
    
    def __init__(
        self,
        include_timestamp: bool = True,
        include_level: bool = True,
        include_logger: bool = True,
        include_thread: bool = True,
    ):
        """
        Initialize JSON formatter.
        
        Args:
            include_timestamp: Include timestamp in output
            include_level: Include log level in output
            include_logger: Include logger name in output
            include_thread: Include thread information in output
        """
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_logger = include_logger
        self.include_thread = include_thread
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data: Dict[str, Any] = {}
        
        # Standard fields
        if self.include_timestamp:
            log_data["timestamp"] = datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat()
        
        if self.include_level:
            log_data["level"] = record.levelname
        
        if self.include_logger:
            log_data["logger"] = record.name
        
        log_data["message"] = record.getMessage()
        
        # Thread information
        if self.include_thread:
            log_data["thread"] = {
                "id": record.thread,
                "name": record.threadName,
            }
        
        # Source location
        log_data["source"] = {
            "file": record.pathname,
            "line": record.lineno,
            "function": record.funcName,
        }
        
        # Process information
        log_data["process"] = {
            "id": record.process,
            "name": record.processName,
        }
        
        # Exception information
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info),
            }
        
        # Custom context from extra fields
        if hasattr(record, "context"):
            log_data["context"] = record.context
        
        # Add any extra fields
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "created", "filename", "funcName",
                "levelname", "levelno", "lineno", "module", "msecs",
                "message", "pathname", "process", "processName", "relativeCreated",
                "thread", "threadName", "exc_info", "exc_text", "stack_info",
                "context"
            ]:
                log_data[key] = value
        
        return json.dumps(log_data, default=str)


class ContextLogger(logging.LoggerAdapter):
    """
    Logger adapter that adds context to log records.
    
    Allows adding persistent context (e.g., user_id, data_type) to all log messages.
    """
    
    def __init__(self, logger: logging.Logger, context: Optional[Dict[str, Any]] = None):
        """
        Initialize context logger.
        
        Args:
            logger: Base logger
            context: Context dictionary to add to all log records
        """
        super().__init__(logger, context or {})
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """
        Process log message and add context.
        
        Args:
            msg: Log message
            kwargs: Keyword arguments
            
        Returns:
            Tuple of (message, kwargs)
        """
        # Add context to extra
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        
        kwargs["extra"]["context"] = {**self.extra, **kwargs["extra"].get("context", {})}
        
        return msg, kwargs


def setup_logging(
    log_level: Optional[str] = None,
    json_format: bool = True,
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    Setup structured logging for the application.
    
    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                  If None, reads from LOG_LEVEL environment variable
        json_format: Use JSON formatting for logs
        log_file: Optional log file path
        max_bytes: Maximum log file size before rotation
        backup_count: Number of backup log files to keep
        
    Returns:
        Configured root logger
    """
    # Determine log level
    if log_level is None:
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Validate log level
    numeric_level = getattr(logging, log_level, logging.INFO)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    if json_format:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Log initial configuration
    root_logger.info(
        "Logging configured",
        extra={
            "context": {
                "log_level": log_level,
                "json_format": json_format,
                "log_file": log_file,
            }
        }
    )
    
    return root_logger


def get_logger(name: str, context: Optional[Dict[str, Any]] = None) -> ContextLogger:
    """
    Get a context-aware logger.
    
    Args:
        name: Logger name (typically __name__)
        context: Optional context dictionary
        
    Returns:
        Context logger instance
    """
    base_logger = logging.getLogger(name)
    return ContextLogger(base_logger, context)


def log_with_context(
    logger: logging.Logger,
    level: str,
    message: str,
    context: Optional[Dict[str, Any]] = None,
    exc_info: bool = False,
):
    """
    Log a message with additional context.
    
    Args:
        logger: Logger instance
        level: Log level (debug, info, warning, error, critical)
        message: Log message
        context: Additional context dictionary
        exc_info: Include exception information
    """
    log_func = getattr(logger, level.lower())
    log_func(message, extra={"context": context or {}}, exc_info=exc_info)


def log_error_with_context(
    logger: logging.Logger,
    message: str,
    error: Exception,
    context: Optional[Dict[str, Any]] = None,
):
    """
    Log an error with exception details and context.
    
    Args:
        logger: Logger instance
        message: Error message
        error: Exception object
        context: Additional context dictionary
    """
    error_context = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        **(context or {})
    }
    
    logger.error(
        message,
        extra={"context": error_context},
        exc_info=True
    )


def log_validation_error(
    logger: logging.Logger,
    reason: str,
    row: Dict[str, Any],
):
    """
    Log a validation error with row context.
    
    Args:
        logger: Logger instance
        reason: Validation failure reason
        row: Health data row that failed validation
    """
    context = {
        "validation_reason": reason,
        "sample_id": row.get("sample_id"),
        "user_id": row.get("user_id"),
        "data_type": row.get("data_type"),
        "value": row.get("value"),
    }
    
    logger.warning(
        f"Validation failed: {reason}",
        extra={"context": context}
    )


def log_kafka_error(
    logger: logging.Logger,
    message: str,
    topic: str,
    partition: Optional[int] = None,
    offset: Optional[int] = None,
    error: Optional[Exception] = None,
):
    """
    Log a Kafka-related error with context.
    
    Args:
        logger: Logger instance
        message: Error message
        topic: Kafka topic
        partition: Kafka partition (optional)
        offset: Kafka offset (optional)
        error: Exception object (optional)
    """
    context = {
        "kafka_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
    }
    
    if error:
        context["error_type"] = type(error).__name__
        context["error_message"] = str(error)
    
    logger.error(
        message,
        extra={"context": context},
        exc_info=error is not None
    )


def log_iceberg_error(
    logger: logging.Logger,
    message: str,
    table_name: str,
    database: str,
    error: Optional[Exception] = None,
):
    """
    Log an Iceberg-related error with context.
    
    Args:
        logger: Logger instance
        message: Error message
        table_name: Iceberg table name
        database: Iceberg database name
        error: Exception object (optional)
    """
    context = {
        "iceberg_table": table_name,
        "iceberg_database": database,
    }
    
    if error:
        context["error_type"] = type(error).__name__
        context["error_message"] = str(error)
    
    logger.error(
        message,
        extra={"context": context},
        exc_info=error is not None
    )


def log_checkpoint_event(
    logger: logging.Logger,
    event_type: str,
    checkpoint_id: Optional[int] = None,
    duration_ms: Optional[int] = None,
    size_bytes: Optional[int] = None,
):
    """
    Log a checkpoint-related event.
    
    Args:
        logger: Logger instance
        event_type: Type of checkpoint event (started, completed, failed)
        checkpoint_id: Checkpoint ID
        duration_ms: Checkpoint duration in milliseconds
        size_bytes: Checkpoint size in bytes
    """
    context = {
        "checkpoint_event": event_type,
        "checkpoint_id": checkpoint_id,
        "duration_ms": duration_ms,
        "size_bytes": size_bytes,
    }
    
    level = "info" if event_type == "completed" else "warning"
    log_with_context(
        logger,
        level,
        f"Checkpoint {event_type}",
        context
    )


# Configure logging on module import
if os.getenv("CONFIGURE_LOGGING_ON_IMPORT", "true").lower() == "true":
    setup_logging()


if __name__ == "__main__":
    # Example usage
    logger = get_logger(__name__, context={"component": "example"})
    
    logger.info("Application started")
    logger.debug("Debug information", extra={"context": {"detail": "value"}})
    logger.warning("Warning message")
    
    try:
        raise ValueError("Example error")
    except ValueError as e:
        log_error_with_context(
            logger.logger,
            "An error occurred",
            e,
            {"operation": "example"}
        )
