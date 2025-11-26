"""
Retry handler with exponential backoff
"""
import asyncio
import logging
from typing import Callable, Any, Optional
from confluent_kafka import KafkaError

logger = logging.getLogger(__name__)


class RetryHandler:
    """Handler for retry logic with exponential backoff"""
    
    # Retryable Kafka errors
    RETRYABLE_ERRORS = {
        KafkaError._MSG_TIMED_OUT,
        KafkaError.REQUEST_TIMED_OUT,
        KafkaError._QUEUE_FULL,
        KafkaError.BROKER_NOT_AVAILABLE,
        KafkaError.NETWORK_EXCEPTION,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
        KafkaError.NOT_COORDINATOR,
        KafkaError._TRANSPORT,
    }
    
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 0.1,
        max_delay: float = 5.0,
        backoff_factor: float = 2.0
    ):
        """
        Initialize retry handler
        
        Args:
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            backoff_factor: Exponential backoff factor
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
    
    def is_retryable_error(self, error: Exception) -> bool:
        """
        Check if error is retryable
        
        Args:
            error: Exception to check
            
        Returns:
            True if error is retryable
        """
        if hasattr(error, 'code'):
            return error.code() in self.RETRYABLE_ERRORS
        
        # Check error message for retryable patterns
        error_str = str(error).lower()
        retryable_patterns = [
            'timeout',
            'network',
            'connection',
            'broker not available',
            'queue full'
        ]
        
        return any(pattern in error_str for pattern in retryable_patterns)
    
    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with retry logic
        
        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If all retries fail
        """
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                # Execute function
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                if attempt > 0:
                    logger.info(f"Retry succeeded on attempt {attempt + 1}")
                
                return result
                
            except Exception as e:
                last_error = e
                
                # Check if we should retry
                if attempt < self.max_retries and self.is_retryable_error(e):
                    # Calculate delay with exponential backoff
                    delay = min(
                        self.initial_delay * (self.backoff_factor ** attempt),
                        self.max_delay
                    )
                    
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    
                    await asyncio.sleep(delay)
                else:
                    # Non-retryable error or max retries reached
                    if attempt >= self.max_retries:
                        logger.error(
                            f"Max retries ({self.max_retries}) reached. "
                            f"Last error: {e}"
                        )
                    else:
                        logger.error(f"Non-retryable error: {e}")
                    
                    raise last_error
        
        # Should not reach here, but just in case
        raise last_error


# Singleton instance
retry_handler = RetryHandler(
    max_retries=3,
    initial_delay=0.1,
    max_delay=5.0,
    backoff_factor=2.0
)
