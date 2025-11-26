"""
Example: Test checkpoint and recovery configuration

This example demonstrates how to configure Flink checkpoint and recovery settings
for the health data consumer application.

Requirements: 5.1, 5.2, 5.3, 5.4, 5.5
"""

import logging
from pyflink.datastream import StreamExecutionEnvironment

from flink_consumer.config.settings import settings
from flink_consumer.config.checkpoint import setup_checkpoint_and_state
from flink_consumer.config.recovery import setup_recovery_strategy, setup_checkpoint_and_recovery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def example_checkpoint_only():
    """
    Example 1: Configure checkpoint and state management only
    
    This example shows how to set up checkpoint configuration without recovery strategy.
    """
    logger.info("=" * 80)
    logger.info("EXAMPLE 1: Checkpoint Configuration Only")
    logger.info("=" * 80)
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism
    env.set_parallelism(settings.flink.parallelism)
    
    # Configure checkpoint and state management
    setup_checkpoint_and_state(env, settings.flink)
    
    logger.info("Checkpoint configuration completed")
    logger.info("=" * 80)


def example_recovery_only():
    """
    Example 2: Configure recovery strategy only
    
    This example shows how to set up recovery strategy without checkpoint configuration.
    """
    logger.info("=" * 80)
    logger.info("EXAMPLE 2: Recovery Strategy Configuration Only")
    logger.info("=" * 80)
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism
    env.set_parallelism(settings.flink.parallelism)
    
    # Configure recovery strategy
    setup_recovery_strategy(env, settings.flink)
    
    logger.info("Recovery strategy configuration completed")
    logger.info("=" * 80)


def example_complete_configuration():
    """
    Example 3: Configure both checkpoint and recovery (Recommended)
    
    This is the recommended approach for production deployments.
    """
    logger.info("=" * 80)
    logger.info("EXAMPLE 3: Complete Checkpoint and Recovery Configuration")
    logger.info("=" * 80)
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism
    env.set_parallelism(settings.flink.parallelism)
    
    # Configure both checkpoint and recovery
    setup_checkpoint_and_recovery(env, settings.flink)
    
    logger.info("Complete configuration applied")
    logger.info("=" * 80)


def example_custom_configuration():
    """
    Example 4: Custom configuration using individual components
    
    This example shows how to use individual configuration classes for fine-grained control.
    """
    logger.info("=" * 80)
    logger.info("EXAMPLE 4: Custom Configuration")
    logger.info("=" * 80)
    
    from flink_consumer.config.checkpoint import CheckpointConfig
    from flink_consumer.config.recovery import RecoveryConfig
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism
    env.set_parallelism(settings.flink.parallelism)
    
    # Create configuration objects
    checkpoint_config = CheckpointConfig(settings.flink)
    recovery_config = RecoveryConfig(settings.flink)
    
    # Apply configurations step by step
    logger.info("Step 1: Configure state backend")
    checkpoint_config.configure_state_backend(env)
    
    logger.info("Step 2: Configure checkpoint settings")
    checkpoint_config.configure_checkpoint(env)
    
    logger.info("Step 3: Configure restart strategy")
    recovery_config.configure_restart_strategy(env)
    
    logger.info("Step 4: Configure checkpoint failure handling")
    recovery_config.configure_checkpoint_failure_handling(env)
    
    logger.info("Custom configuration completed")
    logger.info("=" * 80)


def example_configuration_summary():
    """
    Example 5: Display configuration summary
    
    This example shows the current configuration settings.
    """
    logger.info("=" * 80)
    logger.info("EXAMPLE 5: Configuration Summary")
    logger.info("=" * 80)
    
    logger.info("Checkpoint Settings:")
    logger.info(f"  - Interval: {settings.flink.checkpoint_interval_ms}ms")
    logger.info(f"  - Timeout: {settings.flink.checkpoint_timeout_ms}ms")
    logger.info(f"  - Min Pause: {settings.flink.min_pause_between_checkpoints_ms}ms")
    logger.info(f"  - Max Concurrent: {settings.flink.max_concurrent_checkpoints}")
    logger.info(f"  - Mode: {settings.flink.checkpoint_mode}")
    logger.info(f"  - State Backend: {settings.flink.state_backend}")
    logger.info(f"  - Storage: {settings.flink.checkpoint_storage}")
    
    logger.info("\nRestart Strategy Settings:")
    logger.info(f"  - Strategy: {settings.flink.restart_strategy}")
    logger.info(f"  - Attempts: {settings.flink.restart_attempts}")
    logger.info(f"  - Delay: {settings.flink.restart_delay_ms}ms")
    
    if settings.flink.restart_strategy == "failure-rate":
        logger.info("\nFailure Rate Settings:")
        logger.info(f"  - Max Failures: {settings.flink.failure_rate_max_failures}")
        logger.info(f"  - Interval: {settings.flink.failure_rate_interval_ms}ms")
        logger.info(f"  - Delay: {settings.flink.failure_rate_delay_ms}ms")
    
    logger.info("=" * 80)


def main():
    """
    Run all examples
    """
    logger.info("Starting Checkpoint and Recovery Configuration Examples")
    logger.info("")
    
    # Run examples
    example_checkpoint_only()
    print()
    
    example_recovery_only()
    print()
    
    example_complete_configuration()
    print()
    
    example_custom_configuration()
    print()
    
    example_configuration_summary()
    print()
    
    logger.info("All examples completed successfully!")
    logger.info("")
    logger.info("RECOMMENDED USAGE:")
    logger.info("  from flink_consumer.config import setup_checkpoint_and_recovery")
    logger.info("  setup_checkpoint_and_recovery(env, settings.flink)")


if __name__ == "__main__":
    main()
