"""Recovery strategy configuration for Flink"""

import logging
from typing import Optional
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import RestartStrategies, Time

from flink_consumer.config.settings import FlinkSettings

logger = logging.getLogger(__name__)


class RecoveryConfig:
    """Configure Flink recovery and restart strategies"""

    def __init__(self, settings: FlinkSettings):
        """
        Initialize recovery configuration

        Args:
            settings: Flink configuration settings
        """
        self.settings = settings

    def configure_restart_strategy(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure restart strategy for failure recovery

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.3, 5.4, 5.5
        """
        logger.info(f"Configuring restart strategy: {self.settings.restart_strategy}")

        if self.settings.restart_strategy == "fixed-delay":
            self._configure_fixed_delay_restart(env)

        elif self.settings.restart_strategy == "failure-rate":
            self._configure_failure_rate_restart(env)

        elif self.settings.restart_strategy == "exponential-delay":
            self._configure_exponential_delay_restart(env)

        elif self.settings.restart_strategy == "none":
            self._configure_no_restart(env)

        else:
            logger.warning(
                f"Unknown restart strategy: {self.settings.restart_strategy}, "
                "defaulting to fixed-delay"
            )
            self._configure_fixed_delay_restart(env)

        logger.info("Restart strategy configured successfully")

    def _configure_fixed_delay_restart(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure fixed delay restart strategy

        Restarts the job a fixed number of times with a fixed delay between attempts.
        Recommended for production use.

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.3, 5.4
        """
        restart_attempts = self.settings.restart_attempts
        delay_ms = self.settings.restart_delay_ms

        env.set_restart_strategy(
            RestartStrategies.fixed_delay_restart(
                restart_attempts=restart_attempts,
                delay=Time.milliseconds(delay_ms)
            )
        )

        logger.info(
            f"Fixed delay restart strategy configured: "
            f"{restart_attempts} attempts with {delay_ms}ms delay"
        )
        logger.info(
            f"Job will restart up to {restart_attempts} times, "
            f"waiting {delay_ms / 1000}s between attempts"
        )

    def _configure_failure_rate_restart(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure failure rate restart strategy

        Restarts the job if the failure rate (failures per time interval) is not exceeded.
        Useful for handling transient failures while preventing restart loops.

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.3, 5.4
        """
        max_failures = self.settings.failure_rate_max_failures
        interval_ms = self.settings.failure_rate_interval_ms
        delay_ms = self.settings.failure_rate_delay_ms

        env.set_restart_strategy(
            RestartStrategies.failure_rate_restart(
                max_failures_per_interval=max_failures,
                failure_rate_interval=Time.milliseconds(interval_ms),
                delay=Time.milliseconds(delay_ms)
            )
        )

        logger.info(
            f"Failure rate restart strategy configured: "
            f"max {max_failures} failures per {interval_ms}ms interval, "
            f"{delay_ms}ms delay between restarts"
        )
        logger.info(
            f"Job will restart if fewer than {max_failures} failures occur "
            f"within {interval_ms / 1000}s"
        )

    def _configure_exponential_delay_restart(
        self, env: StreamExecutionEnvironment
    ) -> None:
        """
        Configure exponential delay restart strategy

        Restarts with exponentially increasing delays between attempts.
        Useful for handling temporary resource unavailability.

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.3, 5.4
        """
        # Note: PyFlink may not have direct exponential-delay support
        # Fall back to fixed delay with a warning
        logger.warning(
            "Exponential delay restart strategy not directly supported in PyFlink, "
            "falling back to fixed delay restart"
        )
        self._configure_fixed_delay_restart(env)

    def _configure_no_restart(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure no restart strategy

        Job will not restart on failure. Useful for development/debugging.

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.3
        """
        env.set_restart_strategy(RestartStrategies.no_restart())
        logger.warning(
            "No restart strategy configured - job will NOT restart on failure"
        )
        logger.warning("This is NOT recommended for production use")

    def configure_failover_strategy(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure failover strategy for task-level recovery

        Flink supports different failover strategies:
        - Region failover: Only restart affected tasks (default, recommended)
        - Full failover: Restart entire job

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.4, 5.5
        """
        logger.info("Configuring failover strategy")

        # Note: Failover strategy is typically configured via flink-conf.yaml
        # Default is 'region' which is recommended for most use cases

        logger.info(
            "Failover strategy configuration:\n"
            "  - Default: Region failover (restarts only affected tasks)\n"
            "  - To change: Set 'jobmanager.execution.failover-strategy' in flink-conf.yaml\n"
            "  - Options: 'region' (recommended) or 'full'\n"
            "  - Region failover minimizes recovery time and resource usage"
        )

    def configure_checkpoint_failure_handling(
        self, env: StreamExecutionEnvironment
    ) -> None:
        """
        Configure checkpoint failure handling

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.5
        """
        logger.info("Configuring checkpoint failure handling")

        checkpoint_config = env.get_checkpoint_config()

        # Tolerate checkpoint failures
        # If set to 0, job will fail on first checkpoint failure
        # If set to n > 0, job will tolerate n consecutive checkpoint failures
        tolerable_checkpoint_failures = 3
        checkpoint_config.set_tolerable_checkpoint_failure_number(
            tolerable_checkpoint_failures
        )

        logger.info(
            f"Tolerable checkpoint failures: {tolerable_checkpoint_failures}\n"
            f"Job will fail if {tolerable_checkpoint_failures + 1} consecutive "
            "checkpoints fail"
        )

        # Enable unaligned checkpoints for faster checkpointing under backpressure
        # Note: This may not be available in all PyFlink versions
        try:
            checkpoint_config.enable_unaligned_checkpoints()
            logger.info("Unaligned checkpoints enabled for faster recovery under backpressure")
        except AttributeError:
            logger.warning(
                "Unaligned checkpoints not available in this PyFlink version"
            )

    def log_recovery_configuration_summary(self) -> None:
        """
        Log a summary of recovery configuration

        Requirements: 5.3, 5.4, 5.5
        """
        logger.info("=" * 60)
        logger.info("RECOVERY CONFIGURATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Restart Strategy: {self.settings.restart_strategy}")

        if self.settings.restart_strategy == "fixed-delay":
            logger.info(f"  - Restart Attempts: {self.settings.restart_attempts}")
            logger.info(f"  - Restart Delay: {self.settings.restart_delay_ms}ms")

        elif self.settings.restart_strategy == "failure-rate":
            logger.info(
                f"  - Max Failures: {self.settings.failure_rate_max_failures}"
            )
            logger.info(
                f"  - Interval: {self.settings.failure_rate_interval_ms}ms"
            )
            logger.info(f"  - Delay: {self.settings.failure_rate_delay_ms}ms")

        logger.info(f"Checkpoint Storage: {self.settings.checkpoint_storage}")
        logger.info(f"State Backend: {self.settings.state_backend}")
        logger.info(f"Checkpoint Mode: {self.settings.checkpoint_mode}")
        logger.info("=" * 60)

    def apply_all_recovery_configurations(
        self, env: StreamExecutionEnvironment
    ) -> None:
        """
        Apply all recovery configurations

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.3, 5.4, 5.5
        """
        logger.info("Applying all recovery configurations")

        # Configure restart strategy
        self.configure_restart_strategy(env)

        # Log failover strategy information
        self.configure_failover_strategy(env)

        # Configure checkpoint failure handling
        self.configure_checkpoint_failure_handling(env)

        # Log configuration summary
        self.log_recovery_configuration_summary()

        logger.info("All recovery configurations applied successfully")


def setup_recovery_strategy(
    env: StreamExecutionEnvironment, settings: FlinkSettings
) -> None:
    """
    Convenience function to set up recovery strategy

    Args:
        env: Flink StreamExecutionEnvironment
        settings: Flink configuration settings

    Requirements: 5.3, 5.4, 5.5

    Example:
        >>> from pyflink.datastream import StreamExecutionEnvironment
        >>> from flink_consumer.config.settings import settings
        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> setup_recovery_strategy(env, settings.flink)
    """
    recovery_config = RecoveryConfig(settings)
    recovery_config.apply_all_recovery_configurations(env)


def setup_checkpoint_and_recovery(
    env: StreamExecutionEnvironment, settings: FlinkSettings
) -> None:
    """
    Convenience function to set up both checkpoint and recovery configurations

    Args:
        env: Flink StreamExecutionEnvironment
        settings: Flink configuration settings

    Requirements: 5.1, 5.2, 5.3, 5.4, 5.5

    Example:
        >>> from pyflink.datastream import StreamExecutionEnvironment
        >>> from flink_consumer.config.settings import settings
        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> setup_checkpoint_and_recovery(env, settings.flink)
    """
    from flink_consumer.config.checkpoint import setup_checkpoint_and_state

    logger.info("Setting up checkpoint and recovery configurations")

    # Setup checkpoint and state management
    setup_checkpoint_and_state(env, settings)

    # Setup recovery strategy
    setup_recovery_strategy(env, settings)

    logger.info("Checkpoint and recovery setup completed successfully")
