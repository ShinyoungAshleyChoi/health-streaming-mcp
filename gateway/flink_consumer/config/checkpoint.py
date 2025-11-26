"""Checkpoint and state management configuration for Flink"""

import logging
from typing import Optional
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpointing_mode import CheckpointingMode as CPMode

from flink_consumer.config.settings import FlinkSettings

logger = logging.getLogger(__name__)


class CheckpointConfig:
    """Configure Flink checkpoint and state management"""

    def __init__(self, settings: FlinkSettings):
        """
        Initialize checkpoint configuration

        Args:
            settings: Flink configuration settings
        """
        self.settings = settings

    def configure_checkpoint(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure checkpoint settings for exactly-once semantics

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.1, 5.2, 5.3
        """
        logger.info("Configuring Flink checkpoint settings")

        # Enable checkpointing with specified interval
        env.enable_checkpointing(self.settings.checkpoint_interval_ms)
        logger.info(
            f"Checkpoint interval: {self.settings.checkpoint_interval_ms}ms "
            f"({self.settings.checkpoint_interval_ms / 1000}s)"
        )

        # Get checkpoint configuration
        checkpoint_config = env.get_checkpoint_config()

        # Set checkpointing mode (EXACTLY_ONCE or AT_LEAST_ONCE)
        checkpoint_mode = self._get_checkpoint_mode()
        checkpoint_config.set_checkpointing_mode(checkpoint_mode)
        logger.info(f"Checkpoint mode: {self.settings.checkpoint_mode}")

        # Set minimum pause between checkpoints
        checkpoint_config.set_min_pause_between_checkpoints(
            self.settings.min_pause_between_checkpoints_ms
        )
        logger.info(
            f"Min pause between checkpoints: {self.settings.min_pause_between_checkpoints_ms}ms"
        )

        # Set checkpoint timeout
        checkpoint_config.set_checkpoint_timeout(self.settings.checkpoint_timeout_ms)
        logger.info(f"Checkpoint timeout: {self.settings.checkpoint_timeout_ms}ms")

        # Set maximum concurrent checkpoints
        checkpoint_config.set_max_concurrent_checkpoints(
            self.settings.max_concurrent_checkpoints
        )
        logger.info(
            f"Max concurrent checkpoints: {self.settings.max_concurrent_checkpoints}"
        )

        # Enable externalized checkpoints (retain on cancellation)
        checkpoint_config.enable_externalized_checkpoints(
            checkpoint_config.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        )
        logger.info("Externalized checkpoints enabled (RETAIN_ON_CANCELLATION)")

        # Set checkpoint storage location
        checkpoint_config.set_checkpoint_storage(self.settings.checkpoint_storage)
        logger.info(f"Checkpoint storage: {self.settings.checkpoint_storage}")

        logger.info("Checkpoint configuration completed successfully")

    def configure_state_backend(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure state backend (RocksDB or HashMapStateBackend)

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.1, 5.2
        """
        logger.info(f"Configuring state backend: {self.settings.state_backend}")

        if self.settings.state_backend.lower() == "rocksdb":
            # RocksDB state backend for large state and incremental checkpoints
            env.set_state_backend("rocksdb")
            logger.info("RocksDB state backend configured")
            logger.info("Benefits: Handles large state, supports incremental checkpoints")

        elif self.settings.state_backend.lower() == "hashmap":
            # HashMapStateBackend for small state (development/testing)
            env.set_state_backend("hashmap")
            logger.info("HashMapStateBackend configured")
            logger.warning("HashMapStateBackend is limited by heap memory")

        else:
            logger.warning(
                f"Unknown state backend: {self.settings.state_backend}, "
                "using default (hashmap)"
            )
            env.set_state_backend("hashmap")

    def configure_s3_for_checkpoints(self, env: StreamExecutionEnvironment) -> None:
        """
        Configure S3/MinIO settings for checkpoint storage

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.3
        """
        logger.info("Configuring S3 settings for checkpoint storage")

        # Note: S3 configuration is typically done via flink-conf.yaml or
        # environment variables. This method documents the required settings.

        # Required S3 settings (set via environment or flink-conf.yaml):
        # - fs.s3a.endpoint: S3/MinIO endpoint
        # - fs.s3a.access.key: Access key
        # - fs.s3a.secret.key: Secret key
        # - fs.s3a.path.style.access: true (for MinIO)
        # - fs.s3a.connection.ssl.enabled: false (for local MinIO)

        logger.info(
            "S3 checkpoint storage requires the following configuration:\n"
            "  - fs.s3a.endpoint: S3/MinIO endpoint URL\n"
            "  - fs.s3a.access.key: S3 access key\n"
            "  - fs.s3a.secret.key: S3 secret key\n"
            "  - fs.s3a.path.style.access: true (for MinIO)\n"
            "  - fs.s3a.connection.ssl.enabled: false (for local development)\n"
            "These should be set in flink-conf.yaml or as environment variables"
        )

    def _get_checkpoint_mode(self) -> CheckpointingMode:
        """
        Get CheckpointingMode enum from settings

        Returns:
            CheckpointingMode enum value
        """
        if self.settings.checkpoint_mode == "EXACTLY_ONCE":
            return CPMode.EXACTLY_ONCE
        elif self.settings.checkpoint_mode == "AT_LEAST_ONCE":
            return CPMode.AT_LEAST_ONCE
        else:
            logger.warning(
                f"Unknown checkpoint mode: {self.settings.checkpoint_mode}, "
                "defaulting to EXACTLY_ONCE"
            )
            return CPMode.EXACTLY_ONCE

    def apply_all_configurations(self, env: StreamExecutionEnvironment) -> None:
        """
        Apply all checkpoint and state configurations

        Args:
            env: Flink StreamExecutionEnvironment

        Requirements: 5.1, 5.2, 5.3
        """
        logger.info("Applying all checkpoint and state configurations")

        # Configure state backend first
        self.configure_state_backend(env)

        # Configure checkpoint settings
        self.configure_checkpoint(env)

        # Log S3 configuration requirements
        self.configure_s3_for_checkpoints(env)

        logger.info("All checkpoint and state configurations applied successfully")


def setup_checkpoint_and_state(
    env: StreamExecutionEnvironment, settings: FlinkSettings
) -> None:
    """
    Convenience function to set up checkpoint and state management

    Args:
        env: Flink StreamExecutionEnvironment
        settings: Flink configuration settings

    Requirements: 5.1, 5.2, 5.3

    Example:
        >>> from pyflink.datastream import StreamExecutionEnvironment
        >>> from flink_consumer.config.settings import settings
        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> setup_checkpoint_and_state(env, settings.flink)
    """
    checkpoint_config = CheckpointConfig(settings)
    checkpoint_config.apply_all_configurations(env)
