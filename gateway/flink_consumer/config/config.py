"""Unified configuration module with validation and initialization"""

import logging
import sys
from typing import Optional

from pydantic import ValidationError

from flink_consumer.config.settings import (
    Settings,
    KafkaSettings,
    SchemaRegistrySettings,
    IcebergSettings,
    S3Settings,
    FlinkSettings,
    BatchSettings,
    MonitoringSettings,
    AppSettings,
)

logger = logging.getLogger(__name__)


class Config:
    """
    Unified configuration manager for the Flink Iceberg Consumer application.
    
    Provides centralized access to all configuration settings with validation
    and initialization support. Ensures all required environment variables are
    present and valid before the application starts.
    
    Requirements: 8.3
    """

    def __init__(self):
        """Initialize configuration manager"""
        self._settings: Optional[Settings] = None
        self._validated = False

    def load(self) -> Settings:
        """
        Load and validate all configuration settings.
        
        Loads configuration from environment variables and .env.local file,
        validates all settings using Pydantic, and ensures required fields
        are present.
        
        Returns:
            Settings instance with all configuration
            
        Raises:
            SystemExit: If configuration validation fails
        """
        if self._settings is not None and self._validated:
            logger.debug("Configuration already loaded and validated")
            return self._settings

        try:
            logger.info("Loading application configuration...")
            
            # Load settings (Pydantic will handle env vars and .env.local)
            self._settings = Settings()
            
            # Validate configuration
            self._validate_configuration()
            
            # Log configuration summary (without sensitive data)
            self._log_configuration_summary()
            
            self._validated = True
            logger.info("Configuration loaded and validated successfully")
            
            return self._settings
            
        except ValidationError as e:
            logger.error(
                "Configuration validation failed",
                extra={"validation_errors": str(e)},
                exc_info=True
            )
            self._print_validation_errors(e)
            sys.exit(1)
            
        except Exception as e:
            logger.error(
                f"Failed to load configuration: {str(e)}",
                extra={"error": str(e)},
                exc_info=True
            )
            sys.exit(1)

    def _validate_configuration(self) -> None:
        """
        Perform additional validation checks beyond Pydantic validation.
        
        Validates logical constraints and dependencies between settings.
        
        Raises:
            ValueError: If validation fails
        """
        if not self._settings:
            raise ValueError("Settings not loaded")
        
        logger.info("Performing additional configuration validation...")
        
        # Validate Kafka settings
        self._validate_kafka_settings()
        
        # Validate Iceberg settings
        self._validate_iceberg_settings()
        
        # Validate S3/MinIO settings
        self._validate_s3_settings()
        
        # Validate Flink settings
        self._validate_flink_settings()
        
        # Validate batch settings
        self._validate_batch_settings()
        
        logger.info("Additional configuration validation passed")

    def _validate_kafka_settings(self) -> None:
        """Validate Kafka configuration settings"""
        kafka = self._settings.kafka
        
        # Validate brokers format
        if not kafka.brokers or not kafka.brokers.strip():
            raise ValueError("KAFKA_BROKERS cannot be empty")
        
        # Validate topic
        if not kafka.topic or not kafka.topic.strip():
            raise ValueError("KAFKA_TOPIC cannot be empty")
        
        # Validate group ID
        if not kafka.group_id or not kafka.group_id.strip():
            raise ValueError("KAFKA_GROUP_ID cannot be empty")
        
        # Validate offset reset strategy
        valid_strategies = ["earliest", "latest", "committed"]
        if kafka.auto_offset_reset.lower() not in valid_strategies:
            raise ValueError(
                f"KAFKA_AUTO_OFFSET_RESET must be one of {valid_strategies}, "
                f"got: {kafka.auto_offset_reset}"
            )
        
        # Validate security settings if enabled
        if kafka.security_protocol:
            if kafka.sasl_mechanism and not (kafka.sasl_username and kafka.sasl_password):
                raise ValueError(
                    "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required "
                    "when KAFKA_SASL_MECHANISM is set"
                )
        
        logger.debug("Kafka settings validation passed")

    def _validate_iceberg_settings(self) -> None:
        """Validate Iceberg configuration settings"""
        iceberg = self._settings.iceberg
        
        # Validate catalog type
        valid_catalog_types = ["rest", "hive", "hadoop"]
        if iceberg.catalog_type.lower() not in valid_catalog_types:
            raise ValueError(
                f"ICEBERG_CATALOG_TYPE must be one of {valid_catalog_types}, "
                f"got: {iceberg.catalog_type}"
            )
        
        # Validate catalog name
        if not iceberg.catalog_name or not iceberg.catalog_name.strip():
            raise ValueError("ICEBERG_CATALOG_NAME cannot be empty")
        
        # Validate warehouse path
        if not iceberg.warehouse or not iceberg.warehouse.strip():
            raise ValueError("ICEBERG_WAREHOUSE cannot be empty")
        
        # Validate warehouse path format (should start with s3a:// or hdfs://)
        if not (iceberg.warehouse.startswith("s3a://") or 
                iceberg.warehouse.startswith("hdfs://") or
                iceberg.warehouse.startswith("file://")):
            logger.warning(
                f"ICEBERG_WAREHOUSE path '{iceberg.warehouse}' does not start with "
                f"s3a://, hdfs://, or file://. This may cause issues."
            )
        
        # Validate database name
        if not iceberg.database or not iceberg.database.strip():
            raise ValueError("ICEBERG_DATABASE cannot be empty")
        
        # Validate table names
        if not iceberg.table_raw or not iceberg.table_raw.strip():
            raise ValueError("ICEBERG_TABLE_RAW cannot be empty")
        
        if not iceberg.table_errors or not iceberg.table_errors.strip():
            raise ValueError("ICEBERG_TABLE_ERRORS cannot be empty")
        
        logger.debug("Iceberg settings validation passed")

    def _validate_s3_settings(self) -> None:
        """Validate S3/MinIO configuration settings"""
        s3 = self._settings.s3
        
        # Validate endpoint
        if not s3.endpoint or not s3.endpoint.strip():
            raise ValueError("S3_ENDPOINT cannot be empty")
        
        # Validate endpoint format (should start with http:// or https://)
        if not (s3.endpoint.startswith("http://") or s3.endpoint.startswith("https://")):
            raise ValueError(
                f"S3_ENDPOINT must start with http:// or https://, got: {s3.endpoint}"
            )
        
        # Validate access key
        if not s3.access_key or not s3.access_key.strip():
            raise ValueError("S3_ACCESS_KEY cannot be empty")
        
        # Validate secret key
        if not s3.secret_key or not s3.secret_key.strip():
            raise ValueError("S3_SECRET_KEY cannot be empty")
        
        # Validate bucket name
        if not s3.bucket or not s3.bucket.strip():
            raise ValueError("S3_BUCKET cannot be empty")
        
        # Validate bucket name format (lowercase, no special chars except dash)
        import re
        if not re.match(r'^[a-z0-9][a-z0-9-]*[a-z0-9]$', s3.bucket):
            logger.warning(
                f"S3_BUCKET '{s3.bucket}' may not follow S3 naming conventions. "
                f"Bucket names should be lowercase alphanumeric with hyphens only."
            )
        
        # Warn if using default MinIO credentials
        if s3.access_key == "minioadmin" and s3.secret_key == "minioadmin":
            logger.warning(
                "Using default MinIO credentials (minioadmin/minioadmin). "
                "This is NOT recommended for production environments."
            )
        
        # Validate path style access setting
        if not isinstance(s3.path_style_access, bool):
            raise ValueError(
                f"S3_PATH_STYLE_ACCESS must be a boolean, got: {type(s3.path_style_access)}"
            )
        
        # Recommend path style access for MinIO
        if "minio" in s3.endpoint.lower() and not s3.path_style_access:
            logger.warning(
                "S3_PATH_STYLE_ACCESS is False but endpoint appears to be MinIO. "
                "MinIO typically requires path-style access (S3_PATH_STYLE_ACCESS=true)."
            )
        
        logger.debug("S3/MinIO settings validation passed")

    def _validate_flink_settings(self) -> None:
        """Validate Flink configuration settings"""
        flink = self._settings.flink
        
        # Validate parallelism
        if flink.parallelism < 1:
            raise ValueError(
                f"FLINK_PARALLELISM must be >= 1, got: {flink.parallelism}"
            )
        
        if flink.parallelism > 128:
            logger.warning(
                f"FLINK_PARALLELISM is very high ({flink.parallelism}). "
                f"This may cause resource issues."
            )
        
        # Validate checkpoint interval
        if flink.checkpoint_interval_ms < 1000:
            raise ValueError(
                f"FLINK_CHECKPOINT_INTERVAL_MS must be >= 1000ms, "
                f"got: {flink.checkpoint_interval_ms}"
            )
        
        # Validate checkpoint timeout
        if flink.checkpoint_timeout_ms < flink.checkpoint_interval_ms:
            raise ValueError(
                f"FLINK_CHECKPOINT_TIMEOUT_MS ({flink.checkpoint_timeout_ms}) "
                f"must be >= FLINK_CHECKPOINT_INTERVAL_MS ({flink.checkpoint_interval_ms})"
            )
        
        # Validate min pause between checkpoints
        if flink.min_pause_between_checkpoints_ms < 0:
            raise ValueError(
                f"FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS must be >= 0, "
                f"got: {flink.min_pause_between_checkpoints_ms}"
            )
        
        # Validate max concurrent checkpoints
        if flink.max_concurrent_checkpoints < 1:
            raise ValueError(
                f"FLINK_MAX_CONCURRENT_CHECKPOINTS must be >= 1, "
                f"got: {flink.max_concurrent_checkpoints}"
            )
        
        # Validate checkpoint mode
        valid_modes = ["EXACTLY_ONCE", "AT_LEAST_ONCE"]
        if flink.checkpoint_mode not in valid_modes:
            raise ValueError(
                f"FLINK_CHECKPOINT_MODE must be one of {valid_modes}, "
                f"got: {flink.checkpoint_mode}"
            )
        
        # Validate state backend
        valid_backends = ["rocksdb", "hashmap", "filesystem"]
        if flink.state_backend.lower() not in valid_backends:
            logger.warning(
                f"FLINK_STATE_BACKEND '{flink.state_backend}' is not a standard backend. "
                f"Expected one of {valid_backends}"
            )
        
        # Validate checkpoint storage
        if not flink.checkpoint_storage or not flink.checkpoint_storage.strip():
            raise ValueError("FLINK_CHECKPOINT_STORAGE cannot be empty")
        
        # Validate restart strategy
        valid_strategies = ["fixed-delay", "failure-rate", "exponential-delay", "none"]
        if flink.restart_strategy not in valid_strategies:
            raise ValueError(
                f"FLINK_RESTART_STRATEGY must be one of {valid_strategies}, "
                f"got: {flink.restart_strategy}"
            )
        
        # Validate restart attempts
        if flink.restart_attempts < 0:
            raise ValueError(
                f"FLINK_RESTART_ATTEMPTS must be >= 0, got: {flink.restart_attempts}"
            )
        
        logger.debug("Flink settings validation passed")

    def _validate_batch_settings(self) -> None:
        """Validate batch writing configuration settings"""
        batch = self._settings.batch
        
        # Validate batch size
        if batch.batch_size < 1:
            raise ValueError(
                f"BATCH_SIZE must be >= 1, got: {batch.batch_size}"
            )
        
        if batch.batch_size > 100000:
            logger.warning(
                f"BATCH_SIZE is very high ({batch.batch_size}). "
                f"This may cause memory issues."
            )
        
        # Validate batch timeout
        if batch.batch_timeout_seconds < 1:
            raise ValueError(
                f"BATCH_TIMEOUT_SECONDS must be >= 1, got: {batch.batch_timeout_seconds}"
            )
        
        # Validate target file size
        if batch.target_file_size_mb < 1:
            raise ValueError(
                f"TARGET_FILE_SIZE_MB must be >= 1, got: {batch.target_file_size_mb}"
            )
        
        if batch.target_file_size_mb < 128 or batch.target_file_size_mb > 512:
            logger.warning(
                f"TARGET_FILE_SIZE_MB ({batch.target_file_size_mb}) is outside "
                f"recommended range (128-512 MB). This may affect query performance."
            )
        
        logger.debug("Batch settings validation passed")

    def _log_configuration_summary(self) -> None:
        """Log configuration summary without sensitive data"""
        if not self._settings:
            return
        
        logger.info(
            "Configuration Summary",
            extra={
                "app_name": self._settings.app.app_name,
                "app_version": self._settings.app.app_version,
                "environment": self._settings.app.environment,
                "kafka_topic": self._settings.kafka.topic,
                "kafka_group_id": self._settings.kafka.group_id,
                "iceberg_catalog": self._settings.iceberg.catalog_name,
                "iceberg_database": self._settings.iceberg.database,
                "iceberg_table_raw": self._settings.iceberg.table_raw,
                "s3_endpoint": self._settings.s3.endpoint,
                "s3_bucket": self._settings.s3.bucket,
                "flink_parallelism": self._settings.flink.parallelism,
                "flink_checkpoint_interval_ms": self._settings.flink.checkpoint_interval_ms,
                "flink_checkpoint_mode": self._settings.flink.checkpoint_mode,
                "batch_size": self._settings.batch.batch_size,
                "batch_timeout_seconds": self._settings.batch.batch_timeout_seconds,
                "log_level": self._settings.monitoring.log_level,
            }
        )

    def _print_validation_errors(self, error: ValidationError) -> None:
        """
        Print validation errors in a user-friendly format.
        
        Args:
            error: Pydantic ValidationError
        """
        print("\n" + "=" * 80)
        print("CONFIGURATION VALIDATION FAILED")
        print("=" * 80)
        print("\nThe following configuration errors were found:\n")
        
        for err in error.errors():
            field = " -> ".join(str(loc) for loc in err['loc'])
            message = err['msg']
            print(f"  • {field}: {message}")
        
        print("\n" + "=" * 80)
        print("Please check your environment variables and .env.local file.")
        print("=" * 80 + "\n")

    def get_settings(self) -> Settings:
        """
        Get the loaded settings instance.
        
        Returns:
            Settings instance
            
        Raises:
            RuntimeError: If settings have not been loaded yet
        """
        if not self._settings or not self._validated:
            raise RuntimeError(
                "Configuration has not been loaded. Call load() first."
            )
        
        return self._settings

    @property
    def kafka(self) -> KafkaSettings:
        """Get Kafka settings"""
        return self.get_settings().kafka

    @property
    def schema_registry(self) -> SchemaRegistrySettings:
        """Get Schema Registry settings"""
        return self.get_settings().schema_registry

    @property
    def iceberg(self) -> IcebergSettings:
        """Get Iceberg settings"""
        return self.get_settings().iceberg

    @property
    def s3(self) -> S3Settings:
        """Get S3 settings"""
        return self.get_settings().s3

    @property
    def flink(self) -> FlinkSettings:
        """Get Flink settings"""
        return self.get_settings().flink

    @property
    def batch(self) -> BatchSettings:
        """Get Batch settings"""
        return self.get_settings().batch

    @property
    def monitoring(self) -> MonitoringSettings:
        """Get Monitoring settings"""
        return self.get_settings().monitoring

    @property
    def app(self) -> AppSettings:
        """Get App settings"""
        return self.get_settings().app


# Global configuration instance
config = Config()


def load_config() -> Settings:
    """
    Convenience function to load and validate configuration.
    
    Returns:
        Settings instance with all configuration
    """
    return config.load()


def get_config() -> Config:
    """
    Get the global configuration manager instance.
    
    Returns:
        Config instance
    """
    return config
