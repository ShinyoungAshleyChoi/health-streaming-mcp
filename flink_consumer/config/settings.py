"""Configuration settings using Pydantic Settings"""

from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Kafka configuration settings"""

    brokers: str = Field(alias="KAFKA_BROKERS")
    topic: str = Field(alias="KAFKA_TOPIC")
    group_id: str = Field(alias="KAFKA_GROUP_ID")
    auto_offset_reset: str = Field(default="earliest", alias="KAFKA_AUTO_OFFSET_RESET")
    partition_discovery_interval_ms: int = Field(
        default=60000, alias="KAFKA_PARTITION_DISCOVERY_INTERVAL_MS"
    )
    security_protocol: str | None = Field(default=None, alias="KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism: str | None = Field(default=None, alias="KAFKA_SASL_MECHANISM")
    sasl_username: str | None = Field(default=None, alias="KAFKA_SASL_USERNAME")
    sasl_password: str | None = Field(default=None, alias="KAFKA_SASL_PASSWORD")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class SchemaRegistrySettings(BaseSettings):
    """Schema Registry configuration settings"""

    url: str = Field(alias="SCHEMA_REGISTRY_URL")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class IcebergSettings(BaseSettings):
    """Iceberg configuration settings"""

    catalog_type: str = Field(alias="ICEBERG_CATALOG_TYPE")
    catalog_name: str = Field(alias="ICEBERG_CATALOG_NAME")
    catalog_uri: str = Field(alias="ICEBERG_CATALOG_URI")
    warehouse: str = Field(alias="ICEBERG_WAREHOUSE")
    database: str = Field(alias="ICEBERG_DATABASE")
    table_raw: str = Field(alias="ICEBERG_TABLE_RAW")
    table_errors: str = Field(alias="ICEBERG_TABLE_ERRORS")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class S3Settings(BaseSettings):
    """S3/MinIO configuration settings"""

    endpoint: str = Field(alias="S3_ENDPOINT")
    access_key: str = Field(alias="S3_ACCESS_KEY")
    secret_key: str = Field(alias="S3_SECRET_KEY")
    path_style_access: bool = Field(default=True, alias="S3_PATH_STYLE_ACCESS")
    bucket: str = Field(alias="S3_BUCKET")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class FlinkSettings(BaseSettings):
    """Flink configuration settings"""

    parallelism: int = Field(alias="FLINK_PARALLELISM")
    checkpoint_interval_ms: int = Field(alias="FLINK_CHECKPOINT_INTERVAL_MS")
    checkpoint_timeout_ms: int = Field(alias="FLINK_CHECKPOINT_TIMEOUT_MS")
    min_pause_between_checkpoints_ms: int = Field(
        alias="FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS"
    )
    max_concurrent_checkpoints: int = Field(alias="FLINK_MAX_CONCURRENT_CHECKPOINTS")
    checkpoint_mode: Literal["EXACTLY_ONCE", "AT_LEAST_ONCE"] = Field(
        alias="FLINK_CHECKPOINT_MODE"
    )
    state_backend: str = Field(alias="FLINK_STATE_BACKEND")
    checkpoint_storage: str = Field(alias="FLINK_CHECKPOINT_STORAGE")
    
    # Restart strategy settings
    restart_strategy: Literal["fixed-delay", "failure-rate", "exponential-delay", "none"] = Field(
        default="fixed-delay", alias="FLINK_RESTART_STRATEGY"
    )
    restart_attempts: int = Field(default=3, alias="FLINK_RESTART_ATTEMPTS")
    restart_delay_ms: int = Field(default=10000, alias="FLINK_RESTART_DELAY_MS")
    
    # Failure rate restart strategy (optional)
    failure_rate_max_failures: int = Field(default=3, alias="FLINK_FAILURE_RATE_MAX_FAILURES")
    failure_rate_interval_ms: int = Field(default=300000, alias="FLINK_FAILURE_RATE_INTERVAL_MS")
    failure_rate_delay_ms: int = Field(default=10000, alias="FLINK_FAILURE_RATE_DELAY_MS")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class BatchSettings(BaseSettings):
    """Batch writing configuration settings"""

    batch_size: int = Field(alias="BATCH_SIZE")
    batch_timeout_seconds: int = Field(alias="BATCH_TIMEOUT_SECONDS")
    target_file_size_mb: int = Field(alias="TARGET_FILE_SIZE_MB")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class MonitoringSettings(BaseSettings):
    """Monitoring configuration settings"""

    metrics_enabled: bool = Field(default=True, alias="METRICS_ENABLED")
    prometheus_port: int = Field(default=9249, alias="PROMETHEUS_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: Literal["json", "text"] = Field(default="json", alias="LOG_FORMAT")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class AppSettings(BaseSettings):
    """Application configuration settings"""

    app_name: str = Field(alias="APP_NAME")
    app_version: str = Field(alias="APP_VERSION")
    environment: str = Field(alias="ENVIRONMENT")

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


class Settings(BaseSettings):
    """Main settings class that aggregates all configuration"""

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    schema_registry: SchemaRegistrySettings = Field(default_factory=SchemaRegistrySettings)
    iceberg: IcebergSettings = Field(default_factory=IcebergSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    flink: FlinkSettings = Field(default_factory=FlinkSettings)
    batch: BatchSettings = Field(default_factory=BatchSettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    app: AppSettings = Field(default_factory=AppSettings)

    model_config = SettingsConfigDict(env_file=".env.local", extra="ignore")


# Global settings instance
settings = Settings()
