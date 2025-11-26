"""Unit tests for configuration settings"""

import pytest
import os
from unittest.mock import patch
from flink_consumer.config.settings import (
    KafkaSettings,
    SchemaRegistrySettings,
    IcebergSettings,
    S3Settings,
    FlinkSettings,
    BatchSettings,
    MonitoringSettings,
    AppSettings,
    Settings
)


class TestKafkaSettings:
    """Test suite for KafkaSettings"""

    def test_kafka_settings_from_env(self):
        """Test loading Kafka settings from environment variables"""
        with patch.dict(os.environ, {
            'KAFKA_BROKERS': 'localhost:9092',
            'KAFKA_TOPIC': 'test-topic',
            'KAFKA_GROUP_ID': 'test-group'
        }):
            settings = KafkaSettings()
            assert settings.brokers == 'localhost:9092'
            assert settings.topic == 'test-topic'
            assert settings.group_id == 'test-group'

    def test_kafka_settings_defaults(self):
        """Test Kafka settings default values"""
        with patch.dict(os.environ, {
            'KAFKA_BROKERS': 'localhost:9092',
            'KAFKA_TOPIC': 'test-topic',
            'KAFKA_GROUP_ID': 'test-group'
        }):
            settings = KafkaSettings()
            assert settings.auto_offset_reset == 'earliest'
            assert settings.partition_discovery_interval_ms == 60000

    def test_kafka_settings_optional_security(self):
        """Test Kafka settings with optional security parameters"""
        with patch.dict(os.environ, {
            'KAFKA_BROKERS': 'localhost:9092',
            'KAFKA_TOPIC': 'test-topic',
            'KAFKA_GROUP_ID': 'test-group',
            'KAFKA_SECURITY_PROTOCOL': 'SASL_SSL',
            'KAFKA_SASL_MECHANISM': 'SCRAM-SHA-512',
            'KAFKA_SASL_USERNAME': 'user',
            'KAFKA_SASL_PASSWORD': 'pass'
        }):
            settings = KafkaSettings()
            assert settings.security_protocol == 'SASL_SSL'
            assert settings.sasl_mechanism == 'SCRAM-SHA-512'
            assert settings.sasl_username == 'user'
            assert settings.sasl_password == 'pass'


class TestSchemaRegistrySettings:
    """Test suite for SchemaRegistrySettings"""

    def test_schema_registry_settings_from_env(self):
        """Test loading Schema Registry settings from environment"""
        with patch.dict(os.environ, {
            'SCHEMA_REGISTRY_URL': 'http://localhost:8081'
        }):
            settings = SchemaRegistrySettings()
            assert settings.url == 'http://localhost:8081'


class TestIcebergSettings:
    """Test suite for IcebergSettings"""

    def test_iceberg_settings_from_env(self):
        """Test loading Iceberg settings from environment"""
        with patch.dict(os.environ, {
            'ICEBERG_CATALOG_TYPE': 'hadoop',
            'ICEBERG_CATALOG_NAME': 'health_catalog',
            'ICEBERG_CATALOG_URI': 'thrift://localhost:9083',
            'ICEBERG_WAREHOUSE': 's3a://data-lake/warehouse',
            'ICEBERG_DATABASE': 'health_db',
            'ICEBERG_TABLE_RAW': 'health_data_raw',
            'ICEBERG_TABLE_ERRORS': 'health_data_errors'
        }):
            settings = IcebergSettings()
            assert settings.catalog_type == 'hadoop'
            assert settings.catalog_name == 'health_catalog'
            assert settings.warehouse == 's3a://data-lake/warehouse'
            assert settings.database == 'health_db'
            assert settings.table_raw == 'health_data_raw'
            assert settings.table_errors == 'health_data_errors'


class TestS3Settings:
    """Test suite for S3Settings"""

    def test_s3_settings_from_env(self):
        """Test loading S3 settings from environment"""
        with patch.dict(os.environ, {
            'S3_ENDPOINT': 'http://localhost:9000',
            'S3_ACCESS_KEY': 'minioadmin',
            'S3_SECRET_KEY': 'minioadmin',
            'S3_BUCKET': 'data-lake'
        }):
            settings = S3Settings()
            assert settings.endpoint == 'http://localhost:9000'
            assert settings.access_key == 'minioadmin'
            assert settings.secret_key == 'minioadmin'
            assert settings.bucket == 'data-lake'
            assert settings.path_style_access is True


class TestFlinkSettings:
    """Test suite for FlinkSettings"""

    def test_flink_settings_from_env(self):
        """Test loading Flink settings from environment"""
        with patch.dict(os.environ, {
            'FLINK_PARALLELISM': '6',
            'FLINK_CHECKPOINT_INTERVAL_MS': '60000',
            'FLINK_CHECKPOINT_TIMEOUT_MS': '600000',
            'FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS': '30000',
            'FLINK_MAX_CONCURRENT_CHECKPOINTS': '1',
            'FLINK_CHECKPOINT_MODE': 'EXACTLY_ONCE',
            'FLINK_STATE_BACKEND': 'rocksdb',
            'FLINK_CHECKPOINT_STORAGE': 's3a://flink-checkpoints/health-consumer'
        }):
            settings = FlinkSettings()
            assert settings.parallelism == 6
            assert settings.checkpoint_interval_ms == 60000
            assert settings.checkpoint_timeout_ms == 600000
            assert settings.checkpoint_mode == 'EXACTLY_ONCE'
            assert settings.state_backend == 'rocksdb'

    def test_flink_settings_restart_strategy_defaults(self):
        """Test Flink restart strategy default values"""
        with patch.dict(os.environ, {
            'FLINK_PARALLELISM': '6',
            'FLINK_CHECKPOINT_INTERVAL_MS': '60000',
            'FLINK_CHECKPOINT_TIMEOUT_MS': '600000',
            'FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS': '30000',
            'FLINK_MAX_CONCURRENT_CHECKPOINTS': '1',
            'FLINK_CHECKPOINT_MODE': 'EXACTLY_ONCE',
            'FLINK_STATE_BACKEND': 'rocksdb',
            'FLINK_CHECKPOINT_STORAGE': 's3a://checkpoints'
        }):
            settings = FlinkSettings()
            assert settings.restart_strategy == 'fixed-delay'
            assert settings.restart_attempts == 3
            assert settings.restart_delay_ms == 10000


class TestBatchSettings:
    """Test suite for BatchSettings"""

    def test_batch_settings_from_env(self):
        """Test loading Batch settings from environment"""
        with patch.dict(os.environ, {
            'BATCH_SIZE': '1000',
            'BATCH_TIMEOUT_SECONDS': '10',
            'TARGET_FILE_SIZE_MB': '256'
        }):
            settings = BatchSettings()
            assert settings.batch_size == 1000
            assert settings.batch_timeout_seconds == 10
            assert settings.target_file_size_mb == 256


class TestMonitoringSettings:
    """Test suite for MonitoringSettings"""

    def test_monitoring_settings_defaults(self):
        """Test Monitoring settings default values"""
        settings = MonitoringSettings()
        assert settings.metrics_enabled is True
        assert settings.prometheus_port == 9249
        assert settings.log_level == 'INFO'
        assert settings.log_format == 'json'

    def test_monitoring_settings_from_env(self):
        """Test loading Monitoring settings from environment"""
        with patch.dict(os.environ, {
            'METRICS_ENABLED': 'false',
            'PROMETHEUS_PORT': '9250',
            'LOG_LEVEL': 'DEBUG',
            'LOG_FORMAT': 'text'
        }):
            settings = MonitoringSettings()
            assert settings.metrics_enabled is False
            assert settings.prometheus_port == 9250
            assert settings.log_level == 'DEBUG'
            assert settings.log_format == 'text'


class TestAppSettings:
    """Test suite for AppSettings"""

    def test_app_settings_from_env(self):
        """Test loading App settings from environment"""
        with patch.dict(os.environ, {
            'APP_NAME': 'flink-iceberg-consumer',
            'APP_VERSION': '1.0.0',
            'ENVIRONMENT': 'production'
        }):
            settings = AppSettings()
            assert settings.app_name == 'flink-iceberg-consumer'
            assert settings.app_version == '1.0.0'
            assert settings.environment == 'production'


class TestSettings:
    """Test suite for main Settings class"""

    def test_settings_aggregation(self):
        """Test that Settings aggregates all sub-settings"""
        with patch.dict(os.environ, {
            'KAFKA_BROKERS': 'localhost:9092',
            'KAFKA_TOPIC': 'test-topic',
            'KAFKA_GROUP_ID': 'test-group',
            'SCHEMA_REGISTRY_URL': 'http://localhost:8081',
            'ICEBERG_CATALOG_TYPE': 'hadoop',
            'ICEBERG_CATALOG_NAME': 'catalog',
            'ICEBERG_CATALOG_URI': 'uri',
            'ICEBERG_WAREHOUSE': 'warehouse',
            'ICEBERG_DATABASE': 'db',
            'ICEBERG_TABLE_RAW': 'raw',
            'ICEBERG_TABLE_ERRORS': 'errors',
            'S3_ENDPOINT': 'http://localhost:9000',
            'S3_ACCESS_KEY': 'key',
            'S3_SECRET_KEY': 'secret',
            'S3_BUCKET': 'bucket',
            'FLINK_PARALLELISM': '6',
            'FLINK_CHECKPOINT_INTERVAL_MS': '60000',
            'FLINK_CHECKPOINT_TIMEOUT_MS': '600000',
            'FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS': '30000',
            'FLINK_MAX_CONCURRENT_CHECKPOINTS': '1',
            'FLINK_CHECKPOINT_MODE': 'EXACTLY_ONCE',
            'FLINK_STATE_BACKEND': 'rocksdb',
            'FLINK_CHECKPOINT_STORAGE': 's3a://checkpoints',
            'BATCH_SIZE': '1000',
            'BATCH_TIMEOUT_SECONDS': '10',
            'TARGET_FILE_SIZE_MB': '256',
            'APP_NAME': 'test-app',
            'APP_VERSION': '1.0.0',
            'ENVIRONMENT': 'test'
        }):
            settings = Settings()
            
            # Verify all sub-settings are accessible
            assert settings.kafka.brokers == 'localhost:9092'
            assert settings.schema_registry.url == 'http://localhost:8081'
            assert settings.iceberg.catalog_type == 'hadoop'
            assert settings.s3.endpoint == 'http://localhost:9000'
            assert settings.flink.parallelism == 6
            assert settings.batch.batch_size == 1000
            assert settings.monitoring.metrics_enabled is True
            assert settings.app.app_name == 'test-app'

    def test_settings_type_validation(self):
        """Test that settings validate types correctly"""
        with patch.dict(os.environ, {
            'KAFKA_BROKERS': 'localhost:9092',
            'KAFKA_TOPIC': 'test-topic',
            'KAFKA_GROUP_ID': 'test-group',
            'FLINK_PARALLELISM': 'not-a-number',  # Invalid type
            'FLINK_CHECKPOINT_INTERVAL_MS': '60000',
            'FLINK_CHECKPOINT_TIMEOUT_MS': '600000',
            'FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS_MS': '30000',
            'FLINK_MAX_CONCURRENT_CHECKPOINTS': '1',
            'FLINK_CHECKPOINT_MODE': 'EXACTLY_ONCE',
            'FLINK_STATE_BACKEND': 'rocksdb',
            'FLINK_CHECKPOINT_STORAGE': 's3a://checkpoints'
        }):
            with pytest.raises(Exception):  # Pydantic validation error
                FlinkSettings()
