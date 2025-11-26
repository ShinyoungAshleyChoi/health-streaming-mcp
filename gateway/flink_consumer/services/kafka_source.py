"""Kafka source connector for PyFlink with Avro deserialization support"""

import logging
from typing import Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema
)

from flink_consumer.config.settings import KafkaSettings, SchemaRegistrySettings
from flink_consumer.converters.avro_deserializer import AvroDeserializationSchema

logger = logging.getLogger(__name__)


class KafkaSourceBuilder:
    """
    Builder class for creating Kafka source connectors with Avro deserialization.
    
    This builder configures the Kafka source with proper settings for consuming
    health data from Kafka topics with Schema Registry integration.
    """

    def __init__(
        self,
        kafka_settings: KafkaSettings,
        schema_registry_settings: SchemaRegistrySettings
    ):
        """
        Initialize Kafka source builder.
        
        Args:
            kafka_settings: Kafka configuration settings
            schema_registry_settings: Schema Registry configuration settings
        """
        self.kafka_settings = kafka_settings
        self.schema_registry_settings = schema_registry_settings
        
        logger.info(
            f"Initialized KafkaSourceBuilder for topic: {kafka_settings.topic}, "
            f"group: {kafka_settings.group_id}"
        )

    def build(self) -> KafkaSource:
        """
        Build and configure the Kafka source connector.
        
        Returns:
            Configured KafkaSource instance ready for use in Flink pipeline
        """
        logger.info("Building Kafka source connector...")
        
        # Create Avro deserialization schema
        deserialization_schema = AvroDeserializationSchema(
            schema_registry_url=self.schema_registry_settings.url,
            subject_name=f"{self.kafka_settings.topic}-value"
        )
        
        # Build Kafka source
        kafka_source_builder = KafkaSource.builder()
        
        # Set bootstrap servers
        kafka_source_builder.set_bootstrap_servers(self.kafka_settings.brokers)
        logger.info(f"Kafka brokers: {self.kafka_settings.brokers}")
        
        # Set topic(s) to consume
        kafka_source_builder.set_topics(self.kafka_settings.topic)
        logger.info(f"Kafka topic: {self.kafka_settings.topic}")
        
        # Set consumer group ID
        kafka_source_builder.set_group_id(self.kafka_settings.group_id)
        logger.info(f"Consumer group ID: {self.kafka_settings.group_id}")
        
        # Set offset initialization strategy
        offset_initializer = self._get_offset_initializer()
        kafka_source_builder.set_starting_offsets(offset_initializer)
        logger.info(f"Offset reset strategy: {self.kafka_settings.auto_offset_reset}")
        
        # Set deserialization schema
        kafka_source_builder.set_value_only_deserializer(deserialization_schema)
        logger.info("Avro deserialization schema configured")
        
        # Set partition discovery interval
        kafka_source_builder.set_property(
            "partition.discovery.interval.ms",
            str(self.kafka_settings.partition_discovery_interval_ms)
        )
        logger.info(
            f"Partition discovery interval: "
            f"{self.kafka_settings.partition_discovery_interval_ms}ms"
        )
        
        # Disable auto-commit (Flink manages offsets via checkpoints)
        kafka_source_builder.set_property("enable.auto.commit", "false")
        logger.info("Auto-commit disabled (checkpoint-based offset management)")
        
        # Configure security if enabled
        if self.kafka_settings.security_protocol:
            self._configure_security(kafka_source_builder)
        
        # Build the source
        kafka_source = kafka_source_builder.build()
        logger.info("Kafka source connector built successfully")
        
        return kafka_source

    def _get_offset_initializer(self) -> KafkaOffsetsInitializer:
        """
        Get the appropriate offset initializer based on configuration.
        
        Returns:
            KafkaOffsetsInitializer instance
        """
        auto_offset_reset = self.kafka_settings.auto_offset_reset.lower()
        
        if auto_offset_reset == "earliest":
            return KafkaOffsetsInitializer.earliest()
        elif auto_offset_reset == "latest":
            return KafkaOffsetsInitializer.latest()
        elif auto_offset_reset == "committed":
            # Start from committed offsets, fallback to earliest if no commits
            return KafkaOffsetsInitializer.committed_offsets()
        else:
            logger.warning(
                f"Unknown offset reset strategy '{auto_offset_reset}', "
                f"defaulting to 'earliest'"
            )
            return KafkaOffsetsInitializer.earliest()

    def _configure_security(self, kafka_source_builder) -> None:
        """
        Configure Kafka security settings (SASL/SSL).
        
        Args:
            kafka_source_builder: KafkaSource builder instance to configure
        """
        logger.info("Configuring Kafka security settings...")
        
        # Set security protocol
        kafka_source_builder.set_property(
            "security.protocol",
            self.kafka_settings.security_protocol
        )
        logger.info(f"Security protocol: {self.kafka_settings.security_protocol}")
        
        # Set SASL mechanism if configured
        if self.kafka_settings.sasl_mechanism:
            kafka_source_builder.set_property(
                "sasl.mechanism",
                self.kafka_settings.sasl_mechanism
            )
            logger.info(f"SASL mechanism: {self.kafka_settings.sasl_mechanism}")
        
        # Set SASL credentials if configured
        if self.kafka_settings.sasl_username and self.kafka_settings.sasl_password:
            jaas_config = (
                f"org.apache.kafka.common.security.scram.ScramLoginModule required "
                f'username="{self.kafka_settings.sasl_username}" '
                f'password="{self.kafka_settings.sasl_password}";'
            )
            kafka_source_builder.set_property("sasl.jaas.config", jaas_config)
            logger.info("SASL credentials configured")


def create_kafka_source(
    kafka_settings: KafkaSettings,
    schema_registry_settings: SchemaRegistrySettings
) -> KafkaSource:
    """
    Convenience function to create a Kafka source connector.
    
    Args:
        kafka_settings: Kafka configuration settings
        schema_registry_settings: Schema Registry configuration settings
        
    Returns:
        Configured KafkaSource instance
    """
    builder = KafkaSourceBuilder(kafka_settings, schema_registry_settings)
    return builder.build()


def add_kafka_source_to_env(
    env: StreamExecutionEnvironment,
    kafka_settings: KafkaSettings,
    schema_registry_settings: SchemaRegistrySettings,
    source_name: str = "Kafka Source"
):
    """
    Create and add Kafka source to Flink execution environment.
    
    Args:
        env: Flink StreamExecutionEnvironment
        kafka_settings: Kafka configuration settings
        schema_registry_settings: Schema Registry configuration settings
        source_name: Name for the source operator
        
    Returns:
        DataStream from the Kafka source
    """
    logger.info(f"Adding Kafka source '{source_name}' to execution environment")
    
    # Create Kafka source
    kafka_source = create_kafka_source(kafka_settings, schema_registry_settings)
    
    # Add source to environment
    data_stream = env.from_source(
        kafka_source,
        watermark_strategy=None,  # Will be configured later with event time
        source_name=source_name
    )
    
    logger.info(f"Kafka source '{source_name}' added successfully")
    return data_stream
