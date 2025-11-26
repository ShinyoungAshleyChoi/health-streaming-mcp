"""Avro deserialization module for Kafka messages with Schema Registry integration"""

import logging
from typing import Any, Dict, Optional

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from pyflink.common.serialization import DeserializationSchema
from pyflink.common.typeinfo import Types

logger = logging.getLogger(__name__)


class AvroDeserializationSchema(DeserializationSchema):
    """
    Custom Avro deserialization schema for PyFlink that integrates with Schema Registry.
    
    This deserializer fetches schemas from Schema Registry and deserializes Avro-encoded
    messages from Kafka into Python dictionaries.
    """

    def __init__(self, schema_registry_url: str, subject_name: Optional[str] = None):
        """
        Initialize Avro deserializer with Schema Registry client.
        
        Args:
            schema_registry_url: URL of the Schema Registry service
            subject_name: Optional subject name for schema lookup (defaults to topic-value)
        """
        self.schema_registry_url = schema_registry_url
        self.subject_name = subject_name
        self._schema_registry_client: Optional[SchemaRegistryClient] = None
        self._avro_deserializer: Optional[AvroDeserializer] = None
        
        logger.info(
            f"Initialized AvroDeserializationSchema with registry: {schema_registry_url}"
        )

    def open(self, runtime_context) -> None:
        """
        Initialize Schema Registry client and Avro deserializer.
        Called once when the deserialization schema is opened.
        
        Args:
            runtime_context: Flink runtime context
        """
        try:
            # Initialize Schema Registry client
            schema_registry_conf = {'url': self.schema_registry_url}
            self._schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            
            # Initialize Avro deserializer
            # The deserializer will automatically fetch schemas from registry
            self._avro_deserializer = AvroDeserializer(
                schema_registry_client=self._schema_registry_client,
                schema_str=None,  # Will be fetched from registry
                from_dict=None    # Use default dict conversion
            )
            
            logger.info("Schema Registry client and Avro deserializer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Schema Registry client: {e}")
            raise

    def deserialize(self, message: bytes) -> Optional[Dict[str, Any]]:
        """
        Deserialize Avro-encoded message bytes into a Python dictionary.
        
        Args:
            message: Raw Avro-encoded message bytes from Kafka
            
        Returns:
            Deserialized message as a dictionary, or None if deserialization fails
        """
        if not message:
            logger.warning("Received empty message, skipping deserialization")
            return None
            
        try:
            # Deserialize using confluent-kafka's AvroDeserializer
            # This handles schema ID extraction and schema lookup automatically
            deserialized = self._avro_deserializer(message, None)
            
            if deserialized is None:
                logger.warning("Deserialization returned None")
                return None
                
            logger.debug(f"Successfully deserialized message: {deserialized.get('deviceId', 'unknown')}")
            return deserialized
            
        except Exception as e:
            logger.error(f"Failed to deserialize Avro message: {e}", exc_info=True)
            # Return None to skip corrupted messages
            return None

    def get_produced_type(self):
        """
        Return the type information for the deserialized data.
        
        Returns:
            TypeInformation indicating the output type is a generic Python object
        """
        # Return generic type info for Python dictionaries
        return Types.PICKLED_BYTE_ARRAY()

    def is_end_of_stream(self, next_element: Any) -> bool:
        """
        Check if the stream has ended.
        
        Args:
            next_element: The next element in the stream
            
        Returns:
            False as Kafka streams are unbounded
        """
        return False


class SchemaRegistryManager:
    """
    Manager class for Schema Registry operations.
    
    Provides utilities for schema lookup, validation, and version management.
    """

    def __init__(self, schema_registry_url: str):
        """
        Initialize Schema Registry manager.
        
        Args:
            schema_registry_url: URL of the Schema Registry service
        """
        self.schema_registry_url = schema_registry_url
        self._client: Optional[SchemaRegistryClient] = None
        
    def connect(self) -> None:
        """Establish connection to Schema Registry."""
        try:
            schema_registry_conf = {'url': self.schema_registry_url}
            self._client = SchemaRegistryClient(schema_registry_conf)
            logger.info(f"Connected to Schema Registry at {self.schema_registry_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Schema Registry: {e}")
            raise

    def get_latest_schema(self, subject: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest schema for a subject.
        
        Args:
            subject: Subject name (e.g., 'health-data-raw-value')
            
        Returns:
            Schema metadata including schema string and version
        """
        if not self._client:
            self.connect()
            
        try:
            schema = self._client.get_latest_version(subject)
            logger.info(f"Retrieved latest schema for subject '{subject}': version {schema.version}")
            return {
                'schema_id': schema.schema_id,
                'schema': schema.schema.schema_str,
                'version': schema.version,
                'subject': subject
            }
        except Exception as e:
            logger.error(f"Failed to get latest schema for subject '{subject}': {e}")
            return None

    def get_schema_by_id(self, schema_id: int) -> Optional[str]:
        """
        Get schema by its ID.
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema string or None if not found
        """
        if not self._client:
            self.connect()
            
        try:
            schema = self._client.get_schema(schema_id)
            logger.debug(f"Retrieved schema with ID {schema_id}")
            return schema.schema_str
        except Exception as e:
            logger.error(f"Failed to get schema with ID {schema_id}: {e}")
            return None

    def check_compatibility(self, subject: str, schema_str: str) -> bool:
        """
        Check if a schema is compatible with the subject's compatibility settings.
        
        Args:
            subject: Subject name
            schema_str: Schema string to check
            
        Returns:
            True if compatible, False otherwise
        """
        if not self._client:
            self.connect()
            
        try:
            is_compatible = self._client.test_compatibility(subject, schema_str)
            logger.info(f"Schema compatibility check for '{subject}': {is_compatible}")
            return is_compatible
        except Exception as e:
            logger.error(f"Failed to check schema compatibility: {e}")
            return False
