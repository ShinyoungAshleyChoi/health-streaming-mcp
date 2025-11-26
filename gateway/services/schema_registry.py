"""
Schema Registry client service
"""
import json
import struct
from typing import Dict, Any, Optional
import httpx
from config import settings


class SchemaRegistryError(Exception):
    """Schema Registry error"""
    pass


class SchemaRegistryClient:
    """Client for Confluent Schema Registry"""
    
    MAGIC_BYTE = 0
    
    def __init__(self, url: str = None):
        """
        Initialize Schema Registry client
        
        Args:
            url: Schema Registry URL
        """
        self.url = url or settings.schema_registry_url
        self.schema_cache: Dict[str, Dict[str, Any]] = {}
        self.id_cache: Dict[int, Dict[str, Any]] = {}
    
    async def register_schema(self, subject: str, schema: Dict[str, Any]) -> int:
        """
        Register a schema with Schema Registry
        
        Args:
            subject: Subject name (e.g., "health-data-raw-value")
            schema: Avro schema as dictionary
            
        Returns:
            Schema ID
            
        Raises:
            SchemaRegistryError: If registration fails
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.url}/subjects/{subject}/versions",
                    json={"schema": json.dumps(schema)},
                    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                    timeout=10.0
                )
                
                if response.status_code in [200, 201]:
                    data = response.json()
                    schema_id = data.get("id")
                    
                    # Cache the schema
                    self.schema_cache[subject] = {
                        "id": schema_id,
                        "schema": schema
                    }
                    self.id_cache[schema_id] = schema
                    
                    return schema_id
                else:
                    raise SchemaRegistryError(
                        f"Failed to register schema: HTTP {response.status_code} - {response.text}"
                    )
                    
        except httpx.RequestError as e:
            raise SchemaRegistryError(f"Schema Registry request failed: {e}")
    
    async def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        """
        Get schema by ID
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema as dictionary
            
        Raises:
            SchemaRegistryError: If retrieval fails
        """
        # Check cache first
        if schema_id in self.id_cache:
            return self.id_cache[schema_id]
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.url}/schemas/ids/{schema_id}",
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    schema = json.loads(data.get("schema"))
                    
                    # Cache the schema
                    self.id_cache[schema_id] = schema
                    
                    return schema
                else:
                    raise SchemaRegistryError(
                        f"Failed to get schema: HTTP {response.status_code}"
                    )
                    
        except httpx.RequestError as e:
            # If request fails, try to use cached schema
            if schema_id in self.id_cache:
                return self.id_cache[schema_id]
            raise SchemaRegistryError(f"Schema Registry request failed: {e}")
    
    async def get_latest_schema(self, subject: str) -> Dict[str, Any]:
        """
        Get latest schema version for a subject
        
        Args:
            subject: Subject name
            
        Returns:
            Dictionary with schema ID and schema
            
        Raises:
            SchemaRegistryError: If retrieval fails
        """
        # Check cache first
        if subject in self.schema_cache:
            return self.schema_cache[subject]
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.url}/subjects/{subject}/versions/latest",
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    schema_id = data.get("id")
                    schema = json.loads(data.get("schema"))
                    
                    # Cache the schema
                    result = {"id": schema_id, "schema": schema}
                    self.schema_cache[subject] = result
                    self.id_cache[schema_id] = schema
                    
                    return result
                else:
                    raise SchemaRegistryError(
                        f"Failed to get latest schema: HTTP {response.status_code}"
                    )
                    
        except httpx.RequestError as e:
            # If request fails, try to use cached schema
            if subject in self.schema_cache:
                return self.schema_cache[subject]
            raise SchemaRegistryError(f"Schema Registry request failed: {e}")
    
    def encode_message(self, schema_id: int, avro_data: bytes) -> bytes:
        """
        Encode message with schema ID for Kafka
        
        Format: [MAGIC_BYTE][SCHEMA_ID][AVRO_DATA]
        
        Args:
            schema_id: Schema ID from Schema Registry
            avro_data: Avro binary data
            
        Returns:
            Encoded message with schema ID
        """
        # Pack: magic byte (1 byte) + schema ID (4 bytes big-endian) + avro data
        return struct.pack('>bI', self.MAGIC_BYTE, schema_id) + avro_data
    
    def decode_message(self, message: bytes) -> tuple[int, bytes]:
        """
        Decode message to extract schema ID and Avro data
        
        Args:
            message: Encoded message
            
        Returns:
            Tuple of (schema_id, avro_data)
            
        Raises:
            SchemaRegistryError: If message format is invalid
        """
        if len(message) < 5:
            raise SchemaRegistryError("Message too short")
        
        magic_byte, schema_id = struct.unpack('>bI', message[:5])
        
        if magic_byte != self.MAGIC_BYTE:
            raise SchemaRegistryError(f"Invalid magic byte: {magic_byte}")
        
        avro_data = message[5:]
        
        return schema_id, avro_data
    
    async def list_subjects(self) -> list[str]:
        """
        List all subjects in Schema Registry
        
        Returns:
            List of subject names
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.url}/subjects",
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    return []
                    
        except httpx.RequestError:
            return []


# Singleton instance
schema_registry_client = SchemaRegistryClient()
