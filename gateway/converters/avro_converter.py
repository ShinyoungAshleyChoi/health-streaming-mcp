"""
Avro converter for health data
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional
from io import BytesIO
import fastavro
from models.health_data import HealthDataPayload


class AvroConversionError(Exception):
    """Avro conversion error"""
    pass


class AvroConverter:
    """Converter for JSON to Avro format"""
    
    def __init__(self, schema_path: str = None):
        """
        Initialize Avro converter
        
        Args:
            schema_path: Path to Avro schema file
        """
        if schema_path is None:
            # Try to get from config, fallback to relative path
            try:
                from config import settings
                schema_path = settings.schema_path
            except:
                # Fallback for when config is not available
                schema_path = Path(__file__).parent.parent.parent / "schemas" / "health-data.avsc"
        
        self.schema_path = Path(schema_path)
        self.schema = self._load_schema()
        self.parsed_schema = fastavro.parse_schema(self.schema)
        self.schema_id: Optional[int] = None
    
    def _load_schema(self) -> Dict[str, Any]:
        """Load Avro schema from file"""
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            raise AvroConversionError(f"Schema file not found: {self.schema_path}")
        except json.JSONDecodeError as e:
            raise AvroConversionError(f"Invalid JSON in schema file: {e}")
    
    def convert(self, payload: HealthDataPayload) -> bytes:
        """
        Convert health data payload to Avro binary format
        
        Args:
            payload: Health data payload
            
        Returns:
            Avro binary data
            
        Raises:
            AvroConversionError: If conversion fails
        """
        try:
            # Convert Pydantic model to dict
            data = self._payload_to_dict(payload)
            
            # Serialize to Avro binary
            output = BytesIO()
            fastavro.schemaless_writer(output, self.parsed_schema, data)
            
            return output.getvalue()
            
        except Exception as e:
            raise AvroConversionError(f"Failed to convert to Avro: {e}")
    
    def _payload_to_dict(self, payload: HealthDataPayload) -> Dict[str, Any]:
        """
        Convert Pydantic payload to dict compatible with Avro schema
        
        Args:
            payload: Health data payload
            
        Returns:
            Dictionary compatible with Avro schema
        """
        return {
            "deviceId": payload.deviceId,
            "userId": payload.userId,
            "samples": [
                {
                    "id": sample.id,
                    "type": sample.type,
                    "value": float(sample.value),
                    "unit": sample.unit,
                    "startDate": sample.startDate,
                    "endDate": sample.endDate,
                    "sourceBundle": sample.sourceBundle,
                    "metadata": sample.metadata,
                    "isSynced": sample.isSynced,
                    "createdAt": sample.createdAt
                }
                for sample in payload.samples
            ],
            "timestamp": payload.timestamp,
            "appVersion": payload.appVersion
        }
    
    def decode(self, avro_data: bytes) -> Dict[str, Any]:
        """
        Decode Avro binary data back to dict
        
        Args:
            avro_data: Avro binary data
            
        Returns:
            Decoded data as dictionary
            
        Raises:
            AvroConversionError: If decoding fails
        """
        try:
            input_stream = BytesIO(avro_data)
            return fastavro.schemaless_reader(input_stream, self.parsed_schema)
        except Exception as e:
            raise AvroConversionError(f"Failed to decode Avro data: {e}")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the loaded Avro schema"""
        return self.schema
    
    def set_schema_id(self, schema_id: int):
        """Set the schema ID from Schema Registry"""
        self.schema_id = schema_id
    
    def get_schema_id(self) -> Optional[int]:
        """Get the schema ID"""
        return self.schema_id


# Singleton instance
avro_converter = AvroConverter()
