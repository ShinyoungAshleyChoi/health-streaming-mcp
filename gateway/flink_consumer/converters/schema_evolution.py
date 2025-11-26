"""Schema evolution and compatibility checking module"""

import json
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

logger = logging.getLogger(__name__)


class CompatibilityType(Enum):
    """Schema compatibility types"""
    BACKWARD = "BACKWARD"  # New schema can read old data
    FORWARD = "FORWARD"  # Old schema can read new data
    FULL = "FULL"  # Both backward and forward compatible
    NONE = "NONE"  # No compatibility checking


class SchemaChangeType(Enum):
    """Types of schema changes"""
    FIELD_ADDED = "FIELD_ADDED"
    FIELD_REMOVED = "FIELD_REMOVED"
    FIELD_TYPE_CHANGED = "FIELD_TYPE_CHANGED"
    FIELD_RENAMED = "FIELD_RENAMED"
    DEFAULT_VALUE_CHANGED = "DEFAULT_VALUE_CHANGED"
    NO_CHANGE = "NO_CHANGE"


@dataclass
class SchemaChange:
    """Represents a schema change"""
    change_type: SchemaChangeType
    field_name: str
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    is_breaking: bool = False
    description: str = ""


@dataclass
class SchemaVersion:
    """Represents a schema version"""
    schema_id: int
    version: int
    schema_str: str
    subject: str


class SchemaCompatibilityChecker:
    """
    Checks schema compatibility between versions.
    
    This class provides utilities to:
    - Fetch latest schema from Schema Registry
    - Compare schema versions
    - Detect incompatible changes
    - Handle schema evolution errors
    """

    def __init__(self, schema_registry_url: str):
        """
        Initialize schema compatibility checker.
        
        Args:
            schema_registry_url: URL of the Schema Registry service
        """
        self.schema_registry_url = schema_registry_url
        self._client: Optional[SchemaRegistryClient] = None
        self._schema_cache: Dict[str, SchemaVersion] = {}
        
        logger.info(f"Initialized SchemaCompatibilityChecker with registry: {schema_registry_url}")

    def connect(self) -> None:
        """Establish connection to Schema Registry."""
        try:
            schema_registry_conf = {'url': self.schema_registry_url}
            self._client = SchemaRegistryClient(schema_registry_conf)
            logger.info(f"Connected to Schema Registry at {self.schema_registry_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Schema Registry: {e}")
            raise

    def get_latest_schema(self, subject: str) -> Optional[SchemaVersion]:
        """
        Get the latest schema version for a subject.
        
        Args:
            subject: Subject name (e.g., 'health-data-raw-value')
            
        Returns:
            SchemaVersion object or None if not found
        """
        if not self._client:
            self.connect()
            
        try:
            schema = self._client.get_latest_version(subject)
            
            schema_version = SchemaVersion(
                schema_id=schema.schema_id,
                version=schema.version,
                schema_str=schema.schema.schema_str,
                subject=subject
            )
            
            # Cache the schema
            self._schema_cache[subject] = schema_version
            
            logger.info(
                f"Retrieved latest schema for subject '{subject}': "
                f"version {schema.version}, ID {schema.schema_id}"
            )
            
            return schema_version
            
        except Exception as e:
            logger.error(f"Failed to get latest schema for subject '{subject}': {e}")
            return None

    def get_schema_by_version(self, subject: str, version: int) -> Optional[SchemaVersion]:
        """
        Get a specific schema version for a subject.
        
        Args:
            subject: Subject name
            version: Schema version number
            
        Returns:
            SchemaVersion object or None if not found
        """
        if not self._client:
            self.connect()
            
        try:
            schema = self._client.get_version(subject, version)
            
            schema_version = SchemaVersion(
                schema_id=schema.schema_id,
                version=schema.version,
                schema_str=schema.schema.schema_str,
                subject=subject
            )
            
            logger.info(
                f"Retrieved schema version {version} for subject '{subject}': "
                f"ID {schema.schema_id}"
            )
            
            return schema_version
            
        except Exception as e:
            logger.error(
                f"Failed to get schema version {version} for subject '{subject}': {e}"
            )
            return None

    def compare_schemas(
        self, 
        old_schema: SchemaVersion, 
        new_schema: SchemaVersion
    ) -> List[SchemaChange]:
        """
        Compare two schema versions and detect changes.
        
        Args:
            old_schema: Previous schema version
            new_schema: New schema version
            
        Returns:
            List of SchemaChange objects describing the differences
        """
        changes: List[SchemaChange] = []
        
        try:
            old_avro = json.loads(old_schema.schema_str)
            new_avro = json.loads(new_schema.schema_str)
            
            # Compare fields
            old_fields = {f['name']: f for f in old_avro.get('fields', [])}
            new_fields = {f['name']: f for f in new_avro.get('fields', [])}
            
            # Check for added fields
            for field_name, field_def in new_fields.items():
                if field_name not in old_fields:
                    # Field added - check if it has a default value
                    has_default = 'default' in field_def
                    is_breaking = not has_default
                    
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.FIELD_ADDED,
                        field_name=field_name,
                        new_value=field_def.get('type'),
                        is_breaking=is_breaking,
                        description=f"Field '{field_name}' added" + 
                                  (" without default value" if is_breaking else " with default value")
                    ))
            
            # Check for removed fields
            for field_name, field_def in old_fields.items():
                if field_name not in new_fields:
                    # Field removed - always breaking for backward compatibility
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.FIELD_REMOVED,
                        field_name=field_name,
                        old_value=field_def.get('type'),
                        is_breaking=True,
                        description=f"Field '{field_name}' removed (breaking change)"
                    ))
            
            # Check for type changes in existing fields
            for field_name in old_fields.keys() & new_fields.keys():
                old_type = self._normalize_type(old_fields[field_name].get('type'))
                new_type = self._normalize_type(new_fields[field_name].get('type'))
                
                if old_type != new_type:
                    # Type changed - check if it's a compatible widening
                    is_breaking = not self._is_type_widening(old_type, new_type)
                    
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.FIELD_TYPE_CHANGED,
                        field_name=field_name,
                        old_value=str(old_type),
                        new_value=str(new_type),
                        is_breaking=is_breaking,
                        description=f"Field '{field_name}' type changed from {old_type} to {new_type}" +
                                  (" (breaking)" if is_breaking else " (compatible widening)")
                    ))
                
                # Check for default value changes
                old_default = old_fields[field_name].get('default')
                new_default = new_fields[field_name].get('default')
                
                if old_default != new_default:
                    changes.append(SchemaChange(
                        change_type=SchemaChangeType.DEFAULT_VALUE_CHANGED,
                        field_name=field_name,
                        old_value=str(old_default),
                        new_value=str(new_default),
                        is_breaking=False,
                        description=f"Field '{field_name}' default value changed"
                    ))
            
            if not changes:
                changes.append(SchemaChange(
                    change_type=SchemaChangeType.NO_CHANGE,
                    field_name="",
                    description="No schema changes detected"
                ))
            
            logger.info(
                f"Schema comparison complete: {len(changes)} change(s) detected, "
                f"{sum(1 for c in changes if c.is_breaking)} breaking"
            )
            
        except Exception as e:
            logger.error(f"Failed to compare schemas: {e}", exc_info=True)
            raise
        
        return changes

    def check_compatibility(
        self, 
        subject: str, 
        new_schema_str: str,
        compatibility_type: CompatibilityType = CompatibilityType.BACKWARD
    ) -> Tuple[bool, List[str]]:
        """
        Check if a new schema is compatible with the subject's compatibility settings.
        
        Args:
            subject: Subject name
            new_schema_str: New schema string to check
            compatibility_type: Type of compatibility to check
            
        Returns:
            Tuple of (is_compatible, list of error messages)
        """
        if not self._client:
            self.connect()
            
        errors: List[str] = []
        
        try:
            # Test compatibility with Schema Registry
            is_compatible = self._client.test_compatibility(subject, new_schema_str)
            
            if not is_compatible:
                errors.append(
                    f"Schema is not compatible with subject '{subject}' "
                    f"according to {compatibility_type.value} compatibility rules"
                )
            
            logger.info(
                f"Schema compatibility check for '{subject}': "
                f"{'PASSED' if is_compatible else 'FAILED'}"
            )
            
            return is_compatible, errors
            
        except Exception as e:
            error_msg = f"Failed to check schema compatibility: {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            return False, errors

    def validate_schema_evolution(
        self, 
        subject: str, 
        allow_breaking_changes: bool = False
    ) -> Tuple[bool, List[SchemaChange]]:
        """
        Validate schema evolution by comparing current and latest versions.
        
        Args:
            subject: Subject name
            allow_breaking_changes: Whether to allow breaking changes
            
        Returns:
            Tuple of (is_valid, list of schema changes)
        """
        try:
            # Get latest schema
            latest_schema = self.get_latest_schema(subject)
            
            if not latest_schema:
                logger.warning(f"No schema found for subject '{subject}'")
                return True, []
            
            # If we have a cached previous version, compare
            if latest_schema.version > 1:
                previous_schema = self.get_schema_by_version(
                    subject, 
                    latest_schema.version - 1
                )
                
                if previous_schema:
                    changes = self.compare_schemas(previous_schema, latest_schema)
                    
                    # Check for breaking changes
                    breaking_changes = [c for c in changes if c.is_breaking]
                    
                    if breaking_changes and not allow_breaking_changes:
                        logger.error(
                            f"Breaking schema changes detected for '{subject}': "
                            f"{len(breaking_changes)} breaking change(s)"
                        )
                        for change in breaking_changes:
                            logger.error(f"  - {change.description}")
                        
                        return False, changes
                    
                    logger.info(
                        f"Schema evolution validation passed for '{subject}': "
                        f"{len(changes)} change(s), {len(breaking_changes)} breaking"
                    )
                    
                    return True, changes
            
            # First version or no previous version to compare
            logger.info(f"Schema version {latest_schema.version} is the first or only version")
            return True, []
            
        except Exception as e:
            logger.error(f"Failed to validate schema evolution: {e}", exc_info=True)
            return False, []

    def handle_incompatible_schema(
        self, 
        subject: str, 
        changes: List[SchemaChange]
    ) -> None:
        """
        Handle incompatible schema changes by logging errors and raising exception.
        
        Args:
            subject: Subject name
            changes: List of schema changes
            
        Raises:
            ValueError: If breaking changes are detected
        """
        breaking_changes = [c for c in changes if c.is_breaking]
        
        if not breaking_changes:
            return
        
        error_msg = (
            f"Incompatible schema changes detected for subject '{subject}'. "
            f"Breaking changes:\n"
        )
        
        for change in breaking_changes:
            error_msg += f"  - {change.description}\n"
        
        error_msg += (
            "\nTo resolve:\n"
            "1. Revert the schema changes to maintain compatibility\n"
            "2. Add default values for new required fields\n"
            "3. Use schema evolution best practices (add optional fields only)\n"
            "4. If intentional, set allow_breaking_changes=True"
        )
        
        logger.error(error_msg)
        raise ValueError(error_msg)

    @staticmethod
    def _normalize_type(field_type) -> str:
        """
        Normalize Avro field type to string representation.
        
        Args:
            field_type: Avro field type (can be string, list, or dict)
            
        Returns:
            Normalized type string
        """
        if isinstance(field_type, str):
            return field_type
        elif isinstance(field_type, list):
            # Union type - extract non-null type
            non_null_types = [t for t in field_type if t != 'null']
            return non_null_types[0] if non_null_types else 'null'
        elif isinstance(field_type, dict):
            return field_type.get('type', 'unknown')
        else:
            return str(field_type)

    @staticmethod
    def _is_type_widening(old_type: str, new_type: str) -> bool:
        """
        Check if type change is a compatible widening.
        
        Compatible widenings:
        - int -> long
        - float -> double
        - string -> string (no change)
        
        Args:
            old_type: Old field type
            new_type: New field type
            
        Returns:
            True if compatible widening, False otherwise
        """
        widening_rules = {
            'int': ['long'],
            'float': ['double'],
        }
        
        if old_type == new_type:
            return True
        
        return new_type in widening_rules.get(old_type, [])
