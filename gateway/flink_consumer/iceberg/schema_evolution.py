"""Iceberg schema evolution utilities"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
    LongType,
    IntegerType,
    FloatType,
    IcebergType,
)

logger = logging.getLogger(__name__)


class SchemaOperationType(Enum):
    """Types of schema operations"""
    ADD_COLUMN = "ADD_COLUMN"
    DROP_COLUMN = "DROP_COLUMN"
    RENAME_COLUMN = "RENAME_COLUMN"
    UPDATE_COLUMN_TYPE = "UPDATE_COLUMN_TYPE"
    MAKE_COLUMN_OPTIONAL = "MAKE_COLUMN_OPTIONAL"


@dataclass
class SchemaOperation:
    """Represents a schema operation"""
    operation_type: SchemaOperationType
    column_name: str
    new_column_name: Optional[str] = None
    column_type: Optional[str] = None
    required: bool = False
    doc: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


@dataclass
class SchemaEvolutionHistory:
    """Represents schema evolution history entry"""
    snapshot_id: int
    timestamp: datetime
    operations: List[SchemaOperation]
    schema_id: int
    description: str


class IcebergSchemaEvolutionManager:
    """
    Manager for Iceberg table schema evolution.
    
    Provides utilities for:
    - Adding columns
    - Removing columns
    - Renaming columns
    - Updating column types
    - Tracking schema change history
    """

    def __init__(
        self, 
        catalog_name: str,
        catalog_type: str,
        warehouse: str,
        catalog_uri: Optional[str] = None
    ):
        """
        Initialize Iceberg schema evolution manager.
        
        Args:
            catalog_name: Name of the Iceberg catalog
            catalog_type: Type of catalog (hadoop, hive, rest)
            warehouse: Warehouse location
            catalog_uri: Optional catalog URI (for Hive metastore)
        """
        self.catalog_name = catalog_name
        self.catalog_type = catalog_type
        self.warehouse = warehouse
        self.catalog_uri = catalog_uri
        self._catalog: Optional[Catalog] = None
        self._schema_history: Dict[str, List[SchemaEvolutionHistory]] = {}
        
        logger.info(
            f"Initialized IcebergSchemaEvolutionManager: "
            f"catalog={catalog_name}, type={catalog_type}"
        )

    def connect(self) -> None:
        """Establish connection to Iceberg catalog."""
        try:
            catalog_config = {
                'type': self.catalog_type,
                'warehouse': self.warehouse,
            }
            
            if self.catalog_uri:
                catalog_config['uri'] = self.catalog_uri
            
            self._catalog = load_catalog(self.catalog_name, **catalog_config)
            
            logger.info(f"Connected to Iceberg catalog '{self.catalog_name}'")
            
        except Exception as e:
            logger.error(f"Failed to connect to Iceberg catalog: {e}")
            raise

    def get_table(self, database: str, table_name: str) -> Optional[Table]:
        """
        Get Iceberg table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Iceberg Table object or None if not found
        """
        if not self._catalog:
            self.connect()
        
        try:
            table_identifier = f"{database}.{table_name}"
            table = self._catalog.load_table(table_identifier)
            
            logger.info(f"Loaded table '{table_identifier}'")
            return table
            
        except Exception as e:
            logger.error(f"Failed to load table '{database}.{table_name}': {e}")
            return None

    def add_column(
        self,
        database: str,
        table_name: str,
        column_name: str,
        column_type: IcebergType,
        required: bool = False,
        doc: Optional[str] = None,
        after_column: Optional[str] = None
    ) -> bool:
        """
        Add a new column to an Iceberg table.
        
        Args:
            database: Database name
            table_name: Table name
            column_name: Name of the new column
            column_type: Iceberg type for the column
            required: Whether the column is required
            doc: Optional documentation string
            after_column: Optional column name to insert after
            
        Returns:
            True if successful, False otherwise
        """
        table = self.get_table(database, table_name)
        
        if not table:
            return False
        
        try:
            with table.update_schema() as update:
                update.add_column(
                    path=column_name,
                    field_type=column_type,
                    required=required,
                    doc=doc
                )
            
            # Log the operation
            operation = SchemaOperation(
                operation_type=SchemaOperationType.ADD_COLUMN,
                column_name=column_name,
                column_type=str(column_type),
                required=required,
                doc=doc
            )
            
            self._log_schema_change(
                f"{database}.{table_name}",
                [operation],
                f"Added column '{column_name}'"
            )
            
            logger.info(
                f"Successfully added column '{column_name}' to table "
                f"'{database}.{table_name}'"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to add column '{column_name}' to table "
                f"'{database}.{table_name}': {e}"
            )
            return False

    def drop_column(
        self,
        database: str,
        table_name: str,
        column_name: str
    ) -> bool:
        """
        Drop a column from an Iceberg table.
        
        Note: Iceberg doesn't physically delete columns, it marks them as deleted.
        
        Args:
            database: Database name
            table_name: Table name
            column_name: Name of the column to drop
            
        Returns:
            True if successful, False otherwise
        """
        table = self.get_table(database, table_name)
        
        if not table:
            return False
        
        try:
            with table.update_schema() as update:
                update.delete_column(column_name)
            
            # Log the operation
            operation = SchemaOperation(
                operation_type=SchemaOperationType.DROP_COLUMN,
                column_name=column_name
            )
            
            self._log_schema_change(
                f"{database}.{table_name}",
                [operation],
                f"Dropped column '{column_name}'"
            )
            
            logger.info(
                f"Successfully dropped column '{column_name}' from table "
                f"'{database}.{table_name}'"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to drop column '{column_name}' from table "
                f"'{database}.{table_name}': {e}"
            )
            return False

    def rename_column(
        self,
        database: str,
        table_name: str,
        old_column_name: str,
        new_column_name: str
    ) -> bool:
        """
        Rename a column in an Iceberg table.
        
        Args:
            database: Database name
            table_name: Table name
            old_column_name: Current column name
            new_column_name: New column name
            
        Returns:
            True if successful, False otherwise
        """
        table = self.get_table(database, table_name)
        
        if not table:
            return False
        
        try:
            with table.update_schema() as update:
                update.rename_column(old_column_name, new_column_name)
            
            # Log the operation
            operation = SchemaOperation(
                operation_type=SchemaOperationType.RENAME_COLUMN,
                column_name=old_column_name,
                new_column_name=new_column_name
            )
            
            self._log_schema_change(
                f"{database}.{table_name}",
                [operation],
                f"Renamed column '{old_column_name}' to '{new_column_name}'"
            )
            
            logger.info(
                f"Successfully renamed column '{old_column_name}' to "
                f"'{new_column_name}' in table '{database}.{table_name}'"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to rename column '{old_column_name}' to "
                f"'{new_column_name}' in table '{database}.{table_name}': {e}"
            )
            return False

    def update_column_type(
        self,
        database: str,
        table_name: str,
        column_name: str,
        new_type: IcebergType
    ) -> bool:
        """
        Update column type in an Iceberg table.
        
        Note: Only compatible type promotions are allowed (e.g., int -> long).
        
        Args:
            database: Database name
            table_name: Table name
            column_name: Column name
            new_type: New Iceberg type
            
        Returns:
            True if successful, False otherwise
        """
        table = self.get_table(database, table_name)
        
        if not table:
            return False
        
        try:
            with table.update_schema() as update:
                update.update_column(column_name, field_type=new_type)
            
            # Log the operation
            operation = SchemaOperation(
                operation_type=SchemaOperationType.UPDATE_COLUMN_TYPE,
                column_name=column_name,
                column_type=str(new_type)
            )
            
            self._log_schema_change(
                f"{database}.{table_name}",
                [operation],
                f"Updated column '{column_name}' type to {new_type}"
            )
            
            logger.info(
                f"Successfully updated column '{column_name}' type to {new_type} "
                f"in table '{database}.{table_name}'"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to update column '{column_name}' type in table "
                f"'{database}.{table_name}': {e}"
            )
            return False

    def make_column_optional(
        self,
        database: str,
        table_name: str,
        column_name: str
    ) -> bool:
        """
        Make a required column optional.
        
        Args:
            database: Database name
            table_name: Table name
            column_name: Column name
            
        Returns:
            True if successful, False otherwise
        """
        table = self.get_table(database, table_name)
        
        if not table:
            return False
        
        try:
            with table.update_schema() as update:
                update.make_column_optional(column_name)
            
            # Log the operation
            operation = SchemaOperation(
                operation_type=SchemaOperationType.MAKE_COLUMN_OPTIONAL,
                column_name=column_name
            )
            
            self._log_schema_change(
                f"{database}.{table_name}",
                [operation],
                f"Made column '{column_name}' optional"
            )
            
            logger.info(
                f"Successfully made column '{column_name}' optional in table "
                f"'{database}.{table_name}'"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to make column '{column_name}' optional in table "
                f"'{database}.{table_name}': {e}"
            )
            return False

    def get_schema_history(
        self,
        database: str,
        table_name: str
    ) -> List[SchemaEvolutionHistory]:
        """
        Get schema evolution history for a table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            List of SchemaEvolutionHistory entries
        """
        table_identifier = f"{database}.{table_name}"
        return self._schema_history.get(table_identifier, [])

    def get_current_schema(
        self,
        database: str,
        table_name: str
    ) -> Optional[Schema]:
        """
        Get current schema for a table.
        
        Args:
            database: Database name
            table_name: Table name
            
        Returns:
            Current Iceberg Schema or None if table not found
        """
        table = self.get_table(database, table_name)
        
        if not table:
            return None
        
        return table.schema()

    def validate_schema_change(
        self,
        database: str,
        table_name: str,
        operation: SchemaOperation
    ) -> tuple[bool, Optional[str]]:
        """
        Validate if a schema change is safe to apply.
        
        Args:
            database: Database name
            table_name: Table name
            operation: Schema operation to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        table = self.get_table(database, table_name)
        
        if not table:
            return False, f"Table '{database}.{table_name}' not found"
        
        current_schema = table.schema()
        
        # Validate based on operation type
        if operation.operation_type == SchemaOperationType.ADD_COLUMN:
            # Check if column already exists
            if operation.column_name in [f.name for f in current_schema.fields]:
                return False, f"Column '{operation.column_name}' already exists"
            
            # Check if adding required column without default
            if operation.required:
                return False, (
                    f"Cannot add required column '{operation.column_name}' "
                    "without default value (breaking change)"
                )
        
        elif operation.operation_type == SchemaOperationType.DROP_COLUMN:
            # Check if column exists
            if operation.column_name not in [f.name for f in current_schema.fields]:
                return False, f"Column '{operation.column_name}' does not exist"
        
        elif operation.operation_type == SchemaOperationType.RENAME_COLUMN:
            # Check if old column exists
            if operation.column_name not in [f.name for f in current_schema.fields]:
                return False, f"Column '{operation.column_name}' does not exist"
            
            # Check if new column name already exists
            if operation.new_column_name in [f.name for f in current_schema.fields]:
                return False, f"Column '{operation.new_column_name}' already exists"
        
        return True, None

    def _log_schema_change(
        self,
        table_identifier: str,
        operations: List[SchemaOperation],
        description: str
    ) -> None:
        """
        Log schema change to history.
        
        Args:
            table_identifier: Full table identifier (database.table)
            operations: List of schema operations
            description: Description of the change
        """
        if table_identifier not in self._schema_history:
            self._schema_history[table_identifier] = []
        
        history_entry = SchemaEvolutionHistory(
            snapshot_id=0,  # Will be updated with actual snapshot ID
            timestamp=datetime.now(timezone.utc),
            operations=operations,
            schema_id=0,  # Will be updated with actual schema ID
            description=description
        )
        
        self._schema_history[table_identifier].append(history_entry)
        
        logger.info(
            f"Logged schema change for '{table_identifier}': {description}"
        )

    def print_schema_history(
        self,
        database: str,
        table_name: str
    ) -> None:
        """
        Print schema evolution history for a table.
        
        Args:
            database: Database name
            table_name: Table name
        """
        table_identifier = f"{database}.{table_name}"
        history = self.get_schema_history(database, table_name)
        
        if not history:
            logger.info(f"No schema evolution history for '{table_identifier}'")
            return
        
        logger.info(f"\nSchema Evolution History for '{table_identifier}':")
        logger.info("=" * 80)
        
        for entry in history:
            logger.info(f"\nTimestamp: {entry.timestamp.isoformat()}")
            logger.info(f"Description: {entry.description}")
            logger.info(f"Operations:")
            
            for op in entry.operations:
                logger.info(f"  - {op.operation_type.value}: {op.column_name}")
                if op.new_column_name:
                    logger.info(f"    New name: {op.new_column_name}")
                if op.column_type:
                    logger.info(f"    Type: {op.column_type}")
                if op.doc:
                    logger.info(f"    Doc: {op.doc}")
        
        logger.info("=" * 80)


def get_iceberg_type_from_string(type_str: str) -> Optional[IcebergType]:
    """
    Convert string type name to Iceberg type.
    
    Args:
        type_str: Type name as string (e.g., 'string', 'long', 'double')
        
    Returns:
        Corresponding Iceberg type or None if not recognized
    """
    type_mapping = {
        'string': StringType(),
        'long': LongType(),
        'int': IntegerType(),
        'integer': IntegerType(),
        'double': DoubleType(),
        'float': FloatType(),
        'boolean': BooleanType(),
        'bool': BooleanType(),
        'timestamp': TimestampType(),
    }
    
    return type_mapping.get(type_str.lower())
