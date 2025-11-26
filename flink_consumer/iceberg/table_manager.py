"""Iceberg table management and auto-creation"""

import logging
from typing import Optional
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.table import Table
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError

from flink_consumer.config.settings import Settings
from flink_consumer.iceberg.catalog import IcebergCatalog
from flink_consumer.iceberg.schemas import (
    get_health_data_raw_schema,
    get_health_data_raw_partition_spec,
    get_health_data_raw_properties,
    get_health_data_errors_schema,
    get_health_data_errors_partition_spec,
    get_health_data_errors_properties,
    get_health_data_daily_agg_schema,
    get_health_data_daily_agg_partition_spec,
    get_health_data_daily_agg_properties,
    get_health_data_weekly_agg_schema,
    get_health_data_weekly_agg_partition_spec,
    get_health_data_weekly_agg_properties,
    get_health_data_monthly_agg_schema,
    get_health_data_monthly_agg_partition_spec,
    get_health_data_monthly_agg_properties,
)

logger = logging.getLogger(__name__)


class IcebergTableManager:
    """
    Manages Iceberg table creation and metadata registration
    
    Handles automatic table creation with proper schemas, partitioning,
    and properties for health data tables.
    """

    def __init__(self, settings: Settings, catalog: IcebergCatalog):
        """
        Initialize table manager
        
        Args:
            settings: Application settings
            catalog: IcebergCatalog instance
        """
        self.settings = settings
        self.catalog = catalog
        self._catalog_instance: Optional[RestCatalog] = None

    def _get_catalog_instance(self) -> RestCatalog:
        """
        Get catalog instance, initializing if necessary
        
        Returns:
            RestCatalog instance
        """
        if self._catalog_instance is None:
            self._catalog_instance = self.catalog.get_catalog()
        return self._catalog_instance

    def ensure_namespace_exists(self) -> bool:
        """
        Ensure the database namespace exists, create if not
        
        Returns:
            True if namespace exists or was created successfully
        """
        namespace = self.settings.iceberg.database
        
        try:
            catalog_instance = self._get_catalog_instance()
            
            # Try to load namespace properties to check existence
            try:
                catalog_instance.load_namespace_properties(namespace)
                logger.info(f"Namespace already exists: {namespace}")
                return True
            except Exception:
                # Namespace doesn't exist, create it
                catalog_instance.create_namespace(namespace)
                logger.info(f"Created namespace: {namespace}")
                return True
                
        except NamespaceAlreadyExistsError:
            logger.info(f"Namespace already exists: {namespace}")
            return True
        except Exception as e:
            logger.error(
                f"Failed to ensure namespace exists: {str(e)}",
                extra={"namespace": namespace, "error": str(e)},
                exc_info=True
            )
            return False

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        namespace = self.settings.iceberg.database
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            catalog_instance = self._get_catalog_instance()
            catalog_instance.load_table(table_identifier)
            logger.info(f"Table exists: {table_identifier}")
            return True
        except NoSuchTableError:
            logger.info(f"Table does not exist: {table_identifier}")
            return False
        except Exception as e:
            logger.error(
                f"Error checking table existence: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return False

    def create_health_data_raw_table(self) -> Optional[Table]:
        """
        Create health_data_raw table with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = self.settings.iceberg.table_raw
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            schema = get_health_data_raw_schema()
            partition_spec = get_health_data_raw_partition_spec()
            properties = get_health_data_raw_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def create_health_data_errors_table(self) -> Optional[Table]:
        """
        Create health_data_errors table (DLQ) with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = self.settings.iceberg.table_errors
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            schema = get_health_data_errors_schema()
            partition_spec = get_health_data_errors_partition_spec()
            properties = get_health_data_errors_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def ensure_raw_table_exists(self) -> bool:
        """Ensure raw data table exists"""
        return self.create_health_data_raw_table() is not None

    def ensure_error_table_exists(self) -> bool:
        """Ensure error table exists"""
        return self.create_health_data_errors_table() is not None

    def initialize_all_aggregation_tables(self) -> bool:
        """
        Initialize all required tables (raw and errors)
        
        Creates namespace if needed and creates both tables.
        
        Returns:
            True if all tables were created/verified successfully
        """
        try:
            # Ensure namespace exists
            if not self.ensure_namespace_exists():
                logger.error("Failed to ensure namespace exists")
                return False
            
            # Create raw data table
            raw_table = self.create_health_data_raw_table()
            if raw_table is None:
                logger.error("Failed to create raw data table")
                return False
            
            # Create errors table
            errors_table = self.create_health_data_errors_table()
            if errors_table is None:
                logger.error("Failed to create errors table")
                return False
            
            logger.info(
                "Successfully initialized all Iceberg tables",
                extra={
                    "namespace": self.settings.iceberg.database,
                    "raw_table": self.settings.iceberg.table_raw,
                    "errors_table": self.settings.iceberg.table_errors,
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to initialize tables: {str(e)}",
                extra={"error": str(e)},
                exc_info=True
            )
            return False

    def get_table(self, table_name: str) -> Optional[Table]:
        """
        Load a table by name
        
        Args:
            table_name: Name of the table to load
            
        Returns:
            Table instance if found, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.load_table(table_identifier)
            logger.info(f"Loaded table: {table_identifier}")
            return table
        except NoSuchTableError:
            logger.warning(f"Table not found: {table_identifier}")
            return None
        except Exception as e:
            logger.error(
                f"Error loading table: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None


    def create_health_data_daily_agg_table(self) -> Optional[Table]:
        """
        Create health_data_daily_agg table with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = "health_data_daily_agg"
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            from flink_consumer.iceberg.schemas import (
                get_health_data_daily_agg_schema,
                get_health_data_daily_agg_partition_spec,
                get_health_data_daily_agg_properties,
            )
            
            schema = get_health_data_daily_agg_schema()
            partition_spec = get_health_data_daily_agg_partition_spec()
            properties = get_health_data_daily_agg_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def create_health_data_weekly_agg_table(self) -> Optional[Table]:
        """
        Create health_data_weekly_agg table with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = "health_data_weekly_agg"
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            from flink_consumer.iceberg.schemas import (
                get_health_data_weekly_agg_schema,
                get_health_data_weekly_agg_partition_spec,
                get_health_data_weekly_agg_properties,
            )
            
            schema = get_health_data_weekly_agg_schema()
            partition_spec = get_health_data_weekly_agg_partition_spec()
            properties = get_health_data_weekly_agg_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def create_health_data_monthly_agg_table(self) -> Optional[Table]:
        """
        Create health_data_monthly_agg table with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = "health_data_monthly_agg"
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            from flink_consumer.iceberg.schemas import (
                get_health_data_monthly_agg_schema,
                get_health_data_monthly_agg_partition_spec,
                get_health_data_monthly_agg_properties,
            )
            
            schema = get_health_data_monthly_agg_schema()
            partition_spec = get_health_data_monthly_agg_partition_spec()
            properties = get_health_data_monthly_agg_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def create_health_data_daily_agg_table(self) -> Optional[Table]:
        """
        Create health_data_daily_agg table with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = self.settings.iceberg.table_daily_agg
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            schema = get_health_data_daily_agg_schema()
            partition_spec = get_health_data_daily_agg_partition_spec()
            properties = get_health_data_daily_agg_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def create_health_data_weekly_agg_table(self) -> Optional[Table]:
        """
        Create health_data_weekly_agg table with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = self.settings.iceberg.table_weekly_agg
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            schema = get_health_data_weekly_agg_schema()
            partition_spec = get_health_data_weekly_agg_partition_spec()
            properties = get_health_data_weekly_agg_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def create_health_data_monthly_agg_table(self) -> Optional[Table]:
        """
        Create health_data_monthly_agg table with schema and partitioning
        
        Returns:
            Table instance if created successfully, None otherwise
        """
        namespace = self.settings.iceberg.database
        table_name = self.settings.iceberg.table_monthly_agg
        table_identifier = f"{namespace}.{table_name}"
        
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table already exists: {table_identifier}")
                catalog_instance = self._get_catalog_instance()
                return catalog_instance.load_table(table_identifier)
            
            # Get schema, partition spec, and properties
            schema = get_health_data_monthly_agg_schema()
            partition_spec = get_health_data_monthly_agg_partition_spec()
            properties = get_health_data_monthly_agg_properties()
            
            # Create table
            catalog_instance = self._get_catalog_instance()
            table = catalog_instance.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
            
            logger.info(
                f"Successfully created table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "schema_fields": len(schema.fields),
                    "partition_fields": len(partition_spec.fields),
                    "location": table.location(),
                }
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to create table {table_identifier}: {str(e)}",
                extra={
                    "table_identifier": table_identifier,
                    "error": str(e)
                },
                exc_info=True
            )
            return None

    def ensure_daily_agg_table_exists(self) -> bool:
        """Ensure daily aggregation table exists"""
        return self.create_health_data_daily_agg_table() is not None

    def ensure_weekly_agg_table_exists(self) -> bool:
        """Ensure weekly aggregation table exists"""
        return self.create_health_data_weekly_agg_table() is not None

    def ensure_monthly_agg_table_exists(self) -> bool:
        """Ensure monthly aggregation table exists"""
        return self.create_health_data_monthly_agg_table() is not None

    def initialize_all_aggregation_tables(self) -> bool:
        """
        Initialize all aggregation tables (daily, weekly, monthly)
        
        Returns:
            True if all tables were created/verified successfully
        """
        try:
            # Ensure namespace exists
            if not self.ensure_namespace_exists():
                logger.error("Failed to ensure namespace exists")
                return False
            
            # Create daily aggregation table
            if not self.ensure_daily_agg_table_exists():
                logger.error("Failed to create daily aggregation table")
                return False
            
            # Create weekly aggregation table
            if not self.ensure_weekly_agg_table_exists():
                logger.error("Failed to create weekly aggregation table")
                return False
            
            # Create monthly aggregation table
            if not self.ensure_monthly_agg_table_exists():
                logger.error("Failed to create monthly aggregation table")
                return False
            
            logger.info(
                "Successfully initialized all aggregation tables",
                extra={
                    "namespace": self.settings.iceberg.database,
                    "daily_table": self.settings.iceberg.table_daily_agg,
                    "weekly_table": self.settings.iceberg.table_weekly_agg,
                    "monthly_table": self.settings.iceberg.table_monthly_agg,
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to initialize aggregation tables: {str(e)}",
                extra={"error": str(e)},
                exc_info=True
            )
            return False
