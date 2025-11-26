"""Iceberg sink implementation for PyFlink"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableDescriptor
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.common import Row
from pyflink.table.expressions import col

from flink_consumer.config.settings import Settings

logger = logging.getLogger(__name__)


class IcebergSink:
    """
    Iceberg sink for writing DataStream to Iceberg tables using PyFlink Table API
    
    Provides integration between PyFlink DataStream API and Iceberg tables,
    handling catalog registration, table creation, and batch writing.
    """

    def __init__(self, settings: Settings, env: StreamExecutionEnvironment):
        """
        Initialize Iceberg sink
        
        Args:
            settings: Application settings
            env: Flink StreamExecutionEnvironment
        """
        self.settings = settings
        self.env = env
        self._table_env: Optional[StreamTableEnvironment] = None
        self._catalog_registered = False

    def _get_table_env(self) -> StreamTableEnvironment:
        """
        Get or create StreamTableEnvironment
        
        Returns:
            StreamTableEnvironment instance
        """
        if self._table_env is None:
            # Create table environment settings for streaming mode
            settings = EnvironmentSettings.new_instance() \
                .in_streaming_mode() \
                .build()
            
            self._table_env = StreamTableEnvironment.create(
                self.env,
                environment_settings=settings
            )
            
            logger.info("Created StreamTableEnvironment for Iceberg sink")
        
        return self._table_env

    def register_iceberg_catalog(self) -> bool:
        """
        Register Iceberg catalog in PyFlink Table API with exactly-once support
        
        Creates and registers the Iceberg catalog with proper configuration
        for S3/MinIO storage, REST catalog, and exactly-once semantics.
        
        Returns:
            True if catalog was registered successfully
        """
        if self._catalog_registered:
            logger.info("Iceberg catalog already registered")
            return True
        
        try:
            table_env = self._get_table_env()
            
            # Build catalog properties
            catalog_properties = {
                'type': 'iceberg',
                'catalog-type': self.settings.iceberg.catalog_type,
                'uri': self.settings.iceberg.catalog_uri,
                'warehouse': self.settings.iceberg.warehouse,
                'io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
                's3.endpoint': self.settings.s3.endpoint,
                's3.access-key-id': self.settings.s3.access_key,
                's3.secret-access-key': self.settings.s3.secret_key,
                's3.path-style-access': str(self.settings.s3.path_style_access).lower(),
            }
            
            # Create catalog SQL with exactly-once configuration
            catalog_sql = f"""
                CREATE CATALOG IF NOT EXISTS {self.settings.iceberg.catalog_name} WITH (
                    'type' = 'iceberg',
                    'catalog-type' = '{self.settings.iceberg.catalog_type}',
                    'uri' = '{self.settings.iceberg.catalog_uri}',
                    'warehouse' = '{self.settings.iceberg.warehouse}',
                    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                    's3.endpoint' = '{self.settings.s3.endpoint}',
                    's3.access-key-id' = '{self.settings.s3.access_key}',
                    's3.secret-access-key' = '{self.settings.s3.secret_key}',
                    's3.path-style-access' = '{str(self.settings.s3.path_style_access).lower()}',
                    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog'
                )
            """
            
            # Execute catalog creation
            table_env.execute_sql(catalog_sql)
            
            # Use the catalog
            table_env.use_catalog(self.settings.iceberg.catalog_name)
            
            logger.info(
                f"Successfully registered Iceberg catalog with exactly-once support: {self.settings.iceberg.catalog_name}",
                extra={
                    "catalog_name": self.settings.iceberg.catalog_name,
                    "catalog_type": self.settings.iceberg.catalog_type,
                    "warehouse": self.settings.iceberg.warehouse,
                    "semantics": "exactly-once",
                }
            )
            
            self._catalog_registered = True
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to register Iceberg catalog: {str(e)}",
                extra={
                    "catalog_name": self.settings.iceberg.catalog_name,
                    "error": str(e),
                },
                exc_info=True
            )
            return False

    def ensure_database_exists(self) -> bool:
        """
        Ensure the database (namespace) exists in the catalog
        
        Returns:
            True if database exists or was created successfully
        """
        try:
            table_env = self._get_table_env()
            
            # Create database if not exists
            database_sql = f"""
                CREATE DATABASE IF NOT EXISTS {self.settings.iceberg.catalog_name}.{self.settings.iceberg.database}
            """
            
            table_env.execute_sql(database_sql)
            
            # Use the database
            table_env.use_database(self.settings.iceberg.database)
            
            logger.info(
                f"Database ensured: {self.settings.iceberg.database}",
                extra={"database": self.settings.iceberg.database}
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to ensure database exists: {str(e)}",
                extra={
                    "database": self.settings.iceberg.database,
                    "error": str(e),
                },
                exc_info=True
            )
            return False

    def datastream_to_table(
        self,
        data_stream: DataStream,
        schema_fields: List[str]
    ) -> Optional[Any]:
        """
        Convert DataStream to Table
        
        Args:
            data_stream: Input DataStream to convert
            schema_fields: List of field names in the schema
            
        Returns:
            Table instance or None if conversion fails
        """
        try:
            table_env = self._get_table_env()
            
            # Convert DataStream to Table
            # The DataStream should contain Row objects with the specified schema
            table = table_env.from_data_stream(data_stream)
            
            logger.info(
                "Successfully converted DataStream to Table",
                extra={"schema_fields": schema_fields}
            )
            
            return table
            
        except Exception as e:
            logger.error(
                f"Failed to convert DataStream to Table: {str(e)}",
                extra={"error": str(e)},
                exc_info=True
            )
            return None

    def initialize(self) -> bool:
        """
        Initialize the Iceberg sink by registering catalog and ensuring database exists
        
        Returns:
            True if initialization was successful
        """
        try:
            # Register catalog
            if not self.register_iceberg_catalog():
                logger.error("Failed to register Iceberg catalog")
                return False
            
            # Ensure database exists
            if not self.ensure_database_exists():
                logger.error("Failed to ensure database exists")
                return False
            
            logger.info("Iceberg sink initialized successfully")
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to initialize Iceberg sink: {str(e)}",
                extra={"error": str(e)},
                exc_info=True
            )
            return False

    def get_table_env(self) -> StreamTableEnvironment:
        """
        Get the StreamTableEnvironment instance
        
        Returns:
            StreamTableEnvironment
        """
        return self._get_table_env()

    def write_to_iceberg(
        self,
        data_stream: DataStream,
        table_name: str,
        overwrite: bool = False
    ) -> bool:
        """
        Write DataStream to Iceberg table with exactly-once semantics
        
        Implements exactly-once guarantee through Flink's checkpoint mechanism
        and Iceberg's 2-phase commit protocol. Data is only committed to Iceberg
        when Flink checkpoint completes successfully.
        
        Args:
            data_stream: Input DataStream to write
            table_name: Target Iceberg table name
            overwrite: Whether to overwrite existing data (default: False for append)
            
        Returns:
            True if write was initiated successfully
        """
        try:
            table_env = self._get_table_env()
            
            # Configure exactly-once semantics for Iceberg sink
            # This ensures data is only committed when checkpoint completes
            table_env.get_config().set(
                "table.exec.sink.not-null-enforcer", "drop"
            )
            
            # Enable 2-phase commit for exactly-once guarantee
            # Data is buffered and only committed when Flink checkpoint succeeds
            table_env.get_config().set(
                "sink.semantic", "exactly-once"
            )
            
            # Disable upsert materialization for append-only mode
            table_env.get_config().set(
                "table.exec.sink.upsert-materialize", "none"
            )
            
            # Configure Iceberg write properties for exactly-once
            # Write distribution mode: none (no shuffle), hash (by partition), or range
            table_env.get_config().set(
                "write.distribution-mode", "none"
            )
            
            # Disable upsert mode - use append-only for exactly-once
            table_env.get_config().set(
                "write.upsert.enabled", "false"
            )
            
            # Configure Parquet file writing
            # Target file size for optimal query performance (128-512MB)
            target_file_size_bytes = self.settings.batch.target_file_size_mb * 1024 * 1024
            
            table_env.get_config().set(
                "write.target-file-size-bytes", str(target_file_size_bytes)
            )
            
            # Set Parquet compression codec
            table_env.get_config().set(
                "write.parquet.compression-codec", "snappy"
            )
            
            # Configure write format (v2 supports row-level operations)
            table_env.get_config().set(
                "write.format.default", "parquet"
            )
            
            # Set write parallelism (should match or be less than source parallelism)
            table_env.get_config().set(
                "write.parallel", str(self.settings.flink.parallelism)
            )
            
            # Convert DataStream to Table
            table = table_env.from_data_stream(data_stream)
            
            # Build full table identifier
            table_identifier = f"{self.settings.iceberg.catalog_name}.{self.settings.iceberg.database}.{table_name}"
            
            logger.info(
                f"Writing DataStream to Iceberg table with exactly-once semantics: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "overwrite": overwrite,
                    "semantics": "exactly-once",
                    "checkpoint_mode": self.settings.flink.checkpoint_mode,
                    "checkpoint_interval_ms": self.settings.flink.checkpoint_interval_ms,
                    "batch_size": self.settings.batch.batch_size,
                    "batch_timeout_seconds": self.settings.batch.batch_timeout_seconds,
                    "target_file_size_mb": self.settings.batch.target_file_size_mb,
                }
            )
            
            # Execute insert into Iceberg table
            # This is non-blocking and will be executed when the job starts
            # Data is buffered and only committed when Flink checkpoint completes
            table_result = table.execute_insert(
                table_identifier,
                overwrite=overwrite
            )
            
            logger.info(
                f"Successfully initiated exactly-once write to Iceberg table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "semantics": "exactly-once"
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to write to Iceberg table: {str(e)}",
                extra={
                    "table_name": table_name,
                    "error": str(e),
                },
                exc_info=True
            )
            return False

    def create_batch_sink(
        self,
        table_name: str
    ) -> Optional[Any]:
        """
        Create a batch sink for Iceberg table with exactly-once semantics
        
        Configures buffering strategy, file size optimization, and exactly-once
        guarantee through checkpoint-based commits for efficient and reliable
        batch writing to Iceberg tables.
        
        Args:
            table_name: Target Iceberg table name
            
        Returns:
            Sink configuration or None if creation fails
        """
        try:
            table_env = self._get_table_env()
            
            # Build full table identifier
            table_identifier = f"{self.settings.iceberg.catalog_name}.{self.settings.iceberg.database}.{table_name}"
            
            # Configure batch writing properties with exactly-once semantics
            batch_config = {
                "connector": "iceberg",
                "catalog-name": self.settings.iceberg.catalog_name,
                "catalog-database": self.settings.iceberg.database,
                "catalog-table": table_name,
                
                # Exactly-once semantics with 2-phase commit
                "sink.semantic": "exactly-once",  # Enable 2PC for exactly-once guarantee
                "write.upsert.enabled": "false",  # Append-only mode for exactly-once
                "write.distribution-mode": "none",  # No shuffle for better performance
                
                # Buffering strategy
                "sink.buffer-flush.max-rows": str(self.settings.batch.batch_size),
                "sink.buffer-flush.interval": f"{self.settings.batch.batch_timeout_seconds}s",
                
                # File size optimization
                "write.target-file-size-bytes": str(self.settings.batch.target_file_size_mb * 1024 * 1024),
                "write.parquet.compression-codec": "snappy",
                "write.parquet.page-size-bytes": "1048576",  # 1MB
                "write.parquet.row-group-size-bytes": "134217728",  # 128MB
                
                # Commit settings for exactly-once
                "commit.retry.num-retries": "3",
                "commit.retry.min-wait-ms": "100",
                "commit.manifest.target-size-bytes": "8388608",  # 8MB manifest files
                
                # Write parallelism
                "write.parallel": str(self.settings.flink.parallelism),
            }
            
            logger.info(
                f"Created exactly-once batch sink configuration for table: {table_identifier}",
                extra={
                    "table_identifier": table_identifier,
                    "batch_config": batch_config,
                    "semantics": "exactly-once",
                }
            )
            
            return batch_config
            
        except Exception as e:
            logger.error(
                f"Failed to create batch sink: {str(e)}",
                extra={
                    "table_name": table_name,
                    "error": str(e),
                },
                exc_info=True
            )
            return None


class IcebergErrorSink:
    """
    Specialized Iceberg sink for error records (Dead Letter Queue)
    
    Handles writing validation errors, deserialization errors, and other
    processing failures to a dedicated error table for debugging and monitoring.
    """

    def __init__(self, settings: Settings, iceberg_sink: IcebergSink):
        """
        Initialize error sink
        
        Args:
            settings: Application settings
            iceberg_sink: Main IcebergSink instance for catalog access
        """
        self.settings = settings
        self.iceberg_sink = iceberg_sink

    def write_errors_to_iceberg(
        self,
        error_stream: DataStream
    ) -> bool:
        """
        Write error records to health_data_errors table
        
        Args:
            error_stream: DataStream containing error records
            
        Returns:
            True if write was initiated successfully
        """
        try:
            table_name = self.settings.iceberg.table_errors
            
            logger.info(
                f"Writing error stream to Iceberg table: {table_name}",
                extra={"table_name": table_name}
            )
            
            # Use the main sink's write method
            success = self.iceberg_sink.write_to_iceberg(
                data_stream=error_stream,
                table_name=table_name,
                overwrite=False  # Always append errors
            )
            
            if success:
                logger.info(
                    f"Successfully initiated error write to table: {table_name}",
                    extra={"table_name": table_name}
                )
            else:
                logger.error(
                    f"Failed to initiate error write to table: {table_name}",
                    extra={"table_name": table_name}
                )
            
            return success
            
        except Exception as e:
            logger.error(
                f"Failed to write errors to Iceberg: {str(e)}",
                extra={"error": str(e)},
                exc_info=True
            )
            return False

    def create_error_record(
        self,
        error_type: str,
        error_message: str,
        raw_payload: Optional[str] = None,
        user_id: Optional[str] = None,
        device_id: Optional[str] = None,
        sample_id: Optional[str] = None,
        data_type: Optional[str] = None,
        kafka_topic: str = "",
        kafka_partition: int = -1,
        kafka_offset: int = -1
    ) -> Dict[str, Any]:
        """
        Create an error record dictionary for writing to error table
        
        Args:
            error_type: Type of error (validation, deserialization, etc.)
            error_message: Error message description
            raw_payload: Original raw payload that caused the error
            user_id: User identifier if available
            device_id: Device identifier if available
            sample_id: Sample identifier if available
            data_type: Health data type if available
            kafka_topic: Source Kafka topic
            kafka_partition: Source Kafka partition
            kafka_offset: Source Kafka offset
            
        Returns:
            Dictionary representing an error record
        """
        import uuid
        
        error_record = {
            "error_id": str(uuid.uuid4()),
            "error_type": error_type,
            "error_message": error_message,
            "error_timestamp": datetime.now(timezone.utc),
            "raw_payload": raw_payload,
            "user_id": user_id,
            "device_id": device_id,
            "sample_id": sample_id,
            "data_type": data_type,
            "kafka_topic": kafka_topic,
            "kafka_partition": kafka_partition,
            "kafka_offset": kafka_offset,
        }
        
        return error_record

    def log_error_metrics(
        self,
        error_type: str,
        count: int = 1
    ) -> None:
        """
        Log error metrics for monitoring
        
        Args:
            error_type: Type of error
            count: Number of errors (default: 1)
        """
        logger.warning(
            f"Error recorded: {error_type}",
            extra={
                "error_type": error_type,
                "count": count,
                "table": self.settings.iceberg.table_errors,
            }
        )
