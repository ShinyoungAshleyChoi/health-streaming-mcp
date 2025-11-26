"""
Iceberg sink implementation for aggregated health data.

This module provides sink functions to write aggregation results to
Iceberg tables with upsert support for handling late-arriving data.
"""

from typing import Dict, Any, Optional
import logging

from flink_consumer.aggregations.windows import WindowType
from flink_consumer.aggregations.schemas import (
    DailyAggregateSchema,
    WeeklyAggregateSchema,
    MonthlyAggregateSchema,
)

logger = logging.getLogger(__name__)


class AggregationSinkConfig:
    """
    Configuration for aggregation sinks.
    """
    
    def __init__(self,
                 catalog_name: str = 'health_catalog',
                 database_name: str = 'health_db',
                 enable_upsert: bool = True,
                 batch_size: int = 1000,
                 batch_interval_seconds: int = 60):
        """
        Initialize sink configuration.
        
        Args:
            catalog_name: Iceberg catalog name
            database_name: Database name
            enable_upsert: Enable upsert mode for late data updates
            batch_size: Number of records to batch before writing
            batch_interval_seconds: Maximum time to wait before flushing batch
        """
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.enable_upsert = enable_upsert
        self.batch_size = batch_size
        self.batch_interval_seconds = batch_interval_seconds
    
    def get_table_name(self, window_type: WindowType) -> str:
        """
        Get table name for window type.
        
        Args:
            window_type: Type of window
            
        Returns:
            Table name
        """
        if window_type == WindowType.DAILY:
            return 'health_data_daily_agg'
        elif window_type == WindowType.WEEKLY:
            return 'health_data_weekly_agg'
        elif window_type == WindowType.MONTHLY:
            return 'health_data_monthly_agg'
        else:
            raise ValueError(f"Unsupported window type: {window_type}")
    
    def get_full_table_name(self, window_type: WindowType) -> str:
        """
        Get fully qualified table name.
        
        Args:
            window_type: Type of window
            
        Returns:
            Fully qualified table name (catalog.database.table)
        """
        table_name = self.get_table_name(window_type)
        return f"{self.catalog_name}.{self.database_name}.{table_name}"


def create_aggregation_sink(stream, window_type: WindowType, 
                            sink_config: AggregationSinkConfig,
                            table_env):
    """
    Create Iceberg sink for aggregation stream.
    
    This function configures and applies an Iceberg sink to write aggregation
    results with upsert support.
    
    Args:
        stream: PyFlink DataStream with aggregation results
        window_type: Type of window (DAILY, WEEKLY, or MONTHLY)
        sink_config: Sink configuration
        table_env: PyFlink StreamTableEnvironment
        
    Returns:
        Configured sink
        
    Example:
        >>> from pyflink.table import StreamTableEnvironment
        >>> table_env = StreamTableEnvironment.create(env)
        >>> config = AggregationSinkConfig()
        >>> sink = create_aggregation_sink(daily_agg_stream, WindowType.DAILY, config, table_env)
    """
    table_name = sink_config.get_table_name(window_type)
    full_table_name = sink_config.get_full_table_name(window_type)
    
    logger.info(f"Creating Iceberg sink for {window_type.value} aggregations: {full_table_name}")
    
    # Ensure catalog is configured
    _configure_iceberg_catalog(table_env, sink_config)
    
    # Ensure table exists
    _ensure_table_exists(table_env, window_type, sink_config)
    
    # Convert DataStream to Table
    table = table_env.from_data_stream(stream)
    
    # Configure sink with upsert mode
    if sink_config.enable_upsert:
        logger.info(f"Enabling upsert mode for {table_name}")
        # Iceberg will handle upserts based on primary key
        table.execute_insert(table_name, overwrite=False)
    else:
        logger.info(f"Using append mode for {table_name}")
        table.execute_insert(table_name, overwrite=False)
    
    return table


def _configure_iceberg_catalog(table_env, sink_config: AggregationSinkConfig):
    """
    Configure Iceberg catalog in table environment.
    
    Args:
        table_env: PyFlink StreamTableEnvironment
        sink_config: Sink configuration
    """
    catalog_name = sink_config.catalog_name
    
    logger.info(f"Configuring Iceberg catalog: {catalog_name}")
    
    # Check if catalog already exists
    try:
        table_env.use_catalog(catalog_name)
        logger.info(f"Catalog {catalog_name} already exists")
        return
    except Exception:
        logger.info(f"Creating new catalog: {catalog_name}")
    
    # Create catalog (configuration should be provided via environment)
    # This is a placeholder - actual configuration depends on deployment
    create_catalog_sql = f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = 's3a://data-lake/warehouse'
        )
    """
    
    try:
        table_env.execute_sql(create_catalog_sql)
        logger.info(f"Created catalog: {catalog_name}")
    except Exception as e:
        logger.warning(f"Could not create catalog (may already exist): {e}")
    
    # Use the catalog
    table_env.use_catalog(catalog_name)
    
    # Create database if not exists
    database_name = sink_config.database_name
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    
    try:
        table_env.execute_sql(create_db_sql)
        logger.info(f"Ensured database exists: {database_name}")
    except Exception as e:
        logger.warning(f"Could not create database (may already exist): {e}")
    
    table_env.use_database(database_name)


def _ensure_table_exists(table_env, window_type: WindowType, 
                        sink_config: AggregationSinkConfig):
    """
    Ensure aggregation table exists in Iceberg.
    
    Args:
        table_env: PyFlink StreamTableEnvironment
        window_type: Type of window
        sink_config: Sink configuration
    """
    table_name = sink_config.get_table_name(window_type)
    
    logger.info(f"Ensuring table exists: {table_name}")
    
    # Get DDL for table type
    if window_type == WindowType.DAILY:
        ddl = DailyAggregateSchema.get_iceberg_ddl(
            sink_config.catalog_name,
            sink_config.database_name,
            table_name
        )
    elif window_type == WindowType.WEEKLY:
        ddl = WeeklyAggregateSchema.get_iceberg_ddl(
            sink_config.catalog_name,
            sink_config.database_name,
            table_name
        )
    elif window_type == WindowType.MONTHLY:
        ddl = MonthlyAggregateSchema.get_iceberg_ddl(
            sink_config.catalog_name,
            sink_config.database_name,
            table_name
        )
    else:
        raise ValueError(f"Unsupported window type: {window_type}")
    
    try:
        table_env.execute_sql(ddl)
        logger.info(f"Created or verified table: {table_name}")
    except Exception as e:
        logger.warning(f"Could not create table (may already exist): {e}")


class AggregationSinkWriter:
    """
    Custom sink writer for aggregation data.
    
    Provides more control over the writing process, including batching
    and error handling.
    """
    
    def __init__(self, window_type: WindowType, sink_config: AggregationSinkConfig):
        """
        Initialize sink writer.
        
        Args:
            window_type: Type of window
            sink_config: Sink configuration
        """
        self.window_type = window_type
        self.sink_config = sink_config
        self.batch = []
        self.records_written = 0
        self.write_errors = 0
    
    def write(self, record: Dict[str, Any]):
        """
        Write a record to the sink.
        
        Args:
            record: Aggregation result to write
        """
        self.batch.append(record)
        
        if len(self.batch) >= self.sink_config.batch_size:
            self.flush()
    
    def flush(self):
        """
        Flush buffered records to Iceberg.
        """
        if not self.batch:
            return
        
        logger.info(f"Flushing {len(self.batch)} records to {self.window_type.value} table")
        
        try:
            # Write batch to Iceberg
            # This is a placeholder - actual implementation depends on PyIceberg or PyFlink Table API
            self._write_batch_to_iceberg(self.batch)
            
            self.records_written += len(self.batch)
            self.batch = []
            
        except Exception as e:
            logger.error(f"Error writing batch to Iceberg: {e}")
            self.write_errors += 1
            # Keep batch for retry
    
    def _write_batch_to_iceberg(self, records: list):
        """
        Write batch of records to Iceberg table.
        
        Args:
            records: List of aggregation results
        """
        # Placeholder for actual Iceberg write logic
        # In production, this would use PyIceberg or PyFlink Table API
        pass
    
    def get_metrics(self) -> Dict[str, int]:
        """
        Get sink metrics.
        
        Returns:
            Dictionary with metrics
        """
        return {
            'records_written': self.records_written,
            'write_errors': self.write_errors,
            'buffered_records': len(self.batch),
        }


def setup_aggregation_sinks(env, table_env, 
                           daily_stream, weekly_stream, monthly_stream,
                           sink_config: Optional[AggregationSinkConfig] = None):
    """
    Setup all aggregation sinks (daily, weekly, monthly).
    
    This is a convenience function to configure all three aggregation sinks
    at once.
    
    Args:
        env: PyFlink StreamExecutionEnvironment
        table_env: PyFlink StreamTableEnvironment
        daily_stream: Daily aggregation stream
        weekly_stream: Weekly aggregation stream
        monthly_stream: Monthly aggregation stream
        sink_config: Sink configuration (uses defaults if None)
        
    Returns:
        Tuple of (daily_sink, weekly_sink, monthly_sink)
        
    Example:
        >>> config = AggregationSinkConfig(catalog_name='health_catalog')
        >>> sinks = setup_aggregation_sinks(env, table_env, daily, weekly, monthly, config)
    """
    if sink_config is None:
        sink_config = AggregationSinkConfig()
    
    logger.info("Setting up aggregation sinks for all window types")
    
    # Create daily sink
    daily_sink = create_aggregation_sink(
        daily_stream,
        WindowType.DAILY,
        sink_config,
        table_env
    )
    
    # Create weekly sink
    weekly_sink = create_aggregation_sink(
        weekly_stream,
        WindowType.WEEKLY,
        sink_config,
        table_env
    )
    
    # Create monthly sink
    monthly_sink = create_aggregation_sink(
        monthly_stream,
        WindowType.MONTHLY,
        sink_config,
        table_env
    )
    
    logger.info("All aggregation sinks configured successfully")
    
    return daily_sink, weekly_sink, monthly_sink


class SinkMetrics:
    """
    Track sink-related metrics.
    """
    
    def __init__(self):
        self.records_written = 0
        self.batches_written = 0
        self.write_errors = 0
        self.upserts_performed = 0
    
    def record_write(self, record_count: int):
        """Record successful write."""
        self.records_written += record_count
        self.batches_written += 1
    
    def record_error(self):
        """Record write error."""
        self.write_errors += 1
    
    def record_upsert(self):
        """Record upsert operation."""
        self.upserts_performed += 1
    
    def get_metrics(self) -> Dict[str, int]:
        """Get current metrics."""
        return {
            'records_written': self.records_written,
            'batches_written': self.batches_written,
            'write_errors': self.write_errors,
            'upserts_performed': self.upserts_performed,
        }
